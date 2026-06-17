// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! RendezvousHrw — взвешенный Rendezvous (HRW) hashing (#2).
//!
//! score(key, shard) = -weight / ln(h01(key, shard)), берём top-R по score.
//! Свойства: при добавлении диска переезжает ≈ 1/(N+1) блоков (vs ~всё у
//! modulo); даёт ранжированный список — top-R и есть реплики; вес = free
//! (≈ least-bytes-used). Гистерезис заполнения (#130): диск > fill_block
//! не получает новых блоков вообще.

use ozd_domain::{BlockKey, Capacity, PlacementPolicy, ShardId, ShardStatus};
use xxhash_rust::xxh3::xxh3_64_with_seed;

pub struct RendezvousHrw {
    /// порог «не класть» (#130, дефолт 0.95)
    pub fill_block: f64,
}

impl Default for RendezvousHrw {
    fn default() -> Self {
        Self { fill_block: 0.95 }
    }
}

fn h01(key: &[u8], shard: ShardId) -> f64 {
    let h = xxh3_64_with_seed(key, shard.0 as u64 + 1);
    // (h+1)/(2^64+2) ∈ (0,1) строго
    ((h as f64) + 1.0) / (u64::MAX as f64 + 2.0)
}

impl PlacementPolicy for RendezvousHrw {
    fn select(
        &self,
        key: &BlockKey,
        topology: &[(ShardId, Capacity, ShardStatus)],
        rf: usize,
    ) -> Vec<ShardId> {
        let mut scored: Vec<(f64, ShardId)> = topology
            .iter()
            .filter(|(_, cap, st)| {
                // compare-cascade (#130): здоровье диска важнее ровности
                if *st == ShardStatus::Faulted {
                    return false;
                }
                if cap.total_bytes > 0 {
                    let used = 1.0 - cap.free_bytes as f64 / cap.total_bytes as f64;
                    if used >= self.fill_block {
                        return false; // полный — не цель размещения
                    }
                }
                true
            })
            .map(|(id, cap, st)| {
                // вес: free-байты; при нулях (тест/пустой statvfs) — равные веса
                let mut w = if cap.free_bytes > 0 { cap.free_bytes as f64 } else { 1.0 };
                // E28: Suspect (ZFS-деградация / disk-slow #129) — вес ×0.01:
                // читается и чинит, но новые записи и read-leg уходят
                if *st == ShardStatus::Suspect {
                    w *= 0.01;
                }
                let score = -w / h01(key.as_bytes(), *id).ln();
                (score, *id)
            })
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().take(rf).map(|(_, id)| id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topo(n: u16) -> Vec<(ShardId, Capacity, ShardStatus)> {
        (0..n)
            .map(|i| {
                (
                    ShardId(i),
                    Capacity { total_bytes: 1 << 40, free_bytes: 1 << 39 },
                    ShardStatus::Online,
                )
            })
            .collect()
    }

    #[test]
    fn deterministic_and_distinct() {
        let p = RendezvousHrw::default();
        let t = topo(8);
        let k = BlockKey::from("/blocks/abc");
        let a = p.select(&k, &t, 2);
        let b = p.select(&k, &t, 2);
        assert_eq!(a, b);
        assert_eq!(a.len(), 2);
        assert_ne!(a[0], a[1]);
    }

    #[test]
    fn add_disk_moves_about_one_over_n() {
        let p = RendezvousHrw::default();
        let t8 = topo(8);
        let t9 = topo(9);
        let total = 4000;
        let mut moved = 0;
        for i in 0..total {
            let k = BlockKey::new(format!("/blocks/key-{i}"));
            if p.select(&k, &t8, 1) != p.select(&k, &t9, 1) {
                moved += 1;
            }
        }
        let frac = moved as f64 / total as f64;
        // ожидаем ≈ 1/9 ≈ 0.111; допускаем широкое окно
        assert!(frac > 0.05 && frac < 0.20, "moved fraction {frac}");
    }

    #[test]
    fn skips_faulted_and_full() {
        let p = RendezvousHrw::default();
        let mut t = topo(3);
        t[0].2 = ShardStatus::Faulted;
        t[1].1 = Capacity { total_bytes: 100, free_bytes: 2 }; // 98% занято
        let k = BlockKey::from("/blocks/zzz");
        let sel = p.select(&k, &t, 3);
        assert_eq!(sel, vec![ShardId(2)]);
    }
}
