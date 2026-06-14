//! E27: скользящий p99 латентности чтения — адаптивный hedge-порог
//! (#121 Cassandra speculative retry «99percentile» вместо фикс-порога).
//!
//! Lock-free на горячем пути: 22 степени-двойки бакета (1µs..~4с) ×
//! ДВЕ эпохи; record бьёт в текущую, p99 считается по обеим (текущая
//! частичная + предыдущая полная = гладкое окно 1–2 window). Ротация —
//! ленивая, под крошечным mutex'ом с double-check.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

const BUCKETS: usize = 22; // idx = floor(log2(µs)), 2^21µs ≈ 2.1с

pub struct RollingP99 {
    base: Instant,
    window_us: u64,
    min_samples: u64,
    cur: AtomicUsize,
    epoch_start_us: AtomicU64,
    buckets: [[AtomicU64; BUCKETS]; 2],
    counts: [AtomicU64; 2],
    rot: Mutex<()>,
}

fn bucket_idx(us: u64) -> usize {
    (63 - us.max(1).leading_zeros() as usize).min(BUCKETS - 1)
}

impl RollingP99 {
    pub fn new(window: Duration, min_samples: u64) -> Self {
        Self {
            base: Instant::now(),
            window_us: window.as_micros() as u64,
            min_samples,
            cur: AtomicUsize::new(0),
            epoch_start_us: AtomicU64::new(0),
            buckets: Default::default(),
            counts: Default::default(),
            rot: Mutex::new(()),
        }
    }

    fn now_us(&self) -> u64 {
        self.base.elapsed().as_micros() as u64
    }

    pub fn record(&self, lat: Duration) {
        self.record_at(lat.as_micros() as u64, self.now_us());
    }

    pub fn p99(&self) -> Option<Duration> {
        self.p99_at(self.now_us())
    }

    /// Ядро с инъекцией времени (детерминируемые тесты, как E19).
    pub fn record_at(&self, lat_us: u64, now_us: u64) {
        self.maybe_rotate(now_us);
        let e = self.cur.load(Relaxed);
        self.buckets[e][bucket_idx(lat_us)].fetch_add(1, Relaxed);
        self.counts[e].fetch_add(1, Relaxed);
    }

    pub fn p99_at(&self, now_us: u64) -> Option<Duration> {
        self.maybe_rotate(now_us);
        let total = self.counts[0].load(Relaxed) + self.counts[1].load(Relaxed);
        if total < self.min_samples {
            return None; // не прогрелись — пусть решает статический fallback
        }
        let target = (total * 99).div_ceil(100);
        let mut cum = 0u64;
        for i in 0..BUCKETS {
            cum += self.buckets[0][i].load(Relaxed) + self.buckets[1][i].load(Relaxed);
            if cum >= target {
                // верхняя граница бакета: консервативно НЕ хеджим раньше p99
                return Some(Duration::from_micros(1u64 << (i + 1)));
            }
        }
        Some(Duration::from_micros(1 << BUCKETS))
    }

    fn maybe_rotate(&self, now_us: u64) {
        if now_us.saturating_sub(self.epoch_start_us.load(Relaxed)) < self.window_us {
            return;
        }
        let _g = self.rot.lock();
        let start = self.epoch_start_us.load(Relaxed);
        if now_us.saturating_sub(start) < self.window_us {
            return; // другой поток уже ротировал
        }
        // окно проспали целиком (тишина > 2×window) → обе эпохи протухли
        let stale_both = now_us.saturating_sub(start) >= self.window_us * 2;
        let next = 1 - self.cur.load(Relaxed);
        for b in &self.buckets[next] {
            b.store(0, Relaxed);
        }
        self.counts[next].store(0, Relaxed);
        self.cur.store(next, Relaxed);
        if stale_both {
            let old = 1 - next;
            for b in &self.buckets[old] {
                b.store(0, Relaxed);
            }
            self.counts[old].store(0, Relaxed);
        }
        self.epoch_start_us.store(now_us, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p99_lands_in_tail_bucket_and_needs_warmup() {
        let h = RollingP99::new(Duration::from_secs(60), 64);
        // до прогрева — None (решает статический fallback)
        for i in 0..50 {
            h.record_at(5_000, i);
        }
        assert_eq!(h.p99_at(100), None, "меньше min_samples");
        // 100×5мс + 2×500мс → p99 в хвостовом бакете (~524мс верхняя граница)
        for i in 0..50 {
            h.record_at(5_000, 100 + i);
        }
        h.record_at(500_000, 200);
        h.record_at(500_000, 201);
        let p = h.p99_at(300).unwrap();
        assert!(
            (Duration::from_millis(400)..Duration::from_millis(700)).contains(&p),
            "{p:?}"
        );
        // ровная нагрузка без хвоста → p99 маленький
        let h2 = RollingP99::new(Duration::from_secs(60), 64);
        for i in 0..200 {
            h2.record_at(3_000, i);
        }
        let p2 = h2.p99_at(300).unwrap();
        assert!(p2 <= Duration::from_millis(8), "{p2:?}");
    }

    #[test]
    fn window_rotation_forgets_old_spikes() {
        let w = Duration::from_secs(10);
        let h = RollingP99::new(w, 10);
        let w_us = w.as_micros() as u64;
        for i in 0..100 {
            h.record_at(400_000, i); // буря 400мс в первой эпохе
        }
        assert!(h.p99_at(1000).unwrap() >= Duration::from_millis(400));
        // следующее окно — ровные 2мс; буря ещё видна (предыдущая эпоха)
        for i in 0..100 {
            h.record_at(2_000, w_us + i);
        }
        assert!(h.p99_at(w_us + 1000).unwrap() >= Duration::from_millis(400));
        // ещё окно — буря забыта, p99 по ровным
        for i in 0..100 {
            h.record_at(2_000, w_us * 2 + i);
        }
        let p = h.p99_at(w_us * 2 + 1000).unwrap();
        assert!(p <= Duration::from_millis(8), "{p:?}");
        // долгая тишина (>2 окон) → обе эпохи протухают → снова прогрев
        assert_eq!(h.p99_at(w_us * 10), None);
    }
}
