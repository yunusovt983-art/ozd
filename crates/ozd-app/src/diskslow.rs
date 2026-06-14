//! E28 (#129, CRDB disk-stall): per-shard EWMA-латентность put/get —
//! диск «жив, но умирает» (растущие сики, пре-отказная механика) ловится
//! ДО того, как ZFS увидит ошибки чтения.
//!
//! Вердикт «slow» — сравнение С ПАРКОМ: 60 одинаковых дисков → медиана
//! EWMA это «здоровье поколения», выброс = rel_factor × медианы И выше
//! абсолютного пола (idle-парк с ровными латентностями не флапает).
//! Гистерезис — снаружи (HealthFsm #142, как у ZFS-входа).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct DiskSlowConfig {
    /// абсолютный пол, мс — ниже него диск slow не бывает (HDD-сики штатны)
    pub abs_floor_ms: u64,
    /// во сколько раз хуже медианы парка = выброс
    pub rel_factor: f64,
    /// минимум сэмплов на шарде для участия в вердикте
    pub min_samples: u64,
}

impl Default for DiskSlowConfig {
    fn default() -> Self {
        Self { abs_floor_ms: 250, rel_factor: 4.0, min_samples: 32 }
    }
}

/// EWMA-трекер латентностей по шардам (lock-free, lossy-гонки безвредны).
pub struct DiskSlowMonitor {
    ewma_us: Vec<AtomicU64>,
    counts: Vec<AtomicU64>,
}

const ALPHA_NUM: u64 = 1; // EWMA α = 1/8 — сглаживает всплеск, ловит тренд
const ALPHA_DEN: u64 = 8;

impl DiskSlowMonitor {
    pub fn new(n: usize) -> Self {
        Self {
            ewma_us: (0..n).map(|_| AtomicU64::new(0)).collect(),
            counts: (0..n).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    pub fn record(&self, shard: usize, lat: Duration) {
        let Some(e) = self.ewma_us.get(shard) else { return };
        let us = lat.as_micros() as u64;
        let n = self.counts[shard].fetch_add(1, Relaxed);
        if n == 0 {
            e.store(us, Relaxed);
        } else {
            let old = e.load(Relaxed);
            e.store((old * (ALPHA_DEN - ALPHA_NUM) + us * ALPHA_NUM) / ALPHA_DEN, Relaxed);
        }
    }

    pub fn ewma_ms(&self, shard: usize) -> u64 {
        self.ewma_us.get(shard).map(|e| e.load(Relaxed) / 1000).unwrap_or(0)
    }

    pub fn samples(&self, shard: usize) -> u64 {
        self.counts.get(shard).map(|c| c.load(Relaxed)).unwrap_or(0)
    }

    /// Вердикты «slow» по текущему срезу (без гистерезиса — он в FSM).
    pub fn verdicts(&self, cfg: &DiskSlowConfig) -> Vec<bool> {
        let n = self.ewma_us.len();
        let mut participating: Vec<u64> = (0..n)
            .filter(|i| self.counts[*i].load(Relaxed) >= cfg.min_samples)
            .map(|i| self.ewma_us[i].load(Relaxed))
            .collect();
        participating.sort_unstable();
        let median_us =
            participating.get(participating.len() / 2).copied().unwrap_or(0);
        let floor_us = cfg.abs_floor_ms * 1000;
        let rel_us = (median_us as f64 * cfg.rel_factor) as u64;
        let threshold = floor_us.max(rel_us);
        (0..n)
            .map(|i| {
                self.counts[i].load(Relaxed) >= cfg.min_samples
                    && self.ewma_us[i].load(Relaxed) > threshold
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(m: &DiskSlowMonitor, shard: usize, ms: u64, n: usize) {
        for _ in 0..n {
            m.record(shard, Duration::from_millis(ms));
        }
    }

    #[test]
    fn outlier_vs_fleet_median_is_slow_idle_fleet_is_not() {
        let cfg = DiskSlowConfig::default();
        let m = DiskSlowMonitor::new(5);
        // ровный парк 12мс — никто не slow (медиана×4 < пола 250мс)
        for i in 0..5 {
            feed(&m, i, 12, 64);
        }
        assert!(m.verdicts(&cfg).iter().all(|v| !v), "ровный парк не флапает");
        // диск 2 деградирует до 400мс: > пола И > 4×медианы → slow
        feed(&m, 2, 400, 64);
        let v = m.verdicts(&cfg);
        assert!(v[2], "выброс пойман: ewma={}мс", m.ewma_ms(2));
        assert_eq!(v.iter().filter(|x| **x).count(), 1, "остальные здоровы");
    }

    #[test]
    fn warmup_and_uniform_slow_fleet_use_abs_floor() {
        let cfg = DiskSlowConfig::default();
        let m = DiskSlowMonitor::new(3);
        feed(&m, 0, 500, 8); // мало сэмплов — не участвует
        assert!(!m.verdicts(&cfg)[0], "до min_samples вердикта нет");
        // ВЕСЬ парк деградировал до 400мс: rel-чек слеп (медиана та же),
        // но это не «выброс» — равномерно умирающий парк не наш кейс,
        // и порог = max(пол, 4×400мс)=1.6с — никто не slow (честно:
        // peer-сравнение ловит ВЫБРОСЫ, не общую деградацию)
        for i in 0..3 {
            feed(&m, i, 400, 64);
        }
        assert!(m.verdicts(&cfg).iter().all(|v| !v));
        // а одиночка на 2с — ловится даже на фоне больного парка
        feed(&m, 1, 2000, 64);
        assert!(m.verdicts(&cfg)[1]);
    }

    #[test]
    fn ewma_smooths_single_spike() {
        let cfg = DiskSlowConfig::default();
        let m = DiskSlowMonitor::new(3);
        for i in 0..3 {
            feed(&m, i, 10, 64);
        }
        // один 1-секундный всплеск на здоровом диске НЕ делает его slow
        // (ewma ≈ 10×7/8 + 1000/8 ≈ 134мс < пола 250мс; 2с-всплеск уже
        // дал бы 259мс — и это ПРАВИЛЬНО подозрительно)
        m.record(0, Duration::from_secs(1));
        assert!(
            !m.verdicts(&cfg)[0],
            "α=1/8 гасит одиночный всплеск: ewma={}мс",
            m.ewma_ms(0)
        );
    }
}
