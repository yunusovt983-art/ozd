//! E19 (#131, CRDB elastic admission): байтовый бюджет фоновых работ
//! (GC/scrub/resilver/heal), чтобы фон не душил foreground put/get.
//!
//! Механика: leaky-bucket «оплата вперёд» — фон списывает байты, ушедшие
//! в минус токены превращаются в сон ПЕРЕД следующей порцией (pacing).
//! Эластика: AIMD по наблюдаемой foreground-нагрузке (puts+gets из
//! OpsMetrics) — занят → бюджет ×0.5 (но не ниже пола), тихо → +10%
//! потолка за окно. Foreground НЕ троттлится никогда (#131: фон подвинься).

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::metrics::OpsMetrics;

#[derive(Clone, Debug)]
pub struct BgThrottleConfig {
    /// потолок бюджета фона, байт/с (0 = троттлинг выключен)
    pub max_bytes_per_sec: u64,
    /// пол — фон никогда не голодает до нуля (resilver обязан закончиться)
    pub min_bytes_per_sec: u64,
    /// порог «foreground занят», операций/с (puts+gets)
    pub fg_busy_ops_per_sec: f64,
}

impl Default for BgThrottleConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_sec: 64 * 1024 * 1024, // 64 МиБ/с на узел
            min_bytes_per_sec: 4 * 1024 * 1024,  // 4 МиБ/с пол
            fg_busy_ops_per_sec: 50.0,
        }
    }
}

struct State {
    /// текущий эластичный бюджет, байт/с (между min и max)
    rate: f64,
    /// накопленные токены; burst ≤ 1 секунда бюджета; минус = долг → сон
    tokens: f64,
    last_refill: Instant,
    /// AIMD-окно наблюдения foreground (≥ 1 с)
    last_obs: Instant,
    last_fg_ops: u64,
}

pub struct BgThrottle {
    cfg: BgThrottleConfig,
    metrics: Arc<OpsMetrics>,
    st: Mutex<State>,
}

impl BgThrottle {
    pub fn new(cfg: BgThrottleConfig, metrics: Arc<OpsMetrics>) -> Self {
        let now = Instant::now();
        let fg = Self::fg_total_of(&metrics);
        metrics.bg_rate_bps.store(cfg.max_bytes_per_sec, Relaxed);
        Self {
            st: Mutex::new(State {
                rate: cfg.max_bytes_per_sec as f64,
                tokens: cfg.max_bytes_per_sec as f64, // стартовый burst = 1 с
                last_refill: now,
                last_obs: now,
                last_fg_ops: fg,
            }),
            cfg,
            metrics,
        }
    }

    fn fg_total_of(m: &OpsMetrics) -> u64 {
        m.puts.load(Relaxed) + m.gets.load(Relaxed)
    }

    /// Списать `bytes` фоновой работы; при долге — заснуть (pacing).
    /// Звать ПОСЛЕ порции (фактические байты известны) — пауза темпирует
    /// следующую порцию. Только из blocking-контекстов (spawn_blocking/потоки).
    pub fn acquire(&self, bytes: u64) {
        if self.cfg.max_bytes_per_sec == 0 || bytes == 0 {
            return;
        }
        let d = self.acquire_delay(bytes, Instant::now(), Self::fg_total_of(&self.metrics));
        if !d.is_zero() {
            self.metrics.bg_throttle_waits.fetch_add(1, Relaxed);
            std::thread::sleep(d);
        }
    }

    /// Ядро (детерминируемое: время и fg-счётчик инъецируются — тестируется
    /// без сна): вернуть, сколько спать после списания `bytes` в момент `now`.
    pub fn acquire_delay(&self, bytes: u64, now: Instant, fg_total_ops: u64) -> Duration {
        let mut st = self.st.lock();

        // эластика AIMD раз в ≥1с окно
        let win = now.saturating_duration_since(st.last_obs).as_secs_f64();
        if win >= 1.0 {
            let fg_rate = fg_total_ops.saturating_sub(st.last_fg_ops) as f64 / win;
            if fg_rate > self.cfg.fg_busy_ops_per_sec {
                // multiplicative decrease: foreground занят — фон подвинься
                st.rate = (st.rate * 0.5).max(self.cfg.min_bytes_per_sec as f64);
            } else {
                // additive increase: тихо — отъедаем бюджет обратно
                st.rate = (st.rate + 0.1 * self.cfg.max_bytes_per_sec as f64)
                    .min(self.cfg.max_bytes_per_sec as f64);
            }
            st.last_obs = now;
            st.last_fg_ops = fg_total_ops;
            self.metrics.bg_rate_bps.store(st.rate as u64, Relaxed);
        }

        // leaky-bucket: долить по текущему rate, списать, минус → сон
        let dt = now.saturating_duration_since(st.last_refill).as_secs_f64();
        st.last_refill = now;
        st.tokens = (st.tokens + st.rate * dt).min(st.rate);
        st.tokens -= bytes as f64;
        self.metrics.bg_throttle_bytes.fetch_add(bytes, Relaxed);
        if st.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-st.tokens / st.rate)
        }
    }

    /// Текущий бюджет (для тестов/диагностики).
    pub fn rate_bps(&self) -> u64 {
        self.st.lock().rate as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(max: u64, min: u64, busy: f64) -> BgThrottle {
        BgThrottle::new(
            BgThrottleConfig {
                max_bytes_per_sec: max,
                min_bytes_per_sec: min,
                fg_busy_ops_per_sec: busy,
            },
            Arc::new(OpsMetrics::default()),
        )
    }

    #[test]
    fn pays_forward_and_paces_by_rate() {
        // rate фиксирован (min=max, busy недостижим) → чистая математика бакета
        let t = mk(1 << 20, 1 << 20, f64::MAX);
        let base = Instant::now();
        // стартовый burst = 1 МиБ: первое списание ровно в бюджет — без сна
        assert_eq!(t.acquire_delay(1 << 20, base, 0), Duration::ZERO);
        // долг 512 КиБ при 1 МиБ/с → сон ~0.5 с
        let d = t.acquire_delay(512 << 10, base, 0);
        assert!(
            (0.4..=0.6).contains(&d.as_secs_f64()),
            "ожидали ~0.5с, получили {d:?}"
        );
        // спустя 2 с токены восстановились (cap = 1 с бюджета) — снова без сна
        let later = base + Duration::from_secs(2);
        assert_eq!(t.acquire_delay(1 << 20, later, 0), Duration::ZERO);
    }

    #[test]
    fn aimd_halves_on_busy_floor_and_recovers() {
        let mb = 1 << 20;
        let t = mk(100 * mb, 10 * mb, 50.0);
        let base = Instant::now();
        assert_eq!(t.rate_bps(), 100 * mb);
        // окно 1.1с, 200 fg-операций (~181 оп/с > 50) → ×0.5
        let _ = t.acquire_delay(1, base + Duration::from_millis(1100), 200);
        assert_eq!(t.rate_bps() / mb, 50);
        // ещё два занятых окна → 25 → 12.5, но пол 10 МиБ держит
        let _ = t.acquire_delay(1, base + Duration::from_millis(2200), 400);
        let _ = t.acquire_delay(1, base + Duration::from_millis(3300), 600);
        assert_eq!(t.rate_bps() / mb, 12);
        let _ = t.acquire_delay(1, base + Duration::from_millis(4400), 800);
        assert_eq!(t.rate_bps() / mb, 10, "пол min_bytes_per_sec");
        // тихие окна → +10% потолка за окно: 10 → 20 → 30
        let _ = t.acquire_delay(1, base + Duration::from_millis(5500), 805);
        assert_eq!(t.rate_bps() / mb, 20);
        let _ = t.acquire_delay(1, base + Duration::from_millis(6600), 810);
        assert_eq!(t.rate_bps() / mb, 30);
    }

    #[test]
    fn zero_max_disables() {
        let t = mk(0, 0, 50.0);
        t.acquire(10 << 20); // не должен спать/паниковать
        // ядро при max=0: rate=0 — acquire() отсекает раньше, проверим сам отсек
        assert_eq!(t.rate_bps(), 0);
    }
}
