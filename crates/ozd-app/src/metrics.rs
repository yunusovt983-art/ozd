// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! E14: операционные метрики пула (lock-free атомики, Prometheus-text).
//! Без внешних crates: счётчики + суммы латентностей (avg/rate — в PromQL).
//! W4.1: histogram-бакеты для put/get латентности (стандартные Prometheus бакеты).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use ozd_domain::GcReport;

/// Стандартные Prometheus histogram-бакеты (верхняя граница, микросекунды).
const HIST_BUCKETS_US: &[u64] = &[
    1_000,      // 1ms
    5_000,      // 5ms
    10_000,     // 10ms
    25_000,     // 25ms
    50_000,     // 50ms
    100_000,    // 100ms
    250_000,    // 250ms
    500_000,    // 500ms
    1_000_000,  // 1s
    2_500_000,  // 2.5s
    5_000_000,  // 5s
    10_000_000, // 10s
];

/// Lock-free histogram: массив бакетов-счётчиков (AtomicU64).
pub struct Histogram {
    buckets: [AtomicU64; 12],
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: Default::default(),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    /// Записать наблюдение (микросекунды).
    pub fn observe(&self, us: u64) {
        self.count.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
        for (i, &bound) in HIST_BUCKETS_US.iter().enumerate() {
            if us <= bound {
                self.buckets[i].fetch_add(1, Relaxed);
                return;
            }
        }
        // > 10s — в последний бакет (inf логически)
        self.buckets[HIST_BUCKETS_US.len() - 1].fetch_add(1, Relaxed);
    }

    /// Prometheus text: cumulative бакеты + sum + count.
    fn prometheus(&self, name: &str, out: &mut String) {
        out.push_str(&format!("# TYPE {name} histogram\n"));
        let mut cum = 0u64;
        for (i, &bound) in HIST_BUCKETS_US.iter().enumerate() {
            cum += self.buckets[i].load(Relaxed);
            let le = bound as f64 / 1_000_000.0;
            out.push_str(&format!("{name}_bucket{{le=\"{le}\"}} {cum}\n"));
        }
        let total = self.count.load(Relaxed);
        out.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {total}\n"));
        out.push_str(&format!("{name}_sum {:.6}\n", self.sum_us.load(Relaxed) as f64 / 1e6));
        out.push_str(&format!("{name}_count {total}\n"));
    }
}

#[derive(Default)]
pub struct OpsMetrics {
    // горячий путь
    pub puts: AtomicU64,
    pub put_errors: AtomicU64,
    pub put_micros: AtomicU64,
    /// W4.1: histogram PUT-латентности
    pub put_hist: Histogram,
    pub gets: AtomicU64,
    pub get_not_found: AtomicU64,
    pub get_errors: AtomicU64,
    pub get_micros: AtomicU64,
    /// W4.1: histogram GET-латентности
    pub get_hist: Histogram,
    pub deletes: AtomicU64,
    // отказоустойчивость записи/чтения
    pub hedged_reads: AtomicU64,
    pub handoff_writes: AtomicU64,
    pub mrf_enqueued: AtomicU64,
    pub mrf_healed: AtomicU64,
    // фоновые службы
    pub scrub_checked: AtomicU64,
    pub scrub_corrupt: AtomicU64,
    pub scrub_repaired: AtomicU64,
    pub scrub_unrepairable: AtomicU64,
    pub resilver_repaired: AtomicU64,
    pub resilver_errors: AtomicU64,
    pub gc_victims: AtomicU64,
    pub gc_moved: AtomicU64,
    pub gc_reclaimed_bytes: AtomicU64,
    pub gc_orphans: AtomicU64,
    pub gc_orphan_bytes: AtomicU64,
    // E20 (#138): erasure-кодирование
    pub ec_puts: AtomicU64,
    pub ec_reconstructs: AtomicU64,
    pub ec_pieces_repaired: AtomicU64,
    // E21 (#145): миграция mirror→erasure
    pub migrate_migrated: AtomicU64,
    pub migrate_canary_failed: AtomicU64,
    pub migrate_errors: AtomicU64,
    /// полировка E21b: легаси-кускам проставлен era-бит на migrate-проходе
    pub migrate_era_backfilled: AtomicU64,
    // E25 (#143/#144): СуперДиск — NVMe read-leg + coalescing
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_coalesced: AtomicU64,
    pub cache_self_heals: AtomicU64,
    pub cache_populated_bytes: AtomicU64,
    pub cache_evicted_segments: AtomicU64,
    pub cache_evicted_bytes: AtomicU64,
    // E19 (#131): elastic-троттлинг фона
    pub bg_throttle_waits: AtomicU64,
    pub bg_throttle_bytes: AtomicU64,
    /// гейдж: текущий байт/с бюджет фона (AIMD между min и max)
    pub bg_rate_bps: AtomicU64,
    /// E27: гейдж — текущий hedge-порог, мс (адаптивный p99 либо статика)
    pub hedge_threshold_ms: AtomicU64,
}

impl OpsMetrics {
    pub fn record_gc(&self, r: &GcReport) {
        if r.victim_seg.is_some() {
            self.gc_victims.fetch_add(1, Relaxed);
        }
        self.gc_moved.fetch_add(r.live_moved as u64, Relaxed);
        self.gc_reclaimed_bytes.fetch_add(r.reclaimed_bytes, Relaxed);
        self.gc_orphans.fetch_add(r.orphans_removed as u64, Relaxed);
        self.gc_orphan_bytes.fetch_add(r.orphan_bytes, Relaxed);
    }

    /// Prometheus text exposition (counters; *_seconds_sum — float-секунды).
    pub fn prometheus(&self) -> String {
        let c = |n: &str, v: u64, out: &mut String| {
            out.push_str(&format!("# TYPE {n} counter\n{n} {v}\n"));
        };
        let mut o = String::with_capacity(2048);
        c("ozd_puts_total", self.puts.load(Relaxed), &mut o);
        c("ozd_put_errors_total", self.put_errors.load(Relaxed), &mut o);
        o.push_str(&format!(
            "# TYPE ozd_put_seconds_sum counter\nozd_put_seconds_sum {:.6}\n",
            self.put_micros.load(Relaxed) as f64 / 1e6
        ));
        self.put_hist.prometheus("ozd_put_duration_seconds", &mut o);
        c("ozd_gets_total", self.gets.load(Relaxed), &mut o);
        c("ozd_get_not_found_total", self.get_not_found.load(Relaxed), &mut o);
        c("ozd_get_errors_total", self.get_errors.load(Relaxed), &mut o);
        o.push_str(&format!(
            "# TYPE ozd_get_seconds_sum counter\nozd_get_seconds_sum {:.6}\n",
            self.get_micros.load(Relaxed) as f64 / 1e6
        ));
        self.get_hist.prometheus("ozd_get_duration_seconds", &mut o);
        c("ozd_deletes_total", self.deletes.load(Relaxed), &mut o);
        c("ozd_hedged_reads_total", self.hedged_reads.load(Relaxed), &mut o);
        c("ozd_handoff_writes_total", self.handoff_writes.load(Relaxed), &mut o);
        c("ozd_mrf_enqueued_total", self.mrf_enqueued.load(Relaxed), &mut o);
        c("ozd_mrf_healed_total", self.mrf_healed.load(Relaxed), &mut o);
        c("ozd_scrub_checked_total", self.scrub_checked.load(Relaxed), &mut o);
        c("ozd_scrub_corrupt_total", self.scrub_corrupt.load(Relaxed), &mut o);
        c("ozd_scrub_repaired_total", self.scrub_repaired.load(Relaxed), &mut o);
        c(
            "ozd_scrub_unrepairable_total",
            self.scrub_unrepairable.load(Relaxed),
            &mut o,
        );
        c("ozd_resilver_repaired_total", self.resilver_repaired.load(Relaxed), &mut o);
        c("ozd_resilver_errors_total", self.resilver_errors.load(Relaxed), &mut o);
        c("ozd_ec_puts_total", self.ec_puts.load(Relaxed), &mut o);
        c("ozd_ec_reconstructs_total", self.ec_reconstructs.load(Relaxed), &mut o);
        c("ozd_ec_pieces_repaired_total", self.ec_pieces_repaired.load(Relaxed), &mut o);
        c("ozd_migrate_migrated_total", self.migrate_migrated.load(Relaxed), &mut o);
        c("ozd_migrate_canary_failed_total", self.migrate_canary_failed.load(Relaxed), &mut o);
        c("ozd_migrate_errors_total", self.migrate_errors.load(Relaxed), &mut o);
        c(
            "ozd_migrate_era_backfilled_total",
            self.migrate_era_backfilled.load(Relaxed),
            &mut o,
        );
        c("ozd_cache_hits_total", self.cache_hits.load(Relaxed), &mut o);
        c("ozd_cache_misses_total", self.cache_misses.load(Relaxed), &mut o);
        c("ozd_cache_coalesced_total", self.cache_coalesced.load(Relaxed), &mut o);
        c("ozd_cache_self_heals_total", self.cache_self_heals.load(Relaxed), &mut o);
        c("ozd_cache_populated_bytes_total", self.cache_populated_bytes.load(Relaxed), &mut o);
        c("ozd_cache_evicted_segments_total", self.cache_evicted_segments.load(Relaxed), &mut o);
        c("ozd_cache_evicted_bytes_total", self.cache_evicted_bytes.load(Relaxed), &mut o);
        c("ozd_bg_throttle_waits_total", self.bg_throttle_waits.load(Relaxed), &mut o);
        c("ozd_bg_throttle_bytes_total", self.bg_throttle_bytes.load(Relaxed), &mut o);
        o.push_str("# TYPE ozd_bg_rate_bps gauge\n");
        o.push_str(&format!("ozd_bg_rate_bps {}\n", self.bg_rate_bps.load(Relaxed)));
        o.push_str("# TYPE ozd_hedge_threshold_ms gauge\n");
        o.push_str(&format!(
            "ozd_hedge_threshold_ms {}\n",
            self.hedge_threshold_ms.load(Relaxed)
        ));
        c("ozd_gc_victims_total", self.gc_victims.load(Relaxed), &mut o);
        c("ozd_gc_moved_total", self.gc_moved.load(Relaxed), &mut o);
        c("ozd_gc_reclaimed_bytes_total", self.gc_reclaimed_bytes.load(Relaxed), &mut o);
        c("ozd_gc_orphans_total", self.gc_orphans.load(Relaxed), &mut o);
        c("ozd_gc_orphan_bytes_total", self.gc_orphan_bytes.load(Relaxed), &mut o);
        o
    }
}
