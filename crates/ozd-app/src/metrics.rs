// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! E14: операционные метрики пула (lock-free атомики, Prometheus-text).
//! Без внешних crates: счётчики + суммы латентностей (avg/rate — в PromQL).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use ozd_domain::GcReport;

#[derive(Default)]
pub struct OpsMetrics {
    // горячий путь
    pub puts: AtomicU64,
    pub put_errors: AtomicU64,
    pub put_micros: AtomicU64,
    pub gets: AtomicU64,
    pub get_not_found: AtomicU64,
    pub get_errors: AtomicU64,
    pub get_micros: AtomicU64,
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
        c("ozd_gets_total", self.gets.load(Relaxed), &mut o);
        c("ozd_get_not_found_total", self.get_not_found.load(Relaxed), &mut o);
        c("ozd_get_errors_total", self.get_errors.load(Relaxed), &mut o);
        o.push_str(&format!(
            "# TYPE ozd_get_seconds_sum counter\nozd_get_seconds_sum {:.6}\n",
            self.get_micros.load(Relaxed) as f64 / 1e6
        ));
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
