// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! ozd-admin — admin/metrics поверхность (Фаза 5: scrub/resilver/balancer).
//! v1: usage-репорт по шардам + ручной запуск GC (#122).

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    routing::{get, post},
    Router,
};
use ozd_app::Pool;
use ozd_domain::ShardEngine;

#[derive(Clone)]
pub struct AdminState {
    pub shards: Vec<Arc<dyn ShardEngine>>,
    pub pool: Arc<Pool>,
    pub gc_discard_ratio: f64,
    /// ZFS-пул на шард (None = шард не на ZFS / не сконфигурирован)
    pub zfs: Vec<Option<ozd_zfs::ZfsPool>>,
}

pub fn router(
    shards: Vec<Arc<dyn ShardEngine>>,
    pool: Arc<Pool>,
    gc_discard_ratio: f64,
    zfs: Vec<Option<ozd_zfs::ZfsPool>>,
) -> Router {
    Router::new()
        .route("/admin/usage", get(usage))
        .route("/admin/gc", post(run_gc))
        .route("/admin/resilver", post(run_resilver))
        .route("/admin/structure", get(structure))
        .route("/admin/zfs", get(zfs_health))
        .route("/admin/zfs/scrub", post(zfs_scrub))
        .route("/admin/scrub", post(run_scrub))
        .route("/admin/ballast/release", post(ballast_release))
        .route("/admin/migrate", post(run_migrate))
        .route("/admin/car/import", post(car_import))
        .route("/admin/car/export", post(car_export))
        .route("/metrics", get(metrics))
        .with_state(AdminState { shards, pool, gc_discard_ratio, zfs })
}

/// POST /admin/scrub?shard=N&batch=M — один deep-scrub шаг (CRC + self-heal).
async fn run_scrub(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> serde_json_like::Value {
    let batch = q.get("batch").and_then(|s| s.parse().ok()).unwrap_or(1000);
    let want: Option<usize> = q.get("shard").and_then(|s| s.parse().ok());
    let mut out = Vec::new();
    for i in 0..st.shards.len() {
        if want.is_some_and(|w| w != i) {
            continue;
        }
        let p = st.pool.clone();
        match tokio::task::spawn_blocking(move || p.scrub_shard_step(i, None, batch)).await {
            Ok(Ok(r)) => out.push(format!(
                "{{\"shard\":{i},\"checked\":{},\"corrupt\":{},\"repaired\":{},\"unrepairable\":{}}}",
                r.checked, r.corrupt, r.repaired, r.unrepairable
            )),
            Ok(Err(e)) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
            Err(e) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
        }
    }
    serde_json_like::Value(format!("[{}]", out.join(",")))
}

/// POST /admin/zfs/scrub[?shard=N] — делегировать проверку контрольных сумм
/// нижнему ярусу: запустить `zpool scrub` (статус виден в GET /admin/zfs).
async fn zfs_scrub(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> serde_json_like::Value {
    let want: Option<usize> = q.get("shard").and_then(|s| s.parse().ok());
    let mut out = Vec::new();
    for (i, zp) in st.zfs.iter().enumerate() {
        if want.is_some_and(|w| w != i) {
            continue;
        }
        let Some(zp) = zp.clone() else { continue };
        let r = tokio::task::spawn_blocking(move || zp.scrub_start()).await;
        match r {
            Ok(Ok(())) => out.push(format!("{{\"shard\":{i},\"scrub\":\"started\"}}")),
            Ok(Err(e)) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
            Err(e) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
        }
    }
    serde_json_like::Value(format!("[{}]", out.join(",")))
}

/// POST /admin/car/import?path=/x.car[&prefix=/blocks/&parallelism=8&verify=true]
/// E22 (#123): bulk-залив CARv1 с файла на сервере — мимо S3-пути.
async fn car_import(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> serde_json_like::Value {
    let Some(path) = q.get("path").cloned() else {
        return serde_json_like::Value("{\"error\":\"path required\"}".into());
    };
    let prefix = q.get("prefix").cloned().unwrap_or_else(|| "/blocks/".into());
    let par: usize = q.get("parallelism").and_then(|s| s.parse().ok()).unwrap_or(8);
    let verify: bool = q.get("verify").and_then(|s| s.parse().ok()).unwrap_or(true);
    let pool = st.pool.clone();
    let res = tokio::task::spawn_blocking(move || {
        let f = std::fs::File::open(&path)
            .map_err(|e| ozd_domain::DomainError::Io(format!("{path}: {e}")))?;
        let store: std::sync::Arc<dyn ozd_domain::BlockStore> = pool;
        ozd_app::car::car_import(
            store,
            std::io::BufReader::with_capacity(1 << 20, f),
            prefix.as_bytes(),
            par,
            verify,
        )
    })
    .await;
    match res {
        Ok(Ok(r)) => serde_json_like::Value(format!(
            "{{\"blocks\":{},\"bytes\":{},\"skipped\":{},\"corrupt\":{},\"errors\":{}}}",
            r.blocks, r.bytes, r.skipped, r.corrupt, r.errors
        )),
        Ok(Err(e)) => serde_json_like::Value(format!("{{\"error\":\"{e}\"}}")),
        Err(e) => serde_json_like::Value(format!("{{\"error\":\"{e}\"}}")),
    }
}

/// POST /admin/car/export?path=/x.car[&prefix=/blocks/] — выгрузка в CARv1
/// (CIDv1+raw из multihash ключа; температура файла — забота оператора).
async fn car_export(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> serde_json_like::Value {
    let Some(path) = q.get("path").cloned() else {
        return serde_json_like::Value("{\"error\":\"path required\"}".into());
    };
    let prefix = q.get("prefix").cloned().unwrap_or_else(|| "/blocks/".into());
    let pool = st.pool.clone();
    let res = tokio::task::spawn_blocking(move || {
        let f = std::fs::File::create(&path)
            .map_err(|e| ozd_domain::DomainError::Io(format!("{path}: {e}")))?;
        ozd_app::car::car_export(
            &*pool,
            std::io::BufWriter::with_capacity(1 << 20, f),
            prefix.as_bytes(),
        )
    })
    .await;
    match res {
        Ok(Ok(r)) => serde_json_like::Value(format!(
            "{{\"blocks\":{},\"bytes\":{}}}",
            r.blocks, r.bytes
        )),
        Ok(Err(e)) => serde_json_like::Value(format!("{{\"error\":\"{e}\"}}")),
        Err(e) => serde_json_like::Value(format!("{{\"error\":\"{e}\"}}")),
    }
}

/// POST /admin/migrate?batch=N — один шаг миграции mirror→erasure (#145)
/// с персистентного курсора "migrate" (E17); фоновый таск — в конфиге.
async fn run_migrate(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> serde_json_like::Value {
    let batch = q.get("batch").and_then(|s| s.parse().ok()).unwrap_or(2000);
    let p = st.pool.clone();
    let s0 = st.shards[0].clone();
    let res = tokio::task::spawn_blocking(move || {
        let cur = s0.load_cursor("migrate").ok().flatten();
        let r = p.migrate_step(cur.as_ref(), batch)?;
        let next = if r.done { None } else { r.last_key.clone() };
        let _ = s0.save_cursor("migrate", next.as_ref());
        Ok::<_, ozd_domain::DomainError>(r)
    })
    .await;
    match res {
        Ok(Ok(r)) => serde_json_like::Value(format!(
            "{{\"scanned\":{},\"migrated\":{},\"skipped_small\":{},\"skipped_ec\":{},\"canary_failed\":{},\"errors\":{},\"done\":{}}}",
            r.scanned, r.migrated, r.skipped_small, r.skipped_ec, r.canary_failed, r.errors, r.done
        )),
        Ok(Err(e)) => serde_json_like::Value(format!("{{\"error\":\"{e}\"}}")),
        Err(e) => serde_json_like::Value(format!("{{\"error\":\"{e}\"}}")),
    }
}

/// POST /admin/ballast/release[?shard=N] — вручную сбросить балласт (#127):
/// вернуть зарезервированное место на переполненном диске (graceful recovery).
async fn ballast_release(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> serde_json_like::Value {
    let want: Option<usize> = q.get("shard").and_then(|s| s.parse().ok());
    let mut out = Vec::new();
    for (i, s) in st.shards.iter().enumerate() {
        if want.is_some_and(|w| w != i) {
            continue;
        }
        match s.release_ballast() {
            Ok(b) => out.push(format!("{{\"shard\":{i},\"released\":{b}}}")),
            Err(e) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
        }
    }
    serde_json_like::Value(format!("[{}]", out.join(",")))
}

/// GET /metrics — Prometheus text exposition (GO-MIGRATION P2, без зависимостей).
async fn metrics(State(st): State<AdminState>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let mut body = String::with_capacity(1024);
    body.push_str("# TYPE ozd_shard_total_bytes gauge\n");
    body.push_str("# TYPE ozd_shard_free_bytes gauge\n");
    body.push_str("# TYPE ozd_shard_status gauge\n");
    for (i, s) in st.shards.iter().enumerate() {
        let cap = s.usage().unwrap_or_default();
        body.push_str(&format!("ozd_shard_total_bytes{{shard=\"{i}\"}} {}\n", cap.total_bytes));
        body.push_str(&format!("ozd_shard_free_bytes{{shard=\"{i}\"}} {}\n", cap.free_bytes));
        let stv = match st.pool.shard_status(i) {
            Some(ozd_domain::ShardStatus::Online) => 0,
            Some(ozd_domain::ShardStatus::Suspect) => 1,
            Some(ozd_domain::ShardStatus::Faulted) => 2,
            None => -1,
        };
        body.push_str(&format!("ozd_shard_status{{shard=\"{i}\"}} {stv}\n"));
        // E18 (#127): 1 = балласт настроен, но сброшен (диск под давлением)
        body.push_str(&format!(
            "ozd_shard_ballast_released{{shard=\"{i}\"}} {}\n",
            s.ballast_released() as u8
        ));
        // E28 (#129): EWMA-латентность шарда и slow-флаг
        body.push_str(&format!(
            "ozd_shard_lat_ewma_ms{{shard=\"{i}\"}} {}\n",
            st.pool.shard_ewma_ms(i)
        ));
        body.push_str(&format!(
            "ozd_shard_slow{{shard=\"{i}\"}} {}\n",
            st.pool.shard_slow(i) as u8
        ));
    }
    body.push_str("# TYPE ozd_mrf_queue gauge\n");
    body.push_str(&format!("ozd_mrf_queue {}\n", st.pool.mrf_len()));
    // E14: операционные счётчики пула (put/get/латентности/handoff/scrub/gc)
    body.push_str(&st.pool.metrics().prometheus());
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

/// GET /admin/structure — структурный чек индекс↔сегменты по всем шардам
/// (без чтения тел; порт Go DetectMissingPacks).
async fn structure(State(st): State<AdminState>) -> serde_json_like::Value {
    let mut out = Vec::new();
    for (i, s) in st.shards.iter().enumerate() {
        let s = s.clone();
        let r = tokio::task::spawn_blocking(move || s.verify_structure()).await;
        match r {
            Ok(Ok(rep)) => out.push(format!(
                "{{\"shard\":{i},\"healthy\":{},\"segments\":{},\"missing\":{:?},\"keys_at_risk\":{},\"orphans\":{:?}}}",
                rep.is_healthy(),
                rep.segments_referenced,
                rep.segments_missing,
                rep.keys_at_risk,
                rep.orphan_segments
            )),
            Ok(Err(e)) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
            Err(e) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
        }
    }
    serde_json_like::Value(format!("[{}]", out.join(",")))
}

/// GET /admin/zfs — здоровье ZFS-пулов + метрики (#150) + дрифт-аудит (#148).
async fn zfs_health(State(st): State<AdminState>) -> serde_json_like::Value {
    let mut out = Vec::new();
    for (i, zp) in st.zfs.iter().enumerate() {
        let Some(zp) = zp.clone() else {
            continue;
        };
        let r = tokio::task::spawn_blocking(move || {
            let h = zp.status()?;
            let m = zp.pool_metrics().unwrap_or_default();
            let drift = zp
                .dataset_properties()
                .map(|p| ozd_zfs::audit_drift(&p, ozd_zfs::EXPECTED_TUNING))
                .unwrap_or_default();
            Ok::<_, ozd_zfs::ZfsError>((h, m, drift))
        })
        .await;
        match r {
            Ok(Ok((h, m, drift))) => {
                let (re, we, ce) = h.total_errors();
                let status = ozd_zfs::to_shard_status(&h);
                let drift_json: Vec<String> = drift
                    .iter()
                    .map(|d| {
                        format!(
                            "\"{}: expected {}, got {} (source={:?})\"",
                            d.property, d.expected, d.actual, d.source
                        )
                    })
                    .collect();
                out.push(format!(
                    "{{\"shard\":{i},\"pool\":\"{}\",\"state\":\"{}\",\"shard_status\":\"{:?}\",\
                     \"read_errors\":{re},\"write_errors\":{we},\"cksum_errors\":{ce},\
                     \"scrub_in_progress\":{},\"free\":{},\"freeing\":{},\
                     \"effective_free\":{},\"fragmentation_pct\":{},\"drift\":[{}]}}",
                    h.pool,
                    h.state.as_str(),
                    status,
                    h.scrub.in_progress,
                    m.free,
                    m.freeing,
                    m.effective_free(),
                    m.fragmentation_pct,
                    drift_json.join(",")
                ));
            }
            Ok(Err(e)) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
            Err(e) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
        }
    }
    serde_json_like::Value(format!("[{}]", out.join(",")))
}

/// POST /admin/resilver[?batch=1000] — полный walk-resilver (Фаза 3):
/// восстановить R копий после потери/замены/добавления диска.
/// ⚠️ Синхронный полный проход — на большом сторе может идти долго.
async fn run_resilver(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> serde_json_like::Value {
    let batch = q.get("batch").and_then(|s| s.parse::<usize>().ok()).unwrap_or(1000);
    let pool = st.pool.clone();
    match tokio::task::spawn_blocking(move || pool.resilver_full(batch)).await {
        Ok(Ok(r)) => serde_json_like::Value(format!(
            "{{\"scanned\":{},\"repaired\":{},\"errors\":{},\"done\":{}}}",
            r.scanned, r.repaired, r.errors, r.done
        )),
        Ok(Err(e)) => serde_json_like::Value(format!("{{\"error\":\"{e}\"}}")),
        Err(e) => serde_json_like::Value(format!("{{\"error\":\"{e}\"}}")),
    }
}

async fn usage(State(st): State<AdminState>) -> serde_json_like::Value {
    let mut disks = Vec::new();
    for (i, s) in st.shards.iter().enumerate() {
        let cap = s.usage().unwrap_or_default();
        disks.push(format!(
            "{{\"shard\":{i},\"total\":{},\"free\":{}}}",
            cap.total_bytes, cap.free_bytes
        ));
    }
    serde_json_like::Value(format!("[{}]", disks.join(",")))
}

/// POST /admin/gc[?ratio=0.5] — один GC-проход на каждом шарде (#122).
async fn run_gc(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> serde_json_like::Value {
    let ratio = q
        .get("ratio")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(st.gc_discard_ratio);
    let mut out = Vec::new();
    for (i, s) in st.shards.iter().enumerate() {
        let s = s.clone();
        let r = tokio::task::spawn_blocking(move || s.gc(ratio)).await;
        match r {
            Ok(Ok(rep)) => {
                st.pool.metrics().record_gc(&rep); // E14
                out.push(format!(
                "{{\"shard\":{i},\"victim\":{},\"moved\":{},\"reclaimed\":{},\"orphans\":{},\"orphan_bytes\":{}}}",
                rep.victim_seg.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
                rep.live_moved,
                rep.reclaimed_bytes,
                rep.orphans_removed,
                rep.orphan_bytes
                ));
            }
            Ok(Err(e)) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
            Err(e) => out.push(format!("{{\"shard\":{i},\"error\":\"{e}\"}}")),
        }
    }
    serde_json_like::Value(format!("[{}]", out.join(",")))
}

/// Микро-обёртка, чтобы не тянуть serde_json в v1.
mod serde_json_like {
    pub struct Value(pub String);
    impl axum::response::IntoResponse for Value {
        fn into_response(self) -> axum::response::Response {
            (
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                self.0,
            )
                .into_response()
        }
    }
}
