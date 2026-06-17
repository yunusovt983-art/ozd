// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! ozd-admin — admin/metrics поверхность (Фаза 5: scrub/resilver/balancer).
//! v1: usage-репорт по шардам + ручной запуск GC (#122).

pub mod types;

use std::collections::HashMap;
use std::path::PathBuf;
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
    /// W23: data_path каждого шарда (для snapshot-hardlinks)
    pub data_paths: Vec<PathBuf>,
    /// W26: общий каталог снимков (None = per-shard <data_path>/snapshots)
    pub snapshot_dir: Option<PathBuf>,
}

pub fn router(
    shards: Vec<Arc<dyn ShardEngine>>,
    pool: Arc<Pool>,
    gc_discard_ratio: f64,
    zfs: Vec<Option<ozd_zfs::ZfsPool>>,
    data_paths: Vec<PathBuf>,
    snapshot_dir: Option<PathBuf>,
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
        .route("/admin/capacity", get(capacity_report))
        .route("/admin/snapshot", post(create_snapshot).delete(delete_snapshot))
        .route("/admin/snapshots", get(list_snapshots))
        .route("/metrics", get(metrics))
        .with_state(AdminState { shards, pool, gc_discard_ratio, zfs, data_paths, snapshot_dir })
}

/// POST /admin/scrub?shard=N&batch=M — один deep-scrub шаг (CRC + self-heal).
async fn run_scrub(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::Json<Vec<types::ScrubItem>> {
    let batch = q.get("batch").and_then(|s| s.parse().ok()).unwrap_or(1000);
    let want: Option<usize> = q.get("shard").and_then(|s| s.parse().ok());
    let mut items = Vec::new();
    for i in 0..st.shards.len() {
        if want.is_some_and(|w| w != i) {
            continue;
        }
        let p = st.pool.clone();
        match tokio::task::spawn_blocking(move || p.scrub_shard_step(i, None, batch)).await {
            Ok(Ok(r)) => items.push(types::ScrubItem {
                shard: i,
                checked: Some(r.checked),
                corrupt: Some(r.corrupt),
                repaired: Some(r.repaired),
                unrepairable: Some(r.unrepairable),
                error: None,
            }),
            Ok(Err(e)) => items.push(types::ScrubItem {
                shard: i, checked: None, corrupt: None, repaired: None, unrepairable: None,
                error: Some(e.to_string()),
            }),
            Err(e) => items.push(types::ScrubItem {
                shard: i, checked: None, corrupt: None, repaired: None, unrepairable: None,
                error: Some(e.to_string()),
            }),
        }
    }
    axum::Json(items)
}

/// POST /admin/zfs/scrub[?shard=N] — делегировать проверку контрольных сумм
/// нижнему ярусу: запустить `zpool scrub` (статус виден в GET /admin/zfs).
async fn zfs_scrub(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::Json<Vec<types::ZfsScrubItem>> {
    let want: Option<usize> = q.get("shard").and_then(|s| s.parse().ok());
    let mut items = Vec::new();
    for (i, zp) in st.zfs.iter().enumerate() {
        if want.is_some_and(|w| w != i) {
            continue;
        }
        let Some(zp) = zp.clone() else { continue };
        let r = tokio::task::spawn_blocking(move || zp.scrub_start()).await;
        match r {
            Ok(Ok(())) => items.push(types::ZfsScrubItem { shard: i, scrub: Some("started".into()), error: None }),
            Ok(Err(e)) => items.push(types::ZfsScrubItem { shard: i, scrub: None, error: Some(e.to_string()) }),
            Err(e) => items.push(types::ZfsScrubItem { shard: i, scrub: None, error: Some(e.to_string()) }),
        }
    }
    axum::Json(items)
}

/// POST /admin/car/import?path=/x.car[&prefix=/blocks/&parallelism=8&verify=true]
/// E22 (#123): bulk-залив CARv1 с файла на сервере — мимо S3-пути.
async fn car_import(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(path) = q.get("path").cloned() else {
        return axum::Json(types::ErrorResponse::new("path required")).into_response();
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
        Ok(Ok(r)) => axum::Json(types::CarImportResponse {
            blocks: r.blocks,
            bytes: r.bytes,
            skipped: r.skipped,
            corrupt: r.corrupt,
            errors: r.errors,
        }).into_response(),
        Ok(Err(e)) => axum::Json(types::ErrorResponse::new(e)).into_response(),
        Err(e) => axum::Json(types::ErrorResponse::new(e)).into_response(),
    }
}

/// POST /admin/car/export?path=/x.car[&prefix=/blocks/] — выгрузка в CARv1
/// (CIDv1+raw из multihash ключа; температура файла — забота оператора).
async fn car_export(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(path) = q.get("path").cloned() else {
        return axum::Json(types::ErrorResponse::new("path required")).into_response();
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
        Ok(Ok(r)) => axum::Json(types::CarExportResponse {
            blocks: r.blocks,
            bytes: r.bytes,
        }).into_response(),
        Ok(Err(e)) => axum::Json(types::ErrorResponse::new(e)).into_response(),
        Err(e) => axum::Json(types::ErrorResponse::new(e)).into_response(),
    }
}

/// POST /admin/migrate?batch=N — один шаг миграции mirror→erasure (#145)
/// с персистентного курсора "migrate" (E17); фоновый таск — в конфиге.
async fn run_migrate(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
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
        Ok(Ok(r)) => axum::Json(types::MigrateResponse {
            scanned: r.scanned,
            migrated: r.migrated,
            skipped_small: r.skipped_small,
            skipped_ec: r.skipped_ec,
            canary_failed: r.canary_failed,
            errors: r.errors,
            done: r.done,
        }).into_response(),
        Ok(Err(e)) => axum::Json(types::ErrorResponse::new(e)).into_response(),
        Err(e) => axum::Json(types::ErrorResponse::new(e)).into_response(),
    }
}

/// POST /admin/ballast/release[?shard=N] — вручную сбросить балласт (#127):
/// вернуть зарезервированное место на переполненном диске (graceful recovery).
async fn ballast_release(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::Json<Vec<types::BallastItem>> {
    let want: Option<usize> = q.get("shard").and_then(|s| s.parse().ok());
    let mut items = Vec::new();
    for (i, s) in st.shards.iter().enumerate() {
        if want.is_some_and(|w| w != i) {
            continue;
        }
        match s.release_ballast() {
            Ok(b) => items.push(types::BallastItem { shard: i, released: Some(b), error: None }),
            Err(e) => items.push(types::BallastItem { shard: i, released: None, error: Some(e.to_string()) }),
        }
    }
    axum::Json(items)
}

/// W18: GET /admin/capacity — ёмкость и прогноз заполнения.
async fn capacity_report(State(st): State<AdminState>) -> axum::Json<types::CapacityResponse> {
    use std::sync::atomic::Ordering::Relaxed;
    let mut total_bytes = 0u64;
    let mut free_bytes = 0u64;
    let mut shards = Vec::new();
    for (i, s) in st.shards.iter().enumerate() {
        let cap = s.usage().unwrap_or_default();
        total_bytes += cap.total_bytes;
        free_bytes += cap.free_bytes;
        let fill_pct = if cap.total_bytes > 0 {
            100.0 * (1.0 - cap.free_bytes as f64 / cap.total_bytes as f64)
        } else {
            0.0
        };
        shards.push(types::CapacityShard {
            shard: i,
            total: cap.total_bytes,
            free: cap.free_bytes,
            fill_pct: (fill_pct * 10.0).round() / 10.0,
        });
    }
    let overall_fill = if total_bytes > 0 {
        100.0 * (1.0 - free_bytes as f64 / total_bytes as f64)
    } else {
        0.0
    };
    let written = st.pool.metrics().bytes_written.load(Relaxed);
    let free_until_95 = free_bytes.saturating_sub(total_bytes / 20);
    axum::Json(types::CapacityResponse {
        total_bytes,
        free_bytes,
        fill_pct: (overall_fill * 10.0).round() / 10.0,
        bytes_written_total: written,
        free_until_95pct: free_until_95,
        shards,
    })
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
async fn structure(State(st): State<AdminState>) -> axum::Json<Vec<types::StructureItem>> {
    let mut items = Vec::new();
    for (i, s) in st.shards.iter().enumerate() {
        let s = s.clone();
        let r = tokio::task::spawn_blocking(move || s.verify_structure()).await;
        match r {
            Ok(Ok(rep)) => items.push(types::StructureItem {
                shard: i,
                healthy: rep.is_healthy(),
                segments: rep.segments_referenced,
                missing: rep.segments_missing,
                keys_at_risk: rep.keys_at_risk,
                orphans: rep.orphan_segments,
                error: None,
            }),
            Ok(Err(e)) => items.push(types::StructureItem {
                shard: i, healthy: false, segments: 0, missing: vec![],
                keys_at_risk: 0, orphans: vec![], error: Some(e.to_string()),
            }),
            Err(e) => items.push(types::StructureItem {
                shard: i, healthy: false, segments: 0, missing: vec![],
                keys_at_risk: 0, orphans: vec![], error: Some(e.to_string()),
            }),
        }
    }
    axum::Json(items)
}

/// GET /admin/zfs — здоровье ZFS-пулов + метрики (#150) + дрифт-аудит (#148).
async fn zfs_health(State(st): State<AdminState>) -> axum::Json<Vec<types::ZfsHealthItem>> {
    let mut items = Vec::new();
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
                items.push(types::ZfsHealthItem {
                    shard: i,
                    pool: h.pool,
                    state: h.state.as_str().to_string(),
                    shard_status: format!("{status:?}"),
                    read_errors: re,
                    write_errors: we,
                    cksum_errors: ce,
                    scrub_in_progress: h.scrub.in_progress,
                    free: m.free,
                    freeing: m.freeing,
                    effective_free: m.effective_free(),
                    fragmentation_pct: m.fragmentation_pct,
                    drift: drift.iter().map(|d| {
                        format!("{}: expected {}, got {} (source={:?})",
                            d.property, d.expected, d.actual, d.source)
                    }).collect(),
                    error: None,
                });
            }
            Ok(Err(e)) => items.push(types::ZfsHealthItem {
                shard: i, pool: String::new(), state: String::new(),
                shard_status: String::new(), read_errors: 0, write_errors: 0,
                cksum_errors: 0, scrub_in_progress: false, free: 0, freeing: 0,
                effective_free: 0, fragmentation_pct: 0, drift: vec![],
                error: Some(e.to_string()),
            }),
            Err(e) => items.push(types::ZfsHealthItem {
                shard: i, pool: String::new(), state: String::new(),
                shard_status: String::new(), read_errors: 0, write_errors: 0,
                cksum_errors: 0, scrub_in_progress: false, free: 0, freeing: 0,
                effective_free: 0, fragmentation_pct: 0, drift: vec![],
                error: Some(e.to_string()),
            }),
        }
    }
    axum::Json(items)
}

/// POST /admin/resilver[?batch=1000] — полный walk-resilver (Фаза 3):
/// восстановить R копий после потери/замены/добавления диска.
/// ⚠️ Синхронный полный проход — на большом сторе может идти долго.
async fn run_resilver(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let batch = q.get("batch").and_then(|s| s.parse::<usize>().ok()).unwrap_or(1000);
    let pool = st.pool.clone();
    match tokio::task::spawn_blocking(move || pool.resilver_full(batch)).await {
        Ok(Ok(r)) => axum::Json(types::ResilverResponse {
            scanned: r.scanned,
            repaired: r.repaired,
            errors: r.errors,
            done: r.done,
        }).into_response(),
        Ok(Err(e)) => axum::Json(types::ErrorResponse::new(e)).into_response(),
        Err(e) => axum::Json(types::ErrorResponse::new(e)).into_response(),
    }
}

async fn usage(State(st): State<AdminState>) -> axum::Json<Vec<types::UsageItem>> {
    let items: Vec<types::UsageItem> = st
        .shards
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let cap = s.usage().unwrap_or_default();
            types::UsageItem { shard: i, total: cap.total_bytes, free: cap.free_bytes }
        })
        .collect();
    axum::Json(items)
}

/// POST /admin/gc[?ratio=0.5] — один GC-проход на каждом шарде (#122).
async fn run_gc(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::Json<Vec<types::GcItem>> {
    let ratio = q
        .get("ratio")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(st.gc_discard_ratio);
    let mut items = Vec::new();
    for (i, s) in st.shards.iter().enumerate() {
        let s = s.clone();
        let r = tokio::task::spawn_blocking(move || s.gc(ratio)).await;
        match r {
            Ok(Ok(rep)) => {
                st.pool.metrics().record_gc(&rep);
                items.push(types::GcItem {
                    shard: i,
                    victim: rep.victim_seg,
                    moved: rep.live_moved,
                    reclaimed: rep.reclaimed_bytes,
                    orphans: rep.orphans_removed,
                    orphan_bytes: rep.orphan_bytes,
                    error: None,
                });
            }
            Ok(Err(e)) => items.push(types::GcItem {
                shard: i, victim: None, moved: 0, reclaimed: 0, orphans: 0, orphan_bytes: 0,
                error: Some(e.to_string()),
            }),
            Err(e) => items.push(types::GcItem {
                shard: i, victim: None, moved: 0, reclaimed: 0, orphans: 0, orphan_bytes: 0,
                error: Some(e.to_string()),
            }),
        }
    }
    axum::Json(items)
}

/// W23: POST /admin/snapshot — мгновенный snapshot через hardlinks запечатанных
/// сегментов. Создаёт `snapshots/<id>/shard-<i>/seg.XXXXXXXX.dat` для каждого шарда.
/// Активный (записываемый) сегмент НЕ включён — он flush'ится перед snapshot.
async fn create_snapshot(State(st): State<AdminState>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let data_paths = st.data_paths.clone();
    let snapshot_dir = st.snapshot_dir.clone();
    let pool = st.pool.clone();
    let res = tokio::task::spawn_blocking(move || {
        // flush — переводит данные write-буфера в запечатанные сегменты
        let _ = pool.flush_all();

        // id: ISO timestamp + 4 hex из nanoseconds (уникальность без rand)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let nanos = now.subsec_nanos();
        let id = format!(
            "{}-{:04x}",
            chrono_lite(secs),
            nanos & 0xFFFF
        );

        let mut total_segs = 0usize;
        let mut total_bytes = 0u64;

        for (i, dp) in data_paths.iter().enumerate() {
            let snap_dir = snap_path_for(&snapshot_dir, dp, &id, i);
            std::fs::create_dir_all(&snap_dir)
                .map_err(|e| format!("mkdir {}: {e}", snap_dir.display()))?;

            // scan sealed segments: seg.XXXXXXXX.dat
            let rd = std::fs::read_dir(dp)
                .map_err(|e| format!("readdir shard {i}: {e}"))?;
            for ent in rd.flatten() {
                let name = ent.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with("seg.") && name_str.ends_with(".dat") {
                    let src = ent.path();
                    let dst = snap_dir.join(&*name);
                    if std::fs::hard_link(&src, &dst).is_ok() {
                        total_segs += 1;
                        total_bytes += ent.metadata().map(|m| m.len()).unwrap_or(0);
                    }
                }
            }
        }

        Ok::<_, String>(types::SnapshotResponse {
            id,
            shards: data_paths.len(),
            segments: total_segs,
            bytes: total_bytes,
            path: "snapshots/<id>/ inside each data_path".into(),
        })
    })
    .await;
    match res {
        Ok(Ok(r)) => axum::Json(r).into_response(),
        Ok(Err(e)) => axum::Json(types::ErrorResponse::new(e)).into_response(),
        Err(e) => axum::Json(types::ErrorResponse::new(e)).into_response(),
    }
}

/// W23: GET /admin/snapshots — список существующих snapshot'ов.
async fn list_snapshots(State(st): State<AdminState>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let data_paths = st.data_paths.clone();
    let snapshot_dir = st.snapshot_dir.clone();
    let res = tokio::task::spawn_blocking(move || {
        let mut items: Vec<types::SnapshotListItem> = Vec::new();

        // W26: корневая директория для поиска snapshot-id
        let snap_root = snap_root_for(&snapshot_dir, &data_paths[0]);
        let rd = match std::fs::read_dir(&snap_root) {
            Ok(r) => r,
            Err(_) => return items, // нет snapshot'ов
        };

        for ent in rd.flatten() {
            if !ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let id = ent.file_name().to_string_lossy().to_string();

            // подсчитать суммарные сегменты/байты по всем шардам
            let mut segs = 0usize;
            let mut bytes = 0u64;
            for (i, dp) in data_paths.iter().enumerate() {
                let sdir = snap_path_for(&snapshot_dir, dp, &id, i);
                if let Ok(rd2) = std::fs::read_dir(&sdir) {
                    for e2 in rd2.flatten() {
                        let n = e2.file_name();
                        let n = n.to_string_lossy();
                        if n.starts_with("seg.") && n.ends_with(".dat") {
                            segs += 1;
                            bytes += e2.metadata().map(|m| m.len()).unwrap_or(0);
                        }
                    }
                }
            }

            let created = id.clone();
            items.push(types::SnapshotListItem { id, created, segments: segs, bytes });
        }

        items.sort_by(|a, b| a.id.cmp(&b.id));
        items
    })
    .await;
    match res {
        Ok(items) => axum::Json(items).into_response(),
        Err(e) => axum::Json(types::ErrorResponse::new(e)).into_response(),
    }
}

/// W25: DELETE /admin/snapshot?id=X — удалить snapshot со всех шардов.
async fn delete_snapshot(
    State(st): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(id) = q.get("id").cloned() else {
        return (axum::http::StatusCode::BAD_REQUEST,
            axum::Json(types::ErrorResponse::new("id parameter required"))).into_response();
    };
    if id.is_empty() || id.contains('/') || id.contains("..") {
        return (axum::http::StatusCode::BAD_REQUEST,
            axum::Json(types::ErrorResponse::new("invalid snapshot id"))).into_response();
    }
    let data_paths = st.data_paths.clone();
    let snapshot_dir = st.snapshot_dir.clone();
    let res = tokio::task::spawn_blocking(move || {
        let mut deleted_files = 0usize;
        let mut deleted_dirs = 0usize;
        let mut found = false;
        for (i, dp) in data_paths.iter().enumerate() {
            let snap_dir = snap_path_for(&snapshot_dir, dp, &id, i);
            if !snap_dir.is_dir() {
                continue;
            }
            found = true;
            if let Ok(rd) = std::fs::read_dir(&snap_dir) {
                for ent in rd.flatten() {
                    if std::fs::remove_file(ent.path()).is_ok() {
                        deleted_files += 1;
                    }
                }
            }
            if std::fs::remove_dir(&snap_dir).is_ok() {
                deleted_dirs += 1;
            }
        }
        if !found {
            return Err("snapshot not found");
        }
        Ok(types::SnapshotDeleteResponse { id, deleted_files, deleted_dirs })
    })
    .await;
    match res {
        Ok(Ok(r)) => axum::Json(r).into_response(),
        Ok(Err(msg)) => (axum::http::StatusCode::NOT_FOUND,
            axum::Json(types::ErrorResponse::new(msg))).into_response(),
        Err(e) => axum::Json(types::ErrorResponse::new(e)).into_response(),
    }
}

/// W26: вычислить путь snapshot-директории для шарда.
/// Если snapshot_dir задан — `<snapshot_dir>/<id>/shard-<i>/`
/// Иначе (дефолт) — `<data_path>/snapshots/<id>/`
fn snap_path_for(snapshot_dir: &Option<PathBuf>, data_path: &PathBuf, id: &str, shard_idx: usize) -> PathBuf {
    match snapshot_dir {
        Some(dir) => dir.join(id).join(format!("shard-{shard_idx}")),
        None => data_path.join("snapshots").join(id),
    }
}

/// W26: корневая директория для перечисления snapshot-id.
fn snap_root_for(snapshot_dir: &Option<PathBuf>, data_path_0: &PathBuf) -> PathBuf {
    match snapshot_dir {
        Some(dir) => dir.clone(),
        None => data_path_0.join("snapshots"),
    }
}

/// Минимальный ISO-подобный форматировщик без зависимости chrono.
fn chrono_lite(epoch_secs: u64) -> String {
    // дни с 1970-01-01
    let days = epoch_secs / 86400;
    let rem = epoch_secs % 86400;
    let h = rem / 3600;
    let m = (rem % 3600) / 60;
    let s = rem % 60;

    // год/месяц/день по гражданскому алгоритму (Casssini/Howard Hinnant)
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let yr = if mo <= 2 { y + 1 } else { y };

    format!("{yr:04}{mo:02}{d:02}-{h:02}{m:02}{s:02}")
}
