// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! ozd — OpenZFS Daemon: S3-шлюз для Kubo поверх 60 HDD (sharding + packing).
//!
//! Запуск: `ozd --config ozd.toml` (дефолт ./ozd.toml).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

use ozd_app::{Pool, PoolConfig, RendezvousHrw};
use ozd_domain::{BlockStore, ShardEngine};
use ozd_engine::{DiskEngine, EngineConfig};

#[derive(Deserialize, Debug)]
struct Config {
    /// адрес S3-шлюза, напр. "127.0.0.1:9100"
    listen: String,
    /// число реплик R (Часть 1: mirror)
    #[serde(default = "default_r")]
    replicas: usize,
    /// write-кворум W
    #[serde(default = "default_w")]
    write_quorum: usize,
    #[serde(default = "default_ttl")]
    free_space_cache_ttl_secs: u64,
    /// hedged read (#121/#143): порог дубль-чтения write-mostly-ноге, мс; 0 = off
    #[serde(default = "default_spec_ms")]
    speculative_retry_ms: u64,
    /// GC сегментов (#122): порог окупаемости rewrite (мусор ≥ ratio×size)
    #[serde(default = "default_gc_ratio")]
    gc_discard_ratio: f64,
    /// период фонового GC, сек; 0 = только вручную (POST /admin/gc)
    #[serde(default = "default_gc_interval")]
    gc_interval_secs: u64,
    /// период опроса zpool status для дисков с zfs_pool, сек; 0 = off
    #[serde(default = "default_zfs_health_interval")]
    zfs_health_interval_secs: u64,
    /// FSM #142: сбоев подряд до эскалации / успехов подряд до возврата
    #[serde(default = "default_suspect_after")]
    drive_suspect_threshold: u32,
    #[serde(default = "default_recover_after")]
    drive_recover_threshold: u32,
    /// фоновый scrub (#102/#141): период цикла, сек; 0 = только вручную
    #[serde(default = "default_scrub_interval")]
    scrub_interval_secs: u64,
    /// бюджет цикла: ключей на шард за цикл (#141 cycle-budget)
    #[serde(default = "default_scrub_keys")]
    scrub_keys_per_cycle: usize,
    /// MRF (#140): быстрый точечный heal недавно-сбойных записей
    #[serde(default = "default_true")]
    heal_mrf: bool,
    /// E16: параллелизм дренажа heal-очереди / bulkhead на шард
    #[serde(default = "default_heal_par")]
    heal_parallelism: usize,
    #[serde(default = "default_heal_cap")]
    heal_max_per_shard: usize,
    /// E19 (#131): потолок бюджета фоновых работ, байт/с (0 = троттлинг выкл)
    #[serde(default = "default_bg_max")]
    bg_max_bytes_per_sec: u64,
    /// пол бюджета — resilver/GC не голодают до нуля
    #[serde(default = "default_bg_min")]
    bg_min_bytes_per_sec: u64,
    /// порог «foreground занят», операций/с (puts+gets)
    #[serde(default = "default_bg_busy")]
    bg_fg_busy_ops: f64,
    /// E20 (#138): "mirror" (R-копии) | "erasure" (K+M кусков, 1.5× при 4+2)
    #[serde(default = "default_redundancy")]
    redundancy: String,
    #[serde(default = "default_ec_data")]
    ec_data: usize,
    #[serde(default = "default_ec_parity")]
    ec_parity: usize,
    /// тела меньше порога остаются зеркалом (EC мелочи не окупается)
    #[serde(default = "default_ec_min")]
    ec_min_size: usize,
    /// кворум записи кусков (дефолт K+1)
    ec_write_quorum: Option<usize>,
    /// E28 (#129): период disk-slow вердикта, сек (0 = выкл)
    #[serde(default = "default_disk_slow_interval")]
    disk_slow_interval_secs: u64,
    #[serde(default = "default_disk_slow_floor")]
    disk_slow_floor_ms: u64,
    #[serde(default = "default_disk_slow_factor")]
    disk_slow_factor: f64,
    /// E27: hedge-порог из скользящего p99 чтений (дефолт true);
    /// speculative_retry_ms остаётся fallback'ом прогрева и override'ом
    #[serde(default = "default_true")]
    adaptive_hedge: bool,
    /// E23 (#79): BLAKE3 outboard — verified Range-чтения (false = выкл)
    #[serde(default)]
    blake3_outboard: bool,
    #[serde(default = "default_ob_min")]
    ob_min_size: usize,
    /// E21 (#145): фоновая миграция mirror→erasure, период шага (0 = выкл)
    #[serde(default)]
    migrate_interval_secs: u64,
    #[serde(default = "default_migrate_keys")]
    migrate_keys_per_cycle: usize,
    /// до 60 дисков; каждый — точка монтирования ZFS-датасета
    disks: Vec<DiskCfg>,
    #[serde(default)]
    engine: EngineCfg,
    /// E25 (#143): СуперДиск — NVMe read-leg (нет секции = выключен)
    cache: Option<CacheCfg>,
    /// E13: SigV4-аутентификация S3-шлюза; отсутствует = dev (только loopback!)
    auth: Option<AuthCfg>,
}

#[derive(Deserialize, Debug)]
struct AuthCfg {
    access_key: String,
    secret_key: String,
    /// допуск рассинхрона часов, сек (0 = off)
    #[serde(default = "default_skew")]
    max_skew_secs: i64,
}

fn default_skew() -> i64 {
    900
}

#[derive(Deserialize, Debug)]
struct CacheCfg {
    /// NVMe-датасет под кэш тел (НЕ тот же путь, что index_path дисков)
    path: PathBuf,
    index_path: Option<PathBuf>,
    /// бюджет кэша, байт (обязателен: NVMe конечен)
    max_bytes: u64,
    #[serde(default = "default_cache_min")]
    min_size: usize,
    #[serde(default = "default_cache_seg")]
    segment_max_size: u64,
}

fn default_cache_min() -> usize {
    4096
}
fn default_cache_seg() -> u64 {
    256 * 1024 * 1024
}

#[derive(Deserialize, Debug)]
struct DiskCfg {
    /// тела блоков (pack-сегменты) — ZFS-датасет HDD
    data_path: PathBuf,
    /// индекс (redb) — в идеале NVMe; иначе тот же датасет
    index_path: Option<PathBuf>,
    /// имя ZFS-пула диска (напр. "disk01") — включает zpool-health-монитор
    zfs_pool: Option<String>,
    /// датасет для capacity (дефолт = zfs_pool, напр. "disk01/ozd")
    zfs_dataset: Option<String>,
    /// E18 (#128): запасной каталог сегментов (ДРУГОЙ диск/NVMe) —
    /// экстренная ротация туда при отказе data_path
    failover_path: Option<PathBuf>,
}

#[derive(Deserialize, Debug, Default)]
struct EngineCfg {
    segment_max_size: Option<u64>,
    inline_min: Option<u32>,
    fsync_items: Option<u32>,
    /// E10: "none" | "zstd"
    compress: Option<String>,
    compress_min: Option<u32>,
    /// E18 (#127): балласт-файл на каждом data_path, байт (0 = выключен)
    ballast_bytes: Option<u64>,
    /// E26 (#63): DONTNEED write-once байтов сегментов (Linux; дефолт true)
    fadvise_dontneed: Option<bool>,
}

fn default_r() -> usize {
    2
}
fn default_w() -> usize {
    2
}
fn default_ttl() -> u64 {
    5
}
fn default_spec_ms() -> u64 {
    100
}
fn default_gc_ratio() -> f64 {
    0.5
}
fn default_gc_interval() -> u64 {
    300
}
fn default_zfs_health_interval() -> u64 {
    30
}
fn default_suspect_after() -> u32 {
    3
}
fn default_recover_after() -> u32 {
    2
}
fn default_scrub_interval() -> u64 {
    600
}
fn default_scrub_keys() -> usize {
    5000
}
fn default_true() -> bool {
    true
}
fn default_heal_par() -> usize {
    4
}
fn default_bg_max() -> u64 {
    64 * 1024 * 1024
}
fn default_bg_min() -> u64 {
    4 * 1024 * 1024
}
fn default_bg_busy() -> f64 {
    50.0
}
fn default_ob_min() -> usize {
    256 * 1024
}
fn default_migrate_keys() -> usize {
    2000
}
fn default_disk_slow_interval() -> u64 {
    10
}
fn default_disk_slow_floor() -> u64 {
    250
}
fn default_disk_slow_factor() -> f64 {
    4.0
}
fn default_redundancy() -> String {
    "mirror".into()
}
fn default_ec_data() -> usize {
    4
}
fn default_ec_parity() -> usize {
    2
}
fn default_ec_min() -> usize {
    64 * 1024
}
fn default_heal_cap() -> usize {
    2
}

/// W1.1: заглушка для недоступного шарда (degraded start) — все операции
/// возвращают ошибку, Pool пометит его Faulted и HRW исключит из placement.
struct NullEngine;

impl ShardEngine for NullEngine {
    fn put(&self, _: &ozd_domain::BlockKey, _: &[u8]) -> ozd_domain::DomainResult<()> {
        Err(ozd_domain::DomainError::Io("shard unavailable (degraded start)".into()))
    }
    fn get(&self, _: &ozd_domain::BlockKey) -> ozd_domain::DomainResult<Vec<u8>> {
        Err(ozd_domain::DomainError::Io("shard unavailable (degraded start)".into()))
    }
    fn has(&self, _: &ozd_domain::BlockKey) -> ozd_domain::DomainResult<bool> {
        Err(ozd_domain::DomainError::Io("shard unavailable (degraded start)".into()))
    }
    fn delete(&self, _: &ozd_domain::BlockKey) -> ozd_domain::DomainResult<()> {
        Err(ozd_domain::DomainError::Io("shard unavailable (degraded start)".into()))
    }
    fn list(&self, _: &[u8], _: Option<&ozd_domain::BlockKey>, _: usize) -> ozd_domain::DomainResult<Vec<(ozd_domain::BlockKey, u64)>> {
        Ok(vec![])
    }
    fn usage(&self) -> ozd_domain::DomainResult<ozd_domain::Capacity> {
        Ok(ozd_domain::Capacity { total_bytes: 0, free_bytes: 0 })
    }
    fn flush(&self) -> ozd_domain::DomainResult<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg_path = std::env::args()
        .skip_while(|a| a != "--config")
        .nth(1)
        .unwrap_or_else(|| "ozd.toml".to_string());
    let raw = std::fs::read_to_string(&cfg_path)
        .with_context(|| format!("reading config {cfg_path}"))?;
    let cfg: Config = toml::from_str(&raw).context("parsing config")?;
    anyhow::ensure!(!cfg.disks.is_empty(), "config: at least one disk required");
    anyhow::ensure!(
        cfg.replicas <= cfg.disks.len(),
        "config: replicas R={} > disks {}",
        cfg.replicas,
        cfg.disks.len()
    );

    tracing::info!(
        disks = cfg.disks.len(),
        r = cfg.replicas,
        w = cfg.write_quorum,
        "opening shard engines"
    );

    let mut shards: Vec<Arc<dyn ShardEngine>> = Vec::with_capacity(cfg.disks.len());
    let mut faulted_at_start: Vec<usize> = Vec::new();
    for (i, d) in cfg.disks.iter().enumerate() {
        match DiskEngine::open(EngineConfig {
            data_path: d.data_path.clone(),
            index_path: d.index_path.clone(),
            segment_max_size: cfg.engine.segment_max_size.unwrap_or(2 << 30),
            inline_min: cfg.engine.inline_min.unwrap_or(4096),
            fsync_items: cfg.engine.fsync_items.unwrap_or(256),
            compress_zstd: cfg.engine.compress.as_deref() == Some("zstd"),
            compress_min: cfg.engine.compress_min.unwrap_or(512),
            ballast_bytes: cfg.engine.ballast_bytes.unwrap_or(0),
            failover_path: d.failover_path.clone(),
            fadvise_dontneed: cfg.engine.fadvise_dontneed.unwrap_or(true),
        }) {
            Ok(e) => shards.push(Arc::new(e)),
            Err(e) => {
                // W1.1: degraded start — шард недоступен, стартуем без него
                tracing::error!(
                    shard = i, path = %d.data_path.display(), err = %e,
                    "DEGRADED START: шард не открылся — помечен Faulted, продолжаем"
                );
                shards.push(Arc::new(NullEngine));
                faulted_at_start.push(i);
            }
        }
    }
    // Минимум живых шардов: хотя бы R штук (иначе запись невозможна)
    let alive = cfg.disks.len() - faulted_at_start.len();
    anyhow::ensure!(
        alive >= cfg.replicas,
        "degraded start: живых шардов {alive} < replicas R={} — невозможно обеспечить запись",
        cfg.replicas
    );

    // E20 (#138): erasure-конфиг
    let ec = match cfg.redundancy.as_str() {
        "erasure" => {
            let (k, m) = (cfg.ec_data, cfg.ec_parity);
            anyhow::ensure!(k >= 1 && m >= 1, "config: ec_data/ec_parity >= 1");
            anyhow::ensure!(
                cfg.disks.len() >= k + m,
                "config: erasure {k}+{m} требует >= {} дисков, есть {}",
                k + m,
                cfg.disks.len()
            );
            tracing::info!(k, m, min = cfg.ec_min_size, "redundancy: erasure (#138)");
            Some(ozd_app::erasure::EcConfig {
                data: k,
                parity: m,
                min_size: cfg.ec_min_size,
                write_quorum: cfg.ec_write_quorum.unwrap_or(k + 1).clamp(k, k + m),
            })
        }
        "mirror" => None,
        other => anyhow::bail!("config: redundancy = \"{other}\" (ждали mirror|erasure)"),
    };

    let ec_enabled = ec.is_some();
    let pool = Arc::new(Pool::new(
        shards.clone(),
        Box::new(RendezvousHrw::default()),
        PoolConfig {
            replicas: cfg.replicas,
            write_quorum: cfg.write_quorum,
            free_space_cache_ttl: Duration::from_secs(cfg.free_space_cache_ttl_secs),
            speculative_retry_after: (cfg.speculative_retry_ms > 0)
                .then(|| Duration::from_millis(cfg.speculative_retry_ms)),
            heal_parallelism: cfg.heal_parallelism,
            heal_max_per_shard: cfg.heal_max_per_shard,
            bg_throttle: ozd_app::throttle::BgThrottleConfig {
                max_bytes_per_sec: cfg.bg_max_bytes_per_sec,
                min_bytes_per_sec: cfg.bg_min_bytes_per_sec,
                fg_busy_ops_per_sec: cfg.bg_fg_busy_ops,
            },
            ec,
            adaptive_hedge: cfg.adaptive_hedge,
            disk_slow: ozd_app::diskslow::DiskSlowConfig {
                abs_floor_ms: cfg.disk_slow_floor_ms,
                rel_factor: cfg.disk_slow_factor,
                min_samples: 32,
            },
            outboard: cfg
                .blake3_outboard
                .then(|| ozd_app::verified::ObConfig { min_size: cfg.ob_min_size }),
        },
    ));

    // W1.1: degraded start — пометить сбойные шарды Faulted в Pool
    for &i in &faulted_at_start {
        pool.set_shard_status(i, ozd_domain::ShardStatus::Faulted);
    }
    if !faulted_at_start.is_empty() {
        tracing::warn!(
            faulted = ?faulted_at_start,
            alive = cfg.disks.len() - faulted_at_start.len(),
            "DEGRADED MODE: {} шардов недоступны при старте",
            faulted_at_start.len()
        );
    }

    // ZFS-пулы шардов (для health-монитора и /admin/zfs)
    let zfs_pools: Vec<Option<ozd_zfs::ZfsPool>> = cfg
        .disks
        .iter()
        .map(|d| {
            d.zfs_pool.as_ref().map(|p| {
                let ds = d.zfs_dataset.clone().unwrap_or_else(|| p.clone());
                ozd_zfs::ZfsPool::new(p.clone(), ds)
            })
        })
        .collect();

    // #149: сверка идентичности дисков через user-props ozd:* НА датасете —
    // ловим перепутанные диски/сбитый конфиг ДО приёма трафика
    for (i, zp) in zfs_pools.iter().enumerate() {
        let Some(zp) = zp else { continue };
        match zp.ensure_identity(i as u16, "v1") {
            Ok(ozd_zfs::IdentityCheck::Verified) => {
                tracing::debug!(shard = i, dataset = %zp.dataset, "zfs identity verified")
            }
            Ok(ozd_zfs::IdentityCheck::Initialized) => {
                tracing::info!(shard = i, dataset = %zp.dataset, "zfs identity initialized (ozd:shard_id)")
            }
            Ok(ozd_zfs::IdentityCheck::Mismatch { found, expected }) => {
                anyhow::bail!(
                    "disk identity mismatch on {}: dataset carries ozd:shard_id={found}, \
                     config expects {expected} — диски перепутаны местами или конфиг сбит; \
                     откажемся стартовать, чтобы не писать не туда",
                    zp.dataset
                );
            }
            Err(e) => {
                // zfs недоступен (dev-машина) — не блокируем старт
                tracing::warn!(shard = i, err = %e, "zfs identity check skipped");
            }
        }
    }

    let auth = cfg.auth.as_ref().map(|a| {
        let mut c = ozd_ipfs::SigV4Config::new(a.access_key.clone(), a.secret_key.clone());
        c.max_skew_secs = a.max_skew_secs;
        c
    });
    if auth.is_none() {
        tracing::warn!("auth: SigV4 DISABLED — слушать только loopback/доверенную сеть!");
    }
    // E25 (#143): СуперДиск — шлюз читает через NVMe-ногу (CacheTier);
    // admin/фоновые службы работают с пулом напрямую (bulk не греет кэш)
    let store: Arc<dyn BlockStore> = match &cfg.cache {
        Some(c) => {
            let eng = DiskEngine::open(EngineConfig {
                data_path: c.path.clone(),
                index_path: c.index_path.clone(),
                segment_max_size: c.segment_max_size,
                inline_min: 64,
                fsync_items: 4096, // кэш: потеря хвоста при креше безвредна
                compress_zstd: false, // не жечь CPU: durable-нога уже жмёт
                ..Default::default()
            })
            .map_err(|e| anyhow::anyhow!("cache ({}): {e}", c.path.display()))?;
            tracing::info!(
                path = %c.path.display(),
                max_bytes = c.max_bytes,
                "super-disk (#143): NVMe read-leg enabled"
            );
            Arc::new(ozd_app::cache::CacheTier::new(
                pool.clone(),
                Arc::new(eng),
                ozd_app::cache::CacheConfig { max_bytes: c.max_bytes, min_size: c.min_size },
                pool.metrics(),
            ))
        }
        None => pool.clone(),
    };
    let app = ozd_ipfs::router(store, auth)
        .merge(ozd_admin::router(
            shards.clone(),
            pool.clone(),
            cfg.gc_discard_ratio,
            zfs_pools.clone(),
        ));

    // фоновый ZFS-health-монитор (GO-MIGRATION P1): zpool status → ShardStatus
    // (#142): ONLINE чисто → Online; ошибки/DEGRADED → Suspect; FAULTED →
    // Faulted (HRW исключает из placement немедленно)
    if cfg.zfs_health_interval_secs > 0 && zfs_pools.iter().any(|z| z.is_some()) {
        let mon_pool = pool.clone();
        let mon_zfs = zfs_pools.clone();
        let period = Duration::from_secs(cfg.zfs_health_interval_secs);
        // FSM #142: гистерезис поверх сырых наблюдений ZFS-монитора
        let mut fsms: Vec<ozd_app::HealthFsm> = (0..mon_zfs.len())
            .map(|_| {
                ozd_app::HealthFsm::new(cfg.drive_suspect_threshold, cfg.drive_recover_threshold)
            })
            .collect();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(period);
            loop {
                iv.tick().await;
                for (i, zp) in mon_zfs.iter().enumerate() {
                    let Some(zp) = zp.clone() else { continue };
                    let res = tokio::task::spawn_blocking(move || {
                        let h = zp.status()?;
                        let cap = zp.effective_capacity(); // #150: free+freeing
                        Ok::<_, ozd_zfs::ZfsError>((h, cap))
                    })
                    .await;
                    // сырое наблюдение → FSM (#142) → доменный статус
                    let obs = match &res {
                        Ok(Ok((h, _))) => match ozd_zfs::to_shard_status(h) {
                            ozd_domain::ShardStatus::Online => ozd_app::Observation::Healthy,
                            ozd_domain::ShardStatus::Suspect => ozd_app::Observation::Degraded,
                            ozd_domain::ShardStatus::Faulted => ozd_app::Observation::Down,
                        },
                        _ => ozd_app::Observation::Down,
                    };
                    let st = fsms[i].observe(obs);
                    mon_pool.set_shard_status(i, st);
                    if let Ok(Ok((h, cap))) = res {
                        if st != ozd_domain::ShardStatus::Online {
                            let (re, we, ce) = h.total_errors();
                            tracing::warn!(
                                shard = i, pool = %h.pool, state = h.state.as_str(),
                                read = re, write = we, cksum = ce, ?st, "zfs health (fsm)"
                            );
                        }
                        if let Ok(c) = cap {
                            mon_pool.set_shard_capacity(i, c); // вес HRW (#150)
                        }
                    } else {
                        tracing::warn!(shard = i, ?st, "zpool status failed (fsm observed Down)");
                    }
                }
            }
        });
    }

    // MRF-дренаж (#140): недавно-сбойные записи чиним точечно и быстро
    if cfg.heal_mrf {
        let mrf_pool = pool.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(5));
            loop {
                iv.tick().await;
                if mrf_pool.mrf_len() == 0 {
                    continue;
                }
                let p = mrf_pool.clone();
                match tokio::task::spawn_blocking(move || p.mrf_drain(256)).await {
                    Ok(Ok((healed, requeued))) if healed + requeued > 0 => {
                        tracing::info!(healed, requeued, "mrf drain");
                    }
                    Ok(Err(e)) => tracing::warn!(err = %e, "mrf drain failed"),
                    _ => {}
                }
            }
        });
    }

    // фоновый scrub (#102/#141): курсор на шард, бюджет ключей на цикл,
    // джиттер ±10% (анти-thundering-herd на 60 дисках)
    if cfg.scrub_interval_secs > 0 {
        let scrub_pool = pool.clone();
        let scrub_shards = shards.clone();
        let n_shards = shards.len();
        let base = cfg.scrub_interval_secs;
        let batch = cfg.scrub_keys_per_cycle;
        tokio::spawn(async move {
            // E17 (#102): курсоры scrub персистентны — рестарт с места
            let mut cursors: Vec<Option<ozd_domain::BlockKey>> = scrub_shards
                .iter()
                .map(|s| s.load_cursor("scrub").ok().flatten())
                .collect();
            if cursors.iter().any(|c| c.is_some()) {
                tracing::info!("scrub: resuming from persisted cursors");
            }
            loop {
                // джиттер из субсекундных наносекунд (без rand-зависимости)
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_nanos() as u64)
                    .unwrap_or(0);
                let jitter = (base / 10).max(1);
                let sleep = base - jitter / 2 + (nanos % jitter.max(1));
                tokio::time::sleep(Duration::from_secs(sleep)).await;

                for i in 0..n_shards {
                    let p = scrub_pool.clone();
                    let after = cursors[i].clone();
                    let res = tokio::task::spawn_blocking(move || {
                        p.scrub_shard_step(i, after.as_ref(), batch)
                    })
                    .await;
                    match res {
                        Ok(Ok(r)) => {
                            if r.corrupt > 0 {
                                tracing::warn!(
                                    shard = i, corrupt = r.corrupt, repaired = r.repaired,
                                    unrepairable = r.unrepairable, "scrub cycle"
                                );
                            }
                            // курсор: дальше или сначала (полный обход за N циклов)
                            cursors[i] = if r.done { None } else { r.last_key };
                            // E17: персист курсора (рестарт продолжит с места)
                            let eng = scrub_shards[i].clone();
                            let cur = cursors[i].clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                eng.save_cursor("scrub", cur.as_ref())
                            })
                            .await;
                        }
                        Ok(Err(e)) => tracing::warn!(shard = i, err = %e, "scrub failed"),
                        Err(e) => tracing::warn!(shard = i, err = %e, "scrub join"),
                    }
                }
            }
        });
    }

    // E28 (#129): disk-slow монитор — EWMA-вердикты через FSM-гистерезис
    // (тот же #142, что у ZFS-входа) → slow-флаг → Suspect в topology
    if cfg.disk_slow_interval_secs > 0 {
        let ds_pool = pool.clone();
        let n = shards.len();
        let (sa, ra) = (cfg.drive_suspect_threshold, cfg.drive_recover_threshold);
        let period = Duration::from_secs(cfg.disk_slow_interval_secs);
        tokio::spawn(async move {
            let mut fsms: Vec<ozd_app::HealthFsm> =
                (0..n).map(|_| ozd_app::HealthFsm::new(sa, ra)).collect();
            let mut iv = tokio::time::interval(period);
            iv.tick().await;
            loop {
                iv.tick().await;
                let verdicts = ds_pool.disk_slow_verdicts();
                for (i, slow) in verdicts.into_iter().enumerate() {
                    let obs = if slow {
                        ozd_app::Observation::Degraded
                    } else {
                        ozd_app::Observation::Healthy
                    };
                    let st = fsms[i].observe(obs);
                    ds_pool.set_shard_slow(i, st != ozd_domain::ShardStatus::Online);
                }
            }
        });
    }

    // E21 (#145): фоновая миграция mirror→erasure — шаг за тик,
    // курсор персистентен (E17, имя "migrate"), throttle платит E19
    if cfg.migrate_interval_secs > 0 {
        anyhow::ensure!(
            ec_enabled,
            "config: migrate_interval_secs требует redundancy=\"erasure\""
        );
        let mig_pool = pool.clone();
        let mig_shard0 = shards[0].clone();
        let period = Duration::from_secs(cfg.migrate_interval_secs);
        let batch = cfg.migrate_keys_per_cycle;
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(period);
            iv.tick().await;
            let mut pass_migrated = 0usize;
            loop {
                iv.tick().await;
                let p = mig_pool.clone();
                let s0 = mig_shard0.clone();
                let res = tokio::task::spawn_blocking(move || {
                    let cur = s0.load_cursor("migrate").ok().flatten();
                    let r = p.migrate_step(cur.as_ref(), batch)?;
                    let next = if r.done { None } else { r.last_key.clone() };
                    let _ = s0.save_cursor("migrate", next.as_ref());
                    Ok::<_, ozd_domain::DomainError>(r)
                })
                .await;
                match res {
                    Ok(Ok(r)) => {
                        pass_migrated += r.migrated;
                        if r.migrated > 0 || r.canary_failed > 0 || r.errors > 0 {
                            tracing::info!(
                                migrated = r.migrated,
                                canary_failed = r.canary_failed,
                                errors = r.errors,
                                "migration step (#145)"
                            );
                        }
                        if r.done {
                            tracing::info!(
                                migrated = pass_migrated,
                                "migration pass complete — следующий проход с начала"
                            );
                            pass_migrated = 0;
                        }
                    }
                    Ok(Err(e)) => tracing::warn!(err = %e, "migration step failed"),
                    Err(e) => tracing::warn!(err = %e, "migration task join failed"),
                }
            }
        });
    }

    // фоновый GC сегментов (#122): период gc_interval_secs, 0 = off
    if cfg.gc_interval_secs > 0 {
        let gc_shards = shards.clone();
        let gc_metrics = pool.metrics(); // E14
        let gc_bg = pool.bg(); // E19: GC платит токенами за байты
        let ratio = cfg.gc_discard_ratio;
        let period = Duration::from_secs(cfg.gc_interval_secs);
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(period);
            iv.tick().await; // первый тик мгновенный — пропускаем
            loop {
                iv.tick().await;
                for (i, s) in gc_shards.iter().enumerate() {
                    let s = s.clone();
                    let bg = gc_bg.clone();
                    match tokio::task::spawn_blocking(move || {
                        let r = s.gc(ratio);
                        if let Ok(rep) = &r {
                            // E19: жертва переписана + orphan-байты — фон платит
                            bg.acquire(rep.reclaimed_bytes + rep.orphan_bytes);
                        }
                        r
                    })
                    .await
                    {
                        Ok(Ok(r)) if r.victim_seg.is_some() || r.orphans_removed > 0 => {
                            gc_metrics.record_gc(&r); // E14
                            tracing::info!(
                                disk = i,
                                seg = r.victim_seg.unwrap(),
                                moved = r.live_moved,
                                reclaimed = r.reclaimed_bytes,
                                "background gc"
                            );
                        }
                        Ok(Err(e)) => tracing::warn!(disk = i, err = %e, "gc failed"),
                        _ => {}
                    }
                }
            }
        });
    }

    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("binding {}", cfg.listen))?;
    tracing::info!(addr = %cfg.listen, "ozd S3-gateway listening (point Kubo go-ds-s3 here)");

    // graceful shutdown: flush сегментов (recovery-point) на выходе
    let pool_for_shutdown = pool.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown: flushing segments");
            let _ = pool_for_shutdown.flush_all();
        })
        .await?;
    Ok(())
}
