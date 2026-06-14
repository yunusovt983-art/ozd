//! ozd-zfs — адаптер ZFS-операций (порт из Go internal/zfspool, GO-MIGRATION P1;
//! паттерны обвязки — из krystal/go-zfs, идеи #146–150).
//!
//! Зачем: на 60 per-disk ZFS-пулах у нижнего яруса есть СВОЯ телеметрия
//! (checksum-errors!) и свой scrub — кормим ими disk-health (#142) и
//! делегируем проверку контрольных сумм `zpool scrub`'у.

pub mod parser;
pub mod properties;
pub mod runner;

use std::sync::Arc;

use ozd_domain::{Capacity, ShardStatus};
pub use parser::{parse_zfs_capacity, parse_zpool_status, PoolHealth, PoolState, ScrubInfo};
pub use properties::{audit_drift, DriftIssue, Properties, PropertySource, EXPECTED_TUNING};
pub use runner::{default_runner, CmdOutput, FakeRunner, LocalRunner, Runner, SudoRunner};

/// Ошибки ZFS-адаптера (#147): sentinel-таксономия вместо строк —
/// вызывающий матчит варианты, не парсит stderr.
#[derive(Debug)]
pub enum ZfsError {
    /// dataset/pool не существует ("dataset does not exist", "no such pool")
    NotFound(String),
    /// команда завершилась с ошибкой (stderr очищен clean_up_stderr)
    CommandFailed(String),
    ParseFailed,
    Io(String),
}

impl std::fmt::Display for ZfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(m) => write!(f, "zfs: not found: {m}"),
            Self::CommandFailed(m) => write!(f, "zfs command failed: {m}"),
            Self::ParseFailed => write!(f, "failed to parse zfs output"),
            Self::Io(m) => write!(f, "zfs io: {m}"),
        }
    }
}
impl std::error::Error for ZfsError {}

/// Гигиена stderr (#147, порт go-zfs cleanUpStderr): срезать `usage:`-хвост,
/// убрать пустые строки, склеить через ": ".
pub fn clean_up_stderr(stderr: &str) -> String {
    let cut = match stderr.find("\nusage:") {
        Some(i) => &stderr[..i],
        None => stderr,
    };
    cut.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(": ")
}

const NOT_FOUND_PATTERNS: &[&str] =
    &["dataset does not exist", "parent does not exist", "no such pool"];

/// Метрики пула (#150): free учитывает АСИНХРОННОЕ освобождение (freeing).
#[derive(Debug, Clone, Copy, Default)]
pub struct PoolMetrics {
    pub size: u64,
    pub allocated: u64,
    pub free: u64,
    /// байты «в пути» к освобождению после unlink (ZFS освобождает фоном)
    pub freeing: u64,
    pub leaked: u64,
    pub fragmentation_pct: u64,
}

impl PoolMetrics {
    /// Эффективный free для HRW-весов: free + freeing — иначе после
    /// GC-волны (unlink 2ГБ-сегментов) вес диска прыгает.
    pub fn effective_free(&self) -> u64 {
        self.free + self.freeing
    }
}

/// Итог сверки идентичности диска (#149, user-props `ozd:*`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityCheck {
    /// свойства записаны впервые (новый/чистый датасет)
    Initialized,
    /// идентичность совпала с конфигом
    Verified,
    /// ДИСК ПЕРЕПУТАН: на датасете другой shard_id
    Mismatch { found: String, expected: String },
}

/// Управление одним ZFS-пулом (один диск = один пул, ADR-0001).
#[derive(Clone)]
pub struct ZfsPool {
    /// имя пула, напр. "disk01"
    pub pool: String,
    /// датасет с данными ozd, напр. "disk01/ozd"
    pub dataset: String,
    runner: Arc<dyn Runner>,
}

impl std::fmt::Debug for ZfsPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZfsPool")
            .field("pool", &self.pool)
            .field("dataset", &self.dataset)
            .finish()
    }
}

impl ZfsPool {
    pub fn new(pool: impl Into<String>, dataset: impl Into<String>) -> Self {
        Self { pool: pool.into(), dataset: dataset.into(), runner: default_runner() }
    }

    /// #146: подменный runner (FakeRunner в тестах, SudoRunner без root).
    pub fn with_runner(mut self, runner: Arc<dyn Runner>) -> Self {
        self.runner = runner;
        self
    }

    /// Единая обёртка вызова (#146+#147): runner → stderr-гигиена → sentinel.
    fn run(&self, program: &str, args: &[&str]) -> Result<String, ZfsError> {
        let out = self
            .runner
            .run(program, args)
            .map_err(|e| ZfsError::Io(format!("{program}: {e}")))?;
        if !out.success {
            let clean = clean_up_stderr(&out.stderr);
            if NOT_FOUND_PATTERNS.iter().any(|p| clean.contains(p)) {
                return Err(ZfsError::NotFound(clean));
            }
            return Err(ZfsError::CommandFailed(format!(
                "{program} {}: {clean}",
                args.join(" ")
            )));
        }
        Ok(out.stdout)
    }

    /// `zpool status -p <pool>` → распарсенное здоровье.
    pub fn status(&self) -> Result<PoolHealth, ZfsError> {
        let out = self.run("zpool", &["status", "-p", &self.pool])?;
        parse_zpool_status(&out).ok_or(ZfsError::ParseFailed)
    }

    /// `zfs get -Hp used,available <dataset>` → ёмкость датасета.
    pub fn capacity(&self) -> Result<Capacity, ZfsError> {
        let out = self.run(
            "zfs",
            &["get", "-Hp", "-o", "property,value", "used,available", &self.dataset],
        )?;
        let (used, avail) = parse_zfs_capacity(&out).ok_or(ZfsError::ParseFailed)?;
        Ok(Capacity { total_bytes: used + avail, free_bytes: avail })
    }

    /// #150: zpool-метрики (free/freeing/fragmentation/leaked).
    pub fn pool_metrics(&self) -> Result<PoolMetrics, ZfsError> {
        let out = self.run(
            "zpool",
            &[
                "get",
                "-Hp",
                "-o",
                "property,value",
                "size,allocated,free,freeing,leaked,fragmentation",
                &self.pool,
            ],
        )?;
        let mut m = PoolMetrics::default();
        for line in out.lines() {
            let mut it = line.split_whitespace();
            let (Some(k), Some(v)) = (it.next(), it.next()) else { continue };
            let num = |s: &str| properties::parse_size(s.trim_end_matches('%')).unwrap_or(0);
            match k {
                "size" => m.size = num(v),
                "allocated" => m.allocated = num(v),
                "free" => m.free = num(v),
                "freeing" => m.freeing = num(v),
                "leaked" => m.leaked = num(v),
                "fragmentation" => m.fragmentation_pct = num(v),
                _ => {}
            }
        }
        Ok(m)
    }

    /// Ёмкость для HRW-веса (#150): total из пула, free = free + freeing.
    pub fn effective_capacity(&self) -> Result<Capacity, ZfsError> {
        let m = self.pool_metrics()?;
        Ok(Capacity { total_bytes: m.size, free_bytes: m.effective_free() })
    }

    /// #148: все свойства датасета с Source → для дрифт-аудита.
    pub fn dataset_properties(&self) -> Result<Properties, ZfsError> {
        let out = self.run(
            "zfs",
            &["get", "-Hp", "-o", "property,value,source", "all", &self.dataset],
        )?;
        Ok(Properties::parse(&out))
    }

    /// #149: прочитать user-property (`ozd:*`); None если не задано.
    pub fn get_user_prop(&self, name: &str) -> Result<Option<String>, ZfsError> {
        let out = self.run(
            "zfs",
            &["get", "-Hp", "-o", "value", name, &self.dataset],
        )?;
        let v = out.trim();
        Ok(if v.is_empty() || v == "-" { None } else { Some(v.to_string()) })
    }

    /// #149: записать user-properties на датасет.
    pub fn set_user_props(&self, props: &[(&str, &str)]) -> Result<(), ZfsError> {
        let mut args: Vec<String> = vec!["set".into()];
        for (k, v) in props {
            args.push(format!("{k}={v}"));
        }
        args.push(self.dataset.clone());
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        self.run("zfs", &refs).map(|_| ())
    }

    /// #149: сверка идентичности диска через `ozd:shard_id` НА датасете.
    /// Датасет чистый → записать (Initialized); совпало → Verified;
    /// иначе → Mismatch (диски перепутаны местами / конфиг сбит!).
    pub fn ensure_identity(
        &self,
        shard_id: u16,
        format_version: &str,
    ) -> Result<IdentityCheck, ZfsError> {
        let expected = shard_id.to_string();
        match self.get_user_prop("ozd:shard_id")? {
            None => {
                self.set_user_props(&[
                    ("ozd:shard_id", &expected),
                    ("ozd:format_version", format_version),
                ])?;
                Ok(IdentityCheck::Initialized)
            }
            Some(found) if found == expected => Ok(IdentityCheck::Verified),
            Some(found) => Ok(IdentityCheck::Mismatch { found, expected }),
        }
    }

    pub fn scrub_start(&self) -> Result<(), ZfsError> {
        self.run("zpool", &["scrub", &self.pool]).map(|_| ())
    }
    pub fn scrub_stop(&self) -> Result<(), ZfsError> {
        self.run("zpool", &["scrub", "-s", &self.pool]).map(|_| ())
    }
    pub fn scrub_pause(&self) -> Result<(), ZfsError> {
        self.run("zpool", &["scrub", "-p", &self.pool]).map(|_| ())
    }
}

/// Маппинг ZFS-здоровья → доменный ShardStatus (вход disk-health FSM #142).
pub fn to_shard_status(h: &PoolHealth) -> ShardStatus {
    match h.state {
        PoolState::Online => {
            let (r, w, c) = h.total_errors();
            if r == 0 && w == 0 && c == 0 {
                ShardStatus::Online
            } else {
                ShardStatus::Suspect
            }
        }
        PoolState::Degraded => ShardStatus::Suspect,
        _ => ShardStatus::Faulted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_pool(responses: Vec<CmdOutput>) -> (ZfsPool, Arc<FakeRunner>) {
        let fr = Arc::new(FakeRunner::new(responses));
        let zp = ZfsPool::new("disk01", "disk01/ozd").with_runner(fr.clone());
        (zp, fr)
    }

    #[test]
    fn maps_health_to_shard_status() {
        let mk = |state, errs: u64| PoolHealth {
            pool: "p".into(),
            state,
            devices: vec![parser::DeviceStatus {
                name: "d".into(),
                state,
                read_errors: 0,
                write_errors: 0,
                cksum_errors: errs,
            }],
            scrub: Default::default(),
        };
        assert_eq!(to_shard_status(&mk(PoolState::Online, 0)), ShardStatus::Online);
        assert_eq!(to_shard_status(&mk(PoolState::Online, 5)), ShardStatus::Suspect);
        assert_eq!(to_shard_status(&mk(PoolState::Degraded, 0)), ShardStatus::Suspect);
        assert_eq!(to_shard_status(&mk(PoolState::Faulted, 0)), ShardStatus::Faulted);
        assert_eq!(to_shard_status(&mk(PoolState::Unavail, 0)), ShardStatus::Faulted);
    }

    #[test]
    fn stderr_maps_to_not_found_and_is_cleaned() {
        // #147: "no such pool" + usage-хвост → NotFound без usage-мусора
        let (zp, _) = fake_pool(vec![FakeRunner::fail(
            "cannot open 'disk01': no such pool\nusage:\n  zpool status ...\n",
        )]);
        match zp.status() {
            Err(ZfsError::NotFound(m)) => {
                assert!(m.contains("no such pool"));
                assert!(!m.contains("usage"), "usage-хвост должен быть срезан: {m}");
            }
            other => panic!("want NotFound, got {other:?}"),
        }
    }

    #[test]
    fn pool_metrics_effective_free_includes_freeing() {
        // #150: free=100ГБ, freeing=2ГБ → эффективный free = 102ГБ
        let out = "size\t1000000000000\nallocated\t890000000000\nfree\t100000000000\n\
                   freeing\t2000000000\nleaked\t0\nfragmentation\t11%\n";
        let (zp, _) = fake_pool(vec![FakeRunner::ok(out)]);
        let m = zp.pool_metrics().unwrap();
        assert_eq!(m.free, 100_000_000_000);
        assert_eq!(m.freeing, 2_000_000_000);
        assert_eq!(m.effective_free(), 102_000_000_000);
        assert_eq!(m.fragmentation_pct, 11);
    }

    #[test]
    fn ensure_identity_initialized_verified_mismatch() {
        // #149: пусто → Initialized (get + set)
        let (zp, fr) = fake_pool(vec![
            FakeRunner::ok("-\n"),  // get ozd:shard_id → не задано
            FakeRunner::ok(""),     // set
        ]);
        assert_eq!(zp.ensure_identity(7, "v1").unwrap(), IdentityCheck::Initialized);
        let calls = fr.calls.lock().clone();
        assert!(calls[1].contains("ozd:shard_id=7"));
        assert!(calls[1].contains("ozd:format_version=v1"));

        // совпало → Verified
        let (zp2, _) = fake_pool(vec![FakeRunner::ok("7\n")]);
        assert_eq!(zp2.ensure_identity(7, "v1").unwrap(), IdentityCheck::Verified);

        // другой id → Mismatch (диск перепутан!)
        let (zp3, _) = fake_pool(vec![FakeRunner::ok("5\n")]);
        assert_eq!(
            zp3.ensure_identity(7, "v1").unwrap(),
            IdentityCheck::Mismatch { found: "5".into(), expected: "7".into() }
        );
    }

    #[test]
    fn dataset_drift_audit_via_fake() {
        // #148: recordsize=128K source=default → дрифт
        let out = "recordsize\t131072\tdefault\ncompression\tlz4\tlocal\natime\toff\tlocal\n";
        let (zp, _) = fake_pool(vec![FakeRunner::ok(out)]);
        let props = zp.dataset_properties().unwrap();
        let issues = audit_drift(&props, EXPECTED_TUNING);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].property, "recordsize");
    }
}
