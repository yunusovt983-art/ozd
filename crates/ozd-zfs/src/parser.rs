// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! Парсер вывода `zpool status -p` (порт Go zfspool/pool.go parsePoolStatus).
//! Без зависимостей: построчный разбор, числа точные (флаг -p).

/// Состояние пула/устройства ZFS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolState {
    Online,
    Degraded,
    Faulted,
    Offline,
    Unavail,
    Removed,
    Unknown,
}

impl PoolState {
    pub fn parse(s: &str) -> Self {
        match s {
            "ONLINE" => Self::Online,
            "DEGRADED" => Self::Degraded,
            "FAULTED" => Self::Faulted,
            "OFFLINE" => Self::Offline,
            "UNAVAIL" => Self::Unavail,
            "REMOVED" => Self::Removed,
            _ => Self::Unknown,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Online => "ONLINE",
            Self::Degraded => "DEGRADED",
            Self::Faulted => "FAULTED",
            Self::Offline => "OFFLINE",
            Self::Unavail => "UNAVAIL",
            Self::Removed => "REMOVED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeviceStatus {
    pub name: String,
    pub state: PoolState,
    pub read_errors: u64,
    pub write_errors: u64,
    pub cksum_errors: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ScrubInfo {
    pub in_progress: bool,
    /// процент из строки "NN.NN% done" (если идёт)
    pub percent_done: Option<f64>,
    /// строка scan: целиком (для логов/админки)
    pub raw: String,
}

#[derive(Debug, Clone)]
pub struct PoolHealth {
    pub pool: String,
    pub state: PoolState,
    pub devices: Vec<DeviceStatus>,
    pub scrub: ScrubInfo,
}

impl PoolHealth {
    /// Суммарные ошибки по всем устройствам (вкл. строку пула).
    pub fn total_errors(&self) -> (u64, u64, u64) {
        let mut r = 0;
        let mut w = 0;
        let mut c = 0;
        for d in &self.devices {
            r += d.read_errors;
            w += d.write_errors;
            c += d.cksum_errors;
        }
        (r, w, c)
    }

    /// Критерий здоровья как в Go IsPoolHealthy: ONLINE и нулевые ошибки везде.
    pub fn is_healthy(&self) -> bool {
        if self.state != PoolState::Online {
            return false;
        }
        self.devices.iter().all(|d| {
            d.state == PoolState::Online
                && d.read_errors == 0
                && d.write_errors == 0
                && d.cksum_errors == 0
        })
    }
}

/// Разбор вывода `zpool status -p <pool>`.
pub fn parse_zpool_status(output: &str) -> Option<PoolHealth> {
    let mut pool = String::new();
    let mut state = PoolState::Unknown;
    let mut scrub = ScrubInfo::default();
    let mut devices: Vec<DeviceStatus> = Vec::new();

    let mut in_config = false;
    let mut in_scan = false;
    for line in output.lines() {
        let t = line.trim();
        if let Some(v) = t.strip_prefix("pool:") {
            pool = v.trim().to_string();
            continue;
        }
        if let Some(v) = t.strip_prefix("state:") {
            state = PoolState::parse(v.trim());
            continue;
        }
        if let Some(v) = t.strip_prefix("scan:") {
            in_scan = true;
            scrub.raw = v.trim().to_string();
            if v.contains("scrub in progress") {
                scrub.in_progress = true;
            }
            continue;
        }
        if in_scan && !in_config {
            // многострочный scan: (прогресс на отдельных строках)
            if t.starts_with("config:") {
                in_scan = false;
            } else if !t.is_empty() {
                scrub.raw.push(' ');
                scrub.raw.push_str(t);
                if let Some(p) = t.split_whitespace().find(|w| w.ends_with("%")) {
                    // "20.00%" из "..., 20.00% done, ..."
                    let num = p.trim_end_matches('%').trim_end_matches(',');
                    if let Ok(f) = num.parse::<f64>() {
                        scrub.percent_done = Some(f);
                    }
                }
                continue;
            }
        }
        if t.starts_with("config:") {
            in_config = true;
            in_scan = false;
            continue;
        }
        if t.starts_with("errors:") {
            in_config = false;
            continue;
        }
        if in_config {
            // шапка таблицы или пустая строка
            if t.is_empty() || t.starts_with("NAME") {
                continue;
            }
            // NAME STATE READ WRITE CKSUM [note...]
            let cols: Vec<&str> = t.split_whitespace().collect();
            if cols.len() >= 5 {
                let st = PoolState::parse(cols[1]);
                if st == PoolState::Unknown && cols[1] != "UNKNOWN" {
                    continue; // строка не похожа на устройство
                }
                let p = |s: &str| s.parse::<u64>().unwrap_or(0);
                devices.push(DeviceStatus {
                    name: cols[0].to_string(),
                    state: st,
                    read_errors: p(cols[2]),
                    write_errors: p(cols[3]),
                    cksum_errors: p(cols[4]),
                });
            }
        }
    }

    if pool.is_empty() && devices.is_empty() {
        return None;
    }
    Some(PoolHealth { pool, state, devices, scrub })
}

/// Разбор `zfs get -Hp -o property,value used,available <ds>`:
/// (used_bytes, available_bytes).
pub fn parse_zfs_capacity(output: &str) -> Option<(u64, u64)> {
    let mut used = None;
    let mut avail = None;
    for line in output.lines() {
        let mut it = line.split_whitespace();
        match (it.next(), it.next()) {
            (Some("used"), Some(v)) => used = v.parse::<u64>().ok(),
            (Some("available"), Some(v)) => avail = v.parse::<u64>().ok(),
            _ => {}
        }
    }
    Some((used?, avail?))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEALTHY: &str = "\
  pool: disk01
 state: ONLINE
  scan: scrub repaired 0B in 00:00:01 with 0 errors on Tue Jun 10 12:00:00 2026
config:

\tNAME        STATE     READ WRITE CKSUM
\tdisk01      ONLINE       0     0     0
\t  sda       ONLINE       0     0     0

errors: No known data errors
";

    const DEGRADED: &str = "\
  pool: disk02
 state: DEGRADED
status: One or more devices has experienced an unrecoverable error.
  scan: scrub in progress since Tue Jun 10 12:00:00 2026
\t1288490188 scanned at 104857600/s, 524288000 issued at 52428800/s, 2684354560 total
\t0 repaired, 20.00% done, 00:00:40 to go
config:

\tNAME        STATE     READ WRITE CKSUM
\tdisk02      DEGRADED     0     0     0
\t  sdb       DEGRADED     3     1    12

errors: No known data errors
";

    #[test]
    fn parses_healthy_pool() {
        let h = parse_zpool_status(HEALTHY).unwrap();
        assert_eq!(h.pool, "disk01");
        assert_eq!(h.state, PoolState::Online);
        assert!(h.is_healthy());
        assert_eq!(h.devices.len(), 2);
        assert!(!h.scrub.in_progress);
        assert_eq!(h.total_errors(), (0, 0, 0));
    }

    #[test]
    fn parses_degraded_with_errors_and_scrub() {
        let h = parse_zpool_status(DEGRADED).unwrap();
        assert_eq!(h.state, PoolState::Degraded);
        assert!(!h.is_healthy());
        let (r, w, c) = h.total_errors();
        assert_eq!((r, w, c), (3, 1, 12));
        assert!(h.scrub.in_progress);
        assert_eq!(h.scrub.percent_done, Some(20.0));
        let sdb = h.devices.iter().find(|d| d.name == "sdb").unwrap();
        assert_eq!(sdb.cksum_errors, 12);
    }

    #[test]
    fn parses_capacity() {
        let out = "used\t1099511627776\navailable\t8796093022208\n";
        let (used, avail) = parse_zfs_capacity(out).unwrap();
        assert_eq!(used, 1 << 40);
        assert_eq!(avail, 8 << 40);
    }
}
