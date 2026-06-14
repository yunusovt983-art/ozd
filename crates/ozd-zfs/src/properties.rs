//! Типизированный Property-слой с Source-трекингом (#148, паттерн go-zfs
//! properties.go): аксессоры Bytes/Percent/Ratio/Bool + источник значения
//! (local/default/inherited) → дрифт-аудит конфигурации 60 пулов.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertySource {
    /// задано явно на этом датасете
    Local,
    /// дефолт ZFS (наш тюнинг НЕ применён!)
    Default,
    /// унаследовано от предка
    Inherited(String),
    /// read-only статистика ("-")
    None,
    Other(String),
}

impl PropertySource {
    pub fn parse(s: &str) -> Self {
        match s {
            "local" => Self::Local,
            "default" => Self::Default,
            "-" => Self::None,
            other => {
                if let Some(from) = other.strip_prefix("inherited from ") {
                    Self::Inherited(from.to_string())
                } else {
                    Self::Other(other.to_string())
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Property {
    pub value: String,
    pub source: PropertySource,
}

/// Свойства одного датасета/пула: имя → {значение, источник}.
#[derive(Debug, Clone, Default)]
pub struct Properties(pub BTreeMap<String, Property>);

impl Properties {
    /// Разбор `zfs get -Hp -o property,value,source <props|all> <ds>`
    /// (3 TAB-колонки на строку).
    pub fn parse(output: &str) -> Self {
        let mut map = BTreeMap::new();
        for line in output.lines() {
            let cols: Vec<&str> = line.split('\t').collect();
            if cols.len() >= 3 {
                map.insert(
                    cols[0].to_string(),
                    Property {
                        value: cols[1].to_string(),
                        source: PropertySource::parse(cols[2]),
                    },
                );
            }
        }
        Self(map)
    }

    pub fn string(&self, prop: &str) -> Option<&str> {
        self.0.get(prop).filter(|p| p.value != "-").map(|p| p.value.as_str())
    }

    /// Байты: с `-p` ZFS отдаёт точные числа; IEC-суффиксы (336M = MiB) —
    /// фолбэк для не-parseable вывода (нормализация как в go-zfs parseSize).
    pub fn bytes(&self, prop: &str) -> Option<u64> {
        parse_size(self.string(prop)?)
    }

    /// "9%" → 9
    pub fn percent(&self, prop: &str) -> Option<u64> {
        self.string(prop)?.trim_end_matches('%').parse().ok()
    }

    /// "1.50x" → 1.5
    pub fn ratio(&self, prop: &str) -> Option<f64> {
        self.string(prop)?.trim_end_matches('x').parse().ok()
    }

    /// "on"/"enabled" → true
    pub fn bool_(&self, prop: &str) -> Option<bool> {
        Some(matches!(self.string(prop)?, "on" | "enabled" | "yes"))
    }

    pub fn source(&self, prop: &str) -> Option<&PropertySource> {
        self.0.get(prop).map(|p| &p.source)
    }
}

pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Ok(v) = s.parse::<u64>() {
        return Some(v); // -p: точное число
    }
    // IEC-фолбэк: "336M", "1.5G", "2T" (ZFS-суффиксы = степени 1024)
    let digits: String =
        s.chars().take_while(|c| c.is_ascii_digit() || *c == '.').collect();
    let suffix = s[digits.len()..].trim().trim_end_matches(['B', 'b', 'i']);
    let mult: u64 = match suffix {
        "" => 1,
        "K" | "k" => 1 << 10,
        "M" | "m" => 1 << 20,
        "G" | "g" => 1 << 30,
        "T" | "t" => 1 << 40,
        "P" | "p" => 1 << 50,
        _ => return None,
    };
    let num: f64 = digits.parse().ok()?;
    Some((num * mult as f64) as u64)
}

/// Эталонный тюнинг датасета ozd (KUBO-INTEGRATION): значение при `-p`.
pub const EXPECTED_TUNING: &[(&str, &str)] = &[
    ("recordsize", "1048576"), // 1M
    ("compression", "lz4"),
    ("atime", "off"),
];

#[derive(Debug, Clone)]
pub struct DriftIssue {
    pub property: String,
    pub expected: String,
    pub actual: String,
    pub source: PropertySource,
}

/// Дрифт-аудит (#148): свойство отличается от эталона ИЛИ не задано локально
/// (source=default/inherited — тюнинг не применён к этому датасету).
pub fn audit_drift(props: &Properties, expected: &[(&str, &str)]) -> Vec<DriftIssue> {
    let mut issues = Vec::new();
    for (name, want) in expected {
        let actual = props.string(name).unwrap_or("<absent>").to_string();
        let source =
            props.source(name).cloned().unwrap_or(PropertySource::Other("absent".into()));
        let value_ok = actual == *want;
        let source_ok = source == PropertySource::Local;
        if !value_ok || !source_ok {
            issues.push(DriftIssue {
                property: name.to_string(),
                expected: want.to_string(),
                actual,
                source,
            });
        }
    }
    issues
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_properties_with_sources() {
        let out = "recordsize\t1048576\tlocal\ncompression\tlz4\tinherited from disk01\n\
                   atime\toff\tdefault\nused\t123456\t-\ncompressratio\t1.50x\t-\n\
                   capacity\t9%\t-\n";
        let p = Properties::parse(out);
        assert_eq!(p.bytes("recordsize"), Some(1048576));
        assert_eq!(p.string("compression"), Some("lz4"));
        assert_eq!(p.bool_("atime"), Some(false));
        assert_eq!(p.ratio("compressratio"), Some(1.5));
        assert_eq!(p.percent("capacity"), Some(9));
        assert_eq!(p.source("recordsize"), Some(&PropertySource::Local));
        assert_eq!(
            p.source("compression"),
            Some(&PropertySource::Inherited("disk01".into()))
        );
    }

    #[test]
    fn iec_fallback() {
        assert_eq!(parse_size("336M"), Some(336 << 20));
        assert_eq!(parse_size("2T"), Some(2u64 << 40));
        assert_eq!(parse_size("123"), Some(123));
        assert_eq!(parse_size("1.5K"), Some(1536));
    }

    #[test]
    fn drift_audit_catches_default_source_and_wrong_value() {
        let out = "recordsize\t131072\tdefault\ncompression\tlz4\tlocal\natime\toff\tlocal\n";
        let p = Properties::parse(out);
        let issues = audit_drift(&p, EXPECTED_TUNING);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].property, "recordsize");
        assert_eq!(issues[0].actual, "131072"); // 128K — тюнинг не применён
        assert_eq!(issues[0].source, PropertySource::Default);

        // всё локально и по эталону → чисто
        let ok = "recordsize\t1048576\tlocal\ncompression\tlz4\tlocal\natime\toff\tlocal\n";
        assert!(audit_drift(&Properties::parse(ok), EXPECTED_TUNING).is_empty());
    }
}
