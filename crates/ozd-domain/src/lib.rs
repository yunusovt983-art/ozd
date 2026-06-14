//! ozd-domain — ядро домена OpenZFS Daemon (без IO).
//!
//! Демон — слой между IPFS Kubo (через S3-протокол, go-ds-s3) и 60 HDD-дисками
//! (per-disk ZFS-пулы, JBOD-философия ADR-0001: durability через репликацию,
//! а не через RAID/RAIDZ). Дизайн: docs/ARCHITECTURE.md, идеи: docs/Arch_DDD/.
//!
//! Ключ — произвольный datastore-ключ Kubo (например `/blocks/CIQ...`), а не
//! только CID: целостность тел держим per-record CRC32 в сегменте (а не
//! пере-хэшированием CID), как per-micro checksum (#15 OceanBase).

pub mod piece;

use std::fmt;

/// Ключ блока — байтовая строка datastore-ключа Kubo (или S3-object-key).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockKey(pub Vec<u8>);

impl BlockKey {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for BlockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockKey({})", String::from_utf8_lossy(&self.0))
    }
}

impl From<&str> for BlockKey {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

/// Идентификатор шарда (= диска) внутри пула.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ShardId(pub u16);

/// Статус шарда (диск-health; 4-state FSM #142 — Часть 1 упрощённо).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum ShardStatus {
    #[default]
    Online,
    Suspect,
    Faulted,
}

/// Ёмкость/занятость шарда — вес для HRW-by-free (#2).
#[derive(Clone, Copy, Debug, Default)]
pub struct Capacity {
    pub total_bytes: u64,
    pub free_bytes: u64,
}

/// Доменные ошибки.
#[derive(thiserror::Error, Debug)]
pub enum DomainError {
    #[error("block not found")]
    NotFound,
    #[error("shard {0:?} unavailable")]
    ShardUnavailable(ShardId),
    #[error("integrity violation: {0}")]
    IntegrityViolation(String),
    #[error("write quorum not reached: {ok} < {want}")]
    QuorumNotReached { ok: usize, want: usize },
    #[error("io: {0}")]
    Io(String),
}

pub type DomainResult<T> = Result<T, DomainError>;

/// Отчёт одного GC-прохода (#122: выбор жертвы по discard-счётчику).
#[derive(Debug, Default, Clone, Copy)]
pub struct GcReport {
    /// сегмент-жертва (None — кандидата не нашлось / мусора меньше порога)
    pub victim_seg: Option<u32>,
    /// живых записей перенесено в активный сегмент
    pub live_moved: usize,
    /// байт освобождено (размер удалённого файла сегмента)
    pub reclaimed_bytes: u64,
    /// E12: убрано orphan-сегментов (0 ссылок из индекса — «утечки» #134)
    pub orphans_removed: u32,
    /// байт освобождено уборкой orphan'ов
    pub orphan_bytes: u64,
}

/// Шаг deep-scrub одного шарда (#102/#141): партия ключей с курсором.
#[derive(Debug, Default, Clone)]
pub struct ScrubStep {
    pub checked: u64,
    /// байт тел прочитано (E19: бюджет фонового троттлинга #131)
    pub bytes: u64,
    /// ключи с нарушенной целостностью (CRC-mismatch / нечитаемые локально)
    pub corrupt: Vec<BlockKey>,
    pub last_key: Option<BlockKey>,
    pub done: bool,
}

/// Отчёт структурного health-check (порт Go DetectMissingPacks):
/// сверка «индекс ссылается на сегмент ↔ файл сегмента существует»
/// БЕЗ чтения тел — дёшево даже на миллионах ключей.
#[derive(Debug, Default, Clone)]
pub struct StructureReport {
    /// сколько различных сегментов упомянуто в индексе
    pub segments_referenced: usize,
    /// сегменты, на которые ссылается индекс, но файла нет (ПОТЕРЯ — чинить resilver'ом)
    pub segments_missing: Vec<u32>,
    /// сколько ключей указывает на отсутствующие сегменты
    pub keys_at_risk: u64,
    /// файлы сегментов без единой ссылки из индекса (кандидаты GC, не потеря)
    pub orphan_segments: Vec<u32>,
}

impl StructureReport {
    pub fn is_healthy(&self) -> bool {
        self.segments_missing.is_empty()
    }
}

/// Порт data+index-tier одного диска (pack-сегменты + локальный индекс).
/// Реализация — ozd-engine; подключаемый порт (урок Quorum о «замороженном
/// субстрате», PolarVFS-style бэкенды).
pub trait ShardEngine: Send + Sync {
    fn put(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()>;
    fn get(&self, key: &BlockKey) -> DomainResult<Vec<u8>>;
    fn has(&self, key: &BlockKey) -> DomainResult<bool>;
    fn delete(&self, key: &BlockKey) -> DomainResult<()>;
    /// Листинг ключей: от `after` (исключительно), с префиксом, максимум `limit`.
    fn list(
        &self,
        prefix: &[u8],
        after: Option<&BlockKey>,
        limit: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>>;
    fn usage(&self) -> DomainResult<Capacity>;
    fn flush(&self) -> DomainResult<()>;
    /// Один GC-проход (#122): сегмент с максимумом мусора, rewrite живого,
    /// unlink целиком. Дефолт — no-op (для обёрток/моков).
    fn gc(&self, _discard_ratio: f64) -> DomainResult<GcReport> {
        Ok(GcReport::default())
    }
    /// Структурный health-check (индекс ↔ файлы сегментов, без чтения тел).
    fn verify_structure(&self) -> DomainResult<StructureReport> {
        Ok(StructureReport::default())
    }
    /// Шаг deep-scrub (#141): прочитать до `limit` ключей после `after`
    /// с verify CRC; вернуть corrupt-ключи. Дефолт — no-op.
    fn scrub_step(&self, _after: Option<&BlockKey>, _limit: usize) -> DomainResult<ScrubStep> {
        Ok(ScrubStep { done: true, ..Default::default() })
    }
    /// E11: логический размер тела БЕЗ его чтения (HEAD/GetSize дёшев).
    /// Дефолт — через get (для обёрток/моков).
    fn stat(&self, key: &BlockKey) -> DomainResult<u64> {
        self.get(key).map(|d| d.len() as u64)
    }
    /// E17 (#102): персистентный именованный курсор обхода (scrub/resilver)
    /// — рестарт продолжает с места, а не с начала 3,8 млрд ключей.
    fn save_cursor(&self, _name: &str, _pos: Option<&BlockKey>) -> DomainResult<()> {
        Ok(())
    }
    fn load_cursor(&self, _name: &str) -> DomainResult<Option<BlockKey>> {
        Ok(None)
    }
    /// E18 (#127): true = балласт настроен, но сброшен/отсутствует
    /// (диск под давлением места). Дефолт — балласта нет.
    fn ballast_released(&self) -> bool {
        false
    }
    /// E18 (#127): сбросить балласт вручную (ops). true = файл был и удалён.
    fn release_ballast(&self) -> DomainResult<bool> {
        Ok(false)
    }
    /// E21b (era-бит): запись с метаданными объекта — obj_logical = логический
    /// размер ОБЪЕКТА, чьим EC-куском является тело (None = обычное тело).
    /// Дефолт — игнор метаданных (обёртки/моки).
    fn put_meta(
        &self,
        key: &BlockKey,
        data: &[u8],
        _obj_logical: Option<u64>,
    ) -> DomainResult<()> {
        self.put(key, data)
    }
    /// E21b: (размер тела, obj_logical если тело — EC-кусок) БЕЗ чтения тела.
    /// Some = era-бит «это кусок» + честный HEAD/ListV2 за O(lookup).
    fn stat_obj(&self, key: &BlockKey) -> DomainResult<(u64, Option<u64>)> {
        Ok((self.stat(key)?, None))
    }
    /// Полировка E21b: проставить era-бит легаси-куску, правя ТОЛЬКО
    /// индекс-строку (тело не перезаписывается). true = строка обновлена.
    fn set_obj_logical(&self, _key: &BlockKey, _obj: u64) -> DomainResult<bool> {
        Ok(false)
    }
    /// E25 (#143): суммарный размер данных на диске (файлы сегментов) —
    /// бюджет кэш-эвикции. Дефолт 0 (не-кэш движки не считают).
    fn data_bytes(&self) -> DomainResult<u64> {
        Ok(0)
    }
    /// E25 (#143): FIFO-эвикция СТАРЕЙШЕГО запечатанного сегмента целиком
    /// (Kafka/whole-file retention #92/#110 — без LRU-учёта и write-amp).
    /// Возврат: (байт освобождено, ключей убрано); (0,_) = эвиктить нечего.
    fn evict_oldest_segment(&self) -> DomainResult<(u64, usize)> {
        Ok((0, 0))
    }
}

/// Порт политики размещения: детерминированный выбор top-R шардов для ключа.
/// Центрального каталога НЕТ (ARCHITECTURE §2.3): расположение вычисляется.
pub trait PlacementPolicy: Send + Sync {
    fn select(&self, key: &BlockKey, topology: &[(ShardId, Capacity, ShardStatus)], rf: usize)
        -> Vec<ShardId>;
}

/// Высокоуровневый blockstore поверх пула шардов (то, что видит S3-шлюз).
pub trait BlockStore: Send + Sync {
    fn put(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()>;
    fn get(&self, key: &BlockKey) -> DomainResult<Vec<u8>>;
    /// E11: размер без чтения тела (HEAD). Дефолт — через get.
    fn stat(&self, key: &BlockKey) -> DomainResult<u64> {
        self.get(key).map(|d| d.len() as u64)
    }
    fn has(&self, key: &BlockKey) -> DomainResult<bool>;
    fn delete(&self, key: &BlockKey) -> DomainResult<()>;
    fn list(
        &self,
        prefix: &[u8],
        after: Option<&BlockKey>,
        limit: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>>;
}
