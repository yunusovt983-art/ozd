// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! ozd-engine — ShardEngine одного диска: pack-сегменты (data-tier) +
//! redb-индекс (index-tier). Two-tier (#1): тела sequential на HDD,
//! lookup по индексу (в проде — на NVMe, `index_path`).
//!
//! Индекс (неймспейсинг префиксами #7, inline-split #80):
//!   таблица "addr":   key → v2 28Б (seg, off, stored_len, key_len, crc, logical_len, flags) — узкая строка
//!   таблица "inline": key → тело (мелочь < inline_min, минус seek на HDD, #44)
//!   таблица "meta":   служебное (discard-счётчики сегментов #122 — задел GC)

pub mod segment;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use redb::{Database, ReadableTable, TableDefinition};

use ozd_domain::{
    BlockKey, Capacity, DomainError, DomainResult, GcReport, ScrubStep, ShardEngine,
    StructureReport,
};
use segment::{RecordAddr, SegmentWriter};

/// Полная индекс-строка адреса (v2 28Б / v3 36Б): + logical_len, flags,
/// E21b: obj_logical — era-бит «тело = EC-кусок» + логический размер объекта.
#[derive(Debug, Clone, Copy, PartialEq)]
struct AddrEntry {
    addr: RecordAddr,
    crc: u32,
    /// размер ОРИГИНАЛА (до сжатия) — HEAD/stat без чтения тела
    logical_len: u32,
    /// segment::FLAG_ZSTD и т.п.
    flags: u16,
    /// E21b: Some(L) = тело — EC-кусок объекта логического размера L
    /// (честный HEAD/ListV2 для EC за O(lookup), без чтения тела)
    obj_logical: Option<u64>,
}

/// Результат поиска ключа в индексе.
enum Lookup {
    Inline(Vec<u8>),
    Addr(AddrEntry),
    Missing,
}

const T_ADDR: TableDefinition<&[u8], &[u8]> = TableDefinition::new("addr");
const T_INLINE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("inline");
const T_META: TableDefinition<&[u8], u64> = TableDefinition::new("meta");
/// E17: персистентные курсоры обхода (scrub/resilver), name → BlockKey-байты
const T_CURSOR: TableDefinition<&[u8], &[u8]> = TableDefinition::new("cursor");

/// Значение addr-таблицы: v2 28Б LE
/// (seg u32 | off u64 | stored_len u32 | key_len u16 | crc u32 | logical u32 | flags u16),
/// v3 36Б = v2 + obj_logical u64 (длина строки — дискриминатор версии).
#[inline]
fn encode_addr(e: &AddrEntry) -> Vec<u8> {
    let mut v = Vec::with_capacity(36);
    v.extend_from_slice(&e.addr.seg_id.to_le_bytes());
    v.extend_from_slice(&e.addr.offset.to_le_bytes());
    v.extend_from_slice(&e.addr.stored_len.to_le_bytes());
    v.extend_from_slice(&e.addr.key_len.to_le_bytes());
    v.extend_from_slice(&e.crc.to_le_bytes());
    v.extend_from_slice(&e.logical_len.to_le_bytes());
    v.extend_from_slice(&e.flags.to_le_bytes());
    if let Some(obj) = e.obj_logical {
        v.extend_from_slice(&obj.to_le_bytes());
    }
    v
}

#[inline]
fn decode_addr(v: &[u8]) -> Option<AddrEntry> {
    // 22Б = legacy v1 (без сжатия: logical == stored, flags = 0);
    // 28Б = v2; 36Б = v3 (+obj_logical, E21b)
    if v.len() != 22 && v.len() != 28 && v.len() != 36 {
        return None;
    }
    let addr = RecordAddr {
        seg_id: u32::from_le_bytes(v[..4].try_into().ok()?),
        offset: u64::from_le_bytes(v[4..12].try_into().ok()?),
        stored_len: u32::from_le_bytes(v[12..16].try_into().ok()?),
        key_len: u16::from_le_bytes(v[16..18].try_into().ok()?),
    };
    let crc = u32::from_le_bytes(v[18..22].try_into().ok()?);
    let (logical_len, flags) = if v.len() >= 28 {
        (
            u32::from_le_bytes(v[22..26].try_into().ok()?),
            u16::from_le_bytes(v[26..28].try_into().ok()?),
        )
    } else {
        (addr.stored_len, 0)
    };
    let obj_logical = if v.len() == 36 {
        Some(u64::from_le_bytes(v[28..36].try_into().ok()?))
    } else {
        None
    };
    Some(AddrEntry { addr, crc, logical_len, flags, obj_logical })
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Каталог тел (pack-сегменты) — на HDD (ZFS-датасет диска).
    pub data_path: PathBuf,
    /// Каталог индекса (redb) — в проде на NVMe; по умолчанию data_path.
    pub index_path: Option<PathBuf>,
    pub segment_max_size: u64,
    /// Порог inline: тела меньше — прямо в redb (#44; HDD 512КБ по device-профилю).
    pub inline_min: u32,
    /// fsync раз в N записей (durability через репликацию #111, не per-write).
    pub fsync_items: u32,
    /// E10: сжимать тела zstd (CID/ключи — никогда; несжимаемое — как есть)
    pub compress_zstd: bool,
    /// сжимать только тела ≥ порога (мелочь не окупается)
    pub compress_min: u32,
    /// E18 (#127): размер балласт-файла (0 = выключен) — несжимаемый резерв
    /// места; при ENOSPC сбрасывается автоматически (graceful recovery)
    pub ballast_bytes: u64,
    /// E18 (#128): запасной каталог сегментов (другой диск/NVMe) —
    /// экстренная ротация туда при отказе data_path
    pub failover_path: Option<PathBuf>,
    /// E26 (#63): сбрасывать page cache write-once байтов сегментов
    /// (Linux; на macOS no-op). На HDD-шардах — да; на NVMe-кэше — нет.
    pub fadvise_dontneed: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("."),
            index_path: None,
            segment_max_size: 2 * 1024 * 1024 * 1024,
            inline_min: 4096,
            fsync_items: 256,
            compress_zstd: false,
            compress_min: 512,
            ballast_bytes: 0,
            failover_path: None,
            fadvise_dontneed: false,
        }
    }
}

pub struct DiskEngine {
    cfg: EngineConfig,
    db: Database,
    writer: Mutex<SegmentWriter>,
    seg_dir: PathBuf,
    /// E18 (#128): запасной каталог сегментов (failover_path/seg)
    failover_seg_dir: Option<PathBuf>,
    /// E18 (#127): балласт лежит на диске (false = сброшен/не создан)
    ballast_present: std::sync::atomic::AtomicBool,
    /// приблизительный счёт занятого (тел) — для usage
    used_bytes: AtomicU64,
    /// W6.2: счётчик GC-проходов — sweep_orphans запускается раз в N
    gc_pass_count: std::sync::atomic::AtomicU32,
}

fn io_err(e: impl std::fmt::Display) -> DomainError {
    DomainError::Io(e.to_string())
}

impl DiskEngine {
    pub fn open(cfg: EngineConfig) -> DomainResult<Self> {
        let seg_dir = cfg.data_path.join("seg");
        let index_dir = cfg.index_path.clone().unwrap_or_else(|| cfg.data_path.clone());
        std::fs::create_dir_all(&index_dir).map_err(io_err)?;
        let db = Database::create(index_dir.join("index.redb")).map_err(io_err)?;
        // создать таблицы заранее: read-пути (has/get/list) на свежем диске
        // не должны спотыкаться о TableDoesNotExist (важно для resilver)
        {
            let tx = db.begin_write().map_err(io_err)?;
            tx.open_table(T_ADDR).map_err(io_err)?;
            tx.open_table(T_INLINE).map_err(io_err)?;
            tx.open_table(T_META).map_err(io_err)?;
            tx.open_table(T_CURSOR).map_err(io_err)?;
            tx.commit().map_err(io_err)?;
        }

        let failover_seg_dir = cfg.failover_path.as_ref().map(|p| p.join("seg"));
        let (writer, recovered) = SegmentWriter::open(
            &seg_dir,
            failover_seg_dir.as_deref(),
            cfg.segment_max_size,
            cfg.fsync_items,
            cfg.fadvise_dontneed,
        )
        .map_err(io_err)?;
        let ballast_present = ensure_ballast(&cfg);

        // recovery: валидный хвост после flush_offset до-вставляем в индекс
        // (идемпотентно — индекс производный от сегментов, ARCHITECTURE §5)
        if !recovered.is_empty() {
            let tx = db.begin_write().map_err(io_err)?;
            {
                let mut t = tx.open_table(T_ADDR).map_err(io_err)?;
                for r in &recovered {
                    // E21b: era-бит восстановим из самоописанного конверта
                    // куска (#139) — хвост после креша не теряет HEAD/ListV2
                    let obj_logical = {
                        let raw: Option<Vec<u8>> = if r.flags & segment::FLAG_ZSTD != 0 {
                            zstd::bulk::decompress(&r.stored, r.logical_len as usize).ok()
                        } else {
                            Some(r.stored.clone())
                        };
                        raw.as_deref()
                            .and_then(ozd_domain::piece::parse_piece_header)
                            .map(|h| h.logical_len)
                    };
                    let e = AddrEntry {
                        addr: r.addr,
                        crc: r.crc,
                        logical_len: r.logical_len,
                        flags: r.flags,
                        obj_logical,
                    };
                    t.insert(r.key.as_slice(), encode_addr(&e).as_slice())
                        .map_err(io_err)?;
                }
            }
            tx.commit().map_err(io_err)?;
            tracing::info!(count = recovered.len(), "recovered tail records into index");
        }

        let used = writer.len;
        Ok(Self {
            cfg,
            db,
            writer: Mutex::new(writer),
            seg_dir,
            failover_seg_dir,
            ballast_present: std::sync::atomic::AtomicBool::new(ballast_present),
            used_bytes: AtomicU64::new(used),
            gc_pass_count: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// E18 (#128): каталог, где лежит файл сегмента seg_id (primary либо
    /// failover) — чтения/GC прозрачно ходят в оба.
    fn seg_dir_of(&self, seg_id: u32) -> PathBuf {
        if segment::seg_path(&self.seg_dir, seg_id).exists() {
            return self.seg_dir.clone();
        }
        if let Some(f) = &self.failover_seg_dir {
            if segment::seg_path(f, seg_id).exists() {
                return f.clone();
            }
        }
        self.seg_dir.clone()
    }

    /// E18 (#127): сбросить балласт-файл — вернуть зарезервированное место.
    /// true = файл был и удалён.
    fn release_ballast_file(&self) -> bool {
        if self.cfg.ballast_bytes == 0 {
            return false;
        }
        let p = self.cfg.data_path.join(BALLAST_FILE);
        match std::fs::remove_file(&p) {
            Ok(()) => {
                self.ballast_present.store(false, Ordering::Relaxed);
                tracing::error!(
                    bytes = self.cfg.ballast_bytes,
                    "BALLAST RELEASED (#127): диск переполнен — место возвращено, чините GC/удалениями"
                );
                true
            }
            Err(_) => false,
        }
    }

    #[inline]
    fn lookup(&self, key: &BlockKey) -> DomainResult<Lookup> {
        let tx = self.db.begin_read().map_err(io_err)?;
        if let Ok(ti) = tx.open_table(T_INLINE) {
            if let Some(v) = ti.get(key.as_bytes()).map_err(io_err)? {
                return Ok(Lookup::Inline(v.value().to_vec()));
            }
        }
        let ta = tx.open_table(T_ADDR).map_err(io_err)?;
        let Some(v) = ta.get(key.as_bytes()).map_err(io_err)? else {
            return Ok(Lookup::Missing);
        };
        let entry =
            decode_addr(v.value()).ok_or_else(|| DomainError::Io("bad addr entry".into()))?;
        Ok(Lookup::Addr(entry))
    }

    /// Запечатанные сегменты (кроме активного): (seg_id, размер файла).
    /// E18 (#128): сканируются ОБА каталога (primary + failover).
    fn sealed_segments(&self) -> DomainResult<Vec<(u32, u64)>> {
        let active = self.writer.lock().seg_id;
        let mut out = Vec::new();
        let mut dirs: Vec<&Path> = vec![&self.seg_dir];
        if let Some(f) = &self.failover_seg_dir {
            dirs.push(f);
        }
        for dir in dirs {
            let rd = match std::fs::read_dir(dir) {
                Ok(r) => r,
                Err(_) => continue,
            };
            for ent in rd.flatten() {
                let name = ent.file_name();
                let name = name.to_string_lossy();
                if let Some(idstr) =
                    name.strip_prefix("seg.").and_then(|s| s.strip_suffix(".dat"))
                {
                    if let Ok(id) = idstr.parse::<u32>() {
                        if id != active {
                            let size = ent.metadata().map(|m| m.len()).unwrap_or(0);
                            out.push((id, size));
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn discard_of(&self, seg_id: u32) -> DomainResult<u64> {
        let tx = self.db.begin_read().map_err(io_err)?;
        let Ok(t) = tx.open_table(T_META) else { return Ok(0) };
        let k = format!("discard.{seg_id}");
        Ok(t.get(k.as_bytes()).map_err(io_err)?.map(|g| g.value()).unwrap_or(0))
    }

    /// Один GC-проход (#122, чертёж Badger value-log GC):
    /// 1) жертва = запечатанный сегмент с МАКСИМУМОМ discard-байт (O(сегментов));
    /// 2) переписываем только если discard ≥ ratio × size (write-amp ≈ 2× при 0.5);
    /// 3) живость записи = индекс всё ещё указывает на (этот seg, этот offset);
    /// 4) живые re-append в активный сегмент (обычный путь) + CAS-обновление
    ///    индекса; 5) flush (durability новых копий!) → unlink старого файла →
    ///    снять discard-счётчик. Крах в любом месте = утечка, не порча (#134):
    ///    discard-счётчик не убывает → жертва будет выбрана снова.
    pub fn gc_once(&self, discard_ratio: f64) -> DomainResult<GcReport> {
        let mut report = GcReport::default();
        // W6.2: sweep_orphans раз в 5 GC-проходов (не каждый — экономит
        // полный скан addr-таблицы на больших сторах)
        let pass = self.gc_pass_count.fetch_add(1, Ordering::Relaxed);
        let do_sweep = pass % 5 == 0;

        // выбор жертвы: max discard среди запечатанных
        let mut victim: Option<(u32, u64, u64)> = None; // (id, size, discard)
        for (id, size) in self.sealed_segments()? {
            let d = self.discard_of(id)?;
            if d == 0 {
                continue;
            }
            if victim.map(|(_, _, vd)| d > vd).unwrap_or(true) {
                victim = Some((id, size, d));
            }
        }
        let Some((vid, vsize, vdiscard)) = victim else {
            if do_sweep { self.sweep_orphans(&mut report)?; } // E12: уборка и без жертвы
            return Ok(report);
        };
        if vsize > 0 && (vdiscard as f64) < discard_ratio * (vsize as f64) {
            tracing::debug!(seg = vid, discard = vdiscard, size = vsize, "gc: below ratio, skip");
            if do_sweep { self.sweep_orphans(&mut report)?; }
            return Ok(report); // rewrite не окупится (#122 discardRatio)
        }
        report.victim_seg = Some(vid);
        tracing::info!(seg = vid, size = vsize, discard = vdiscard, "gc: rewriting victim");

        // перенос живых записей
        let vdir = self.seg_dir_of(vid); // E18: жертва может лежать в failover
        let mut moved = 0usize;
        let mut orphaned_in_active: Vec<(u32, u64)> = Vec::new(); // (seg, bytes)
        segment::scan_segment(
            &vdir,
            vid,
            self.cfg.fadvise_dontneed,
            |key, old_addr, _crc, flags, logical_len, stored| {
                // быстрая проверка живости вне txn (дешёвый отсев мёртвых)
                let alive = matches!(
                    self.lookup(&BlockKey::new(key.to_vec())),
                    Ok(Lookup::Addr(e))
                        if e.addr.seg_id == old_addr.seg_id && e.addr.offset == old_addr.offset
                );
                if !alive {
                    return Ok(());
                }
                // живое: stored-байты в активный сегмент КАК ЕСТЬ — без
                // декомпрессии/перепаковки (splice-дух #104); flags/logical
                // переносятся (порядок #134: тело → индекс)
                let (new_addr, new_crc) = {
                    let mut w = self.writer.lock();
                    w.append_with_flags(key, stored, logical_len, flags)?
                };
                // CAS в индексе: обновить, только если адрес всё ещё старый
                let cas = (|| -> DomainResult<bool> {
                    let tx = self.db.begin_write().map_err(io_err)?;
                    let mut updated = false;
                    {
                        let mut ta = tx.open_table(T_ADDR).map_err(io_err)?;
                        let cur =
                            ta.get(key).map_err(io_err)?.and_then(|g| decode_addr(g.value()));
                        if let Some(e) = cur {
                            if e.addr.seg_id == old_addr.seg_id
                                && e.addr.offset == old_addr.offset
                            {
                                let ne = AddrEntry {
                                    addr: new_addr,
                                    crc: new_crc,
                                    logical_len,
                                    flags,
                                    obj_logical: e.obj_logical, // E21b: era-бит едет с куском
                                };
                                ta.insert(key, encode_addr(&ne).as_slice()).map_err(io_err)?;
                                updated = true;
                            }
                        }
                    }
                    tx.commit().map_err(io_err)?;
                    Ok(updated)
                })();
                match cas {
                    Ok(true) => moved += 1,
                    Ok(false) => {
                        orphaned_in_active
                            .push((new_addr.seg_id, new_addr.stored_len as u64));
                    }
                    Err(e) => {
                        return Err(std::io::Error::other(e.to_string()));
                    }
                }
                Ok(())
            },
        )
        .map_err(io_err)?;
        // E12: батч-учёт сирот гонок — ОДНА транзакция на весь проход
        if !orphaned_in_active.is_empty() {
            let tx = self.db.begin_write().map_err(io_err)?;
            for (seg, bytes) in &orphaned_in_active {
                Self::bump_in(&tx, *seg, *bytes)?;
            }
            tx.commit().map_err(io_err)?;
        }

        // КРИТИЧЕСКИЙ порядок: сначала durability новых копий, потом unlink
        self.writer.lock().flush().map_err(io_err)?;
        std::fs::remove_file(segment::seg_path(&vdir, vid)).map_err(io_err)?;
        {
            let tx = self.db.begin_write().map_err(io_err)?;
            {
                let mut t = tx.open_table(T_META).map_err(io_err)?;
                let k = format!("discard.{vid}");
                t.remove(k.as_bytes()).map_err(io_err)?;
            }
            tx.commit().map_err(io_err)?;
        }
        self.used_bytes.fetch_sub(vsize.min(self.used_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);

        report.live_moved = moved;
        report.reclaimed_bytes = vsize;
        tracing::info!(seg = vid, moved, reclaimed = vsize, "gc: victim removed");
        if do_sweep { self.sweep_orphans(&mut report)?; } // E12: добить полные «утечки» разом
        Ok(report)
    }

    /// E12: уборка orphan-сегментов — запечатанных файлов БЕЗ единой ссылки
    /// из индекса. Это штатный «уборщик» порядка leak-not-corrupt (#134):
    /// крах GC между move и unlink, полностью вымершие сегменты, чужой мусор.
    /// Безопасность: на sealed-сегмент новые ссылки не появляются никогда
    /// (запись идёт только в активный) → 0 ссылок = можно unlink.
    fn sweep_orphans(&self, report: &mut GcReport) -> DomainResult<()> {
        let refs = self.referenced_segments()?;
        let mut removed_ids: Vec<u32> = Vec::new();
        for (id, size) in self.sealed_segments()? {
            if !refs.contains_key(&id) {
                match std::fs::remove_file(segment::seg_path(&self.seg_dir_of(id), id)) {
                    Ok(()) => {
                        report.orphans_removed += 1;
                        report.orphan_bytes += size;
                        removed_ids.push(id);
                        tracing::info!(seg = id, size, "gc: orphan segment removed");
                    }
                    Err(e) => tracing::warn!(seg = id, err = %e, "orphan unlink failed"),
                }
            }
        }
        if !removed_ids.is_empty() {
            let tx = self.db.begin_write().map_err(io_err)?;
            {
                let mut t = tx.open_table(T_META).map_err(io_err)?;
                for id in &removed_ids {
                    let k = format!("discard.{id}");
                    t.remove(k.as_bytes()).map_err(io_err)?;
                }
            }
            tx.commit().map_err(io_err)?;
        }
        Ok(())
    }

    /// E12: инкремент discard-счётчика ВНУТРИ уже открытой write-txn —
    /// put/delete больше не платят вторую транзакцию за учёт мусора.
    fn bump_in(tx: &redb::WriteTransaction, seg_id: u32, bytes: u64) -> DomainResult<()> {
        let mut t = tx.open_table(T_META).map_err(io_err)?;
        let k = format!("discard.{seg_id}");
        let cur = t.get(k.as_bytes()).map_err(io_err)?.map(|g| g.value()).unwrap_or(0);
        t.insert(k.as_bytes(), cur + bytes).map_err(io_err)?;
        Ok(())
    }

    /// Сегменты, на которые ссылается индекс: seg_id → число ключей
    /// (общая основа verify_structure и orphan-sweep E12).
    fn referenced_segments(&self) -> DomainResult<std::collections::BTreeMap<u32, u64>> {
        let mut refs: std::collections::BTreeMap<u32, u64> = Default::default();
        let tx = self.db.begin_read().map_err(io_err)?;
        let ta = tx.open_table(T_ADDR).map_err(io_err)?;
        for item in ta.iter().map_err(io_err)? {
            let (_, v) = item.map_err(io_err)?;
            if let Some(e) = decode_addr(v.value()) {
                *refs.entry(e.addr.seg_id).or_insert(0) += 1;
            }
        }
        Ok(refs)
    }
}

impl DiskEngine {
    fn put_impl(
        &self,
        key: &BlockKey,
        data: &[u8],
        obj_logical: Option<u64>,
    ) -> DomainResult<()> {
        // Порядок «течь, но не портить» (#134): тело в сегмент → индекс.
        // E21b: EC-куски НИКОГДА не inline (era-бит живёт в addr-строке)
        if obj_logical.is_none() && (data.len() as u32) < self.cfg.inline_min {
            let tx = self.db.begin_write().map_err(io_err)?;
            {
                let mut old_addr: Option<RecordAddr> = None;
                {
                    let mut ti = tx.open_table(T_INLINE).map_err(io_err)?;
                    ti.insert(key.as_bytes(), data).map_err(io_err)?;
                    let mut ta = tx.open_table(T_ADDR).map_err(io_err)?;
                    let removed = ta.remove(key.as_bytes()).map_err(io_err)?;
                    if let Some(old) = removed {
                        old_addr = decode_addr(old.value()).map(|e| e.addr);
                    }
                }
                if let Some(a) = old_addr {
                    Self::bump_in(&tx, a.seg_id, a.stored_len as u64)?; // E12: та же txn
                }
            }
            tx.commit().map_err(io_err)?;
            return Ok(());
        }

        // E10: zstd-сжатие тела (ключи/CID — никогда); несжимаемое — как есть
        let logical_len = data.len() as u32;
        let mut flags: u16 = 0;
        let mut stored_buf: Option<Vec<u8>> = None;
        if self.cfg.compress_zstd && data.len() >= self.cfg.compress_min as usize {
            if let Ok(c) = zstd::bulk::compress(data, 1) {
                if c.len() < data.len() {
                    flags = segment::FLAG_ZSTD;
                    stored_buf = Some(c);
                }
            }
        }
        let stored: &[u8] = stored_buf.as_deref().unwrap_or(data);

        let first = {
            let mut w = self.writer.lock();
            w.append_with_flags(key.as_bytes(), stored, logical_len, flags)
        };
        let (addr, crc) = match first {
            Ok(v) => v,
            // E18 (#127): ENOSPC → сбросить балласт и повторить ОДИН раз
            Err(e) if is_enospc(&e) && self.release_ballast_file() => {
                let mut w = self.writer.lock();
                w.append_with_flags(key.as_bytes(), stored, logical_len, flags)
                    .map_err(io_err)?
            }
            Err(e) => return Err(io_err(e)),
        };
        self.used_bytes.fetch_add(stored.len() as u64, Ordering::Relaxed);

        let entry = AddrEntry { addr, crc, logical_len, flags, obj_logical };
        let tx = self.db.begin_write().map_err(io_err)?;
        {
            let mut old_addr: Option<RecordAddr> = None;
            {
                let mut ta = tx.open_table(T_ADDR).map_err(io_err)?;
                if let Some(old) =
                    ta.insert(key.as_bytes(), encode_addr(&entry).as_slice()).map_err(io_err)?
                {
                    old_addr = decode_addr(old.value()).map(|e| e.addr);
                }
                let mut ti = tx.open_table(T_INLINE).map_err(io_err)?;
                ti.remove(key.as_bytes()).map_err(io_err)?;
            }
            if let Some(a) = old_addr {
                Self::bump_in(&tx, a.seg_id, a.stored_len as u64)?; // E12: та же txn
            }
        }
        tx.commit().map_err(io_err)?;
        Ok(())
    }
}

impl ShardEngine for DiskEngine {
    fn put(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()> {
        self.put_impl(key, data, None)
    }

    /// E21b: запись EC-куска с era-битом (obj_logical в addr-строке v3).
    fn put_meta(
        &self,
        key: &BlockKey,
        data: &[u8],
        obj_logical: Option<u64>,
    ) -> DomainResult<()> {
        self.put_impl(key, data, obj_logical)
    }

    /// E25 (#143): размер данных = запечатанные сегменты + активный хвост.
    fn data_bytes(&self) -> DomainResult<u64> {
        let active = self.writer.lock().len;
        Ok(self.sealed_segments()?.iter().map(|(_, s)| *s).sum::<u64>() + active)
    }

    /// E25 (#143): кэш-эвикция — старейший sealed-сегмент умирает целиком:
    /// индекс-строки, указывающие В НЕГО, снимаются (порядок: индекс →
    /// unlink; гонка чтения видит NotFound = промах кэша, не порчу).
    fn evict_oldest_segment(&self) -> DomainResult<(u64, usize)> {
        let Some((vid, vsize)) =
            self.sealed_segments()?.into_iter().min_by_key(|(id, _)| *id)
        else {
            return Ok((0, 0));
        };
        let vdir = self.seg_dir_of(vid);
        let mut victims: Vec<Vec<u8>> = Vec::new();
        segment::scan_segment(&vdir, vid, self.cfg.fadvise_dontneed, |key, addr, _c, _f, _l, _stored| {
            if let Ok(Lookup::Addr(e)) = self.lookup(&BlockKey::new(key.to_vec())) {
                if e.addr.seg_id == addr.seg_id && e.addr.offset == addr.offset {
                    victims.push(key.to_vec());
                }
            }
            Ok(())
        })
        .map_err(io_err)?;
        let removed = victims.len();
        {
            let tx = self.db.begin_write().map_err(io_err)?;
            {
                let mut ta = tx.open_table(T_ADDR).map_err(io_err)?;
                for k in &victims {
                    ta.remove(k.as_slice()).map_err(io_err)?;
                }
                let mut tm = tx.open_table(T_META).map_err(io_err)?;
                tm.remove(format!("discard.{vid}").as_bytes()).map_err(io_err)?;
            }
            tx.commit().map_err(io_err)?;
        }
        std::fs::remove_file(segment::seg_path(&vdir, vid)).map_err(io_err)?;
        self.used_bytes
            .fetch_sub(vsize.min(self.used_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
        tracing::debug!(seg = vid, freed = vsize, keys = removed, "cache: segment evicted");
        Ok((vsize, removed))
    }

    /// Полировка E21b: era-бит легаси-куску — апгрейд addr-строки v2→v3
    /// на месте (тело НЕ трогаем: ни перезаписи, ни GC-мусора).
    fn set_obj_logical(&self, key: &BlockKey, obj: u64) -> DomainResult<bool> {
        let tx = self.db.begin_write().map_err(io_err)?;
        let updated = {
            let mut ta = tx.open_table(T_ADDR).map_err(io_err)?;
            let cur = ta.get(key.as_bytes()).map_err(io_err)?.and_then(|g| decode_addr(g.value()));
            match cur {
                Some(mut e) if e.obj_logical != Some(obj) => {
                    e.obj_logical = Some(obj);
                    ta.insert(key.as_bytes(), encode_addr(&e).as_slice()).map_err(io_err)?;
                    true
                }
                _ => false,
            }
        };
        tx.commit().map_err(io_err)?;
        Ok(updated)
    }

    /// E21b: (размер тела, obj_logical) из ИНДЕКСА — era-бит за O(lookup).
    fn stat_obj(&self, key: &BlockKey) -> DomainResult<(u64, Option<u64>)> {
        match self.lookup(key)? {
            Lookup::Inline(v) => Ok((v.len() as u64, None)),
            Lookup::Addr(e) => Ok((e.logical_len as u64, e.obj_logical)),
            Lookup::Missing => Err(DomainError::NotFound),
        }
    }

    fn get(&self, key: &BlockKey) -> DomainResult<Vec<u8>> {
        // до 2 попыток: GC мог переместить запись между lookup и read —
        // индекс — источник правды, перечитываем адрес (#106-lite retry)
        let mut prev: Option<AddrEntry> = None;
        for _attempt in 0..2 {
            let entry = match self.lookup(key)? {
                Lookup::Inline(v) => return Ok(v),
                Lookup::Missing => return Err(DomainError::NotFound),
                Lookup::Addr(e) => e,
            };
            if prev == Some(entry) {
                break; // адрес не изменился — ошибка реальная
            }
            match segment::read_record(&self.seg_dir_of(entry.addr.seg_id), &entry.addr, entry.crc) {
                Ok(stored) => {
                    // E10: декомпрессия ПОСЛЕ verify CRC stored-байт
                    if entry.flags & segment::FLAG_ZSTD != 0 {
                        let out = zstd::bulk::decompress(&stored, entry.logical_len as usize)
                            .map_err(|e| {
                                DomainError::IntegrityViolation(format!("zstd: {e}"))
                            })?;
                        if out.len() != entry.logical_len as usize {
                            return Err(DomainError::IntegrityViolation(
                                "zstd: logical length mismatch".into(),
                            ));
                        }
                        return Ok(out);
                    }
                    return Ok(stored);
                }
                Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                    // W12.3: CRC-mismatch → Corrupt (матчится без парсинга строк)
                    return Err(DomainError::Corrupt(format!(
                        "{}: {e}", String::from_utf8_lossy(key.as_bytes())
                    )));
                }
                Err(e) => {
                    prev = Some(entry);
                    tracing::debug!(err = %e, "read failed, re-looking up (gc move?)");
                }
            }
        }
        Err(DomainError::Io("record unreadable after re-lookup".into()))
    }

    fn has(&self, key: &BlockKey) -> DomainResult<bool> {
        let tx = self.db.begin_read().map_err(io_err)?;
        if let Ok(ti) = tx.open_table(T_INLINE) {
            if ti.get(key.as_bytes()).map_err(io_err)?.is_some() {
                return Ok(true);
            }
        }
        let ta = tx.open_table(T_ADDR).map_err(io_err)?;
        Ok(ta.get(key.as_bytes()).map_err(io_err)?.is_some())
    }

    fn delete(&self, key: &BlockKey) -> DomainResult<()> {
        // two-phase delete (#84): тело остаётся в сегменте до GC,
        // удаляется только индекс-строка + discard-счётчик (E12: одна txn).
        let tx = self.db.begin_write().map_err(io_err)?;
        {
            let mut old_addr = None;
            {
                let mut ta = tx.open_table(T_ADDR).map_err(io_err)?;
                if let Some(old) = ta.remove(key.as_bytes()).map_err(io_err)? {
                    old_addr = decode_addr(old.value()).map(|e| e.addr);
                }
                let mut ti = tx.open_table(T_INLINE).map_err(io_err)?;
                ti.remove(key.as_bytes()).map_err(io_err)?;
            }
            if let Some(a) = old_addr {
                Self::bump_in(&tx, a.seg_id, a.stored_len as u64)?;
            }
        }
        tx.commit().map_err(io_err)?;
        Ok(())
    }

    fn list(
        &self,
        prefix: &[u8],
        after: Option<&BlockKey>,
        limit: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>> {
        let tx = self.db.begin_read().map_err(io_err)?;
        let mut out: Vec<(BlockKey, u64)> = Vec::new();

        let start: Vec<u8> = match after {
            Some(k) => {
                let mut s = k.as_bytes().to_vec();
                s.push(0); // строго после
                s
            }
            None => prefix.to_vec(),
        };

        let push = |k: &[u8], size: u64, out: &mut Vec<(BlockKey, u64)>| -> bool {
            if !k.starts_with(prefix) {
                return false;
            }
            out.push((BlockKey::new(k.to_vec()), size));
            out.len() < limit
        };

        // addr-таблица — узкая строка (#80), скан не тащит тела
        let ta = tx.open_table(T_ADDR).map_err(io_err)?;
        for item in ta.range(start.as_slice()..).map_err(io_err)? {
            let (k, v) = item.map_err(io_err)?;
            // листинг отдаёт ЛОГИЧЕСКИЙ размер (E11), для EC-куска —
            // размер ОБЪЕКТА из era-бита (E21b: S3 ListV2 честен и при EC)
            let size = decode_addr(v.value())
                .map(|e| e.obj_logical.unwrap_or(e.logical_len as u64))
                .unwrap_or(0);
            if !push(k.value(), size, &mut out) {
                break;
            }
        }
        if let Ok(ti) = tx.open_table(T_INLINE) {
            for item in ti.range(start.as_slice()..).map_err(io_err)? {
                let (k, v) = item.map_err(io_err)?;
                let size = v.value().len() as u64;
                if !k.value().starts_with(prefix) {
                    break;
                }
                out.push((BlockKey::new(k.value().to_vec()), size));
            }
        }
        out.sort();
        out.dedup_by(|a, b| a.0 == b.0);
        out.truncate(limit);
        Ok(out)
    }

    fn usage(&self) -> DomainResult<Capacity> {
        // free через statvfs данных; TTL-кэш (#137) — на уровне Pool/топологии
        let free = fs_free_bytes(&self.cfg.data_path).unwrap_or(0);
        let total = fs_total_bytes(&self.cfg.data_path).unwrap_or(0);
        Ok(Capacity { total_bytes: total, free_bytes: free })
    }

    fn flush(&self) -> DomainResult<()> {
        self.writer.lock().flush().map_err(io_err)
    }

    fn gc(&self, discard_ratio: f64) -> DomainResult<GcReport> {
        self.gc_once(discard_ratio)
    }

    /// E17 (#102): курсор в redb — переживает рестарт демона.
    fn save_cursor(&self, name: &str, pos: Option<&BlockKey>) -> DomainResult<()> {
        let tx = self.db.begin_write().map_err(io_err)?;
        {
            let mut t = tx.open_table(T_CURSOR).map_err(io_err)?;
            match pos {
                Some(k) => {
                    t.insert(name.as_bytes(), k.as_bytes()).map_err(io_err)?;
                }
                None => {
                    t.remove(name.as_bytes()).map_err(io_err)?;
                }
            }
        }
        tx.commit().map_err(io_err)?;
        Ok(())
    }

    fn load_cursor(&self, name: &str) -> DomainResult<Option<BlockKey>> {
        let tx = self.db.begin_read().map_err(io_err)?;
        let t = tx.open_table(T_CURSOR).map_err(io_err)?;
        Ok(t.get(name.as_bytes())
            .map_err(io_err)?
            .map(|g| BlockKey::new(g.value().to_vec())))
    }

    /// E18 (#127): балласт настроен, но отсутствует = давление места.
    fn ballast_released(&self) -> bool {
        self.cfg.ballast_bytes > 0 && !self.ballast_present.load(Ordering::Relaxed)
    }

    fn release_ballast(&self) -> DomainResult<bool> {
        Ok(self.release_ballast_file())
    }

    /// E11: размер ИЗ ИНДЕКСА — без чтения тела (HEAD/GetSize за O(lookup)).
    fn stat(&self, key: &BlockKey) -> DomainResult<u64> {
        match self.lookup(key)? {
            Lookup::Inline(v) => Ok(v.len() as u64),
            Lookup::Addr(e) => Ok(e.logical_len as u64),
            Lookup::Missing => Err(DomainError::NotFound),
        }
    }

    /// Deep-scrub шаг (#102/#141): партия ключей с курсором, чтение тел
    /// с verify CRC (get уже проверяет). Corrupt = CRC-mismatch ИЛИ тело
    /// нечитаемо локально (файл сегмента пропал) — чинится с реплики (Pool).
    fn scrub_step(&self, after: Option<&BlockKey>, limit: usize) -> DomainResult<ScrubStep> {
        let keys = ShardEngine::list(self, b"", after, limit)?;
        let mut step = ScrubStep { done: keys.len() < limit, ..Default::default() };
        for (key, _) in &keys {
            match ShardEngine::get(self, key) {
                Ok(d) => {
                    step.checked += 1;
                    step.bytes += d.len() as u64; // E19: для bg-троттлинга
                }
                Err(DomainError::NotFound) => {} // гонка с delete — не ошибка
                Err(_) => step.corrupt.push(key.clone()),
            }
        }
        step.last_key = keys.into_iter().next_back().map(|(k, _)| k);
        Ok(step)
    }

    /// Структурный чек (порт Go DetectMissingPacks, GO-MIGRATION P1):
    /// скан УЗКОЙ addr-таблицы (#80 — тела не читаются) → набор seg_id +
    /// счёт ключей; сверка с .dat-файлами на диске. Missing = потеря
    /// (чинит resilver с реплики), orphan = кандидат GC (утечка, не порча).
    fn verify_structure(&self) -> DomainResult<StructureReport> {
        let refs = self.referenced_segments()?;
        let active = self.writer.lock().seg_id;
        let mut on_disk: std::collections::BTreeSet<u32> = Default::default();
        on_disk.insert(active);
        for (id, _) in self.sealed_segments()? {
            on_disk.insert(id);
        }

        let mut rep = StructureReport {
            segments_referenced: refs.len(),
            ..Default::default()
        };
        for (seg, keys) in &refs {
            if !on_disk.contains(seg) {
                rep.segments_missing.push(*seg);
                rep.keys_at_risk += keys;
            }
        }
        for seg in on_disk {
            if seg != active && !refs.contains_key(&seg) {
                rep.orphan_segments.push(seg);
            }
        }
        Ok(rep)
    }
}

const BALLAST_FILE: &str = "ballast.ozd";

fn is_enospc(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(libc::ENOSPC)
}

/// E18 (#127, CRDB ballast): создать/поддержать балласт-файл на data_path.
/// Содержимое ПСЕВДОСЛУЧАЙНОЕ: нули ZFS-lz4 сжал бы в ничто — резерв был бы
/// фиктивным. Не создаём, если свободного < 2× размера (не добивать диск).
/// Возврат: лежит ли балласт на диске.
fn ensure_ballast(cfg: &EngineConfig) -> bool {
    use std::io::Write;
    let p = cfg.data_path.join(BALLAST_FILE);
    if cfg.ballast_bytes == 0 {
        let _ = std::fs::remove_file(&p); // выключили в конфиге — вернуть место
        return false;
    }
    if let Ok(m) = std::fs::metadata(&p) {
        if m.len() == cfg.ballast_bytes {
            return true; // уже на месте
        }
        let _ = std::fs::remove_file(&p); // размер сменили — пересоздать
    }
    if fs_free_bytes(&cfg.data_path).unwrap_or(0) < cfg.ballast_bytes.saturating_mul(2) {
        tracing::warn!("ballast: мало места для создания — пропуск (диск уже под давлением)");
        return false;
    }
    let r = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&p)?;
        let mut x = 0x9E37_79B9_7F4A_7C15u64; // xorshift — несжимаемый поток
        let mut chunk = vec![0u8; 1 << 20];
        let mut left = cfg.ballast_bytes;
        while left > 0 {
            for b in chunk.chunks_exact_mut(8) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                b.copy_from_slice(&x.to_le_bytes());
            }
            let n = left.min(chunk.len() as u64) as usize;
            f.write_all(&chunk[..n])?;
            left -= n as u64;
        }
        f.sync_data()
    })();
    match r {
        Ok(()) => {
            tracing::info!(bytes = cfg.ballast_bytes, "ballast created (#127)");
            true
        }
        Err(e) => {
            tracing::warn!(err = %e, "ballast creation failed");
            let _ = std::fs::remove_file(&p);
            false
        }
    }
}

fn fs_free_bytes(path: &Path) -> Option<u64> {
    statvfs(path).map(|(bsize, _, bavail)| bavail * bsize)
}

fn fs_total_bytes(path: &Path) -> Option<u64> {
    statvfs(path).map(|(bsize, blocks, _)| blocks * bsize)
}

/// (block_size, blocks_total, blocks_avail)
fn statvfs(path: &Path) -> Option<(u64, u64, u64)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut s) };
    if rc != 0 {
        return None;
    }
    Some((s.f_frsize as u64, s.f_blocks as u64, s.f_bavail as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozd_domain::ShardEngine;

    fn engine(dir: &Path) -> DiskEngine {
        DiskEngine::open(EngineConfig {
            data_path: dir.to_path_buf(),
            index_path: None,
            segment_max_size: 1 << 20, // 1МБ — быстрые ротации в тестах
            inline_min: 64,
            fsync_items: 8,
            ..Default::default()
        })
        .unwrap()
    }

    fn engine_zstd(dir: &Path) -> DiskEngine {
        DiskEngine::open(EngineConfig {
            data_path: dir.to_path_buf(),
            index_path: None,
            // крошечный сегмент: сжатые записи ~сотни байт, ротация на каждую
            // запись → у GC есть запечатанные жертвы
            segment_max_size: 256,
            inline_min: 64,
            fsync_items: 8,
            compress_zstd: true,
            compress_min: 256,
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn compressed_roundtrip_stat_and_smaller_file() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine_zstd(tmp.path());
        // хорошо сжимаемое тело 200КБ (повторяющийся паттерн)
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 7) as u8).collect();
        let key = BlockKey::from("/blocks/zc");
        e.put(&key, &data).unwrap();
        e.flush().unwrap();

        assert_eq!(e.get(&key).unwrap(), data, "roundtrip сжатого == оригинал");
        assert_eq!(ShardEngine::stat(&e, &key).unwrap(), 200_000, "stat = ЛОГИЧЕСКИЙ размер");
        // на диске — сжато: активный сегмент сильно меньше 200КБ
        let seg0 = tmp.path().join("seg").join("seg.00000000.dat");
        let on_disk = std::fs::metadata(&seg0).unwrap().len();
        assert!(on_disk < 50_000, "ожидали сжатие, на диске {on_disk} байт");

        // несжимаемое (рандом) — хранится как есть и читается
        let rnd: Vec<u8> = (0..50_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        let k2 = BlockKey::from("/blocks/zr");
        e.put(&k2, &rnd).unwrap();
        assert_eq!(e.get(&k2).unwrap(), rnd);
        assert_eq!(ShardEngine::stat(&e, &k2).unwrap(), 50_000);

        // листинг отдаёт логические размеры
        let listed = ShardEngine::list(&e, b"/blocks/", None, 10).unwrap();
        let zc = listed.iter().find(|(k, _)| k.as_bytes().ends_with(b"zc")).unwrap();
        assert_eq!(zc.1, 200_000);
    }

    #[test]
    fn gc_preserves_compressed_records_verbatim() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine_zstd(tmp.path());
        let data: Vec<u8> = (0..150_000u32).map(|i| (i % 11) as u8).collect();
        for i in 0..8 {
            e.put(&BlockKey::new(format!("/blocks/zg{i}")), &data).unwrap();
        }
        e.flush().unwrap();
        for i in 0..6 {
            e.delete(&BlockKey::new(format!("/blocks/zg{i}"))).unwrap();
        }
        let mut reclaimed = 0;
        for _ in 0..16 {
            let r = e.gc_once(0.5).unwrap();
            if r.victim_seg.is_none() {
                break;
            }
            reclaimed += r.reclaimed_bytes;
        }
        assert!(reclaimed > 0);
        // живые сжатые переехали без перепаковки и читаются с верным stat
        for i in 6..8 {
            let k = BlockKey::new(format!("/blocks/zg{i}"));
            assert_eq!(e.get(&k).unwrap(), data);
            assert_eq!(ShardEngine::stat(&e, &k).unwrap(), 150_000);
        }
    }

    #[test]
    fn gc_sweeps_orphan_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path()); // 1МБ-сегменты
        let data = vec![4u8; 300_000];
        for i in 0..6 {
            e.put(&BlockKey::new(format!("/blocks/or{i}")), &data).unwrap();
        }
        e.flush().unwrap();
        // полностью вымерший сегмент 0: удаляем ВСЕ его ключи… проще все ранние
        for i in 0..4 {
            e.delete(&BlockKey::new(format!("/blocks/or{i}"))).unwrap();
        }
        // + чужой мусорный файл (имитация краха GC между move и unlink, #134)
        std::fs::write(tmp.path().join("seg").join("seg.00000077.dat"), b"junk").unwrap();

        let mut orphans = 0u32;
        let mut reclaimed = 0u64;
        for _ in 0..16 {
            let r = e.gc_once(0.5).unwrap();
            orphans += r.orphans_removed;
            reclaimed += r.reclaimed_bytes + r.orphan_bytes;
            if r.victim_seg.is_none() && r.orphans_removed == 0 {
                break;
            }
        }
        assert!(orphans >= 1, "junk-файл (и/или вымершие сегменты) должны быть убраны");
        assert!(reclaimed > 0);
        assert!(
            !tmp.path().join("seg").join("seg.00000077.dat").exists(),
            "orphan junk должен быть удалён"
        );
        // живые читаются, структура чиста
        for i in 4..6 {
            assert_eq!(e.get(&BlockKey::new(format!("/blocks/or{i}"))).unwrap(), data);
        }
        let sr = e.verify_structure().unwrap();
        assert!(sr.is_healthy());
        assert!(sr.orphan_segments.is_empty(), "{sr:?}");
    }

    #[test]
    fn legacy_addr_v1_decodes() {
        // 22-байтная строка v1 → logical=stored, flags=0 (обратная совместимость)
        let mut v = [0u8; 22];
        v[..4].copy_from_slice(&7u32.to_le_bytes());
        v[4..12].copy_from_slice(&4096u64.to_le_bytes());
        v[12..16].copy_from_slice(&1234u32.to_le_bytes());
        v[16..18].copy_from_slice(&10u16.to_le_bytes());
        v[18..22].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        let e = decode_addr(&v).unwrap();
        assert_eq!(e.addr.seg_id, 7);
        assert_eq!(e.addr.stored_len, 1234);
        assert_eq!(e.logical_len, 1234);
        assert_eq!(e.flags, 0);
        assert_eq!(e.crc, 0xDEADBEEF);
    }

    #[test]
    fn roundtrip_inline_and_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());

        let small = BlockKey::from("/blocks/small");
        e.put(&small, b"tiny").unwrap(); // < inline_min → redb
        assert_eq!(e.get(&small).unwrap(), b"tiny");

        let big_data = vec![0xABu8; 100_000];
        let big = BlockKey::from("/blocks/big");
        e.put(&big, &big_data).unwrap(); // → сегмент
        assert_eq!(e.get(&big).unwrap(), big_data);
        assert!(e.has(&big).unwrap());

        e.delete(&big).unwrap();
        assert!(matches!(e.get(&big), Err(DomainError::NotFound)));
    }

    #[test]
    fn segment_rotation() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        let data = vec![7u8; 300_000];
        for i in 0..8 {
            e.put(&BlockKey::new(format!("/blocks/k{i}")), &data).unwrap();
        }
        e.flush().unwrap();
        // 8×300КБ > 1МБ → было ≥1 ротации; всё читается
        for i in 0..8 {
            assert_eq!(e.get(&BlockKey::new(format!("/blocks/k{i}"))).unwrap(), data);
        }
        let segs = std::fs::read_dir(tmp.path().join("seg"))
            .unwrap()
            .filter(|f| {
                f.as_ref().unwrap().file_name().to_string_lossy().ends_with(".dat")
            })
            .count();
        assert!(segs >= 2, "expected rotation, got {segs} segments");
    }

    #[test]
    fn crash_recovery_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let data = vec![9u8; 10_000];
        {
            let e = engine(tmp.path());
            e.put(&BlockKey::from("/blocks/a"), &data).unwrap();
            e.flush().unwrap(); // recovery-point
            e.put(&BlockKey::from("/blocks/b"), &data).unwrap();
            // НЕ flush — b лишь в page-cache/файле, flush_offset позади
        }
        // симулируем torn tail: дописываем мусор в активный сегмент
        let seg0 = tmp.path().join("seg").join("seg.00000000.dat");
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&seg0).unwrap();
            f.write_all(b"OZB1garbage-without-full-record").unwrap();
        }
        // reopen: recovery должен (1) до-вставить валидный b, (2) срезать мусор
        let e = engine(tmp.path());
        assert_eq!(e.get(&BlockKey::from("/blocks/a")).unwrap(), data);
        assert_eq!(e.get(&BlockKey::from("/blocks/b")).unwrap(), data);
        let len_after = std::fs::metadata(&seg0).unwrap().len();
        // мусор отброшен: файл заканчивается на валидной записи
        let e2_data = vec![1u8; 10_000];
        e.put(&BlockKey::from("/blocks/c"), &e2_data).unwrap();
        assert_eq!(e.get(&BlockKey::from("/blocks/c")).unwrap(), e2_data);
        assert!(len_after > 0);
    }

    fn dat_count(dir: &Path) -> usize {
        std::fs::read_dir(dir.join("seg"))
            .map(|rd| {
                rd.filter(|f| {
                    f.as_ref().unwrap().file_name().to_string_lossy().ends_with(".dat")
                })
                .count()
            })
            .unwrap_or(0)
    }

    #[test]
    fn gc_reclaims_dead_segments_and_keeps_live() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path()); // segment_max = 1МБ
        let data = vec![0x42u8; 200_000];
        // ~10 записей × 200КБ → несколько сегментов
        for i in 0..10 {
            e.put(&BlockKey::new(format!("/blocks/g{i}")), &data).unwrap();
        }
        e.flush().unwrap();
        let segs_before = dat_count(tmp.path());
        assert!(segs_before >= 2);

        // убиваем 70% ключей → в старых сегментах много мусора
        for i in 0..7 {
            e.delete(&BlockKey::new(format!("/blocks/g{i}"))).unwrap();
        }

        // гоняем GC до исчерпания жертв
        let mut reclaimed = 0u64;
        let mut moved = 0usize;
        for _ in 0..16 {
            let r = e.gc_once(0.5).unwrap();
            if r.victim_seg.is_none() {
                break;
            }
            reclaimed += r.reclaimed_bytes;
            moved += r.live_moved;
        }
        assert!(reclaimed > 0, "GC must reclaim space");
        let segs_after = dat_count(tmp.path());
        assert!(segs_after < segs_before, "{segs_after} < {segs_before}");

        // живые читаются (в т.ч. переехавшие), мёртвые — NotFound
        for i in 7..10 {
            assert_eq!(
                e.get(&BlockKey::new(format!("/blocks/g{i}"))).unwrap(),
                data,
                "live key g{i} must survive GC (moved={moved})"
            );
        }
        for i in 0..7 {
            assert!(matches!(
                e.get(&BlockKey::new(format!("/blocks/g{i}"))),
                Err(DomainError::NotFound)
            ));
        }
    }

    #[test]
    fn gc_respects_discard_ratio() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        let data = vec![7u8; 200_000];
        for i in 0..10 {
            e.put(&BlockKey::new(format!("/blocks/r{i}")), &data).unwrap();
        }
        e.flush().unwrap();
        // удаляем ОДИН ключ — мусора заведомо < 50% любого сегмента
        e.delete(&BlockKey::from("/blocks/r0")).unwrap();
        let r = e.gc_once(0.5).unwrap();
        assert!(r.victim_seg.is_none(), "below ratio must not rewrite");
        // а с порогом ~0 — жертва найдётся
        let r2 = e.gc_once(0.001).unwrap();
        assert!(r2.victim_seg.is_some());
    }

    #[test]
    fn gc_overwrite_counts_as_discard() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        let v1 = vec![1u8; 150_000];
        let v2 = vec![2u8; 150_000];
        for i in 0..6 {
            e.put(&BlockKey::new(format!("/blocks/o{i}")), &v1).unwrap();
        }
        // перезапись → старые версии мёртвые (discard растёт без delete)
        for i in 0..6 {
            e.put(&BlockKey::new(format!("/blocks/o{i}")), &v2).unwrap();
        }
        e.flush().unwrap();
        let mut reclaimed = 0u64;
        for _ in 0..16 {
            let r = e.gc_once(0.5).unwrap();
            if r.victim_seg.is_none() {
                break;
            }
            reclaimed += r.reclaimed_bytes;
        }
        assert!(reclaimed > 0);
        for i in 0..6 {
            assert_eq!(e.get(&BlockKey::new(format!("/blocks/o{i}"))).unwrap(), v2);
        }
    }

    #[test]
    fn verify_structure_detects_missing_and_orphan() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        let data = vec![3u8; 300_000];
        for i in 0..8 {
            e.put(&BlockKey::new(format!("/blocks/v{i}")), &data).unwrap();
        }
        e.flush().unwrap();

        // здоровое состояние
        let r0 = e.verify_structure().unwrap();
        assert!(r0.is_healthy());
        assert!(r0.segments_referenced >= 2);
        assert!(r0.orphan_segments.is_empty());

        // missing: «потеряли» запечатанный сегмент 0
        std::fs::remove_file(tmp.path().join("seg").join("seg.00000000.dat")).unwrap();
        // orphan: чужой файл сегмента без ссылок
        std::fs::write(tmp.path().join("seg").join("seg.00000099.dat"), b"junk").unwrap();

        let r = e.verify_structure().unwrap();
        assert!(!r.is_healthy());
        assert_eq!(r.segments_missing, vec![0]);
        assert!(r.keys_at_risk > 0);
        assert_eq!(r.orphan_segments, vec![99]);
    }

    #[test]
    fn list_prefix_pagination() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        for i in 0..5 {
            e.put(&BlockKey::new(format!("/blocks/x{i}")), &vec![0u8; 200]).unwrap();
        }
        e.put(&BlockKey::from("/pins/p1"), &vec![0u8; 200]).unwrap();

        let page1 = e.list(b"/blocks/", None, 3).unwrap();
        assert_eq!(page1.len(), 3);
        let page2 = e.list(b"/blocks/", Some(&page1.last().unwrap().0), 10).unwrap();
        assert_eq!(page2.len(), 2);
        assert!(page2.iter().all(|(k, _)| k.as_bytes().starts_with(b"/blocks/")));
    }

    #[test]
    fn cursor_persists_across_reopen() {
        // E17 (#102): save_cursor → reopen → load_cursor отдаёт то же место;
        // save None снимает курсор (обход завершён)
        let tmp = tempfile::tempdir().unwrap();
        {
            let e = engine(tmp.path());
            assert_eq!(e.load_cursor("scrub").unwrap(), None, "свежий движок — без курсора");
            e.save_cursor("scrub", Some(&BlockKey::from("/blocks/CIQmid"))).unwrap();
        }
        {
            let e = engine(tmp.path());
            assert_eq!(
                e.load_cursor("scrub").unwrap(),
                Some(BlockKey::from("/blocks/CIQmid")),
                "после рестарта курсор на месте"
            );
            // независимые имена не мешают друг другу
            assert_eq!(e.load_cursor("resilver").unwrap(), None);
            e.save_cursor("scrub", None).unwrap(); // обход завершён — снять
        }
        let e = engine(tmp.path());
        assert_eq!(e.load_cursor("scrub").unwrap(), None, "снятый курсор не возвращается");
    }

    #[test]
    fn ballast_lifecycle_create_release_recreate() {
        // E18 (#127): балласт создаётся при open, несжимаемый по содержимому,
        // release возвращает место, reopen пересоздаёт
        let tmp = tempfile::tempdir().unwrap();
        let cfg = EngineConfig {
            data_path: tmp.path().to_path_buf(),
            ballast_bytes: 64 * 1024,
            ..Default::default()
        };
        let bp = tmp.path().join("ballast.ozd");
        {
            let e = DiskEngine::open(cfg.clone()).unwrap();
            assert_eq!(std::fs::metadata(&bp).unwrap().len(), 64 * 1024);
            assert!(!e.ballast_released());
            // анти-lz4: содержимое не нули (иначе ZFS сожмёт резерв в ничто)
            let head = &std::fs::read(&bp).unwrap()[..64];
            assert!(head.iter().any(|b| *b != 0), "балласт должен быть несжимаемым");

            assert!(ShardEngine::release_ballast(&e).unwrap(), "файл был — должен удалиться");
            assert!(!bp.exists());
            assert!(e.ballast_released(), "флаг давления места после сброса");
            assert!(!ShardEngine::release_ballast(&e).unwrap(), "повторный сброс — no-op");
        }
        // рестарт демона: место есть → балласт восстановлен
        let e = DiskEngine::open(cfg).unwrap();
        assert_eq!(std::fs::metadata(&bp).unwrap().len(), 64 * 1024);
        assert!(!e.ballast_released());
        // ballast_bytes=0 в конфиге → файл убирается (вернуть место)
        drop(e);
        let e = DiskEngine::open(EngineConfig {
            data_path: tmp.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap();
        assert!(!bp.exists(), "выключенный балласт удаляется при open");
        assert!(!e.ballast_released(), "не настроен — давления нет");
    }

    #[cfg(unix)]
    #[test]
    fn wal_failover_rotates_to_spare_and_fails_back() {
        use std::os::unix::fs::PermissionsExt;
        // E18 (#128): primary seg-каталог отказал → ротация уходит на запасной
        // путь, чтения работают из обоих, reopen продолжает в failover,
        // после починки primary ротация возвращается (failback)
        let prim = tempfile::tempdir().unwrap();
        let spare = tempfile::tempdir().unwrap();
        let cfg = EngineConfig {
            data_path: prim.path().to_path_buf(),
            failover_path: Some(spare.path().to_path_buf()),
            segment_max_size: 4096, // ротация после каждого тела 5КБ
            inline_min: 64,
            fsync_items: 4,
            ..Default::default()
        };
        let body = |i: u8| vec![i; 5000];
        let prim_seg = prim.path().join("seg");
        let spare_seg = spare.path().join("seg");
        let ro = std::fs::Permissions::from_mode(0o555);
        let rw = std::fs::Permissions::from_mode(0o755);

        {
            let e = DiskEngine::open(cfg.clone()).unwrap();
            e.put(&BlockKey::from("/blocks/f0"), &body(0)).unwrap(); // seg0 primary
            e.flush().unwrap();
            // «отказ» primary: каталог сегментов только-чтение
            std::fs::set_permissions(&prim_seg, ro.clone()).unwrap();
            e.put(&BlockKey::from("/blocks/f1"), &body(1)).unwrap(); // → failover
            e.put(&BlockKey::from("/blocks/f2"), &body(2)).unwrap();
            e.flush().unwrap();
            assert!(
                spare_seg.join("seg.00000001.dat").exists(),
                "сегмент 1 должен уехать в failover"
            );
            // чтения прозрачно из обоих каталогов
            assert_eq!(e.get(&BlockKey::from("/blocks/f0")).unwrap(), body(0));
            assert_eq!(e.get(&BlockKey::from("/blocks/f1")).unwrap(), body(1));
            assert_eq!(e.get(&BlockKey::from("/blocks/f2")).unwrap(), body(2));
            assert!(ShardEngine::verify_structure(&e).unwrap().is_healthy());
        }
        // reopen при всё ещё мёртвом primary: meta с max seg_id = failover
        {
            let e = DiskEngine::open(cfg.clone()).unwrap();
            assert_eq!(e.get(&BlockKey::from("/blocks/f1")).unwrap(), body(1));
            e.put(&BlockKey::from("/blocks/f3"), &body(3)).unwrap();
            assert_eq!(e.get(&BlockKey::from("/blocks/f3")).unwrap(), body(3));
            // primary починили → следующая ротация ОБЯЗАНА вернуться (failback)
            std::fs::set_permissions(&prim_seg, rw.clone()).unwrap();
            e.put(&BlockKey::from("/blocks/f4"), &body(4)).unwrap();
            e.put(&BlockKey::from("/blocks/f5"), &body(5)).unwrap();
            e.flush().unwrap();
            let back = std::fs::read_dir(&prim_seg)
                .unwrap()
                .flatten()
                .filter(|d| {
                    let n = d.file_name();
                    let n = n.to_string_lossy().into_owned();
                    n.starts_with("seg.") && n.ends_with(".dat") && n != "seg.00000000.dat"
                })
                .count();
            assert!(back >= 1, "после починки primary новые сегменты должны вернуться туда");
            for i in 0..=5u8 {
                let k = BlockKey::new(format!("/blocks/f{i}"));
                assert_eq!(e.get(&k).unwrap(), body(i), "ключ f{i} читается после failback");
            }
        }
        std::fs::set_permissions(&prim_seg, rw).ok();
    }

    /// E21b: валидный конверт куска для тестов (логический размер 1000, k=4).
    fn mk_piece(idx: u8, logical: u64) -> Vec<u8> {
        let h = ozd_domain::piece::PieceHeader { k: 4, m: 2, piece_idx: idx, logical_len: logical };
        let stripe = (logical as usize).div_ceil(4);
        let mut p = ozd_domain::piece::encode_piece_header(&h).to_vec();
        p.extend(std::iter::repeat_n(0xABu8, stripe));
        p
    }

    #[test]
    fn era_bit_obj_logical_roundtrip_list_and_gc() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        let kp = BlockKey::from("/blocks/piece");
        let km = BlockKey::from("/blocks/plain");
        e.put_meta(&kp, &mk_piece(0, 1000), Some(1000)).unwrap();
        e.put(&km, &vec![9u8; 500]).unwrap();
        e.flush().unwrap();
        // stat_obj: era-бит за O(lookup)
        let (sz, obj) = e.stat_obj(&kp).unwrap();
        assert_eq!(obj, Some(1000), "era-бит на месте");
        assert_eq!(sz, mk_piece(0, 1000).len() as u64);
        assert_eq!(e.stat_obj(&km).unwrap().1, None, "тело без era-бита");
        // list: для куска — логический размер ОБЪЕКТА
        let listed = ShardEngine::list(&e, b"/blocks/", None, 10).unwrap();
        let lp = listed.iter().find(|(k, _)| k == &kp).unwrap();
        assert_eq!(lp.1, 1000, "ListV2 честен при EC");
        // reopen: v3-строка переживает рестарт
        drop(e);
        let e = engine(tmp.path());
        assert_eq!(e.stat_obj(&kp).unwrap().1, Some(1000));
        // GC: era-бит едет вместе с куском при переносе
        let e2dir = tempfile::tempdir().unwrap();
        let e2 = DiskEngine::open(EngineConfig {
            data_path: e2dir.path().to_path_buf(),
            segment_max_size: 64, // ротация на каждую запись
            inline_min: 16,
            fsync_items: 4,
            ..Default::default()
        })
        .unwrap();
        e2.put_meta(&kp, &mk_piece(1, 1000), Some(1000)).unwrap();
        e2.put(&BlockKey::from("/blocks/junk"), &vec![1u8; 200]).unwrap();
        ShardEngine::delete(&e2, &BlockKey::from("/blocks/junk")).unwrap();
        e2.flush().unwrap();
        e2.gc_once(0.0).unwrap();
        assert_eq!(e2.stat_obj(&kp).unwrap().1, Some(1000), "era-бит пережил GC-перенос");
        assert_eq!(ShardEngine::get(&e2, &kp).unwrap(), mk_piece(1, 1000));
    }

    #[test]
    fn era_bit_rederived_by_tail_recovery() {
        // E21b: креш до flush → recovery восстанавливает era-бит ПАРСОМ
        // самоописанного конверта куска (#139)
        let tmp = tempfile::tempdir().unwrap();
        let kp = BlockKey::from("/blocks/tailpiece");
        {
            let e = engine(tmp.path());
            e.put_meta(&kp, &mk_piece(2, 2000), Some(2000)).unwrap();
            // НЕТ flush: meta flush_offset отстаёт — запись только в хвосте
        }
        let e = engine(tmp.path()); // recovery сканирует хвост
        let (_, obj) = e.stat_obj(&kp).unwrap();
        assert_eq!(obj, Some(2000), "era-бит восстановлен из конверта при recovery");
    }

    #[test]
    fn evict_oldest_segment_fifo_and_data_bytes() {
        // E25: data_bytes считает сегменты; эвикция сносит СТАРЕЙШИЙ
        // целиком — его ключи NotFound, новые живы; повторно — следующий
        let tmp = tempfile::tempdir().unwrap();
        let e = DiskEngine::open(EngineConfig {
            data_path: tmp.path().to_path_buf(),
            segment_max_size: 8 * 1024, // мелкие сегменты — частые ротации
            inline_min: 64,
            fsync_items: 64,
            ..Default::default()
        })
        .unwrap();
        for i in 0..12u8 {
            e.put(&BlockKey::new(format!("/blocks/fi{i:02}")), &vec![i; 4000]).unwrap();
        }
        e.flush().unwrap();
        let total = e.data_bytes().unwrap();
        assert!(total > 40_000, "12×4КБ на диске: {total}");

        let (freed, removed) = e.evict_oldest_segment().unwrap();
        assert!(freed > 0 && removed > 0, "({freed}, {removed})");
        assert!(e.data_bytes().unwrap() < total);
        // старейшие ключи ушли, свежие живы
        assert!(matches!(
            ShardEngine::get(&e, &BlockKey::from("/blocks/fi00")),
            Err(ozd_domain::DomainError::NotFound)
        ));
        assert_eq!(ShardEngine::get(&e, &BlockKey::from("/blocks/fi11")).unwrap(), vec![11u8; 4000]);
        // структура чиста (нет dangling-ссылок на снесённый сегмент)
        assert!(ShardEngine::verify_structure(&e).unwrap().is_healthy());
        // эвикция до упора оставляет только активный сегмент
        loop {
            let (f, _) = e.evict_oldest_segment().unwrap();
            if f == 0 {
                break;
            }
        }
        assert_eq!(ShardEngine::get(&e, &BlockKey::from("/blocks/fi11")).unwrap(), vec![11u8; 4000], "активный не эвиктится");
    }

    #[test]
    fn fadvise_hygiene_changes_no_behavior() {
        // E26 (#63): с включённым DONTNEED все пути (put/flush/rotate/get/
        // gc/evict/recovery) ведут себя байт-в-байт как без него
        let tmp = tempfile::tempdir().unwrap();
        let cfg = EngineConfig {
            data_path: tmp.path().to_path_buf(),
            segment_max_size: 8 * 1024, // частые ротации → sealed-DONTNEED путь
            inline_min: 64,
            fsync_items: 2, // частые flush → инкрементальный путь
            fadvise_dontneed: true,
            ..Default::default()
        };
        {
            let e = DiskEngine::open(cfg.clone()).unwrap();
            for i in 0..10u8 {
                e.put(&BlockKey::new(format!("/blocks/fa{i}")), &vec![i; 4000]).unwrap();
            }
            e.flush().unwrap();
            for i in 0..10u8 {
                let k = BlockKey::new(format!("/blocks/fa{i}"));
                assert_eq!(ShardEngine::get(&e, &k).unwrap(), vec![i; 4000]);
            }
            // GC-скан с drop_cache=true
            ShardEngine::delete(&e, &BlockKey::from("/blocks/fa0")).unwrap();
            e.gc_once(0.0).unwrap();
            // eviction-скан с drop_cache=true
            let _ = e.evict_oldest_segment().unwrap();
            assert_eq!(
                ShardEngine::get(&e, &BlockKey::from("/blocks/fa9")).unwrap(),
                vec![9u8; 4000]
            );
        }
        // reopen + recovery при включённой гигиене
        let e = DiskEngine::open(cfg).unwrap();
        assert_eq!(ShardEngine::get(&e, &BlockKey::from("/blocks/fa9")).unwrap(), vec![9u8; 4000]);
        assert!(ShardEngine::verify_structure(&e).unwrap().is_healthy());
    }
}
