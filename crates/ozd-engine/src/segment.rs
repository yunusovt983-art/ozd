// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! Pack-сегменты: append-only файлы ≤ segment_max_size (формат TON .pack /
//! geth freezer / Kafka log, идеи #1/#10/#110-112).
//!
//! Формат записи v2 (E10: +logical_len, flags.bit0=zstd):
//!   MAGIC "OZB2" (4) | key_len u16 | flags u16 | stored_len u32
//!   | logical_len u32 | crc32 u32  — заголовок 20 байт, затем key и stored-тело.
//! crc32 — по key+stored (целостность того, что НА ДИСКЕ; декомпрессия после
//! verify). logical_len = размер оригинала (для HEAD/stat без чтения тела, E11).
//! Сегмент самоописан (#139): recovery/scan восстанавливают индекс-строку
//! целиком из заголовка, включая флаг сжатия.
//!
//! Crash-safety: `meta`-файл хранит flush_offset (recovery-point #111);
//! хвост после flush_offset CRC-валидируется, torn tail отбрасывается (#99).

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub const MAGIC: [u8; 4] = *b"OZB2";
pub const HEADER_LEN: usize = 20;

/// flags.bit0: тело сжато zstd
pub const FLAG_ZSTD: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordAddr {
    pub seg_id: u32,
    /// offset НАЧАЛА записи (заголовка) в сегменте
    pub offset: u64,
    /// длина тела НА ДИСКЕ (после сжатия, если было)
    pub stored_len: u32,
    pub key_len: u16,
}

/// E26 (#63): горячий хвост активного сегмента, который НЕ сбрасываем
/// из page cache (свежие блоки часто тут же читаются DAG-обходом).
const KEEP_HOT_TAIL: u64 = 8 * 1024 * 1024;

/// E26 (#63, Redis): посоветовать ядру выбросить страницы write-once
/// данных — большие последовательные записи не вымывают горячие чтения.
/// Linux-only (macOS: no-op); best-effort — ошибки игнорируем.
#[cfg(target_os = "linux")]
pub(crate) fn drop_page_cache(file: &File, offset: u64, len: u64) {
    use std::os::unix::io::AsRawFd;
    unsafe {
        libc::posix_fadvise(
            file.as_raw_fd(),
            offset as libc::off_t,
            len as libc::off_t,
            libc::POSIX_FADV_DONTNEED,
        );
    }
}
#[cfg(not(target_os = "linux"))]
pub(crate) fn drop_page_cache(_file: &File, _offset: u64, _len: u64) {}

pub fn seg_path(dir: &Path, seg_id: u32) -> PathBuf {
    dir.join(format!("seg.{seg_id:08}.dat"))
}

fn meta_path(dir: &Path) -> PathBuf {
    dir.join("seg.meta")
}

/// meta: active_seg_id u32 | flush_offset u64 (LE)
fn write_meta(dir: &Path, seg_id: u32, flush_offset: u64) -> std::io::Result<()> {
    let tmp = dir.join("seg.meta.tmp");
    let mut buf = [0u8; 12];
    buf[..4].copy_from_slice(&seg_id.to_le_bytes());
    buf[4..].copy_from_slice(&flush_offset.to_le_bytes());
    fs::write(&tmp, buf)?;
    fs::rename(&tmp, meta_path(dir))? ; // durable swap (#67)
    Ok(())
}

fn read_meta(dir: &Path) -> Option<(u32, u64)> {
    let buf = fs::read(meta_path(dir)).ok()?;
    if buf.len() != 12 {
        return None;
    }
    let seg_id = u32::from_le_bytes(buf[..4].try_into().unwrap());
    let off = u64::from_le_bytes(buf[4..].try_into().unwrap());
    Some((seg_id, off))
}

fn encode_header(key_len: u16, flags: u16, stored_len: u32, logical_len: u32, crc: u32) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..4].copy_from_slice(&MAGIC);
    h[4..6].copy_from_slice(&key_len.to_le_bytes());
    h[6..8].copy_from_slice(&flags.to_le_bytes());
    h[8..12].copy_from_slice(&stored_len.to_le_bytes());
    h[12..16].copy_from_slice(&logical_len.to_le_bytes());
    h[16..20].copy_from_slice(&crc.to_le_bytes());
    h
}

struct ParsedHeader {
    key_len: usize,
    flags: u16,
    stored_len: usize,
    logical_len: u32,
    crc: u32,
}

fn parse_header(h: &[u8]) -> Option<ParsedHeader> {
    if h.len() < HEADER_LEN || h[..4] != MAGIC {
        return None;
    }
    Some(ParsedHeader {
        key_len: u16::from_le_bytes(h[4..6].try_into().ok()?) as usize,
        flags: u16::from_le_bytes(h[6..8].try_into().ok()?),
        stored_len: u32::from_le_bytes(h[8..12].try_into().ok()?) as usize,
        logical_len: u32::from_le_bytes(h[12..16].try_into().ok()?),
        crc: u32::from_le_bytes(h[16..20].try_into().ok()?),
    })
}

pub struct SegmentWriter {
    dir: PathBuf,
    /// E18 (#128): запасной каталог сегментов (другой диск) — экстренная
    /// ротация туда при отказе primary; failback на следующей ротации.
    failover: Option<PathBuf>,
    /// каталог АКТИВНОГО сегмента (= dir либо failover)
    active_dir: PathBuf,
    pub seg_id: u32,
    file: File,
    pub len: u64,
    pub flush_offset: u64,
    unflushed_items: u32,
    max_size: u64,
    fsync_items: u32,
    pub failed_over: bool,
    /// E26 (#63): сбрасывать page cache write-once данных
    drop_cache: bool,
}

pub struct RecoveredRecord {
    pub key: Vec<u8>,
    pub addr: RecordAddr,
    pub crc: u32,
    pub flags: u16,
    pub logical_len: u32,
    /// stored-байты записи (E21b: recovery парсит конверт куска для era-бита)
    pub stored: Vec<u8>,
}

impl SegmentWriter {
    /// Открыть активный сегмент с recovery: скан хвоста после flush_offset,
    /// возврат валидных записей (для до-вставки в индекс), truncate torn tail.
    /// E18 (#128): meta читается из ОБОИХ каталогов, выигрывает больший seg_id
    /// (id монотонен через failover/failback) — активным становится его каталог.
    pub fn open(
        dir: &Path,
        failover: Option<&Path>,
        max_size: u64,
        fsync_items: u32,
        drop_cache: bool,
    ) -> std::io::Result<(Self, Vec<RecoveredRecord>)> {
        match fs::create_dir_all(dir) {
            Ok(()) => {}
            // primary может быть мёртв уже при старте — failover решит на ротации
            Err(e) if failover.is_some() => {
                tracing::warn!(err = %e, dir = %dir.display(), "primary seg dir unavailable at open");
            }
            Err(e) => return Err(e),
        }
        let pm = read_meta(dir);
        let fm = failover.and_then(read_meta);
        let (active_dir, seg_id, flush_offset) = match (pm, fm) {
            (Some((ps, _)), Some((fs_, fo))) if fs_ > ps => {
                (failover.unwrap().to_path_buf(), fs_, fo)
            }
            (Some((ps, po)), _) => (dir.to_path_buf(), ps, po),
            (None, Some((fs_, fo))) => (failover.unwrap().to_path_buf(), fs_, fo),
            (None, None) => (dir.to_path_buf(), 0, 0),
        };
        let path = seg_path(&active_dir, seg_id);
        let mut file =
            OpenOptions::new().create(true).read(true).append(true).open(&path)?;
        let file_len = file.metadata()?.len();

        let mut recovered = Vec::new();
        let mut pos = flush_offset.min(file_len);
        if pos < file_len {
            let mut rdr = File::open(&path)?;
            rdr.seek(SeekFrom::Start(pos))?;
            let mut buf = Vec::new();
            rdr.read_to_end(&mut buf)?;
            let mut cur = 0usize;
            loop {
                if buf.len() - cur < HEADER_LEN {
                    break;
                }
                let Some(h) = parse_header(&buf[cur..cur + HEADER_LEN]) else { break };
                let total = HEADER_LEN + h.key_len + h.stored_len;
                if buf.len() - cur < total {
                    break; // недописанный хвост
                }
                let key = &buf[cur + HEADER_LEN..cur + HEADER_LEN + h.key_len];
                let data = &buf[cur + HEADER_LEN + h.key_len..cur + total];
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(key);
                hasher.update(data);
                if hasher.finalize() != h.crc {
                    break; // torn record — стоп на первом несовпадении
                }
                recovered.push(RecoveredRecord {
                    stored: data.to_vec(),
                    key: key.to_vec(),
                    addr: RecordAddr {
                        seg_id,
                        offset: pos,
                        stored_len: h.stored_len as u32,
                        key_len: h.key_len as u16,
                    },
                    crc: h.crc,
                    flags: h.flags,
                    logical_len: h.logical_len,
                });
                cur += total;
                pos += total as u64;
            }
            if pos < file_len {
                file.set_len(pos)?;
                file.seek(SeekFrom::End(0))?;
            }
        }

        let len = pos.max(flush_offset);
        let failed_over = active_dir != dir;
        if failed_over {
            tracing::warn!(dir = %active_dir.display(), "active segment on FAILOVER path (#128)");
        }
        Ok((
            Self {
                dir: dir.to_path_buf(),
                failover: failover.map(|p| p.to_path_buf()),
                active_dir,
                seg_id,
                file,
                len,
                flush_offset: len,
                unflushed_items: 0,
                max_size,
                fsync_items,
                failed_over,
                drop_cache,
            },
            recovered,
        ))
    }

    /// Append записи v2: stored-байты (возможно сжатые) + logical_len + flags.
    /// E18 (#128): ошибка записи/ротации при настроенном failover → экстренная
    /// ротация на запасной путь и ОДИН повтор (доступность записи переживает
    /// отказ data-каталога; durability хвоста — через репликацию #111).
    pub fn append_with_flags(
        &mut self,
        key: &[u8],
        stored: &[u8],
        logical_len: u32,
        flags: u16,
    ) -> std::io::Result<(RecordAddr, u32)> {
        if self.len >= self.max_size {
            self.rotate()?; // внутри — fallback на failover
        }
        match self.write_record(key, stored, logical_len, flags) {
            Ok(r) => Ok(r),
            Err(e) if self.failover.is_some() && !self.failed_over => {
                tracing::warn!(err = %e, "append failed — emergency WAL-failover (#128)");
                self.failover_rotate()?;
                self.write_record(key, stored, logical_len, flags)
            }
            Err(e) => Err(e),
        }
    }

    fn write_record(
        &mut self,
        key: &[u8],
        stored: &[u8],
        logical_len: u32,
        flags: u16,
    ) -> std::io::Result<(RecordAddr, u32)> {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key);
        hasher.update(stored);
        let crc = hasher.finalize();

        let header =
            encode_header(key.len() as u16, flags, stored.len() as u32, logical_len, crc);
        let offset = self.len;
        self.file.write_all(&header)?;
        self.file.write_all(key)?;
        self.file.write_all(stored)?;
        self.len += (HEADER_LEN + key.len() + stored.len()) as u64;
        self.unflushed_items += 1;
        if self.unflushed_items >= self.fsync_items {
            self.flush()?;
        }
        Ok((
            RecordAddr {
                seg_id: self.seg_id,
                offset,
                stored_len: stored.len() as u32,
                key_len: key.len() as u16,
            },
            crc,
        ))
    }

    /// Несжатый append (logical == stored).
    pub fn append(&mut self, key: &[u8], data: &[u8]) -> std::io::Result<(RecordAddr, u32)> {
        self.append_with_flags(key, data, data.len() as u32, 0)
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.flush_offset == self.len && self.unflushed_items == 0 {
            return Ok(());
        }
        self.file.sync_data()?;
        self.flush_offset = self.len;
        self.unflushed_items = 0;
        // E26 (#63): засинканный префикс (кроме горячего хвоста) ядру
        // больше не нужен — страницы чистые, выбрасываем инкрементально
        if self.drop_cache && self.flush_offset > KEEP_HOT_TAIL {
            drop_page_cache(&self.file, 0, self.flush_offset - KEEP_HOT_TAIL);
        }
        write_meta(&self.active_dir, self.seg_id, self.flush_offset)
    }

    /// Ротация: ВСЕГДА сначала primary (автоматический failback после починки
    /// диска, как в CRDB #128), затем failover.
    fn rotate(&mut self) -> std::io::Result<()> {
        if let Err(e) = self.flush() {
            if self.failover.is_none() {
                return Err(e);
            }
            // recovery-point не записать — reopen доскандирует хвост по CRC (#99)
            let _ = self.file.sync_data();
            tracing::warn!(err = %e, "flush before rotation failed (degrading dir?)");
        }
        if self.drop_cache {
            drop_page_cache(&self.file, 0, 0); // sealed: write-once целиком
        }
        let next = self.seg_id + 1;
        let primary = (|| -> std::io::Result<File> {
            fs::create_dir_all(&self.dir)?;
            let f = open_active(&seg_path(&self.dir, next))?;
            write_meta(&self.dir, next, 0)?;
            Ok(f)
        })();
        match primary {
            Ok(file) => {
                if self.failed_over {
                    tracing::info!("rotation: failback to primary seg dir (#128)");
                }
                self.install(file, self.dir.clone(), next, false);
                Ok(())
            }
            Err(e) => {
                if self.failover.is_none() {
                    return Err(e);
                }
                tracing::warn!(err = %e, "rotation to primary failed — failover (#128)");
                self.failover_rotate()
            }
        }
    }

    /// Экстренная ротация на запасной путь: хвост текущего сегмента
    /// синкаем best-effort и бросаем (leak-not-corrupt #134, индекс уже
    /// указывает на durable-часть; недописанное отбросит CRC-скан).
    fn failover_rotate(&mut self) -> std::io::Result<()> {
        let Some(fo) = self.failover.clone() else {
            return Err(std::io::Error::other("no failover path configured"));
        };
        let _ = self.file.sync_data();
        if self.drop_cache {
            drop_page_cache(&self.file, 0, 0);
        }
        let next = self.seg_id + 1;
        fs::create_dir_all(&fo)?;
        let file = open_active(&seg_path(&fo, next))?;
        write_meta(&fo, next, 0)?;
        self.install(file, fo, next, true);
        Ok(())
    }

    fn install(&mut self, file: File, dir: PathBuf, seg_id: u32, failed_over: bool) {
        self.file = file;
        self.active_dir = dir;
        self.seg_id = seg_id;
        self.failed_over = failed_over;
        self.len = 0;
        self.flush_offset = 0;
        self.unflushed_items = 0;
    }
}

fn open_active(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).read(true).append(true).open(path)
}

/// Потоковый скан запечатанного сегмента (GC #122 / scrub): валидные записи
/// по порядку; стоп на EOF/CRC-mismatch. Память O(записи).
/// Колбэк: (key, addr, crc, flags, logical_len, stored-байты).
pub fn scan_segment(
    dir: &Path,
    seg_id: u32,
    drop_cache: bool,
    mut f: impl FnMut(&[u8], RecordAddr, u32, u16, u32, &[u8]) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::io::BufReader;
    let file = File::open(seg_path(dir, seg_id))?;
    let mut rdr = BufReader::with_capacity(1 << 20, file);
    let mut offset: u64 = 0;
    loop {
        let mut header = [0u8; HEADER_LEN];
        match rdr.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let Some(h) = parse_header(&header) else { break };
        let mut key = vec![0u8; h.key_len];
        let mut data = vec![0u8; h.stored_len];
        if rdr.read_exact(&mut key).is_err() || rdr.read_exact(&mut data).is_err() {
            break;
        }
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&key);
        hasher.update(&data);
        if hasher.finalize() != h.crc {
            break;
        }
        let addr = RecordAddr {
            seg_id,
            offset,
            stored_len: h.stored_len as u32,
            key_len: h.key_len as u16,
        };
        f(&key, addr, h.crc, h.flags, h.logical_len, &data)?;
        offset += (HEADER_LEN + h.key_len + h.stored_len) as u64;
    }
    // E26 (#63): холодный полный скан (GC/eviction) не должен оставлять
    // за собой сегмент в page cache
    if drop_cache {
        drop_page_cache(rdr.get_ref(), 0, 0);
    }
    Ok(())
}

/// Чтение stored-тела по адресу с проверкой CRC (verify-on-read).
/// Декомпрессия — забота вызывающего (по flags из индекса).
pub fn read_record(dir: &Path, addr: &RecordAddr, expect_crc: u32) -> std::io::Result<Vec<u8>> {
    let f = File::open(seg_path(dir, addr.seg_id))?;
    let total = HEADER_LEN + addr.key_len as usize + addr.stored_len as usize;
    let mut buf = vec![0u8; total];
    f.read_exact_at(&mut buf, addr.offset)?;
    let key = &buf[HEADER_LEN..HEADER_LEN + addr.key_len as usize];
    let data = &buf[HEADER_LEN + addr.key_len as usize..];
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key);
    hasher.update(data);
    if hasher.finalize() != expect_crc {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "segment record crc mismatch (bitrot?)",
        ));
    }
    Ok(data.to_vec())
}
