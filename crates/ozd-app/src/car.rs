// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! E22 (#123, Badger StreamWriter-дух): bulk-залив/выгрузка CARv1 —
//! массовое наполнение 60 дисков МИМО S3-пути запрос-за-запросом.
//!
//! CARv1: varint-фрейм заголовка (DAG-CBOR {version:1, roots:[...]}),
//! далее фреймы varint(len) | CID | тело. Ключ блока — Kubo-совместимый:
//! `/blocks/` + base32-upper-nopad(multihash) (dshelp.MultihashToDsKey) —
//! импортированное видно Kubo через go-ds-s3 как родное.
//!
//! Импорт: reader-поток → bounded-канал (backpressure) → воркеры put
//! (параллелизм поверх параллельных реплик Pool). Verify: sha2-256
//! multihash сверяется с телом (битый CAR не заражает хранилище).
//! Экспорт: CID восстановим из ключа как CIDv1+raw (codec в blockstore
//! утерян — Kubo сам хранит блоки по multihash, байты/хэши сходятся).

use std::io::{Read, Write};
use std::sync::Arc;

use ozd_domain::{BlockKey, BlockStore, DomainError, DomainResult};

/// «version» в DAG-CBOR — для отсечения CARv2-прагмы.
const CARV2_PRAGMA: &[u8] = &[0xa1, 0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n', 0x02];

const B32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// RFC4648 base32 UPPER без паддинга (go base32.RawStdEncoding) — формат
/// ключей Kubo dshelp.
pub fn base32_nopad(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(5) * 8);
    let mut buf: u64 = 0;
    let mut bits = 0u32;
    for b in data {
        buf = (buf << 8) | *b as u64;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(B32[((buf >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(B32[((buf << (5 - bits)) & 31) as usize] as char);
    }
    out
}

pub fn base32_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let mut buf: u64 = 0;
    let mut bits = 0u32;
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'2'..=b'7' => c - b'2' + 26,
            _ => return None,
        } as u64;
        buf = (buf << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xFF) as u8);
        }
    }
    Some(out)
}

fn read_varint(r: &mut impl Read) -> DomainResult<Option<u64>> {
    let mut x: u64 = 0;
    let mut shift = 0u32;
    let mut first = true;
    loop {
        let mut b = [0u8; 1];
        match r.read_exact(&mut b) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && first => {
                return Ok(None); // чистый конец CAR
            }
            Err(e) => return Err(DomainError::Io(format!("car varint: {e}"))),
        }
        first = false;
        if shift >= 63 {
            return Err(DomainError::Io("car varint overflow".into()));
        }
        x |= ((b[0] & 0x7F) as u64) << shift;
        if b[0] & 0x80 == 0 {
            return Ok(Some(x));
        }
        shift += 7;
    }
}

fn write_varint(w: &mut impl Write, mut x: u64) -> DomainResult<()> {
    loop {
        let b = (x & 0x7F) as u8;
        x >>= 7;
        let byte = if x > 0 { b | 0x80 } else { b };
        w.write_all(&[byte]).map_err(|e| DomainError::Io(e.to_string()))?;
        if x == 0 {
            return Ok(());
        }
    }
}

fn slice_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut x: u64 = 0;
    let mut shift = 0u32;
    while *pos < buf.len() && shift < 63 {
        let b = buf[*pos];
        *pos += 1;
        x |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(x);
        }
        shift += 7;
    }
    None
}

/// Разобрать CID в начале фрейма: (длина CID в байтах, multihash-байты).
/// CIDv0 = голый sha2-256 multihash (0x12 0x20 ...), CIDv1 = ver|codec|mh.
fn parse_cid(buf: &[u8]) -> Option<(usize, &[u8])> {
    if buf.len() >= 34 && buf[0] == 0x12 && buf[1] == 0x20 {
        return Some((34, &buf[..34])); // CIDv0
    }
    let mut pos = 0usize;
    let ver = slice_varint(buf, &mut pos)?;
    if ver != 1 {
        return None;
    }
    let _codec = slice_varint(buf, &mut pos)?;
    let mh_start = pos;
    let _code = slice_varint(buf, &mut pos)?;
    let dlen = slice_varint(buf, &mut pos)? as usize;
    let end = pos.checked_add(dlen)?;
    if end > buf.len() {
        return None;
    }
    Some((end, &buf[mh_start..end]))
}

/// Kubo-ключ блока из multihash (dshelp.MultihashToDsKey + mountpoint).
pub fn key_for_multihash(prefix: &[u8], mh: &[u8]) -> BlockKey {
    let mut k = prefix.to_vec();
    k.extend_from_slice(base32_nopad(mh).as_bytes());
    BlockKey::new(k)
}

#[derive(Debug, Default, Clone)]
pub struct CarImportReport {
    pub blocks: usize,
    pub bytes: u64,
    /// уже были в хранилище (идемпотентный повторный импорт)
    pub skipped: usize,
    /// фреймы с битым CID/не сошёлся sha2-256 — НЕ записаны
    pub corrupt: usize,
    pub errors: usize,
}

#[derive(Debug, Default, Clone)]
pub struct CarExportReport {
    pub blocks: usize,
    pub bytes: u64,
}

/// Импорт CARv1: стрим → воркеры (#123 StreamWriter: bulk мимо S3-пути).
/// `verify` сверяет sha2-256 multihash с телом (битые фреймы отбрасываются).
pub fn car_import(
    store: Arc<dyn BlockStore>,
    mut r: impl Read,
    key_prefix: &[u8],
    parallelism: usize,
    verify: bool,
) -> DomainResult<CarImportReport> {
    // заголовок: varint-длина + DAG-CBOR (роуты нам не нужны — пропуск)
    let hlen = read_varint(&mut r)?
        .ok_or_else(|| DomainError::Io("car: empty file".into()))? as usize;
    if hlen > 1 << 20 {
        return Err(DomainError::Io("car: header too large".into()));
    }
    let mut hdr = vec![0u8; hlen];
    r.read_exact(&mut hdr).map_err(|e| DomainError::Io(format!("car header: {e}")))?;
    if hdr == CARV2_PRAGMA {
        return Err(DomainError::Io("car: CARv2 не поддерживается (ждали CARv1)".into()));
    }

    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
    use std::sync::mpsc;
    let blocks = AtomicUsize::new(0);
    let bytes = AtomicU64::new(0);
    let skipped = AtomicUsize::new(0);
    let corrupt = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    let workers = parallelism.clamp(1, 64);
    // bounded-канал = backpressure: reader не убегает от дисков
    let (tx, rx) = mpsc::sync_channel::<(BlockKey, Vec<u8>)>(workers * 8);
    let rx = std::sync::Mutex::new(rx);

    let read_err: DomainResult<()> = std::thread::scope(|sc| {
        for _ in 0..workers {
            let store = store.clone();
            let rx = &rx;
            let (blocks, bytes, skipped, errors) = (&blocks, &bytes, &skipped, &errors);
            sc.spawn(move || loop {
                let msg = { rx.lock().unwrap().recv() };
                let Ok((key, body)) = msg else { break };
                // идемпотентность: повторный импорт того же CAR — no-op
                if store.has(&key).unwrap_or(false) {
                    skipped.fetch_add(1, Relaxed);
                    continue;
                }
                match store.put(&key, &body) {
                    Ok(()) => {
                        blocks.fetch_add(1, Relaxed);
                        bytes.fetch_add(body.len() as u64, Relaxed);
                    }
                    Err(e) => {
                        tracing::warn!(?key, err = %e, "car import: put failed");
                        errors.fetch_add(1, Relaxed);
                    }
                }
            });
        }

        // reader: последовательный разбор фреймов (формат стримовый)
        loop {
            let Some(flen) = read_varint(&mut r)? else { break };
            if flen == 0 || flen > 1 << 30 {
                return Err(DomainError::Io(format!("car: bad frame len {flen}")));
            }
            let mut frame = vec![0u8; flen as usize];
            r.read_exact(&mut frame)
                .map_err(|e| DomainError::Io(format!("car frame: {e}")))?;
            let Some((cid_len, mh)) = parse_cid(&frame) else {
                corrupt.fetch_add(1, Relaxed);
                continue;
            };
            let body = &frame[cid_len..];
            // verify (#15-дух): sha2-256 multihash обязан сойтись с телом
            if verify && mh.len() >= 2 && mh[0] == 0x12 && mh[1] == 0x20 {
                use sha2::{Digest, Sha256};
                if Sha256::digest(body).as_slice() != &mh[2..34] {
                    tracing::warn!("car import: sha2-256 mismatch — frame dropped");
                    corrupt.fetch_add(1, Relaxed);
                    continue;
                }
            }
            let key = key_for_multihash(key_prefix, mh);
            if tx.send((key, body.to_vec())).is_err() {
                break;
            }
        }
        drop(tx);
        Ok(())
    });
    read_err?;

    Ok(CarImportReport {
        blocks: blocks.into_inner(),
        bytes: bytes.into_inner(),
        skipped: skipped.into_inner(),
        corrupt: corrupt.into_inner(),
        errors: errors.into_inner(),
    })
}

/// Экспорт блоков по префиксу в CARv1. CID = CIDv1 + raw-codec поверх
/// multihash из ключа (codec в blockstore утерян — Kubo тоже хранит блоки
/// по multihash, так что байты и хэши сходятся; roots пустые).
pub fn car_export(
    store: &dyn BlockStore,
    mut w: impl Write,
    key_prefix: &[u8],
) -> DomainResult<CarExportReport> {
    // DAG-CBOR {"roots": [], "version": 1} — канонический порядок ключей
    let header: &[u8] = &[
        0xA2, 0x65, b'r', b'o', b'o', b't', b's', 0x80, 0x67, b'v', b'e', b'r', b's',
        b'i', b'o', b'n', 0x01,
    ];
    write_varint(&mut w, header.len() as u64)?;
    w.write_all(header).map_err(|e| DomainError::Io(e.to_string()))?;

    let mut rep = CarExportReport::default();
    let mut after: Option<BlockKey> = None;
    loop {
        let keys = store.list(key_prefix, after.as_ref(), 1024)?;
        let done = keys.len() < 1024;
        for (key, _) in &keys {
            let body = match store.get(key) {
                Ok(b) => b,
                Err(DomainError::NotFound) => continue, // гонка с delete
                Err(e) => return Err(e),
            };
            let b32 = &key.as_bytes()[key_prefix.len()..];
            let mh = std::str::from_utf8(b32)
                .ok()
                .and_then(base32_decode)
                .ok_or_else(|| {
                    DomainError::Io(format!("car export: ключ не base32-multihash: {key:?}"))
                })?;
            let cid: Vec<u8> = [&[0x01, 0x55][..], &mh].concat(); // CIDv1 | raw | mh
            write_varint(&mut w, (cid.len() + body.len()) as u64)?;
            w.write_all(&cid).map_err(|e| DomainError::Io(e.to_string()))?;
            w.write_all(&body).map_err(|e| DomainError::Io(e.to_string()))?;
            rep.blocks += 1;
            rep.bytes += body.len() as u64;
        }
        after = keys.into_iter().next_back().map(|(k, _)| k);
        if done || after.is_none() {
            break;
        }
    }
    w.flush().map_err(|e| DomainError::Io(e.to_string()))?;
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;

    /// CARv1 c CIDv1(raw, sha2-256) — как делает `ipfs dag export` для raw.
    fn mk_car(blocks: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        let header: &[u8] = &[
            0xA2, 0x65, b'r', b'o', b'o', b't', b's', 0x80, 0x67, b'v', b'e', b'r',
            b's', b'i', b'o', b'n', 0x01,
        ];
        write_varint(&mut out, header.len() as u64).unwrap();
        out.extend_from_slice(header);
        for b in blocks {
            let digest = Sha256::digest(b);
            let mut cid = vec![0x01, 0x55, 0x12, 0x20];
            cid.extend_from_slice(&digest);
            write_varint(&mut out, (cid.len() + b.len()) as u64).unwrap();
            out.extend_from_slice(&cid);
            out.extend_from_slice(b);
        }
        out
    }

    #[derive(Default)]
    struct MemStore(Mutex<BTreeMap<BlockKey, Vec<u8>>>);
    impl BlockStore for MemStore {
        fn put(&self, k: &BlockKey, d: &[u8]) -> DomainResult<()> {
            self.0.lock().insert(k.clone(), d.to_vec());
            Ok(())
        }
        fn get(&self, k: &BlockKey) -> DomainResult<Vec<u8>> {
            self.0.lock().get(k).cloned().ok_or(DomainError::NotFound)
        }
        fn has(&self, k: &BlockKey) -> DomainResult<bool> {
            Ok(self.0.lock().contains_key(k))
        }
        fn delete(&self, k: &BlockKey) -> DomainResult<()> {
            self.0.lock().remove(k);
            Ok(())
        }
        fn list(
            &self,
            p: &[u8],
            a: Option<&BlockKey>,
            l: usize,
        ) -> DomainResult<Vec<(BlockKey, u64)>> {
            Ok(self
                .0
                .lock()
                .iter()
                .filter(|(k, _)| k.as_bytes().starts_with(p))
                .filter(|(k, _)| a.map(|a| k.as_bytes() > a.as_bytes()).unwrap_or(true))
                .take(l)
                .map(|(k, v)| (k.clone(), v.len() as u64))
                .collect())
        }
    }

    #[test]
    fn base32_matches_go_rawstd() {
        // go: base32.RawStdEncoding.EncodeToString([]byte("hello")) = "NBSWY3DP"
        assert_eq!(base32_nopad(b"hello"), "NBSWY3DP");
        assert_eq!(base32_decode("NBSWY3DP").unwrap(), b"hello");
        // sha2-256 multihash начинается с 0x12 0x20 → ключ начинается с "CIQ"
        let mh = [&[0x12u8, 0x20][..], &[0u8; 32]].concat();
        assert!(base32_nopad(&mh).starts_with("CIQ"), "Kubo-стиль CIQ-ключей");
    }

    #[test]
    fn cid_parse_v0_and_v1() {
        let mh = [&[0x12u8, 0x20][..], &[7u8; 32]].concat();
        // v0 — голый multihash
        let frame = [&mh[..], b"body"].concat();
        let (l, got) = parse_cid(&frame).unwrap();
        assert_eq!((l, got), (34, &mh[..]));
        // v1 — ver|codec|mh
        let cid1 = [&[0x01u8, 0x70][..], &mh].concat(); // dag-pb
        let frame = [&cid1[..], b"body"].concat();
        let (l, got) = parse_cid(&frame).unwrap();
        assert_eq!((l, got), (36, &mh[..]));
        assert!(parse_cid(b"\x02junk").is_none(), "неизвестная версия CID");
    }

    #[test]
    fn import_export_roundtrip_idempotent_and_verify() {
        let store = Arc::new(MemStore::default());
        let bodies: Vec<Vec<u8>> =
            (0..5u8).map(|i| vec![i; 1000 + i as usize * 100]).collect();
        let refs: Vec<&[u8]> = bodies.iter().map(|b| b.as_slice()).collect();
        let car = mk_car(&refs);

        let r = car_import(store.clone(), car.as_slice(), b"/blocks/", 4, true).unwrap();
        assert_eq!((r.blocks, r.skipped, r.corrupt, r.errors), (5, 0, 0, 0), "{r:?}");
        // ключи — Kubo-совместимые CIQ…
        for b in &bodies {
            let mh = [&[0x12u8, 0x20][..], Sha256::digest(b).as_slice()].concat();
            let key = key_for_multihash(b"/blocks/", &mh);
            assert!(String::from_utf8_lossy(key.as_bytes()).starts_with("/blocks/CIQ"));
            assert_eq!(store.get(&key).unwrap(), *b);
        }
        // повторный импорт идемпотентен
        let r2 = car_import(store.clone(), car.as_slice(), b"/blocks/", 4, true).unwrap();
        assert_eq!((r2.blocks, r2.skipped), (0, 5), "{r2:?}");
        // экспорт → импорт в пустой стор → те же тела
        let mut out = Vec::new();
        let e = car_export(&*store, &mut out, b"/blocks/").unwrap();
        assert_eq!(e.blocks, 5);
        let store2 = Arc::new(MemStore::default());
        let r3 = car_import(store2.clone(), out.as_slice(), b"/blocks/", 2, true).unwrap();
        assert_eq!(r3.blocks, 5);
        assert_eq!(*store.0.lock(), *store2.0.lock(), "roundtrip бит-в-бит");

        // битый фрейм (подменённое тело) отбрасывается verify
        let mut bad = mk_car(&[b"good block"]);
        let n = bad.len();
        bad[n - 1] ^= 0xFF;
        let store3 = Arc::new(MemStore::default());
        let r4 = car_import(store3.clone(), bad.as_slice(), b"/blocks/", 1, true).unwrap();
        assert_eq!((r4.blocks, r4.corrupt), (0, 1), "битый sha2 не заражает: {r4:?}");
        assert!(store3.0.lock().is_empty());
    }

    #[test]
    fn carv2_pragma_rejected() {
        let mut car = Vec::new();
        write_varint(&mut car, CARV2_PRAGMA.len() as u64).unwrap();
        car.extend_from_slice(CARV2_PRAGMA);
        let store = Arc::new(MemStore::default());
        let err = car_import(store, car.as_slice(), b"/blocks/", 1, true).unwrap_err();
        assert!(err.to_string().contains("CARv2"), "{err}");
    }
}
