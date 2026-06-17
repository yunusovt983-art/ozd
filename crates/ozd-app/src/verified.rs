// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! E23 (#79, iroh-blobs): BLAKE3 outboard + verified range reads.
//!
//! Outboard = меркл-дерево BLAKE3 над телом (chunk-группы 16КБ → ~0.4%
//! объёма), считается ОДИН раз на put и хранится отдельным ключом
//! `/ozd/ob3<key>` (обычный объект: реплики/heal/scrub бесплатно).
//! Чтение диапазона верифицируется против write-time root БЕЗ хэширования
//! всего тела — криптографическая целостность сильнее CRC32 и фундамент
//! P2P verified-streaming (отдача bao-слайса с доказательством — Ч3).

use std::io::{Cursor, Read};

use ozd_domain::{BlockKey, DomainError, DomainResult};

/// Неймспейс outboard-записей (вне "/blocks/" — Kubo ListV2 их не видит).
pub const OB_PREFIX: &[u8] = b"/ozd/ob3";

#[derive(Clone, Debug)]
pub struct ObConfig {
    /// считать outboard для тел ≥ порога (мелочи хватает CRC движка)
    pub min_size: usize,
}

impl Default for ObConfig {
    fn default() -> Self {
        Self { min_size: 256 * 1024 }
    }
}

/// Ключ outboard-записи для ключа тела.
pub fn ob_key(key: &BlockKey) -> BlockKey {
    let mut k = OB_PREFIX.to_vec();
    k.extend_from_slice(key.as_bytes());
    BlockKey::new(k)
}

pub fn is_ob_key(key: &BlockKey) -> bool {
    key.as_bytes().starts_with(OB_PREFIX)
}

/// Outboard-запись: root-хэш (32Б) + меркл-байты.
pub fn make_outboard(body: &[u8]) -> Vec<u8> {
    let (outboard, hash) = abao::encode::outboard(body);
    let mut v = Vec::with_capacity(32 + outboard.len());
    v.extend_from_slice(hash.as_bytes());
    v.extend_from_slice(&outboard);
    v
}

/// Верифицированное чтение диапазона [start, start+len): слайс-экстракция
/// по outboard → декод с проверкой цепочки хэшей до root. Любая порча
/// затронутых chunk-групп → IntegrityViolation (байты НЕ отдаются).
pub fn verified_slice(
    body: &[u8],
    ob_record: &[u8],
    start: u64,
    len: u64,
) -> DomainResult<Vec<u8>> {
    if ob_record.len() < 32 {
        return Err(DomainError::IntegrityViolation("ob3: record too short".into()));
    }
    let root: [u8; 32] = ob_record[..32].try_into().unwrap();
    let hash = abao::Hash::from(root);
    let outboard = &ob_record[32..];

    let mut extractor = abao::encode::SliceExtractor::new_outboard(
        Cursor::new(body),
        Cursor::new(outboard),
        start,
        len,
    );
    let mut slice = Vec::new();
    extractor
        .read_to_end(&mut slice)
        .map_err(|e| DomainError::Io(format!("ob3 extract: {e}")))?;

    let mut decoder = abao::decode::SliceDecoder::new(Cursor::new(&slice), &hash, start, len);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| DomainError::IntegrityViolation(format!("ob3 verify: {e}")))?;
    Ok(out)
}

/// Полировка E23: bao-слайс НАРУЖУ (P2P-фундамент) — extracted slice
/// (хэши промежуточных узлов + чанки) + root. Недоверенный клиент
/// верифицирует слайс сам (verify_bao_slice), не доверяя серверу.
/// Сервер перед отдачей прогоняет локальный декод (мусор не уходит).
pub fn bao_slice(
    body: &[u8],
    ob_record: &[u8],
    start: u64,
    len: u64,
) -> DomainResult<(Vec<u8>, [u8; 32])> {
    if ob_record.len() < 32 {
        return Err(DomainError::IntegrityViolation("ob3: record too short".into()));
    }
    let root: [u8; 32] = ob_record[..32].try_into().unwrap();
    let outboard = &ob_record[32..];
    let mut extractor = abao::encode::SliceExtractor::new_outboard(
        Cursor::new(body),
        Cursor::new(outboard),
        start,
        len,
    );
    let mut slice = Vec::new();
    extractor
        .read_to_end(&mut slice)
        .map_err(|e| DomainError::Io(format!("ob3 extract: {e}")))?;
    verify_bao_slice(&slice, &root, start, len)?; // server-side sanity
    Ok((slice, root))
}

/// Клиентская сторона: проверить bao-слайс против root → байты диапазона.
/// (Используется и нашими тестами, и будущим P2P-фетчером.)
pub fn verify_bao_slice(
    slice: &[u8],
    root: &[u8; 32],
    start: u64,
    len: u64,
) -> DomainResult<Vec<u8>> {
    let hash = abao::Hash::from(*root);
    let mut decoder = abao::decode::SliceDecoder::new(Cursor::new(slice), &hash, start, len);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| DomainError::IntegrityViolation(format!("bao slice verify: {e}")))?;
    Ok(out)
}

pub fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outboard_is_small_and_slices_verify() {
        let body: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
        let ob = make_outboard(&body);
        // 16КБ chunk-группы: outboard ~0.4% тела (а не 6% при 1КБ-чанках)
        assert!(ob.len() < body.len() / 100, "outboard {}Б на 1МБ", ob.len());

        for (start, len) in [(0u64, 1000u64), (123_456, 50_000), (900_000, 5_000)] {
            let got = verified_slice(&body, &ob, start, len).unwrap();
            assert_eq!(got, &body[start as usize..(start + len) as usize]);
        }
        // диапазон с выходом за конец — отдаётся доступное
        let tail = verified_slice(&body, &ob, 999_990, 1000).unwrap();
        assert_eq!(tail, &body[999_990..]);
    }

    #[test]
    fn corruption_in_range_is_caught_crc_would_not_be() {
        let body: Vec<u8> = (0..500_000u32).map(|i| (i * 7 % 256) as u8).collect();
        let ob = make_outboard(&body);
        let mut bad = body.clone();
        bad[200_000] ^= 0x01; // одиночный бит-флип в середине
        // диапазон, накрывающий порчу → отказ
        let err = verified_slice(&bad, &ob, 190_000, 20_000).unwrap_err();
        assert!(matches!(err, DomainError::IntegrityViolation(_)), "{err}");
        // диапазон ВНЕ порченой chunk-группы — читается (verify покрывает
        // только затронутые группы — в этом и смысл частичной верификации)
        let ok = verified_slice(&bad, &ob, 0, 10_000).unwrap();
        assert_eq!(ok, &body[..10_000]);
        // подменённый root → отказ на любом диапазоне
        let mut bad_ob = ob.clone();
        bad_ob[0] ^= 0xFF;
        assert!(verified_slice(&body, &bad_ob, 0, 1000).is_err());
    }

    #[test]
    fn bao_slice_roundtrip_and_tamper_detection() {
        let body: Vec<u8> = (0..300_000u32).map(|i| (i * 3 % 256) as u8).collect();
        let ob = make_outboard(&body);
        let (slice, root) = bao_slice(&body, &ob, 50_000, 10_000).unwrap();
        // слайс = ЦЕЛЫЕ накрытые 16КБ-группы + хэш-доказательство
        // (диапазон 10К может зацепить до двух групп → до ~33КБ)
        assert!(slice.len() >= 10_000 && slice.len() < 40_000, "{}", slice.len());
        let got = verify_bao_slice(&slice, &root, 50_000, 10_000).unwrap();
        assert_eq!(got, &body[50_000..60_000]);
        // подмена байта В СЛАЙСЕ → клиент ловит без сервера
        let mut bad = slice.clone();
        let n = bad.len();
        bad[n - 1] ^= 0xFF;
        assert!(verify_bao_slice(&bad, &root, 50_000, 10_000).is_err());
        // чужой root → отказ
        let mut wrong = root;
        wrong[0] ^= 1;
        assert!(verify_bao_slice(&slice, &wrong, 50_000, 10_000).is_err());
    }

    #[test]
    fn ob_key_namespacing() {
        let k = BlockKey::from("/blocks/CIQAAA");
        let okk = ob_key(&k);
        assert_eq!(okk.as_bytes(), b"/ozd/ob3/blocks/CIQAAA");
        assert!(is_ob_key(&okk));
        assert!(!is_ob_key(&k));
        // Kubo ListV2 с префиксом /blocks/ outboard-записей не видит
        assert!(!okk.as_bytes().starts_with(b"/blocks/"));
    }
}
