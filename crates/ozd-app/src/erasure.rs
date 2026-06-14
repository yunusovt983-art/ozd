//! E20 (#138, RustFS erasure-set): K data + M parity вместо зеркала —
//! 1.5× по месту (4+2) против 2× (R=2) при выживании M отказов.
//!
//! Distribution-array = HRW-ранжирование ключа: кусок i едет на диск
//! ранга i (top-R зеркала — префикс top-(K+M) → эры сосуществуют, E21).
//! Кусок самоописан (#139): заголовок несёт k/m/индекс/логический размер —
//! ремонт и чтение не требуют центрального каталога.
//!
//! Формат куска: "OZEC" (4) | ver u8 | k u8 | m u8 | piece_idx u8
//!               | logical_len u64 LE — 16 байт, затем stripe-байты.
//! Все куски одной длины: stripe = ceil(len/k), хвост добит нулями
//! (logical_len отсекает при сборке).

use reed_solomon_erasure::galois_8::ReedSolomon;

use ozd_domain::{DomainError, DomainResult};

pub use ozd_domain::piece::{
    encode_piece_header, parse_piece_header, PieceHeader, EC_HEADER_LEN, EC_MAGIC, EC_VER,
};

#[derive(Clone, Debug)]
pub struct EcConfig {
    /// K — data-кусков
    pub data: usize,
    /// M — parity-кусков (переживаем M отказов)
    pub parity: usize,
    /// тела меньше порога остаются в зеркале R=2 (EC мелочи не окупается)
    pub min_size: usize,
    /// успешных записей кусков для подтверждения put (дефолт K+1:
    /// потеря одного диска сразу после ack не теряет данные)
    pub write_quorum: usize,
}

impl Default for EcConfig {
    fn default() -> Self {
        Self { data: 4, parity: 2, min_size: 64 * 1024, write_quorum: 5 }
    }
}

impl EcConfig {
    pub fn total(&self) -> usize {
        self.data + self.parity
    }
}

fn rs(k: usize, m: usize) -> DomainResult<ReedSolomon> {
    ReedSolomon::new(k, m)
        .map_err(|e| DomainError::Io(format!("reed-solomon init ({k}+{m}): {e}")))
}

/// Разрезать тело на K+M самоописанных кусков (data + parity).
pub fn ec_encode(data: &[u8], cfg: &EcConfig) -> DomainResult<Vec<Vec<u8>>> {
    let k = cfg.data;
    let m = cfg.parity;
    let stripe = data.len().div_ceil(k).max(1);
    // K stripe'ов с нулевым паддингом хвоста + M пустых под parity
    let mut shards: Vec<Vec<u8>> = (0..k)
        .map(|i| {
            let start = (i * stripe).min(data.len());
            let end = ((i + 1) * stripe).min(data.len());
            let mut s = data[start..end].to_vec();
            s.resize(stripe, 0);
            s
        })
        .chain((0..m).map(|_| vec![0u8; stripe]))
        .collect();
    rs(k, m)?
        .encode(&mut shards)
        .map_err(|e| DomainError::Io(format!("rs encode: {e}")))?;

    let logical_len = data.len() as u64;
    Ok(shards
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            let h = PieceHeader { k: k as u8, m: m as u8, piece_idx: i as u8, logical_len };
            let mut piece = Vec::with_capacity(EC_HEADER_LEN + s.len());
            piece.extend_from_slice(&encode_piece_header(&h));
            piece.extend_from_slice(&s);
            piece
        })
        .collect())
}

/// Собрать тело из ≥K кусков (слоты по piece_idx; None = кусок недоступен).
/// Куски — С заголовками (как лежат на дисках).
pub fn ec_decode(slots: Vec<Option<Vec<u8>>>, k: usize, m: usize) -> DomainResult<Vec<u8>> {
    if slots.len() != k + m {
        return Err(DomainError::Io(format!(
            "ec decode: want {} slots, got {}",
            k + m,
            slots.len()
        )));
    }
    let mut logical: Option<u64> = None;
    let mut stripes: Vec<Option<Vec<u8>>> = Vec::with_capacity(k + m);
    for (i, s) in slots.into_iter().enumerate() {
        match s {
            Some(p) => {
                let h = parse_piece_header(&p).ok_or_else(|| {
                    DomainError::IntegrityViolation(format!("piece {i}: bad EC header"))
                })?;
                if h.piece_idx as usize != i {
                    return Err(DomainError::IntegrityViolation(format!(
                        "piece slot {i} holds idx {}",
                        h.piece_idx
                    )));
                }
                logical = Some(h.logical_len);
                stripes.push(Some(p[EC_HEADER_LEN..].to_vec()));
            }
            None => stripes.push(None),
        }
    }
    let logical = logical.ok_or(DomainError::NotFound)? as usize;
    let have = stripes.iter().filter(|s| s.is_some()).count();
    if have < k {
        return Err(DomainError::Io(format!("ec decode: only {have} of {k} pieces")));
    }
    // быстрый путь: все K data-кусков на месте — конкатенация без RS-математики
    if stripes[..k].iter().all(|s| s.is_some()) {
        let mut out = Vec::with_capacity(logical);
        for s in stripes[..k].iter().flatten() {
            out.extend_from_slice(s);
        }
        out.truncate(logical);
        return Ok(out);
    }
    rs(k, m)?
        .reconstruct(&mut stripes)
        .map_err(|e| DomainError::Io(format!("rs reconstruct: {e}")))?;
    let mut out = Vec::with_capacity(logical);
    for s in stripes[..k].iter().flatten() {
        out.extend_from_slice(s);
    }
    out.truncate(logical);
    Ok(out)
}

/// Восстановить ВСЕ недостающие куски (для repair: дописать на диски).
/// Возврат — полный набор K+M кусков с заголовками.
pub fn ec_repair_pieces(
    slots: Vec<Option<Vec<u8>>>,
    k: usize,
    m: usize,
) -> DomainResult<Vec<Vec<u8>>> {
    let body = ec_decode_keep(&slots, k, m)?;
    ec_encode(&body, &EcConfig { data: k, parity: m, ..Default::default() })
}

fn ec_decode_keep(slots: &[Option<Vec<u8>>], k: usize, m: usize) -> DomainResult<Vec<u8>> {
    ec_decode(slots.to_vec(), k, m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(k: usize, m: usize) -> EcConfig {
        EcConfig { data: k, parity: m, ..Default::default() }
    }

    #[test]
    fn roundtrip_all_pieces_fast_path() {
        let data: Vec<u8> = (0..300_001u32).map(|i| (i % 251) as u8).collect();
        let pieces = ec_encode(&data, &cfg(4, 2)).unwrap();
        assert_eq!(pieces.len(), 6);
        // все куски одной длины: ceil(300001/4)+16
        let want = 300_001usize.div_ceil(4) + EC_HEADER_LEN;
        assert!(pieces.iter().all(|p| p.len() == want));
        let out = ec_decode(pieces.into_iter().map(Some).collect(), 4, 2).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn reconstructs_from_any_k_of_km() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i * 7 % 256) as u8).collect();
        let pieces = ec_encode(&data, &cfg(4, 2)).unwrap();
        // худший случай: потеряны 2 data-куска — собираем из 2 data + 2 parity
        let mut slots: Vec<Option<Vec<u8>>> = pieces.iter().cloned().map(Some).collect();
        slots[0] = None;
        slots[2] = None;
        assert_eq!(ec_decode(slots, 4, 2).unwrap(), data);
        // потеря M+1 = невосстановимо
        let mut slots: Vec<Option<Vec<u8>>> = pieces.into_iter().map(Some).collect();
        slots[0] = None;
        slots[1] = None;
        slots[4] = None;
        assert!(ec_decode(slots, 4, 2).is_err());
    }

    #[test]
    fn repair_rebuilds_missing_pieces_bit_exact() {
        let data = vec![42u8; 50_000];
        let pieces = ec_encode(&data, &cfg(4, 2)).unwrap();
        let mut slots: Vec<Option<Vec<u8>>> = pieces.iter().cloned().map(Some).collect();
        slots[1] = None;
        slots[5] = None;
        let rebuilt = ec_repair_pieces(slots, 4, 2).unwrap();
        assert_eq!(rebuilt[1], pieces[1], "data-кусок восстановлен бит-в-бит");
        assert_eq!(rebuilt[5], pieces[5], "parity-кусок восстановлен бит-в-бит");
    }

    #[test]
    fn header_rejects_raw_bodies_and_size_mismatch() {
        assert!(parse_piece_header(b"raw user data, not a piece").is_none());
        // даже с magic: длина stripe не сходится с logical/k → не кусок
        let mut fake = encode_piece_header(&PieceHeader {
            k: 4,
            m: 2,
            piece_idx: 0,
            logical_len: 1000,
        })
        .to_vec();
        fake.extend_from_slice(&[0u8; 17]); // ceil(1000/4)=250 ≠ 17
        assert!(parse_piece_header(&fake).is_none());
        // честный кусок парсится
        let pieces = ec_encode(&[7u8; 1000], &cfg(4, 2)).unwrap();
        let h = parse_piece_header(&pieces[3]).unwrap();
        assert_eq!((h.k, h.m, h.piece_idx, h.logical_len), (4, 2, 3, 1000));
    }
}
