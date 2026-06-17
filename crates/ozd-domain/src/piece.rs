// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! E20/E21b: самоописанный конверт EC-куска (#139) — общий формат для
//! ozd-app (encode/decode) и ozd-engine (recovery восстанавливает era-бит
//! из хвоста сегмента парсом конверта). Без зависимостей.
//!
//! Формат: "OZEC" (4) | ver u8 | k u8 | m u8 | piece_idx u8
//!         | logical_len u64 LE — 16 байт, затем stripe-байты.

pub const EC_MAGIC: [u8; 4] = *b"OZEC";
pub const EC_VER: u8 = 1;
pub const EC_HEADER_LEN: usize = 16;

/// Заголовок куска (распарсенный).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceHeader {
    pub k: u8,
    pub m: u8,
    pub piece_idx: u8,
    /// логический размер ИСХОДНОГО объекта (не куска)
    pub logical_len: u64,
}

pub fn encode_piece_header(h: &PieceHeader) -> [u8; EC_HEADER_LEN] {
    let mut b = [0u8; EC_HEADER_LEN];
    b[..4].copy_from_slice(&EC_MAGIC);
    b[4] = EC_VER;
    b[5] = h.k;
    b[6] = h.m;
    b[7] = h.piece_idx;
    b[8..16].copy_from_slice(&h.logical_len.to_le_bytes());
    b
}

/// None = это не EC-кусок (сырое тело). Анти-коллизия против случайного
/// magic в пользовательских данных: границы k/m/idx + длина stripe обязана
/// сходиться с ceil(logical/k).
pub fn parse_piece_header(body: &[u8]) -> Option<PieceHeader> {
    if body.len() < EC_HEADER_LEN || body[..4] != EC_MAGIC || body[4] != EC_VER {
        return None;
    }
    let h = PieceHeader {
        k: body[5],
        m: body[6],
        piece_idx: body[7],
        logical_len: u64::from_le_bytes(body[8..16].try_into().ok()?),
    };
    if h.k == 0 || h.m == 0 || h.piece_idx as usize >= (h.k + h.m) as usize {
        return None;
    }
    let stripe = body.len() - EC_HEADER_LEN;
    let want = (h.logical_len as usize).div_ceil(h.k as usize);
    if stripe != want {
        return None;
    }
    Some(h)
}
