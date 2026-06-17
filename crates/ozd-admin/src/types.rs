// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! W19: типизированные response-структуры admin API.
//! Все derive(Serialize) → axum::Json<T> гарантирует валидный JSON.

use serde::Serialize;

#[derive(Serialize)]
pub struct UsageItem {
    pub shard: usize,
    pub total: u64,
    pub free: u64,
}

#[derive(Serialize)]
pub struct GcItem {
    pub shard: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub victim: Option<u32>,
    pub moved: usize,
    pub reclaimed: u64,
    pub orphans: u32,
    pub orphan_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ScrubItem {
    pub shard: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checked: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub corrupt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repaired: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unrepairable: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ResilverResponse {
    pub scanned: usize,
    pub repaired: usize,
    pub errors: usize,
    pub done: bool,
}

#[derive(Serialize)]
pub struct MigrateResponse {
    pub scanned: usize,
    pub migrated: usize,
    pub skipped_small: usize,
    pub skipped_ec: usize,
    pub canary_failed: usize,
    pub errors: usize,
    pub done: bool,
}

#[derive(Serialize)]
pub struct CarImportResponse {
    pub blocks: usize,
    pub bytes: u64,
    pub skipped: usize,
    pub corrupt: usize,
    pub errors: usize,
}

#[derive(Serialize)]
pub struct CarExportResponse {
    pub blocks: usize,
    pub bytes: u64,
}

#[derive(Serialize)]
pub struct CapacityResponse {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub fill_pct: f64,
    pub bytes_written_total: u64,
    pub free_until_95pct: u64,
    pub shards: Vec<CapacityShard>,
}

#[derive(Serialize)]
pub struct CapacityShard {
    pub shard: usize,
    pub total: u64,
    pub free: u64,
    pub fill_pct: f64,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl ErrorResponse {
    pub fn new(e: impl std::fmt::Display) -> Self {
        Self { error: e.to_string() }
    }
}
