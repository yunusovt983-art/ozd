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

#[derive(Serialize)]
pub struct ZfsHealthItem {
    pub shard: usize,
    pub pool: String,
    pub state: String,
    pub shard_status: String,
    pub read_errors: u64,
    pub write_errors: u64,
    pub cksum_errors: u64,
    pub scrub_in_progress: bool,
    pub free: u64,
    pub freeing: u64,
    pub effective_free: u64,
    pub fragmentation_pct: u64,
    pub drift: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct StructureItem {
    pub shard: usize,
    pub healthy: bool,
    pub segments: usize,
    pub missing: Vec<u32>,
    pub keys_at_risk: u64,
    pub orphans: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ZfsScrubItem {
    pub shard: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scrub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct BallastItem {
    pub shard: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub released: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct SnapshotResponse {
    pub id: String,
    pub shards: usize,
    pub segments: usize,
    pub bytes: u64,
    pub path: String,
}

#[derive(Serialize)]
pub struct SnapshotListItem {
    pub id: String,
    pub created: String,
    pub segments: usize,
    pub bytes: u64,
}

#[derive(Serialize)]
pub struct SnapshotDeleteResponse {
    pub id: String,
    pub deleted_files: usize,
    pub deleted_dirs: usize,
}
