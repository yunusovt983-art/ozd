// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! W8.2: SpawnBlockingAdapter — async-обёртка над sync BlockStore.
//! Каждая операция уходит в tokio::task::spawn_blocking, результат
//! await'ится. Pool/CacheTier остаются sync — рефакторинг не нужен.

use std::sync::Arc;

use ozd_domain::{AsyncBlockStore, BlockKey, BlockStore, DomainError, DomainResult};

/// Адаптер: sync `BlockStore` → `AsyncBlockStore` через spawn_blocking.
pub struct SpawnBlockingAdapter {
    inner: Arc<dyn BlockStore>,
}

impl SpawnBlockingAdapter {
    pub fn new(inner: Arc<dyn BlockStore>) -> Self {
        Self { inner }
    }
}

impl AsyncBlockStore for SpawnBlockingAdapter {
    async fn put(&self, key: &BlockKey, data: Vec<u8>) -> DomainResult<()> {
        let store = self.inner.clone();
        let k = key.clone();
        tokio::task::spawn_blocking(move || store.put(&k, &data))
            .await
            .map_err(|e| DomainError::Io(format!("spawn_blocking join: {e}")))?
    }

    async fn get(&self, key: &BlockKey) -> DomainResult<Vec<u8>> {
        let store = self.inner.clone();
        let k = key.clone();
        tokio::task::spawn_blocking(move || store.get(&k))
            .await
            .map_err(|e| DomainError::Io(format!("spawn_blocking join: {e}")))?
    }

    async fn stat(&self, key: &BlockKey) -> DomainResult<u64> {
        let store = self.inner.clone();
        let k = key.clone();
        tokio::task::spawn_blocking(move || store.stat(&k))
            .await
            .map_err(|e| DomainError::Io(format!("spawn_blocking join: {e}")))?
    }

    async fn has(&self, key: &BlockKey) -> DomainResult<bool> {
        let store = self.inner.clone();
        let k = key.clone();
        tokio::task::spawn_blocking(move || store.has(&k))
            .await
            .map_err(|e| DomainError::Io(format!("spawn_blocking join: {e}")))?
    }

    async fn delete(&self, key: &BlockKey) -> DomainResult<()> {
        let store = self.inner.clone();
        let k = key.clone();
        tokio::task::spawn_blocking(move || store.delete(&k))
            .await
            .map_err(|e| DomainError::Io(format!("spawn_blocking join: {e}")))?
    }

    async fn list(
        &self,
        prefix: Vec<u8>,
        after: Option<BlockKey>,
        limit: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>> {
        let store = self.inner.clone();
        tokio::task::spawn_blocking(move || store.list(&prefix, after.as_ref(), limit))
            .await
            .map_err(|e| DomainError::Io(format!("spawn_blocking join: {e}")))?
    }
}
