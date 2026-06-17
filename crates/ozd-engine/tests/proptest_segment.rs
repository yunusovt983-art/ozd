// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! W7.2: Property-тесты DiskEngine — произвольные put → get == put,
//! crash-recovery (torn-tail) корректен, delete идемпотентен.

use proptest::prelude::*;

use ozd_domain::{BlockKey, DomainError, ShardEngine};
use ozd_engine::{DiskEngine, EngineConfig};

fn engine(dir: &std::path::Path) -> DiskEngine {
    DiskEngine::open(EngineConfig {
        data_path: dir.to_path_buf(),
        segment_max_size: 64 * 1024, // маленькие сегменты → частые ротации
        inline_min: 128,
        fsync_items: 8,
        compress_zstd: true,
        compress_min: 64,
        ..Default::default()
    })
    .unwrap()
}

// Ограничиваем число случаев (CI-friendly: < 30с)
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// put(key, data) → get(key) == data для произвольных ключей и тел.
    #[test]
    fn put_get_roundtrip(
        key in "/blocks/[a-z0-9]{4,16}",
        data in proptest::collection::vec(any::<u8>(), 0..50_000),
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        let k = BlockKey::new(key.as_bytes().to_vec());
        e.put(&k, &data).unwrap();
        let got = e.get(&k).unwrap();
        prop_assert_eq!(got, data);
    }

    /// delete идемпотентен: повторный delete не падает, get → NotFound.
    #[test]
    fn delete_idempotent(
        key in "/blocks/[a-z]{3,16}",
        data in proptest::collection::vec(any::<u8>(), 1..5_000),
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        let k = BlockKey::new(key.as_bytes().to_vec());
        e.put(&k, &data).unwrap();
        e.delete(&k).unwrap();
        e.delete(&k).unwrap(); // повторный — идемпотентен
        prop_assert!(matches!(e.get(&k), Err(DomainError::NotFound)));
    }

    /// stat возвращает логический размер (до сжатия).
    #[test]
    fn stat_returns_logical_size(
        key in "/blocks/[a-z]{4,20}",
        data in proptest::collection::vec(any::<u8>(), 0..20_000),
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine(tmp.path());
        let k = BlockKey::new(key.as_bytes().to_vec());
        e.put(&k, &data).unwrap();
        let sz = ShardEngine::stat(&e, &k).unwrap();
        prop_assert_eq!(sz, data.len() as u64);
    }

    /// Reopen после put → данные сохранены (crash-recovery хвоста).
    #[test]
    fn reopen_preserves_data(
        key in "/blocks/[a-z]{5,15}",
        data in proptest::collection::vec(any::<u8>(), 100..20_000),
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let k = BlockKey::new(key.as_bytes().to_vec());
        {
            let e = engine(tmp.path());
            e.put(&k, &data).unwrap();
            // НЕ flush — проверяем recovery хвоста
        }
        let e2 = engine(tmp.path());
        let got = e2.get(&k).unwrap();
        prop_assert_eq!(got, data);
    }
}
