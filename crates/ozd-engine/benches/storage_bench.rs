// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! W15.1: Criterion-бенчмарки storage-слоя.
//! Группы: put (inline + segment), get, stat.
//! Запуск: cargo bench -p ozd-engine

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ozd_domain::{BlockKey, ShardEngine};
use ozd_engine::{DiskEngine, EngineConfig};

fn mk_engine() -> (tempfile::TempDir, DiskEngine) {
    let dir = tempfile::tempdir().unwrap();
    let e = DiskEngine::open(EngineConfig {
        data_path: dir.path().to_path_buf(),
        segment_max_size: 256 * 1024 * 1024,
        inline_min: 4096,
        fsync_items: 256,
        ..Default::default()
    })
    .unwrap();
    (dir, e)
}

fn bench_put_inline(c: &mut Criterion) {
    let (_dir, e) = mk_engine();
    let data = vec![7u8; 100]; // < inline_min → redb
    let mut i = 0u64;
    c.bench_function("put_inline_100B", |b| {
        b.iter(|| {
            let key = BlockKey::new(format!("/b/i{i}"));
            e.put(&key, black_box(&data)).unwrap();
            i += 1;
        })
    });
}

fn bench_put_segment(c: &mut Criterion) {
    let (_dir, e) = mk_engine();
    let data = vec![0xABu8; 256 * 1024]; // 256 КиБ → сегмент
    let mut i = 0u64;
    let mut group = c.benchmark_group("put_segment");
    group.throughput(Throughput::Bytes(256 * 1024));
    group.bench_function("256KiB", |b| {
        b.iter(|| {
            let key = BlockKey::new(format!("/b/s{i}"));
            e.put(&key, black_box(&data)).unwrap();
            i += 1;
        })
    });
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let (_dir, e) = mk_engine();
    let data = vec![0xCDu8; 64 * 1024];
    // предзаписать 100 ключей
    for i in 0..100u32 {
        e.put(&BlockKey::new(format!("/b/g{i}")), &data).unwrap();
    }
    e.flush().unwrap();
    let mut i = 0u32;
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Bytes(64 * 1024));
    group.bench_function("64KiB", |b| {
        b.iter(|| {
            let key = BlockKey::new(format!("/b/g{}", i % 100));
            let v = e.get(&key).unwrap();
            black_box(&v);
            i += 1;
        })
    });
    group.finish();
}

fn bench_stat(c: &mut Criterion) {
    let (_dir, e) = mk_engine();
    let data = vec![0u8; 50_000];
    for i in 0..100u32 {
        e.put(&BlockKey::new(format!("/b/st{i}")), &data).unwrap();
    }
    e.flush().unwrap();
    let mut i = 0u32;
    c.bench_function("stat_from_index", |b| {
        b.iter(|| {
            let key = BlockKey::new(format!("/b/st{}", i % 100));
            black_box(ShardEngine::stat(&e, &key).unwrap());
            i += 1;
        })
    });
}

criterion_group!(benches, bench_put_inline, bench_put_segment, bench_get, bench_stat);
criterion_main!(benches);
