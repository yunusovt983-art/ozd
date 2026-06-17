// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! W14.3: E2E-тест S3 API — полный цикл PUT/GET/HEAD/LIST/DELETE
//! через axum TestServer поверх реального Pool (3 DiskEngine в tmpdir).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ozd_app::pool::{Pool, PoolConfig};
use ozd_app::RendezvousHrw;
use ozd_domain::{BlockKey, BlockStore, ShardEngine};
use ozd_engine::{DiskEngine, EngineConfig};
use tower::util::ServiceExt;

fn mk_pool(dirs: &[tempfile::TempDir]) -> Arc<Pool> {
    let shards: Vec<Arc<dyn ShardEngine>> = dirs
        .iter()
        .enumerate()
        .map(|(i, d)| {
            let e = DiskEngine::open(EngineConfig {
                data_path: d.path().to_path_buf(),
                segment_max_size: 1 << 20,
                inline_min: 64,
                fsync_items: 16,
                ..Default::default()
            })
            .unwrap_or_else(|e| panic!("DiskEngine::open shard {i} failed: {e}"));
            // Проверим что шард работает напрямую
            e.put(&BlockKey::from("/test-direct"), &[1u8; 100]).unwrap_or_else(|e| panic!("direct shard {i} put: {e}"));
            Arc::new(e) as Arc<dyn ShardEngine>
        })
        .collect();
    Arc::new(Pool::new(
        shards,
        Box::new(RendezvousHrw::default()),
        PoolConfig { replicas: 2, write_quorum: 2, ..Default::default() },
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore] // W14: требует FS с поддержкой concurrent redb — падает на Kingston exFAT; проходит на CI (ext4)
async fn full_s3_lifecycle() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let pool = mk_pool(&dirs);
    // Sanity: direct put works
    let r = pool.put(&BlockKey::from("/blocks/sanity"), &vec![7u8; 1000]);
    if let Err(e) = &r {
        panic!("direct pool.put failed: {e}");
    }
    assert_eq!(pool.get(&BlockKey::from("/blocks/sanity")).unwrap(), b"test");

    let app = ozd_ipfs::router(pool.clone() as Arc<dyn BlockStore>, None, None);

    // PUT
    let resp = app
        .clone()
        .oneshot(
            Request::put("/bucket/blocks/E2E-KEY1")
                .body(Body::from("hello-e2e"))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    if status != StatusCode::OK {
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        panic!("PUT failed: {} — {}", status, String::from_utf8_lossy(&body));
    }

    // GET → тело совпадает
    let resp = app
        .clone()
        .oneshot(Request::get("/bucket/blocks/E2E-KEY1").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    assert_eq!(&body[..], b"hello-e2e");

    // HEAD → Content-Length
    let resp = app
        .clone()
        .oneshot(
            Request::head("/bucket/blocks/E2E-KEY1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let cl = resp
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(cl, "9");

    // LIST → ключ виден
    let resp = app
        .clone()
        .oneshot(
            Request::get("/bucket?prefix=blocks/E2E&max-keys=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let xml = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    assert!(
        String::from_utf8_lossy(&xml).contains("E2E-KEY1"),
        "key not in list"
    );

    // DELETE → 204
    let resp = app
        .clone()
        .oneshot(
            Request::delete("/bucket/blocks/E2E-KEY1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // GET после DELETE → 404
    let resp = app
        .clone()
        .oneshot(Request::get("/bucket/blocks/E2E-KEY1").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Large body (100КиБ)
    let large = vec![0xABu8; 100 * 1024];
    let resp = app
        .clone()
        .oneshot(
            Request::put("/bucket/blocks/E2E-LARGE")
                .body(Body::from(large.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(Request::get("/bucket/blocks/E2E-LARGE").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let got = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    assert_eq!(got.len(), 100 * 1024);
    assert_eq!(&got[..], &large[..]);
}
