// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! E23: Range GET — 206 + Content-Range, верификация BLAKE3 при наличии
//! outboard, 502 на порче в диапазоне, обычный 200 без Range.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ozd_domain::{BlockKey, BlockStore, DomainError, DomainResult};
use parking_lot::Mutex;
use tower::util::ServiceExt;

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
        _p: &[u8],
        _a: Option<&BlockKey>,
        _l: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>> {
        Ok(vec![])
    }
}

async fn call(app: &axum::Router, req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 24).await.unwrap();
    (status, headers, bytes.to_vec())
}

#[tokio::test]
async fn range_get_verified_and_corruption_rejected() {
    let store = Arc::new(MemStore::default());
    let body: Vec<u8> = (0..400_000u32).map(|i| (i % 251) as u8).collect();
    let key = BlockKey::from("/blocks/RNG1");
    store.put(&key, &body).unwrap();
    store
        .put(&ozd_app::verified::ob_key(&key), &ozd_app::verified::make_outboard(&body))
        .unwrap();
    let app = ozd_ipfs::router(store.clone(), None, None);
    let url = "/blocks/%2Fblocks%2FRNG1";

    // верифицированный 206
    let (st, hdrs, got) = call(
        &app,
        Request::get(url).header("range", "bytes=100000-104999").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(hdrs.get("content-range").unwrap(), "bytes 100000-104999/400000");
    assert_eq!(hdrs.get("x-ozd-verified").unwrap(), "blake3");
    assert_eq!(got, &body[100_000..105_000]);

    // открытый хвост "bytes=399000-"
    let (st, _, got) = call(
        &app,
        Request::get(url).header("range", "bytes=399000-").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(got, &body[399_000..]);

    // полный GET без Range — обычный 200
    let (st, _, got) = call(&app, Request::get(url).body(Body::empty()).unwrap()).await;
    assert_eq!((st, got.len()), (StatusCode::OK, 400_000));

    // порча в диапазоне → ошибка целостности, байты НЕ отдаются
    let mut bad = body.clone();
    bad[102_000] ^= 0x01;
    store.put(&key, &bad).unwrap(); // тело подменено, outboard прежний
    let (st, _, _) = call(
        &app,
        Request::get(url).header("range", "bytes=100000-104999").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR, "bitrot в диапазоне → 500, не мусор");

    // без outboard — unverified 206 (CRC движка уже отработал на get)
    let k2 = BlockKey::from("/blocks/RNG2");
    store.put(&k2, &body).unwrap();
    let (st, hdrs, got) = call(
        &app,
        Request::get("/blocks/%2Fblocks%2FRNG2")
            .header("range", "bytes=0-99")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(hdrs.get("x-ozd-verified").unwrap(), "none");
    assert_eq!(got, &body[..100]);
}

#[tokio::test]
async fn suffix_range_and_bao_slice() {
    let store = Arc::new(MemStore::default());
    let body: Vec<u8> = (0..200_000u32).map(|i| (i * 11 % 256) as u8).collect();
    let key = BlockKey::from("/blocks/SFX1");
    store.put(&key, &body).unwrap();
    store
        .put(&ozd_app::verified::ob_key(&key), &ozd_app::verified::make_outboard(&body))
        .unwrap();
    let app = ozd_ipfs::router(store.clone(), None, None);
    let url = "/blocks/%2Fblocks%2FSFX1";

    // суффикс-форма: последние 500 байт, верифицировано
    let (st, hdrs, got) = call(
        &app,
        Request::get(url).header("range", "bytes=-500").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(hdrs.get("content-range").unwrap(), "bytes 199500-199999/200000");
    assert_eq!(hdrs.get("x-ozd-verified").unwrap(), "blake3");
    assert_eq!(got, &body[199_500..]);

    // суффикс больше тела → всё тело как 206
    let (st, hdrs, got) = call(
        &app,
        Request::get(url).header("range", "bytes=-999999").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(hdrs.get("content-range").unwrap(), "bytes 0-199999/200000");
    assert_eq!(got, body);

    // bao-слайс наружу: клиент верифицирует сам против root из заголовка
    let (st, hdrs, slice) = call(
        &app,
        Request::get(url)
            .header("range", "bytes=50000-59999")
            .header("x-ozd-bao", "1")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(hdrs.get("content-type").unwrap(), "application/vnd.ozd.bao-slice");
    let root_hex = hdrs.get("x-ozd-bao-root").unwrap().to_str().unwrap();
    let mut root = [0u8; 32];
    for i in 0..32 {
        root[i] = u8::from_str_radix(&root_hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    let verified =
        ozd_app::verified::verify_bao_slice(&slice, &root, 50_000, 10_000).unwrap();
    assert_eq!(verified, &body[50_000..60_000], "клиент проверил слайс сам");
    // испорченный слайс клиент ловит без сервера
    let mut bad = slice.clone();
    let n = bad.len();
    bad[n - 1] ^= 0xFF;
    assert!(ozd_app::verified::verify_bao_slice(&bad, &root, 50_000, 10_000).is_err());

    // bao без outboard → ошибка, не мусор
    let k2 = BlockKey::from("/blocks/SFX2");
    store.put(&k2, &body).unwrap();
    let (st, _, _) = call(
        &app,
        Request::get("/blocks/%2Fblocks%2FSFX2")
            .header("x-ozd-bao", "1")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
}

