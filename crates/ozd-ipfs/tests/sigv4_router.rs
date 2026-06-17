// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! E13: интеграционный тест SigV4-middleware на роутере (tower oneshot).

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ozd_domain::{BlockKey, BlockStore, DomainError, DomainResult};
use ozd_ipfs::{sigv4, SigV4Config};
use sha2::{Digest, Sha256};
use tower::util::ServiceExt;

/// Память-store для теста шлюза.
#[derive(Default)]
struct MemStore(parking_lot::Mutex<HashMap<Vec<u8>, Vec<u8>>>);

impl BlockStore for MemStore {
    fn put(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()> {
        self.0.lock().insert(key.as_bytes().to_vec(), data.to_vec());
        Ok(())
    }
    fn get(&self, key: &BlockKey) -> DomainResult<Vec<u8>> {
        self.0.lock().get(key.as_bytes()).cloned().ok_or(DomainError::NotFound)
    }
    fn has(&self, key: &BlockKey) -> DomainResult<bool> {
        Ok(self.0.lock().contains_key(key.as_bytes()))
    }
    fn delete(&self, key: &BlockKey) -> DomainResult<()> {
        self.0.lock().remove(key.as_bytes());
        Ok(())
    }
    fn list(
        &self,
        prefix: &[u8],
        _after: Option<&BlockKey>,
        limit: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>> {
        let g = self.0.lock();
        let mut v: Vec<_> = g
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, d)| (BlockKey::new(k.clone()), d.len() as u64))
            .collect();
        v.sort();
        v.truncate(limit);
        Ok(v)
    }
}

fn now_amz() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let days = secs.div_euclid(86400);
    let tod = secs.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", tod / 3600, (tod % 3600) / 60, tod % 60)
}

const SH: &[&str] = &["host", "x-amz-content-sha256", "x-amz-date"];

fn signed_request(cfg: &SigV4Config, method: &str, path: &str, body: &[u8]) -> Request<Body> {
    let date = now_amz();
    let body_sha = sigv4::hex(&Sha256::digest(body));
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("host", "test:1".parse().unwrap());
    headers.insert("x-amz-date", date.parse().unwrap());
    headers.insert("x-amz-content-sha256", body_sha.parse().unwrap());
    let auth = sigv4::sign_for_test(cfg, method, path, "", &headers, SH, &date, "us-east-1");
    let mut rb = Request::builder().method(method).uri(path);
    for (k, v) in headers.iter() {
        rb = rb.header(k, v);
    }
    rb.header("authorization", auth).body(Body::from(body.to_vec())).unwrap()
}

#[tokio::test]
async fn unsigned_rejected_signed_accepted_healthz_open() {
    let cfg = SigV4Config::new("AK1", "topsecret");
    let store = Arc::new(MemStore::default());
    let app = ozd_ipfs::router(store.clone(), Some(cfg.clone()), None, None);

    // healthz — без подписи OK
    let r = app
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // без подписи → 403
    let r = app
        .clone()
        .oneshot(Request::put("/kubo/blocks/K1").body(Body::from("data")).unwrap())
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);

    // с верной подписью → 200, данные легли
    let r = app
        .clone()
        .oneshot(signed_request(&cfg, "PUT", "/kubo/blocks/K1", b"data"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert!(store.has(&BlockKey::from("/blocks/K1")).unwrap());

    // GET с подписью читает
    let r = app
        .clone()
        .oneshot(signed_request(&cfg, "GET", "/kubo/blocks/K1", b""))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // чужой секрет → 403
    let evil = SigV4Config::new("AK1", "WRONG");
    let r = app
        .clone()
        .oneshot(signed_request(&evil, "GET", "/kubo/blocks/K1", b""))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
}
