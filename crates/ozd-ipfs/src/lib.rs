// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! ozd-ipfs — S3-совместимый шлюз для IPFS Kubo (datastore-плагин go-ds-s3).
//!
//! Kubo конфигурируется на наш endpoint как на S3-bucket (см.
//! docs/KUBO-INTEGRATION.md); ключи datastore (`/blocks/...`) становятся
//! object-key, тела блоков — телами объектов. Минимальный subset:
//! PutObject / GetObject / HeadObject / DeleteObject / ListObjectsV2.
//!
//! Аутентификация v1: подпись SigV4 НЕ проверяется (демон слушает локально;
//! Kubo требует ключи в конфиге — подойдут любые). TODO Часть 3: SigV4.

pub mod async_adapter;
pub mod ratelimit;
pub mod sigv4;

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use sha2::{Digest, Sha256};

use ozd_domain::{BlockKey, BlockStore, DomainError};
pub use sigv4::SigV4Config;
pub use ratelimit::{RateLimitConfig, RateLimiter};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn BlockStore>,
}

/// S3-шлюз. `auth = Some(..)` включает обязательный SigV4 (E13) на S3-маршрутах;
/// `/healthz` всегда открыт. None — dev-режим (только loopback!).
/// `rate_limiter` — per-IP лимит запросов (W22); None = без лимита.
pub fn router(
    store: Arc<dyn BlockStore>,
    auth: Option<SigV4Config>,
    rate_limiter: Option<Arc<RateLimiter>>,
) -> Router {
    let st = AppState { store };
    let mut s3 = Router::new()
        .route("/{bucket}", get(list_objects))
        .route(
            "/{bucket}/{*key}",
            get(get_object).put(put_object).head(head_object).delete(delete_object),
        )
        .with_state(st);
    if let Some(cfg) = auth {
        s3 = s3.layer(axum::middleware::from_fn_with_state(Arc::new(cfg), sigv4_mw));
    }
    if let Some(limiter) = rate_limiter {
        s3 = s3.layer(axum::middleware::from_fn_with_state(limiter, ratelimit::rate_limit_mw));
    }
    Router::new().route("/healthz", get(healthz)).merge(s3)
}

/// E13: буферизуем тело → фактический SHA-256 → verify подписи → дальше.
async fn sigv4_mw(
    State(cfg): State<Arc<SigV4Config>>,
    req: Request,
    next: Next,
) -> Response {
    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, 256 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return s3_error(StatusCode::FORBIDDEN, "AccessDenied", &format!("body: {e}"))
        }
    };
    let body_hash = sigv4::hex(&Sha256::digest(&bytes));
    let raw_path = parts.uri.path().to_string();
    let raw_query = parts.uri.query().unwrap_or("").to_string();
    match sigv4::verify(
        &cfg,
        parts.method.as_str(),
        &raw_path,
        &raw_query,
        &parts.headers,
        &body_hash,
    ) {
        Ok(()) => {
            let req = Request::from_parts(parts, axum::body::Body::from(bytes));
            next.run(req).await
        }
        Err(e) => {
            tracing::warn!(path = %raw_path, err = %e, "sigv4 rejected");
            s3_error(StatusCode::FORBIDDEN, "SignatureDoesNotMatch", &e)
        }
    }
}

async fn healthz() -> &'static str {
    "ok"
}

fn bkey(key: &str) -> BlockKey {
    // go-ds-s3 шлёт ключ без ведущего '/': "blocks/CIQ..."; нормализуем с '/'
    let mut k = String::with_capacity(key.len() + 1);
    if !key.starts_with('/') {
        k.push('/');
    }
    k.push_str(key);
    BlockKey::new(k.into_bytes())
}

fn s3_error(code: StatusCode, s3code: &str, msg: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{s3code}</Code><Message>{msg}</Message></Error>"
    );
    (code, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

fn map_err(e: DomainError) -> Response {
    match e {
        DomainError::NotFound => s3_error(StatusCode::NOT_FOUND, "NoSuchKey", "not found"),
        DomainError::IntegrityViolation(m) => {
            s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &m)
        }
        // W24: shutdown → 503 (клиент знает, что сервис уходит; Kubo retry)
        DomainError::Io(ref msg) if msg.starts_with("shutting down") => {
            s3_error(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable", &e.to_string())
        }
        other => s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &other.to_string()),
    }
}

async fn put_object(
    State(st): State<AppState>,
    Path((_bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let k = bkey(&key);
    let store = st.store.clone();
    let res = tokio::task::spawn_blocking(move || store.put(&k, &body)).await;
    match res {
        Ok(Ok(())) => (StatusCode::OK, [(header::ETAG, "\"ozd\"")], "").into_response(),
        Ok(Err(e)) => map_err(e),
        Err(e) => s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string()),
    }
}

async fn get_object(
    State(st): State<AppState>,
    Path((_bucket, key)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Response {
    let k = bkey(&key);
    let store = st.store.clone();
    // E23 (#79): Range GET — диапазон верифицируется BLAKE3-outboard'ом
    // (если записан); без outboard — обычный слайс (CRC движка уже был)
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);
    // полировка E23: x-ozd-bao: 1 → отдать bao-СЛАЙС с доказательством
    // (недоверенный фетчер верифицирует сам — verify_bao_slice)
    let want_bao = headers
        .get("x-ozd-bao")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    let res = tokio::task::spawn_blocking(move || {
        let data = store.get(&k)?;
        let total = data.len() as u64;
        let (start, end) = match range {
            None if want_bao => (0, total.saturating_sub(1)), // bao всего тела
            None => return Ok(GetOut::Full(data)),
            Some(RangeSpec::From(start, end_opt)) => {
                if start >= total {
                    return Err(ozd_domain::DomainError::Io("range start beyond EOF".into()));
                }
                (start, end_opt.unwrap_or(total - 1).min(total - 1))
            }
            Some(RangeSpec::Suffix(n)) => {
                if total == 0 {
                    return Err(ozd_domain::DomainError::Io("range on empty body".into()));
                }
                (total.saturating_sub(n), total - 1)
            }
        };
        let len = end - start + 1;
        let content_range = format!("bytes {start}-{end}/{total}");
        match store.get(&ozd_app::verified::ob_key(&k)) {
            Ok(ob) => {
                if want_bao {
                    let (slice, root) = ozd_app::verified::bao_slice(&data, &ob, start, len)?;
                    return Ok(GetOut::Bao {
                        slice,
                        root: ozd_app::verified::hex32(&root),
                        content_range,
                    });
                }
                // порча в диапазоне → IntegrityViolation (500, байты не отдаём)
                let v = ozd_app::verified::verified_slice(&data, &ob, start, len)?;
                Ok(GetOut::Range(v, content_range, true))
            }
            Err(ozd_domain::DomainError::NotFound) if want_bao => Err(
                ozd_domain::DomainError::Io("bao requested but no outboard for key".into()),
            ),
            Err(ozd_domain::DomainError::NotFound) => {
                let s = data[start as usize..=(end as usize)].to_vec();
                Ok(GetOut::Range(s, content_range, false))
            }
            Err(e) => Err(e),
        }
    })
    .await;
    match res {
        Ok(Ok(GetOut::Full(data))) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            data,
        )
            .into_response(),
        Ok(Ok(GetOut::Range(data, cr, verified))) => (
            StatusCode::PARTIAL_CONTENT,
            [
                (header::CONTENT_TYPE, "application/octet-stream".to_string()),
                (header::CONTENT_RANGE, cr),
                (
                    header::HeaderName::from_static("x-ozd-verified"),
                    if verified { "blake3".to_string() } else { "none".to_string() },
                ),
            ],
            data,
        )
            .into_response(),
        Ok(Ok(GetOut::Bao { slice, root, content_range })) => (
            StatusCode::PARTIAL_CONTENT,
            [
                (
                    header::CONTENT_TYPE,
                    "application/vnd.ozd.bao-slice".to_string(),
                ),
                (header::CONTENT_RANGE, content_range),
                (header::HeaderName::from_static("x-ozd-bao-root"), root),
            ],
            slice,
        )
            .into_response(),
        Ok(Err(e)) => map_err(e),
        Err(e) => s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string()),
    }
}

/// Варианты ответа GET (полировка E23).
enum GetOut {
    Full(Vec<u8>),
    Range(Vec<u8>, String, bool),
    Bao { slice: Vec<u8>, root: String, content_range: String },
}

/// Разобранный Range-заголовок (мульти-диапазоны не поддерживаем).
enum RangeSpec {
    /// "bytes=a-b" | "bytes=a-"
    From(u64, Option<u64>),
    /// "bytes=-n" — последние n байт (полировка E23)
    Suffix(u64),
}

fn parse_range(v: &str) -> Option<RangeSpec> {
    let spec = v.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    if a.is_empty() {
        let n: u64 = b.parse().ok()?;
        if n == 0 {
            return None;
        }
        return Some(RangeSpec::Suffix(n));
    }
    let start: u64 = a.parse().ok()?;
    let end: Option<u64> = if b.is_empty() { None } else { Some(b.parse().ok()?) };
    if let Some(e) = end {
        if e < start {
            return None;
        }
    }
    Some(RangeSpec::From(start, end))
}

async fn head_object(
    State(st): State<AppState>,
    Path((_bucket, key)): Path<(String, String)>,
) -> Response {
    let k = bkey(&key);
    let store = st.store.clone();
    // E11: HEAD = stat из индекса, тело НЕ читается (go-ds-s3 зовёт GetSize часто)
    match tokio::task::spawn_blocking(move || store.stat(&k)).await {
        Ok(Ok(len)) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, len)
            .body(axum::body::Body::empty())
            .unwrap(),
        Ok(Err(DomainError::NotFound)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => map_err(e),
        Err(e) => s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string()),
    }
}

async fn delete_object(
    State(st): State<AppState>,
    Path((_bucket, key)): Path<(String, String)>,
) -> Response {
    let k = bkey(&key);
    let store = st.store.clone();
    match tokio::task::spawn_blocking(move || store.delete(&k)).await {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) => map_err(e),
        Err(e) => s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string()),
    }
}

async fn list_objects(
    State(st): State<AppState>,
    Path(bucket): Path<String>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Response {
    // ListObjectsV2: prefix, max-keys, continuation-token (= последний ключ)
    let prefix_raw = q.get("prefix").cloned().unwrap_or_default();
    let max_keys: usize =
        q.get("max-keys").and_then(|s| s.parse().ok()).unwrap_or(1000).min(10_000);
    let token = q.get("continuation-token").or(q.get("start-after")).cloned();

    let prefix = {
        let mut p = String::new();
        if !prefix_raw.starts_with('/') {
            p.push('/');
        }
        p.push_str(&prefix_raw);
        p
    };
    let after = token.map(|t| bkey(&t));

    let store = st.store.clone();
    let pfx = prefix.clone().into_bytes();
    match tokio::task::spawn_blocking(move || store.list(&pfx, after.as_ref(), max_keys + 1)).await
    {
        Ok(Ok(mut items)) => {
            let truncated = items.len() > max_keys;
            items.truncate(max_keys);
            let mut xml = String::with_capacity(256 + items.len() * 96);
            xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
            xml.push_str(&format!("<Name>{bucket}</Name>"));
            xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(&prefix_raw)));
            xml.push_str(&format!("<KeyCount>{}</KeyCount>", items.len()));
            xml.push_str(&format!("<MaxKeys>{max_keys}</MaxKeys>"));
            xml.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
            if truncated {
                if let Some((last, _)) = items.last() {
                    xml.push_str(&format!(
                        "<NextContinuationToken>{}</NextContinuationToken>",
                        xml_escape(&String::from_utf8_lossy(last.as_bytes()))
                    ));
                }
            }
            for (k, size) in &items {
                let key_str = String::from_utf8_lossy(k.as_bytes());
                let key_no_slash = key_str.strip_prefix('/').unwrap_or(&key_str);
                xml.push_str("<Contents>");
                xml.push_str(&format!("<Key>{}</Key>", xml_escape(key_no_slash)));
                xml.push_str(&format!("<Size>{size}</Size>"));
                xml.push_str("<StorageClass>STANDARD</StorageClass>");
                xml.push_str("</Contents>");
            }
            xml.push_str("</ListBucketResult>");
            (StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], xml).into_response()
        }
        Ok(Err(e)) => map_err(e),
        Err(e) => s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string()),
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}
