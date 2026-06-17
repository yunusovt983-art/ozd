// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! W22: Per-IP token-bucket rate limiter (middleware для S3-маршрутов).
//!
//! Дизайн: один бакет на IP, max_rps токенов за секунду, линейное пополнение.
//! Старые бакеты (>60с без запросов) чистятся при каждом 256-м запросе.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use parking_lot::Mutex;

/// Конфигурация rate-limiter.
#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    /// максимум запросов в секунду на IP; 0 = лимит выключен
    pub max_rps: u32,
}

/// Бакет одного IP.
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Per-IP token-bucket rate limiter.
pub struct RateLimiter {
    max_rps: f64,
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
    request_count: std::sync::atomic::AtomicU64,
}

impl RateLimiter {
    pub fn new(max_rps: u32) -> Self {
        Self {
            max_rps: max_rps as f64,
            buckets: Mutex::new(HashMap::new()),
            request_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Проверить лимит для IP. true = разрешён, false = 429.
    pub fn check(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut map = self.buckets.lock();

        // периодическая чистка старых бакетов (каждые 256 запросов)
        let cnt = self.request_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if cnt % 256 == 0 {
            map.retain(|_, b| now.duration_since(b.last).as_secs() < 60);
        }

        let bucket = map.entry(ip).or_insert(Bucket {
            tokens: self.max_rps,
            last: now,
        });

        // пополнить токены за прошедшее время
        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.max_rps).min(self.max_rps);
        bucket.last = now;

        // попытка взять 1 токен
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Axum middleware: извлекает IP из ConnectInfo или X-Forwarded-For,
/// проверяет лимит, возвращает 429 при превышении.
pub async fn rate_limit_mw(
    State(limiter): axum::extract::State<Arc<RateLimiter>>,
    req: Request,
    next: Next,
) -> Response {
    let ip = extract_ip(&req);
    if !limiter.check(ip) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "1")],
            "429 Too Many Requests",
        )
            .into_response();
    }
    next.run(req).await
}

/// Извлечь IP клиента: X-Forwarded-For (первый) → ConnectInfo → fallback 127.0.0.1.
fn extract_ip(req: &Request) -> IpAddr {
    // X-Forwarded-For: client, proxy1, proxy2
    if let Some(xff) = req.headers().get("x-forwarded-for") {
        if let Ok(val) = xff.to_str() {
            if let Some(first) = val.split(',').next() {
                if let Ok(ip) = first.trim().parse::<IpAddr>() {
                    return ip;
                }
            }
        }
    }
    // ConnectInfo (axum's socket addr extension)
    if let Some(addr) = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        return addr.0.ip();
    }
    // fallback — не должно произойти при нормальном serve()
    IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
}

use axum::extract::State;
