//! E13: проверка AWS Signature V4 (header-auth, как шлёт go-ds-s3/aws-sdk-go).
//!
//! Канонизация — S3-стиль (как MinIO/RustFS): canonical URI = сырой path
//! запроса (SDK уже прислал его закодированным), query — пары как есть,
//! отсортированные по ключу. Совместимость с реальным Kubo подтверждается
//! на стенде (E15); юнит-тесты валидируют наш же подписант (round-trip).
//!
//! Payload: заголовок `x-amz-content-sha256` сверяется с фактическим SHA-256
//! тела (если не UNSIGNED-PAYLOAD) — подмена тела ловится до подписи.

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct SigV4Config {
    pub access_key: String,
    pub secret_key: String,
    /// допуск рассинхрона часов (сек); 0 = не проверять
    pub max_skew_secs: i64,
}

impl SigV4Config {
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            max_skew_secs: 15 * 60,
        }
    }
}

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Разобранный Authorization: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...
struct AuthHeader {
    access_key: String,
    date: String,    // YYYYMMDD из scope
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature_hex: String,
}

fn parse_auth_header(v: &str) -> Result<AuthHeader, String> {
    let rest = v
        .strip_prefix("AWS4-HMAC-SHA256")
        .ok_or("unsupported auth scheme")?
        .trim();
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(c) = part.strip_prefix("Credential=") {
            credential = Some(c.to_string());
        } else if let Some(s) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(s.to_string());
        } else if let Some(s) = part.strip_prefix("Signature=") {
            signature = Some(s.to_string());
        }
    }
    let credential = credential.ok_or("missing Credential")?;
    // AK/YYYYMMDD/region/service/aws4_request
    let cp: Vec<&str> = credential.split('/').collect();
    if cp.len() != 5 || cp[4] != "aws4_request" {
        return Err("bad credential scope".into());
    }
    Ok(AuthHeader {
        access_key: cp[0].to_string(),
        date: cp[1].to_string(),
        region: cp[2].to_string(),
        service: cp[3].to_string(),
        signed_headers: signed_headers
            .ok_or("missing SignedHeaders")?
            .split(';')
            .map(|s| s.to_ascii_lowercase())
            .collect(),
        signature_hex: signature.ok_or("missing Signature")?,
    })
}

/// x-amz-date "YYYYMMDDTHHMMSSZ" → unix-секунды (без chrono).
fn parse_amz_date(s: &str) -> Option<i64> {
    if s.len() != 16 || !s.ends_with('Z') || s.as_bytes()[8] != b'T' {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s[r].parse::<i64>().ok();
    let (y, mo, d) = (num(0..4)?, num(4..6)?, num(6..8)?);
    let (h, mi, sec) = (num(9..11)?, num(11..13)?, num(13..15)?);
    // дни с эпохи (civil_from_days, Howard Hinnant)
    let y_adj = if mo <= 2 { y - 1 } else { y };
    let era = y_adj.div_euclid(400);
    let yoe = y_adj - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days * 86400 + h * 3600 + mi * 60 + sec)
}

/// Проверка подписи. `raw_path`/`raw_query` — как в строке запроса (без декода).
/// `body_sha256_hex` — фактический SHA-256 буферизованного тела.
pub fn verify(
    cfg: &SigV4Config,
    method: &str,
    raw_path: &str,
    raw_query: &str,
    headers: &HeaderMap,
    body_sha256_hex: &str,
) -> Result<(), String> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or("missing Authorization")?;
    let auth = parse_auth_header(auth)?;

    if auth.access_key != cfg.access_key {
        return Err("unknown access key".into());
    }

    let amz_date = headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or("missing x-amz-date")?;
    if !amz_date.starts_with(&auth.date) {
        return Err("x-amz-date / credential date mismatch".into());
    }
    if cfg.max_skew_secs > 0 {
        let t = parse_amz_date(amz_date).ok_or("bad x-amz-date")?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        if (now - t).abs() > cfg.max_skew_secs {
            return Err("request time too skewed".into());
        }
    }

    // payload-хэш: заголовок обязан совпадать с фактическим телом
    let content_sha = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");
    if content_sha != "UNSIGNED-PAYLOAD" && !content_sha.eq_ignore_ascii_case(body_sha256_hex)
    {
        return Err("x-amz-content-sha256 does not match body".into());
    }

    // canonical headers (из SignedHeaders)
    let mut canon_headers = String::new();
    for name in &auth.signed_headers {
        let val = headers
            .get(name.as_str())
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| format!("signed header '{name}' missing"))?;
        let collapsed = val.split_whitespace().collect::<Vec<_>>().join(" ");
        canon_headers.push_str(name);
        canon_headers.push(':');
        canon_headers.push_str(collapsed.trim());
        canon_headers.push('\n');
    }
    let signed_headers_join = auth.signed_headers.join(";");

    // canonical query: пары как есть, сортировка по ключу (затем по значению)
    let mut pairs: Vec<(&str, &str)> = raw_query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k, v),
            None => (p, ""),
        })
        .collect();
    pairs.sort();
    let canon_query =
        pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");

    let canonical_request = format!(
        "{method}\n{raw_path}\n{canon_query}\n{canon_headers}\n{signed_headers_join}\n{content_sha}"
    );

    let scope = format!("{}/{}/{}/aws4_request", auth.date, auth.region, auth.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );

    // цепочка ключей подписи
    let k_date = hmac(format!("AWS4{}", cfg.secret_key).as_bytes(), auth.date.as_bytes());
    let k_region = hmac(&k_date, auth.region.as_bytes());
    let k_service = hmac(&k_region, auth.service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");

    // constant-time сверка через Mac::verify_slice
    let provided = hex_decode(&auth.signature_hex).ok_or("bad signature hex")?;
    let mut mac = HmacSha256::new_from_slice(&k_signing).expect("hmac");
    mac.update(string_to_sign.as_bytes());
    mac.verify_slice(&provided).map_err(|_| "signature mismatch".to_string())
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

/// Тестовый/клиентский подписант (round-trip к verify; пригодится и e2e).
#[doc(hidden)]
pub fn sign_for_test(
    cfg: &SigV4Config,
    method: &str,
    raw_path: &str,
    raw_query: &str,
    headers: &HeaderMap,
    signed_headers: &[&str],
    amz_date: &str,
    region: &str,
) -> String {
    let date = &amz_date[..8];
    let content_sha = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD")
        .to_string();
    let mut canon_headers = String::new();
    for name in signed_headers {
        let val = headers.get(*name).and_then(|v| v.to_str().ok()).unwrap_or("");
        canon_headers.push_str(&name.to_ascii_lowercase());
        canon_headers.push(':');
        canon_headers.push_str(val.trim());
        canon_headers.push('\n');
    }
    let mut pairs: Vec<(&str, &str)> = raw_query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|p| p.split_once('=').unwrap_or((p, "")))
        .collect();
    pairs.sort();
    let canon_query =
        pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let shj = signed_headers.join(";").to_ascii_lowercase();
    let canonical_request =
        format!("{method}\n{raw_path}\n{canon_query}\n{canon_headers}\n{shj}\n{content_sha}");
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac(format!("AWS4{}", cfg.secret_key).as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    let sig = hex(&hmac(&k_signing, sts.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={shj}, Signature={sig}",
        cfg.access_key
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn now_amz() -> String {
        // формируем YYYYMMDDTHHMMSSZ из unix-времени (обратное parse_amz_date)
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let days = secs.div_euclid(86400);
        let tod = secs.rem_euclid(86400);
        // civil_from_days
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
        format!(
            "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
            tod / 3600,
            (tod % 3600) / 60,
            tod % 60
        )
    }

    fn cfg() -> SigV4Config {
        SigV4Config::new("AKTEST", "secret123")
    }

    fn mk_headers(amz_date: &str, payload: &[u8]) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("host", HeaderValue::from_static("127.0.0.1:9100"));
        h.insert("x-amz-date", HeaderValue::from_str(amz_date).unwrap());
        h.insert(
            "x-amz-content-sha256",
            HeaderValue::from_str(&hex(&Sha256::digest(payload))).unwrap(),
        );
        h
    }

    const SH: &[&str] = &["host", "x-amz-content-sha256", "x-amz-date"];

    #[test]
    fn roundtrip_valid_signature() {
        let c = cfg();
        let date = now_amz();
        let body = b"hello-block";
        let mut h = mk_headers(&date, body);
        let auth =
            sign_for_test(&c, "PUT", "/kubo/blocks/CIQX", "", &h, SH, &date, "us-east-1");
        h.insert("authorization", HeaderValue::from_str(&auth).unwrap());
        let body_hash = hex(&Sha256::digest(body));
        verify(&c, "PUT", "/kubo/blocks/CIQX", "", &h, &body_hash).expect("must verify");
    }

    #[test]
    fn wrong_secret_rejected() {
        let c = cfg();
        let bad = SigV4Config::new("AKTEST", "WRONG");
        let date = now_amz();
        let body = b"x";
        let mut h = mk_headers(&date, body);
        let auth = sign_for_test(&bad, "GET", "/kubo/blocks/K", "", &h, SH, &date, "us-east-1");
        h.insert("authorization", HeaderValue::from_str(&auth).unwrap());
        let bh = hex(&Sha256::digest(body));
        assert!(verify(&c, "GET", "/kubo/blocks/K", "", &h, &bh).is_err());
    }

    #[test]
    fn tampered_body_rejected() {
        let c = cfg();
        let date = now_amz();
        let body = b"original";
        let mut h = mk_headers(&date, body);
        let auth = sign_for_test(&c, "PUT", "/kubo/blocks/K", "", &h, SH, &date, "us-east-1");
        h.insert("authorization", HeaderValue::from_str(&auth).unwrap());
        // тело подменили после подписи → фактический хэш другой
        let tampered_hash = hex(&Sha256::digest(b"EVIL"));
        let err = verify(&c, "PUT", "/kubo/blocks/K", "", &h, &tampered_hash).unwrap_err();
        assert!(err.contains("does not match body"), "{err}");
    }

    #[test]
    fn skewed_date_rejected() {
        let c = cfg();
        let old = "20200101T000000Z";
        let body = b"x";
        let mut h = mk_headers(old, body);
        let auth = sign_for_test(&c, "GET", "/k/b", "", &h, SH, old, "us-east-1");
        h.insert("authorization", HeaderValue::from_str(&auth).unwrap());
        let bh = hex(&Sha256::digest(body));
        let err = verify(&c, "GET", "/k/b", "", &h, &bh).unwrap_err();
        assert!(err.contains("skewed"), "{err}");
    }

    #[test]
    fn query_canonicalization_sorted() {
        let c = cfg();
        let date = now_amz();
        let mut h = mk_headers(&date, b"");
        // подписываем с query в одном порядке, проверяем с тем же raw —
        // канонизация сортирует одинаково с обеих сторон
        let q = "prefix=blocks/&list-type=2&max-keys=100";
        let auth = sign_for_test(&c, "GET", "/kubo", q, &h, SH, &date, "us-east-1");
        h.insert("authorization", HeaderValue::from_str(&auth).unwrap());
        let bh = hex(&Sha256::digest(b""));
        verify(&c, "GET", "/kubo", q, &h, &bh).expect("query roundtrip");
    }
}
