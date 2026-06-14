//! E25: «СуперДиск» (#143 Discord) — NVMe read-leg поверх пула HDD
//! + request coalescing (#144).
//!
//! Discord: асимметричное зеркало — durable-нога (сеть/HDD) write-mostly,
//! быстрая нога (NVMe) обслуживает чтения; dm-cache/bcache отвергнуты —
//! свой слой со своей семантикой. У нас та же идея на нашем же движке:
//! кэш = DiskEngine на NVMe-датасете (pack-сегменты + CRC + redb).
//!
//! - Чтение: NVMe-нога → промах/порча → пул (HDD) → асинхронность не
//!   нужна: populate на NVMe ~мс. Битая кэш-копия выбрасывается и
//!   перечитывается с HDD — self-heal, аналог mirror-resync Discord.
//! - Запись: write-through (свежий IPFS-блок тут же читается DAG-обходом).
//! - Эвикция: FIFO целыми сегментами (#92/#110 whole-file retention) —
//!   без LRU-учёта, ноль write-amp, монотонный seg_id = возраст.
//! - Coalescing (#144): single-flight на ключ — сто конкурентных GET
//!   горячего CID = ОДНО чтение с диска (без него супер-диск Discord
//!   не выстрелил: дубли запросов съедали ногу).
//!
//! Иммутабельность content-addressed тел = у кэша НЕТ проблемы
//! инвалидации: только delete (редкий) сносит обе копии.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use ozd_domain::{BlockKey, BlockStore, DomainError, DomainResult, ShardEngine};

use crate::metrics::OpsMetrics;

#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// бюджет кэша, байт (0 = эвикция выключена — только для тестов!)
    pub max_bytes: u64,
    /// тела меньше порога не кэшируем (мелочь и так inline на NVMe-индексе)
    pub min_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self { max_bytes: 0, min_size: 4096 }
    }
}

/// Single-flight слот (#144): лидер выполняет чтение, попутчики ждут
/// результат (клон байт / строка ошибки — DomainError не Clone).
#[derive(Default)]
struct Flight {
    done: Mutex<Option<Result<Vec<u8>, String>>>,
    cv: Condvar,
}

pub struct CacheTier {
    inner: Arc<dyn BlockStore>,
    cache: Arc<dyn ShardEngine>,
    cfg: CacheConfig,
    /// учёт занятого кэшем (тела; заголовки/redb — мимо, эвикция
    /// корректирует фактическими размерами файлов — дрейф самогасится)
    bytes: AtomicU64,
    metrics: Arc<OpsMetrics>,
    inflight: Mutex<HashMap<BlockKey, Arc<Flight>>>,
}

impl CacheTier {
    pub fn new(
        inner: Arc<dyn BlockStore>,
        cache: Arc<dyn ShardEngine>,
        cfg: CacheConfig,
        metrics: Arc<OpsMetrics>,
    ) -> Self {
        let bytes = cache.data_bytes().unwrap_or(0); // рестарт: тёплый кэш учтён
        Self {
            inner,
            cache,
            cfg,
            bytes: AtomicU64::new(bytes),
            metrics,
            inflight: Mutex::new(HashMap::new()),
        }
    }

    pub fn cached_bytes(&self) -> u64 {
        self.bytes.load(Relaxed)
    }

    /// Однопроходное чтение (внутри single-flight): NVMe → HDD → populate.
    fn read_through(&self, key: &BlockKey) -> DomainResult<Vec<u8>> {
        match self.cache.get(key) {
            Ok(v) => {
                self.metrics.cache_hits.fetch_add(1, Relaxed);
                return Ok(v);
            }
            Err(DomainError::NotFound) => {}
            Err(e) => {
                // self-heal (#143): источник истины — HDD-пул; битую
                // кэш-копию выбрасываем и перечитываем
                tracing::warn!(?key, err = %e, "cache copy bad — self-heal from pool");
                let _ = self.cache.delete(key);
                self.metrics.cache_self_heals.fetch_add(1, Relaxed);
            }
        }
        self.metrics.cache_misses.fetch_add(1, Relaxed);
        let v = self.inner.get(key)?;
        self.populate(key, &v);
        Ok(v)
    }

    fn populate(&self, key: &BlockKey, body: &[u8]) {
        if body.len() < self.cfg.min_size {
            return;
        }
        match self.cache.put(key, body) {
            Ok(()) => {
                self.bytes.fetch_add(body.len() as u64, Relaxed);
                self.metrics.cache_populated_bytes.fetch_add(body.len() as u64, Relaxed);
                self.maybe_evict();
            }
            Err(e) => tracing::warn!(?key, err = %e, "cache populate failed (non-fatal)"),
        }
    }

    /// Гистерезис (#130-дух): эвиктим при > max до ~90% max — без дребезга.
    fn maybe_evict(&self) {
        if self.cfg.max_bytes == 0 {
            return;
        }
        let low = self.cfg.max_bytes - self.cfg.max_bytes / 10;
        while self.bytes.load(Relaxed) > self.cfg.max_bytes {
            match self.cache.evict_oldest_segment() {
                Ok((0, _)) => break, // только активный сегмент — ждём ротации
                Ok((freed, keys)) => {
                    let cur = self.bytes.load(Relaxed);
                    self.bytes.store(cur.saturating_sub(freed), Relaxed);
                    self.metrics.cache_evicted_segments.fetch_add(1, Relaxed);
                    self.metrics.cache_evicted_bytes.fetch_add(freed, Relaxed);
                    tracing::info!(freed, keys, "cache: FIFO segment evicted");
                    if self.bytes.load(Relaxed) <= low {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(err = %e, "cache eviction failed");
                    break;
                }
            }
        }
    }
}

impl BlockStore for CacheTier {
    fn get(&self, key: &BlockKey) -> DomainResult<Vec<u8>> {
        // #144: single-flight — конкурентные чтения одного ключа сливаются
        let (flight, leader) = {
            let mut m = self.inflight.lock();
            match m.get(key) {
                Some(f) => (f.clone(), false),
                None => {
                    let f = Arc::new(Flight::default());
                    m.insert(key.clone(), f.clone());
                    (f, true)
                }
            }
        };
        if leader {
            let res = self.read_through(key);
            {
                let mut d = flight.done.lock();
                *d = Some(res.as_ref().map(|v| v.clone()).map_err(|e| e.to_string()));
            }
            flight.cv.notify_all();
            self.inflight.lock().remove(key);
            res
        } else {
            self.metrics.cache_coalesced.fetch_add(1, Relaxed);
            let mut d = flight.done.lock();
            while d.is_none() {
                flight.cv.wait(&mut d);
            }
            match d.clone().unwrap() {
                Ok(v) => Ok(v),
                Err(s) if s == DomainError::NotFound.to_string() => {
                    Err(DomainError::NotFound)
                }
                Err(s) => Err(DomainError::Io(format!("coalesced read failed: {s}"))),
            }
        }
    }

    fn put(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()> {
        self.inner.put(key, data)?;
        self.populate(key, data); // write-through: best-effort
        Ok(())
    }

    /// HEAD — индекс пула и так O(lookup); кэш не трогаем.
    fn stat(&self, key: &BlockKey) -> DomainResult<u64> {
        self.inner.stat(key)
    }

    fn has(&self, key: &BlockKey) -> DomainResult<bool> {
        self.inner.has(key)
    }

    fn delete(&self, key: &BlockKey) -> DomainResult<()> {
        self.inner.delete(key)?;
        let _ = self.cache.delete(key); // единственная «инвалидация»
        Ok(())
    }

    fn list(
        &self,
        prefix: &[u8],
        after: Option<&BlockKey>,
        limit: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>> {
        self.inner.list(prefix, after, limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozd_engine::{DiskEngine, EngineConfig};
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    /// Внутренний стор со счётчиком чтений и настраиваемой задержкой.
    struct SlowStore {
        data: Mutex<BTreeMap<BlockKey, Vec<u8>>>,
        gets: AtomicUsize,
        delay: Duration,
    }
    impl SlowStore {
        fn new(delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                data: Mutex::new(BTreeMap::new()),
                gets: AtomicUsize::new(0),
                delay,
            })
        }
    }
    impl BlockStore for SlowStore {
        fn put(&self, k: &BlockKey, d: &[u8]) -> DomainResult<()> {
            self.data.lock().insert(k.clone(), d.to_vec());
            Ok(())
        }
        fn get(&self, k: &BlockKey) -> DomainResult<Vec<u8>> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(self.delay);
            self.data.lock().get(k).cloned().ok_or(DomainError::NotFound)
        }
        fn has(&self, k: &BlockKey) -> DomainResult<bool> {
            Ok(self.data.lock().contains_key(k))
        }
        fn delete(&self, k: &BlockKey) -> DomainResult<()> {
            self.data.lock().remove(k);
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

    fn mk_cache(dir: &tempfile::TempDir, seg_max: u64) -> Arc<dyn ShardEngine> {
        Arc::new(
            DiskEngine::open(EngineConfig {
                data_path: dir.path().to_path_buf(),
                segment_max_size: seg_max,
                inline_min: 64,
                fsync_items: 1024,
                compress_zstd: false, // кэш не жмём: тела уже сжаты пулом
                ..Default::default()
            })
            .unwrap(),
        )
    }

    fn tier(
        inner: Arc<SlowStore>,
        dir: &tempfile::TempDir,
        max_bytes: u64,
        seg_max: u64,
    ) -> CacheTier {
        CacheTier::new(
            inner,
            mk_cache(dir, seg_max),
            CacheConfig { max_bytes, min_size: 256 },
            Arc::new(OpsMetrics::default()),
        )
    }

    #[test]
    fn write_through_then_reads_hit_nvme_leg() {
        let inner = SlowStore::new(Duration::ZERO);
        let d = tempfile::tempdir().unwrap();
        let t = tier(inner.clone(), &d, 0, 1 << 20);
        let key = BlockKey::from("/blocks/hot");
        let body = vec![7u8; 50_000];
        t.put(&key, &body).unwrap();
        assert!(inner.has(&key).unwrap(), "write-through: durable-нога получила тело");
        // чтение — из NVMe-ноги, durable-нога НЕ трогается (#143)
        for _ in 0..5 {
            assert_eq!(t.get(&key).unwrap(), body);
        }
        assert_eq!(inner.gets.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(t.metrics.cache_hits.load(Relaxed), 5);
        // мелочь (< min_size) не кэшируется — идёт с durable-ноги
        let ks = BlockKey::from("/blocks/tiny");
        t.put(&ks, &[1u8; 100]).unwrap();
        assert_eq!(t.get(&ks).unwrap(), vec![1u8; 100]);
        assert_eq!(inner.gets.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn miss_populates_and_corruption_self_heals() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let inner = SlowStore::new(Duration::ZERO);
        let d = tempfile::tempdir().unwrap();
        let t = tier(inner.clone(), &d, 0, 1 << 20);
        let key = BlockKey::from("/blocks/mh");
        let body: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
        inner.put(&key, &body).unwrap(); // мимо кэша (холодный старт)

        assert_eq!(t.get(&key).unwrap(), body); // промах → populate
        assert_eq!(inner.gets.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(t.get(&key).unwrap(), body); // хит
        assert_eq!(inner.gets.load(std::sync::atomic::Ordering::SeqCst), 1);

        // bitrot в кэш-копии: CRC движка ловит → self-heal с durable-ноги
        t.cache.flush().unwrap();
        let seg = d.path().join("seg").join("seg.00000000.dat");
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&seg).unwrap();
        let off = 20 + key.len() as u64 + 1000;
        f.seek(SeekFrom::Start(off)).unwrap();
        let mut b = [0u8; 1];
        f.read_exact(&mut b).unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        f.write_all(&[b[0] ^ 0xFF]).unwrap();
        drop(f);

        assert_eq!(t.get(&key).unwrap(), body, "битая кэш-копия → тело с пула");
        assert_eq!(t.metrics.cache_self_heals.load(Relaxed), 1);
        assert_eq!(inner.gets.load(std::sync::atomic::Ordering::SeqCst), 2);
        // кэш вылечен — снова хиты
        assert_eq!(t.get(&key).unwrap(), body);
        assert_eq!(inner.gets.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn fifo_eviction_keeps_budget_and_serves_from_pool() {
        let inner = SlowStore::new(Duration::ZERO);
        let d = tempfile::tempdir().unwrap();
        // сегмент 32КБ, бюджет 100КБ → старые сегменты уезжают целиком
        let t = tier(inner.clone(), &d, 100 * 1024, 32 * 1024);
        let body = |i: u8| vec![i; 16 * 1024];
        for i in 0..20u8 {
            t.put(&BlockKey::new(format!("/blocks/ev{i:02}")), &body(i)).unwrap();
        }
        assert!(
            t.metrics.cache_evicted_segments.load(Relaxed) >= 3,
            "эвикция шла: {}",
            t.metrics.cache_evicted_segments.load(Relaxed)
        );
        assert!(
            t.cached_bytes() <= 110 * 1024,
            "бюджет держится: {}",
            t.cached_bytes()
        );
        // свежайший ключ — ещё в кэше (активный сегмент не эвиктится)
        assert_eq!(t.get(&BlockKey::from("/blocks/ev19")).unwrap(), body(19));
        assert!(t.metrics.cache_hits.load(Relaxed) >= 1, "свежий хвост — из кэша");
        // ВСЁ читаемо: эвикнутое — прозрачно с durable-ноги (+ре-populate
        // с новой эвикцией — бюджет держится и под чёрном)
        for i in 0..20u8 {
            let k = BlockKey::new(format!("/blocks/ev{i:02}"));
            assert_eq!(t.get(&k).unwrap(), body(i), "ev{i:02}");
        }
        assert!(
            inner.gets.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "эвикнутое — с пула"
        );
        assert!(t.cached_bytes() <= 110 * 1024, "бюджет после чёрна: {}", t.cached_bytes());
    }

    #[test]
    fn coalescing_collapses_concurrent_reads_to_one() {
        // #144: 8 конкурентных GET холодного ключа = ОДНО чтение durable-ноги
        let inner = SlowStore::new(Duration::from_millis(200));
        let d = tempfile::tempdir().unwrap();
        let t = Arc::new(tier(inner.clone(), &d, 0, 1 << 20));
        let key = BlockKey::from("/blocks/viral");
        let body = vec![9u8; 30_000];
        inner.put(&key, &body).unwrap();

        std::thread::scope(|sc| {
            for _ in 0..8 {
                let t = t.clone();
                let key = key.clone();
                let body = body.clone();
                sc.spawn(move || {
                    assert_eq!(t.get(&key).unwrap(), body);
                });
            }
        });
        assert_eq!(
            inner.gets.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "single-flight: восемь читателей — одно чтение с диска"
        );
        assert!(t.metrics.cache_coalesced.load(Relaxed) >= 7);
        // ключ горячий: дальше — чистые хиты
        assert_eq!(t.get(&key).unwrap(), body);
        assert_eq!(inner.gets.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn delete_invalidates_both_legs() {
        let inner = SlowStore::new(Duration::ZERO);
        let d = tempfile::tempdir().unwrap();
        let t = tier(inner.clone(), &d, 0, 1 << 20);
        let key = BlockKey::from("/blocks/gone");
        t.put(&key, &vec![3u8; 10_000]).unwrap();
        assert!(t.get(&key).is_ok());
        t.delete(&key).unwrap();
        assert!(matches!(t.get(&key), Err(DomainError::NotFound)));
        assert!(!t.cache.has(&key).unwrap(), "кэш-нога инвалидирована");
    }
}
