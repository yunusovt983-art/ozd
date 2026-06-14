//! ozd-bench (E29) — воспроизводимый нагрузочный харнесс storage-слоя.
//!
//! In-process (без HTTP — шлюз бенчится на стенде E30): Pool (+опц.
//! CacheTier-СуперДиск) на N временных «дисков». Профиль тел — Kubo:
//! 75% × 256КиБ (дефолтный chunker), 15% × 4КБ (dag-узлы),
//! 10% × 1МиБ (rawleaves). Чтения — hot-set: 90% запросов в 10% ключей.
//! Перцентили честные: полный набор сэмплов, сортировка.
//!
//! Запуск:  cargo run -p ozd-bench --release -- \
//!            --disks=6 --objects=1000 --reads=5000 \
//!            --redundancy=erasure --cache-mb=512 --threads=4 --seed=42
//!
//! Отчёт включает справку для вердикта E24 (микроблоки 16КБ): измеряет
//! range-амплификацию текущего пути (читаем тело целиком ради 16КиБ).

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ozd_app::cache::{CacheConfig, CacheTier};
use ozd_app::erasure::EcConfig;
use ozd_app::pool::{Pool, PoolConfig};
use ozd_app::verified::{self, ObConfig};
use ozd_app::RendezvousHrw;
use ozd_domain::{BlockKey, BlockStore, ShardEngine};
use ozd_engine::{DiskEngine, EngineConfig};

struct Args {
    disks: usize,
    objects: usize,
    reads: usize,
    hot_keys_frac: f64,
    hot_traffic: f64,
    redundancy: String,
    cache_mb: u64,
    threads: usize,
    seed: u64,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            disks: 6,
            objects: 1000,
            reads: 5000,
            hot_keys_frac: 0.10,
            hot_traffic: 0.90,
            redundancy: "mirror".into(),
            cache_mb: 0,
            threads: 4,
            seed: 42,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    for arg in std::env::args().skip(1) {
        let Some((k, v)) = arg.strip_prefix("--").and_then(|s| s.split_once('=')) else {
            eprintln!("ожидаю --key=value, получил: {arg}");
            std::process::exit(2);
        };
        match k {
            "disks" => a.disks = v.parse().unwrap(),
            "objects" => a.objects = v.parse().unwrap(),
            "reads" => a.reads = v.parse().unwrap(),
            "hot-keys" => a.hot_keys_frac = v.parse().unwrap(),
            "hot-traffic" => a.hot_traffic = v.parse().unwrap(),
            "redundancy" => a.redundancy = v.into(),
            "cache-mb" => a.cache_mb = v.parse().unwrap(),
            "threads" => a.threads = v.parse().unwrap(),
            "seed" => a.seed = v.parse().unwrap(),
            other => {
                eprintln!("неизвестный ключ: --{other}");
                std::process::exit(2);
            }
        }
    }
    a
}

/// xorshift64* — детерминированный, быстрый (заполнение тел и выбор ключей).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Kubo-микс размеров тел.
fn body_size(rng: &mut Rng) -> usize {
    let r = rng.f64();
    if r < 0.75 {
        256 * 1024
    } else if r < 0.90 {
        4 * 1024
    } else {
        1024 * 1024
    }
}

fn fill_body(rng: &mut Rng, size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size];
    for chunk in v.chunks_exact_mut(8) {
        chunk.copy_from_slice(&rng.next().to_le_bytes());
    }
    v
}

fn pctl(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[idx]
}

fn ms(us: u64) -> String {
    format!("{:.2}мс", us as f64 / 1000.0)
}

fn lat_line(name: &str, mut us: Vec<u64>, total_bytes: u64, wall: Duration) -> Vec<u64> {
    us.sort_unstable();
    let thr = total_bytes as f64 / 1024.0 / 1024.0 / wall.as_secs_f64();
    let ops = us.len() as f64 / wall.as_secs_f64();
    println!(
        "{name:5} p50={} p99={} p999={} max={} | {:.0} оп/с, {:.1} МиБ/с",
        ms(pctl(&us, 0.50)),
        ms(pctl(&us, 0.99)),
        ms(pctl(&us, 0.999)),
        ms(*us.last().unwrap_or(&0)),
        ops,
        thr,
    );
    us
}

fn main() {
    let a = parse_args();
    let root = tempfile::tempdir().expect("tempdir");
    println!(
        "ozd-bench: disks={} redundancy={} cache={}МиБ objects={} reads={} \
         hot={}%ключей/{}%трафика threads={} seed={} dir={}",
        a.disks,
        a.redundancy,
        a.cache_mb,
        a.objects,
        a.reads,
        (a.hot_keys_frac * 100.0) as u32,
        (a.hot_traffic * 100.0) as u32,
        a.threads,
        a.seed,
        root.path().display()
    );

    // --- пул ---
    let shards: Vec<Arc<dyn ShardEngine>> = (0..a.disks)
        .map(|i| {
            let dir = root.path().join(format!("d{i:02}"));
            std::fs::create_dir_all(&dir).unwrap();
            Arc::new(
                DiskEngine::open(EngineConfig {
                    data_path: dir,
                    segment_max_size: 256 * 1024 * 1024,
                    inline_min: 4096,
                    fsync_items: 256,
                    compress_zstd: false, // тела случайные — не жмутся
                    ..Default::default()
                })
                .unwrap(),
            ) as Arc<dyn ShardEngine>
        })
        .collect();
    let ec = match a.redundancy.as_str() {
        "erasure" => Some(EcConfig::default()),
        "mirror" => None,
        other => panic!("redundancy={other}? (mirror|erasure)"),
    };
    let pool = Arc::new(Pool::new(
        shards,
        Box::new(RendezvousHrw::default()),
        PoolConfig {
            replicas: 2,
            write_quorum: 2,
            ec,
            outboard: Some(ObConfig::default()), // нужен для E24-справки
            ..Default::default()
        },
    ));
    let store: Arc<dyn BlockStore> = if a.cache_mb > 0 {
        let cdir = root.path().join("nvme-cache");
        std::fs::create_dir_all(&cdir).unwrap();
        let ceng = Arc::new(
            DiskEngine::open(EngineConfig {
                data_path: cdir,
                segment_max_size: 64 * 1024 * 1024,
                inline_min: 64,
                fsync_items: 4096,
                ..Default::default()
            })
            .unwrap(),
        ) as Arc<dyn ShardEngine>;
        Arc::new(CacheTier::new(
            pool.clone(),
            ceng,
            CacheConfig { max_bytes: a.cache_mb * 1024 * 1024, min_size: 4096 },
            pool.metrics(),
        ))
    } else {
        pool.clone()
    };

    // --- фаза PUT ---
    let mut rng = Rng(a.seed | 1);
    let mut keys: Vec<(BlockKey, usize)> = Vec::with_capacity(a.objects);
    let mut put_us = Vec::with_capacity(a.objects);
    let mut put_bytes = 0u64;
    let t_put = Instant::now();
    for i in 0..a.objects {
        let size = body_size(&mut rng);
        let body = fill_body(&mut rng, size);
        let key = BlockKey::new(format!("/blocks/BENCH{i:08}"));
        let t = Instant::now();
        store.put(&key, &body).expect("put");
        put_us.push(t.elapsed().as_micros() as u64);
        put_bytes += size as u64;
        keys.push((key, size));
    }
    pool.flush_all().expect("flush");
    let put_wall = t_put.elapsed();
    lat_line("PUT", put_us, put_bytes, put_wall);

    // --- фаза GET (hot-set, параллельно) ---
    let hot_n = ((a.objects as f64 * a.hot_keys_frac) as usize).max(1);
    let plan: Vec<usize> = (0..a.reads)
        .map(|_| {
            if rng.f64() < a.hot_traffic {
                rng.below(hot_n)
            } else {
                rng.below(a.objects)
            }
        })
        .collect();
    let next = AtomicUsize::new(0);
    let read_bytes = std::sync::atomic::AtomicU64::new(0);
    let samples: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::new(Vec::new());
    let t_get = Instant::now();
    std::thread::scope(|sc| {
        for _ in 0..a.threads.max(1) {
            sc.spawn(|| {
                let mut local = Vec::new();
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    let Some(ki) = plan.get(i) else { break };
                    let (key, size) = &keys[*ki];
                    let t = Instant::now();
                    let v = store.get(key).expect("get");
                    local.push(t.elapsed().as_micros() as u64);
                    assert_eq!(v.len(), *size);
                    read_bytes.fetch_add(*size as u64, Relaxed);
                }
                samples.lock().unwrap().extend(local);
            });
        }
    });
    let get_wall = t_get.elapsed();
    let get_us = samples.into_inner().unwrap();
    lat_line("GET", get_us, read_bytes.load(Relaxed), get_wall);

    let m = pool.metrics();
    let (h, mi) = (m.cache_hits.load(Relaxed), m.cache_misses.load(Relaxed));
    if a.cache_mb > 0 {
        println!(
            "СуперДиск: hits={h} misses={mi} hit-rate={:.1}% coalesced={} evicted={}сег",
            100.0 * h as f64 / (h + mi).max(1) as f64,
            m.cache_coalesced.load(Relaxed),
            m.cache_evicted_segments.load(Relaxed),
        );
    }
    println!(
        "пул: hedged={} handoff={} ec_reconstructs={} hedge_thr={}мс",
        m.hedged_reads.load(Relaxed),
        m.handoff_writes.load(Relaxed),
        m.ec_reconstructs.load(Relaxed),
        m.hedge_threshold_ms.load(Relaxed),
    );

    // --- справка для вердикта E24 (микроблоки 16КБ) ---
    let big: Vec<&(BlockKey, usize)> =
        keys.iter().filter(|(_, s)| *s == 256 * 1024).take(100).collect();
    if !big.is_empty() {
        let mut full_us = Vec::new();
        let mut slice_us = Vec::new();
        for (key, _) in &big {
            let t = Instant::now();
            let body = pool.get(key).unwrap(); // мимо кэша: честный путь пула
            full_us.push(t.elapsed().as_micros() as u64);
            if let Ok(ob) = pool.get(&verified::ob_key(key)) {
                let t = Instant::now();
                let _ = verified::verified_slice(&body, &ob, 65536, 16384).unwrap();
                slice_us.push(t.elapsed().as_micros() as u64);
            }
        }
        full_us.sort_unstable();
        slice_us.sort_unstable();
        println!("--- E24-справка (микроблоки 16КиБ) ---");
        println!(
            "GET 256КиБ целиком (путь пула): p50={}; verify 16КиБ поверх: p50={}",
            ms(pctl(&full_us, 0.5)),
            ms(pctl(&slice_us, 0.5)),
        );
        println!(
            "range-амплификация ТЕКУЩЕГО пути: 256КиБ с диска ради 16КиБ = 16×;\n\
             НО: Kubo (go-ds-s3) читает блоки ТОЛЬКО целиком — Range-трафика нет;\n\
             вердикт: E24 остаётся 🧊, пересмотреть если в профиле появится\n\
             прямой S3-Range трафик (см. ozd_*range* метрики на стенде E30)."
        );
    }
    println!("done за {:.1}с (отчёт воспроизводим: фиксированный seed)",
        t_put.elapsed().as_secs_f64() + get_wall.as_secs_f64());
}
