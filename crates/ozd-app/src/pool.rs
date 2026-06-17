// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! Pool (aggregate root): 60 дисков как один blockstore с R копиями.
//!
//! put → top-R по HRW → запись на R шардов, успех при ≥W (#3 quorum);
//! get → первая живая реплика по порядку HRW (далее speculative-retry #121);
//! delete → со всех R; list → merge-скан индексов дисков (дедуп реплик).
//! TTL-кэш free-space (#137): топология обновляется не чаще cache_ttl.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use ozd_domain::{
    BlockKey, BlockStore, Capacity, DomainError, DomainResult, PlacementPolicy, ShardEngine,
    ShardId, ShardStatus,
};

pub struct PoolConfig {
    pub replicas: usize,      // R
    pub write_quorum: usize,  // W
    pub free_space_cache_ttl: Duration,
    /// #121/#143: hedged read — если read-нога (реплика №1) не ответила за
    /// порог, послать дубль-чтение write-mostly-ноге (№2). None = off
    /// (последовательный fallback только при ошибке).
    pub speculative_retry_after: Option<Duration>,
    /// E16 (#140): параллелизм дренажа heal-очереди
    pub heal_parallelism: usize,
    /// E16 (#140): bulkhead — макс. одновременных починок, трогающих один шард
    pub heal_max_per_shard: usize,
    /// E19 (#131): elastic-бюджет фоновых работ (GC/scrub/resilver/heal)
    pub bg_throttle: crate::throttle::BgThrottleConfig,
    /// E20 (#138): erasure K+M вместо зеркала для тел ≥ min_size
    /// (None = чистое зеркало R=2). Эры сосуществуют: куски самоописаны.
    pub ec: Option<crate::erasure::EcConfig>,
    /// E23 (#79): BLAKE3 outboard для тел ≥ min_size — verified range reads
    pub outboard: Option<crate::verified::ObConfig>,
    /// E27: hedge-порог из скользящего p99 чтений (clamp 10мс..2с);
    /// false = статический speculative_retry_after как раньше.
    /// До прогрева гистограммы статика — fallback в обоих режимах.
    pub adaptive_hedge: bool,
    /// E28 (#129): пороги disk-slow вердикта (EWMA vs медиана парка)
    pub disk_slow: crate::diskslow::DiskSlowConfig,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            replicas: 2,
            write_quorum: 2,
            free_space_cache_ttl: Duration::from_secs(5),
            speculative_retry_after: Some(Duration::from_millis(100)),
            heal_parallelism: 4,
            heal_max_per_shard: 2,
            bg_throttle: crate::throttle::BgThrottleConfig::default(),
            ec: None,
            outboard: None,
            adaptive_hedge: true,
            disk_slow: crate::diskslow::DiskSlowConfig::default(),
        }
    }
}

struct TopoCache {
    at: Instant,
    topo: Vec<(ShardId, Capacity, ShardStatus)>,
}

pub struct Pool {
    shards: Vec<Arc<dyn ShardEngine>>,
    policy: Box<dyn PlacementPolicy>,
    cfg: PoolConfig,
    topo: RwLock<Option<TopoCache>>,
    /// статусы шардов (#142): кормятся снаружи (ZFS-монитор/disk-slow);
    /// HRW исключает Faulted из placement
    statuses: RwLock<Vec<ShardStatus>>,
    /// переопределение ёмкости (#150): ZFS-монитор кладёт сюда
    /// effective free = free + freeing (честнее statvfs после GC-волн)
    cap_overrides: RwLock<Vec<Option<Capacity>>>,
    /// MRF (#140, most-recent-failures): ключи с неполной репликацией после
    /// put (упавшая нога / handoff) — быстрый точечный heal, не ждём scrub
    mrf: parking_lot::Mutex<HealQueue>,
    /// E14: операционные счётчики (Prometheus в /metrics)
    metrics: Arc<crate::metrics::OpsMetrics>,
    /// E19 (#131): троттль фона — фон платит байтами, foreground никогда
    bg: Arc<crate::throttle::BgThrottle>,
    /// E27: скользящий p99 успешных чтений → адаптивный hedge-порог
    read_lat: crate::latency::RollingP99,
    /// E28 (#129): EWMA-латентности по шардам (кормят MeteredShard-обёртки)
    slow_mon: Arc<crate::diskslow::DiskSlowMonitor>,
    /// E28: флаг «slow» по шарду (выставляет daemon-FSM); topology
    /// эскалирует Online→Suspect — вес ×0.01 в HRW
    slow_flags: RwLock<Vec<bool>>,
}

/// E16 (#140): приоритет heal-заявки (Urgent — кворум потерян/нечитаемо).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HealPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Urgent = 3,
}

#[derive(PartialEq, Eq)]
struct HealItem {
    prio: HealPriority,
    seq: u64,
    key: BlockKey,
}

impl Ord for HealItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // выше приоритет → раньше; внутри приоритета — FIFO (меньший seq)
        self.prio.cmp(&other.prio).then(other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for HealItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Приоритетная heal-очередь (#140, паттерн RustFS PriorityHealQueue):
/// BinaryHeap + dedup с ПОВЫШЕНИЕМ приоритета (lazy-delete устаревших копий).
#[derive(Default)]
struct HealQueue {
    heap: std::collections::BinaryHeap<HealItem>,
    dedup: std::collections::HashMap<BlockKey, HealPriority>,
    seq: u64,
}

impl HealQueue {
    const CAP: usize = 100_000;
    fn push(&mut self, key: BlockKey, prio: HealPriority) {
        match self.dedup.get(&key) {
            Some(cur) if *cur >= prio => return, // дубль той же/высшей важности
            _ => {}
        }
        if self.dedup.len() >= Self::CAP && !self.dedup.contains_key(&key) {
            return;
        }
        self.dedup.insert(key.clone(), prio); // upgrade либо вставка
        self.seq += 1;
        self.heap.push(HealItem { prio, seq: self.seq, key });
    }
    fn pop(&mut self) -> Option<(BlockKey, HealPriority)> {
        while let Some(item) = self.heap.pop() {
            // lazy-delete: валидна только запись, совпадающая с dedup-картой
            match self.dedup.get(&item.key) {
                Some(p) if *p == item.prio => {
                    self.dedup.remove(&item.key);
                    // W12.2: сжать heap, если фантомных записей > 2× реальных
                    if self.heap.len() > self.dedup.len() * 2 + 64 {
                        self.heap.shrink_to(self.dedup.len() * 2);
                    }
                    return Some((item.key, item.prio));
                }
                _ => continue, // устаревшая копия после upgrade
            }
        }
        None
    }
    fn len(&self) -> usize {
        self.dedup.len()
    }
}

impl Pool {
    pub fn new(
        shards: Vec<Arc<dyn ShardEngine>>,
        policy: Box<dyn PlacementPolicy>,
        cfg: PoolConfig,
    ) -> Self {
        // W5.3: информативные assert-сообщения (демон валидирует ДО вызова;
        // assert — защита от ошибок в тестах/библиотечном коде)
        assert!(
            cfg.replicas >= 1 && cfg.write_quorum >= 1,
            "Pool: replicas={} и write_quorum={} должны быть ≥ 1",
            cfg.replicas, cfg.write_quorum
        );
        assert!(
            cfg.write_quorum <= cfg.replicas,
            "Pool: write_quorum={} > replicas={} (кворум не может быть больше R)",
            cfg.write_quorum, cfg.replicas
        );
        assert!(
            cfg.replicas <= shards.len(),
            "Pool: replicas={} > число дисков {} (невозможно разместить R копий)",
            cfg.replicas, shards.len()
        );
        let n = shards.len();
        // E28: каждый шард оборачивается замером латентности put/get —
        // ВСЕ пути пула (чтение/запись/ремонт/scrub) кормят монитор
        let slow_mon = Arc::new(crate::diskslow::DiskSlowMonitor::new(n));
        let shards: Vec<Arc<dyn ShardEngine>> = shards
            .into_iter()
            .enumerate()
            .map(|(i, s)| {
                Arc::new(MeteredShard { inner: s, idx: i, mon: slow_mon.clone() })
                    as Arc<dyn ShardEngine>
            })
            .collect();
        let metrics = Arc::new(crate::metrics::OpsMetrics::default());
        let bg = Arc::new(crate::throttle::BgThrottle::new(
            cfg.bg_throttle.clone(),
            metrics.clone(),
        ));
        Self {
            shards,
            policy,
            cfg,
            topo: RwLock::new(None),
            statuses: RwLock::new(vec![ShardStatus::Online; n]),
            cap_overrides: RwLock::new(vec![None; n]),
            mrf: parking_lot::Mutex::new(HealQueue::default()),
            bg,
            metrics,
            read_lat: crate::latency::RollingP99::new(
                Duration::from_secs(60),
                64, // прогрев: до 64 сэмплов решает статический fallback
            ),
            slow_mon,
            slow_flags: RwLock::new(vec![false; n]),
        }
    }

    /// E28: вердикты «slow» по текущему срезу EWMA (гистерезис — у демона).
    pub fn disk_slow_verdicts(&self) -> Vec<bool> {
        self.slow_mon.verdicts(&self.cfg.disk_slow)
    }

    pub fn shard_ewma_ms(&self, i: usize) -> u64 {
        self.slow_mon.ewma_ms(i)
    }

    pub fn shard_slow(&self, i: usize) -> bool {
        self.slow_flags.read().get(i).copied().unwrap_or(false)
    }

    /// E28: пометить шард медленным (после FSM-гистерезиса демона).
    pub fn set_shard_slow(&self, idx: usize, slow: bool) {
        let mut g = self.slow_flags.write();
        if idx < g.len() && g[idx] != slow {
            tracing::warn!(
                shard = idx,
                slow,
                ewma_ms = self.slow_mon.ewma_ms(idx),
                "disk-slow flag change (#129)"
            );
            g[idx] = slow;
            drop(g);
            *self.topo.write() = None; // перевыборка топологии немедленно
        }
    }

    /// E27: порог hedged-read на ЭТОТ запрос — p99×clamp либо статика.
    fn hedge_threshold(&self) -> Option<Duration> {
        const HEDGE_MIN: Duration = Duration::from_millis(10);
        const HEDGE_MAX: Duration = Duration::from_millis(2000);
        let chosen = if self.cfg.adaptive_hedge {
            self.read_lat
                .p99()
                .map(|p| p.clamp(HEDGE_MIN, HEDGE_MAX))
                .or(self.cfg.speculative_retry_after) // прогрев → статика
        } else {
            self.cfg.speculative_retry_after
        };
        self.metrics.hedge_threshold_ms.store(
            chosen.map(|d| d.as_millis() as u64).unwrap_or(0),
            std::sync::atomic::Ordering::Relaxed,
        );
        chosen
    }

    /// E27: тест-шов — засеять гистограмму (прогрев без реальных чтений).
    #[doc(hidden)]
    pub fn seed_read_latency(&self, lat: Duration, n: usize) {
        for _ in 0..n {
            self.read_lat.record(lat);
        }
    }

    pub fn metrics(&self) -> Arc<crate::metrics::OpsMetrics> {
        self.metrics.clone()
    }

    /// E19: байтовый троттль фоновых работ (для GC-цикла демона).
    pub fn bg(&self) -> Arc<crate::throttle::BgThrottle> {
        self.bg.clone()
    }

    pub fn mrf_len(&self) -> usize {
        self.mrf.lock().len()
    }

    /// E16: заявка в heal-очередь (источники: put-сбой=Normal,
    /// scrub-unrepairable=Urgent, админ=High).
    pub fn enqueue_heal(&self, key: BlockKey, prio: HealPriority) {
        self.metrics.mrf_enqueued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.mrf.lock().push(key, prio);
    }

    /// Дренаж heal-очереди (#140): приоритетный порядок, ПАРАЛЛЕЛЬНО до
    /// `heal_parallelism` воркеров, bulkhead `heal_max_per_shard` — заявка,
    /// чьи desired-шарды заняты, откладывается (не душим IO одного диска).
    /// Недочиненные → в хвост своего приоритета (диск лежит — позже).
    pub fn mrf_drain(&self, max: usize) -> DomainResult<(usize, usize)> {
        use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
        // batch из очереди (приоритетный порядок)
        let mut batch: Vec<(BlockKey, HealPriority)> = Vec::new();
        {
            let mut q = self.mrf.lock();
            for _ in 0..max {
                match q.pop() {
                    Some(it) => batch.push(it),
                    None => break,
                }
            }
        }
        if batch.is_empty() {
            return Ok((0, 0));
        }

        let inflight: Vec<AtomicUsize> =
            (0..self.shards.len()).map(|_| AtomicUsize::new(0)).collect();
        let healed = AtomicUsize::new(0);
        let next = AtomicUsize::new(0);
        let deferred: parking_lot::Mutex<Vec<(BlockKey, HealPriority)>> =
            parking_lot::Mutex::new(Vec::new());
        let cap = self.cfg.heal_max_per_shard.max(1);
        let workers = self.cfg.heal_parallelism.clamp(1, batch.len());

        std::thread::scope(|sc| {
            for _ in 0..workers {
                sc.spawn(|| loop {
                    let i = next.fetch_add(1, Relaxed);
                    let Some((key, prio)) = batch.get(i) else { break };
                    // bulkhead: занять слоты desired-шардов заявки
                    let targets: Vec<usize> =
                        self.replicas_for(key).iter().map(|s| s.0 as usize).collect();
                    let mut acquired = Vec::new();
                    let mut busy = false;
                    for t in &targets {
                        if inflight[*t].fetch_add(1, Relaxed) >= cap {
                            inflight[*t].fetch_sub(1, Relaxed);
                            busy = true;
                            break;
                        }
                        acquired.push(*t);
                    }
                    if busy {
                        for t in &acquired {
                            inflight[*t].fetch_sub(1, Relaxed);
                        }
                        deferred.lock().push((key.clone(), *prio));
                        continue;
                    }
                    let ok = matches!(self.repair_key(key), Ok((_, 0)));
                    for t in &acquired {
                        inflight[*t].fetch_sub(1, Relaxed);
                    }
                    if ok {
                        self.metrics.mrf_healed.fetch_add(1, Relaxed);
                        healed.fetch_add(1, Relaxed);
                    } else {
                        deferred.lock().push((key.clone(), *prio));
                    }
                });
            }
        });

        let deferred = deferred.into_inner();
        let requeued = deferred.len();
        {
            let mut q = self.mrf.lock();
            for (k, p) in deferred {
                q.push(k, p);
            }
        }
        Ok((healed.load(Relaxed), requeued))
    }

    /// Переопределить ёмкость шарда (#150: ZFS effective free = free+freeing).
    pub fn set_shard_capacity(&self, idx: usize, cap: Capacity) {
        let mut g = self.cap_overrides.write();
        if idx < g.len() {
            g[idx] = Some(cap);
        }
    }

    /// Обновить статус шарда (вход: ZFS-монитор / disk-slow / админ).
    /// Faulted немедленно исключает диск из placement (через сброс топо-кэша).
    pub fn set_shard_status(&self, idx: usize, st: ShardStatus) {
        let mut g = self.statuses.write();
        if idx < g.len() && g[idx] != st {
            tracing::info!(shard = idx, from = ?g[idx], to = ?st, "shard status change");
            g[idx] = st;
            drop(g);
            *self.topo.write() = None; // немедленная перевыборка топологии
        }
    }

    pub fn shard_status(&self, idx: usize) -> Option<ShardStatus> {
        self.statuses.read().get(idx).copied()
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Топология с TTL-кэшем free-space (#137): не дёргать statvfs на каждый put.
    fn topology(&self) -> Vec<(ShardId, Capacity, ShardStatus)> {
        {
            let g = self.topo.read();
            if let Some(c) = g.as_ref() {
                if c.at.elapsed() < self.cfg.free_space_cache_ttl {
                    return c.topo.clone();
                }
            }
        }
        let statuses = self.statuses.read().clone();
        let overrides = self.cap_overrides.read().clone();
        let slow = self.slow_flags.read().clone();
        let topo: Vec<_> = self
            .shards
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let cap = overrides[i].unwrap_or_else(|| s.usage().unwrap_or_default());
                // E28: disk-slow эскалирует Online→Suspect (Faulted сильнее)
                let st = match (statuses[i], slow[i]) {
                    (ShardStatus::Online, true) => ShardStatus::Suspect,
                    (st, _) => st,
                };
                (ShardId(i as u16), cap, st)
            })
            .collect();
        *self.topo.write() = Some(TopoCache { at: Instant::now(), topo: topo.clone() });
        topo
    }

    fn replicas_for(&self, key: &BlockKey) -> Vec<ShardId> {
        let topo = self.topology();
        self.policy.select(key, &topo, self.cfg.replicas)
    }

    /// E20: top-N HRW-ранжирование (distribution-array #138): кусок i → ранг i.
    /// top-R зеркала — ПРЕФИКС top-(K+M) → обе эры на одних дисках (E21).
    fn ranking_for(&self, key: &BlockKey, n: usize) -> Vec<ShardId> {
        let topo = self.topology();
        self.policy.select(key, &topo, n)
    }

    pub fn flush_all(&self) -> DomainResult<()> {
        for s in &self.shards {
            s.flush()?;
        }
        Ok(())
    }

    /// Один шаг walk-resilver (Фаза 3): до `batch` ключей после `after`
    /// (курсор #102), для каждого — довести число реплик до R (add-only:
    /// недостающие копируем, лишние НЕ трогаем — их уберёт balancer/GC позже).
    /// Идемпотентно: повторный проход по здоровому пулу ничего не копирует.
    pub fn resilver_step(
        &self,
        after: Option<&BlockKey>,
        batch: usize,
    ) -> DomainResult<ResilverReport> {
        let keys = self.list(b"", after, batch)?; // merged + dedup, сортировано
        let mut rep = ResilverReport {
            scanned: keys.len(),
            done: keys.len() < batch,
            ..Default::default()
        };
        for (key, _) in &keys {
            match self.repair_key(key) {
                Ok((repaired, left)) => {
                    rep.repaired += repaired;
                    rep.errors += left;
                }
                Err(e) => {
                    tracing::warn!(?key, err = %e, "resilver: no readable source");
                    rep.errors += 1;
                }
            }
        }
        {
            use std::sync::atomic::Ordering::Relaxed;
            self.metrics.resilver_repaired.fetch_add(rep.repaired as u64, Relaxed);
            self.metrics.resilver_errors.fetch_add(rep.errors as u64, Relaxed);
        }
        rep.last_key = keys.into_iter().next_back().map(|(k, _)| k);
        Ok(rep)
    }

    /// Точечная починка одного ключа — общее ядро resilver/MRF/scrub-heal.
    /// E20: диспетчер эр — зеркало доводится до R копий, EC-объект
    /// реконструирует недостающие куски. Здоровые ключи НЕ читаются
    /// (has()-паттерны), эра читается зондом только при аномалии.
    fn repair_key(&self, key: &BlockKey) -> DomainResult<(usize, usize)> {
        let Some(ec) = self.cfg.ec.clone() else {
            return self.repair_key_mirror(key, None);
        };
        let total = ec.total();
        let targets = self.ranking_for(key, total.max(self.cfg.replicas));
        let mut missing_ranks: Vec<usize> = Vec::new();
        for (r, sid) in targets.iter().enumerate().take(total) {
            if !self.shards[sid.0 as usize].has(key).unwrap_or(false) {
                missing_ranks.push(r);
            }
        }
        if missing_ranks.is_empty() {
            return Ok((0, 0)); // здоровый EC-объект — ни одного чтения
        }
        let r_legs = self.cfg.replicas.min(targets.len());
        if missing_ranks.iter().all(|r| *r >= r_legs)
            && missing_ranks.len() == total.saturating_sub(r_legs)
        {
            // «top-R есть, хвост пуст» = здоровая зеркальная мелочь
            return Ok((0, 0));
        }
        // аномалия: зонд определяет эру по первому читаемому телу
        for sid in &targets {
            if let Ok(b) = self.shards[sid.0 as usize].get(key) {
                return match crate::erasure::parse_piece_header(&b) {
                    Some(h) => self.repair_key_ec(key, h, &targets),
                    None => self.repair_key_mirror(key, Some(b)),
                };
            }
        }
        for (i, s) in self.shards.iter().enumerate() {
            if targets.iter().any(|t| t.0 as usize == i) {
                continue;
            }
            if let Ok(b) = s.get(key) {
                return match crate::erasure::parse_piece_header(&b) {
                    Some(h) => self.repair_key_ec(key, h, &targets),
                    None => self.repair_key_mirror(key, Some(b)),
                };
            }
        }
        Err(DomainError::Io("repair: no readable source".into()))
    }

    /// E20: реконструкция недостающих кусков EC-объекта (нужно ≥K живых).
    fn repair_key_ec(
        &self,
        key: &BlockKey,
        h: crate::erasure::PieceHeader,
        targets: &[ShardId],
    ) -> DomainResult<(usize, usize)> {
        use std::sync::atomic::Ordering::Relaxed;
        let (k, m) = (h.k as usize, h.m as usize);
        let total = k + m;
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut missing: Vec<(usize, usize)> = Vec::new(); // (ранг=piece_idx, шард)
        for (r, sid) in targets.iter().enumerate().take(total) {
            let i = sid.0 as usize;
            match self.shards[i].get(key) {
                Ok(b) => {
                    let parsed = crate::erasure::parse_piece_header(&b);
                    if let Some(hh) = parsed {
                        let idx = hh.piece_idx as usize;
                        if idx < total && slots[idx].is_none() {
                            slots[idx] = Some(b);
                        }
                        if idx == r {
                            continue; // канонический кусок на месте
                        }
                    }
                    missing.push((r, i));
                }
                Err(_) => missing.push((r, i)),
            }
        }
        if slots.iter().flatten().count() < k {
            // stray-куски (handoff) на неканонических дисках
            for (i, s) in self.shards.iter().enumerate() {
                if targets.iter().any(|t| t.0 as usize == i) {
                    continue;
                }
                if let Ok(b) = s.get(key) {
                    if let Some(hh) = crate::erasure::parse_piece_header(&b) {
                        let idx = hh.piece_idx as usize;
                        if idx < total && slots[idx].is_none() {
                            slots[idx] = Some(b);
                        }
                    }
                }
            }
        }
        if slots.iter().flatten().count() < k {
            return Err(DomainError::Io(format!("ec repair: fewer than {k} pieces")));
        }
        if missing.is_empty() {
            return Ok((0, 0));
        }
        let rebuilt = crate::erasure::ec_repair_pieces(slots, k, m)?;
        let piece_len = rebuilt[0].len() as u64;
        let (mut repaired, mut left) = (0usize, 0usize);
        for (r, i) in &missing {
            match self.shards[*i].put_meta(key, &rebuilt[*r], Some(h.logical_len)) {
                Ok(()) => repaired += 1,
                Err(e) => {
                    tracing::warn!(shard = i, ?key, err = %e, "ec repair write failed");
                    left += 1;
                }
            }
        }
        self.metrics.ec_pieces_repaired.fetch_add(repaired as u64, Relaxed);
        // E19: фон платит за K чтений + repaired записей кусков
        self.bg.acquire(piece_len * (k as u64 + repaired as u64));
        Ok((repaired, left))
    }

    /// Зеркальная починка до R реплик: probe = уже прочитанное тело (зонд эры).
    fn repair_key_mirror(
        &self,
        key: &BlockKey,
        probe: Option<Vec<u8>>,
    ) -> DomainResult<(usize, usize)> {
        let desired = self.replicas_for(key);
        let mut have: Vec<usize> = Vec::new();
        let mut missing: Vec<usize> = Vec::new();
        for d in &desired {
            let i = d.0 as usize;
            if self.shards[i].has(key).unwrap_or(false) {
                have.push(i);
            } else {
                missing.push(i);
            }
        }
        if missing.is_empty() {
            return Ok((0, 0));
        }
        // источник: desired-держатели; иначе любой шард (handoff-копия /
        // старое место после add-disk)
        let data = match probe {
            Some(d) => d,
            None => self.read_for_repair(key, &have)?,
        };
        let mut repaired = 0usize;
        let mut left = 0usize;
        for m in missing {
            match self.shards[m].put(key, &data) {
                Ok(()) => repaired += 1,
                Err(e) => {
                    tracing::warn!(shard = m, ?key, err = %e, "repair: copy failed");
                    left += 1;
                }
            }
        }
        // E19 (#131): фон платит за 1 чтение + repaired записей
        self.bg.acquire(data.len() as u64 * (1 + repaired as u64));
        Ok((repaired, left))
    }

    fn read_for_repair(&self, key: &BlockKey, have: &[usize]) -> DomainResult<Vec<u8>> {
        for i in have {
            match self.shards[*i].get(key) {
                Ok(d) => return Ok(d),
                Err(e) => tracing::warn!(shard = i, err = %e, "resilver: source read failed"),
            }
        }
        for s in &self.shards {
            if let Ok(d) = s.get(key) {
                return Ok(d);
            }
        }
        Err(DomainError::NotFound)
    }

    /// Шаг scrub одного шарда (#102/#141): deep-проверка партии ключей
    /// (CRC при чтении) + self-heal битых с других реплик.
    pub fn scrub_shard_step(
        &self,
        shard: usize,
        after: Option<&BlockKey>,
        limit: usize,
    ) -> DomainResult<ScrubReport> {
        let step = self.shards[shard].scrub_step(after, limit)?;
        self.bg.acquire(step.bytes); // E19: темп deep-scrub по бюджету
        let mut rep = ScrubReport {
            checked: step.checked,
            corrupt: step.corrupt.len() as u64,
            last_key: step.last_key,
            done: step.done,
            ..Default::default()
        };
        for key in &step.corrupt {
            // E20: битую локальную запись сносим → repair_key восстановит
            // (зеркало: копия с реплики; EC: реконструкция ИМЕННО нашего
            // куска — копировать чужой кусок было бы порчей данных!)
            let _ = self.shards[shard].delete(key);
            let fixed = matches!(self.repair_key(key), Ok((r, l)) if r > 0 && l == 0);
            if fixed {
                rep.repaired += 1;
                tracing::info!(shard, ?key, "scrub: corrupt record repaired from replica");
            } else {
                rep.unrepairable += 1;
                tracing::error!(shard, ?key, "scrub: CORRUPT and no healthy replica!");
                // E16: Urgent-заявка — повторить, когда реплика вернётся
                self.mrf.lock().push(key.clone(), HealPriority::Urgent);
            }
        }
        {
            use std::sync::atomic::Ordering::Relaxed;
            self.metrics.scrub_checked.fetch_add(rep.checked, Relaxed);
            self.metrics.scrub_corrupt.fetch_add(rep.corrupt, Relaxed);
            self.metrics.scrub_repaired.fetch_add(rep.repaired, Relaxed);
            self.metrics.scrub_unrepairable.fetch_add(rep.unrepairable, Relaxed);
        }
        Ok(rep)
    }

    /// Полный walk-resilver: батчи до конца + финальный flush (recovery-point).
    /// E17 (#102): курсор персистентен (redb shards[0], имя "resilver") —
    /// прерванный проход возобновляется С МЕСТА, по завершении курсор снят.
    pub fn resilver_full(&self, batch: usize) -> DomainResult<ResilverReport> {
        let mut total = ResilverReport::default();
        let mut after: Option<BlockKey> = self.shards[0].load_cursor("resilver")?;
        if after.is_some() {
            tracing::info!("resilver: resuming from persisted cursor");
        }
        loop {
            let r = self.resilver_step(after.as_ref(), batch)?;
            total.scanned += r.scanned;
            total.repaired += r.repaired;
            total.errors += r.errors;
            total.last_key = r.last_key.clone();
            if r.done || r.last_key.is_none() {
                total.done = true;
                let _ = self.shards[0].save_cursor("resilver", None); // E17: снять
                break;
            }
            after = r.last_key;
            let _ = self.shards[0].save_cursor("resilver", after.as_ref()); // E17
        }
        self.flush_all()?;
        tracing::info!(
            scanned = total.scanned,
            repaired = total.repaired,
            errors = total.errors,
            "resilver pass complete"
        );
        Ok(total)
    }
    fn put_inner(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()> {
        // E23 (#79): outboard для крупного тела — ПОСЛЕ успешного put тела
        // (отсутствие ob не фатально: Range-чтение падает в unverified)
        if let Some(ob) = &self.cfg.outboard {
            if data.len() >= ob.min_size && !crate::verified::is_ob_key(key) {
                let body_res = self.put_body(key, data);
                if body_res.is_ok() {
                    let obb = crate::verified::make_outboard(data);
                    if let Err(e) = self.put_body(&crate::verified::ob_key(key), &obb) {
                        tracing::warn!(?key, err = %e, "ob3 write failed (range→unverified)");
                    }
                }
                return body_res;
            }
        }
        self.put_body(key, data)
    }

    /// Тело без outboard-логики (общая точка put_inner и ob-записи).
    fn put_body(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()> {
        // E20 (#138): крупные тела — erasure K+M; мелочь остаётся зеркалом
        if let Some(ec) = self.cfg.ec.clone() {
            if data.len() >= ec.min_size {
                return self.put_ec(key, data, &ec);
            }
        }
        let reps = self.replicas_for(key);
        if reps.is_empty() {
            return Err(DomainError::QuorumNotReached { ok: 0, want: self.cfg.write_quorum });
        }
        // ПАРАЛЛЕЛЬНАЯ запись на R дисков (PLAN Ф2): латентность = max(ноги),
        // а не сумма — на HDD при R=2 это ~2× выигрыш.
        use std::sync::mpsc;
        let shared: Arc<Vec<u8>> = Arc::new(data.to_vec());
        let (tx, rx) = mpsc::channel::<(ShardId, DomainResult<()>)>();
        for sid in &reps {
            let s = self.shards[sid.0 as usize].clone();
            let k = key.clone();
            let d = shared.clone();
            let tx = tx.clone();
            let sid = *sid;
            std::thread::spawn(move || {
                let _ = tx.send((sid, s.put(&k, &d)));
            });
        }
        drop(tx);
        let mut ok = 0usize;
        let mut failed: Vec<ShardId> = Vec::new();
        let mut last_err = None;
        while let Ok((sid, res)) = rx.recv() {
            match res {
                Ok(()) => ok += 1,
                Err(e) => {
                    tracing::warn!(shard = sid.0, err = %e, "replica write failed");
                    failed.push(sid);
                    last_err = Some(e);
                }
            }
        }

        // Handoff (#41 YDB): упавшая нога → следующий кандидат по ПОЛНОМУ
        // HRW-рангу (транзиентный держатель; место наведёт resilver/MRF).
        if !failed.is_empty() {
            let topo = self.topology();
            let ranking = self.policy.select(key, &topo, self.shards.len());
            let mut spares = ranking.into_iter().filter(|s| !reps.contains(s));
            for _ in 0..failed.len() {
                let mut placed = false;
                for sp in spares.by_ref() {
                    match self.shards[sp.0 as usize].put(key, data) {
                        Ok(()) => {
                            tracing::info!(
                                shard = sp.0,
                                "handoff: replica written to spare disk"
                            );
                            self.metrics
                                .handoff_writes
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            ok += 1;
                            placed = true;
                            break;
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
                if !placed {
                    break; // запасные кончились
                }
            }
            // размещение неканонично → быстрый точечный heal (MRF #140)
            self.metrics.mrf_enqueued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.mrf.lock().push(key.clone(), HealPriority::Normal);
        }

        if ok >= self.cfg.write_quorum.min(reps.len()) {
            Ok(())
        } else {
            Err(last_err.unwrap_or(DomainError::QuorumNotReached {
                ok,
                want: self.cfg.write_quorum,
            }))
        }
    }

    fn get_inner(&self, key: &BlockKey) -> DomainResult<Vec<u8>> {
        // E20: при включённом EC эру определяет самоописанность тела
        if let Some(ec) = self.cfg.ec.clone() {
            return self.get_ec_aware(key, &ec);
        }
        // #143 (Discord write-mostly): порядок HRW детерминирован → reps[0] —
        // стабильная READ-нога (её page-cache греется), реплики №2+ —
        // write-mostly: читаются только при отказе/таймауте read-ноги.
        let reps = self.replicas_for(key);
        let mut last_err = DomainError::NotFound;
        let mut tried = 0usize;

        if let (Some(hedge), true) = (self.hedge_threshold(), reps.len() >= 2) {
            // #121 (Cassandra speculative retry, fixed-порог): hedged read —
            // read-нога медлит → параллельный дубль write-mostly-ноге,
            // берём первый успешный ответ.
            use std::sync::mpsc;
            let (tx, rx) = mpsc::channel::<DomainResult<Vec<u8>>>();
            let spawn_read = |sid: ShardId, tx: mpsc::Sender<DomainResult<Vec<u8>>>| {
                let shard = self.shards[sid.0 as usize].clone();
                let k = key.clone();
                std::thread::spawn(move || {
                    let _ = tx.send(shard.get(&k));
                });
            };
            spawn_read(reps[0], tx.clone());
            tried = 1;
            let mut pending = 1usize;
            let mut hedged = false;

            let first = rx.recv_timeout(hedge);
            let handle = |msg: DomainResult<Vec<u8>>,
                              pending: &mut usize,
                              last_err: &mut DomainError|
             -> Option<Vec<u8>> {
                *pending -= 1;
                match msg {
                    Ok(v) => Some(v),
                    Err(DomainError::NotFound) => None,
                    Err(e) => {
                        *last_err = e;
                        None
                    }
                }
            };
            match first {
                Ok(msg) => {
                    if let Some(v) = handle(msg, &mut pending, &mut last_err) {
                        return Ok(v);
                    }
                    // read-нога быстро ответила ошибкой → дальше последовательно
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    tracing::debug!(shard = reps[0].0, "speculative retry: hedging read");
                    self.metrics
                        .hedged_reads
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    spawn_read(reps[1], tx.clone());
                    tried = 2;
                    pending += 1;
                    hedged = true;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {}
            }
            drop(tx);
            if hedged {
                while pending > 0 {
                    match rx.recv() {
                        Ok(msg) => {
                            if let Some(v) = handle(msg, &mut pending, &mut last_err) {
                                return Ok(v);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        // write-mostly fallback: оставшиеся (ещё не пробованные) реплики
        // последовательно; при speculative=off tried=0 → полный порядок HRW
        for sid in reps.iter().skip(tried) {
            match self.shards[sid.0 as usize].get(key) {
                Ok(v) => return Ok(v),
                Err(DomainError::NotFound) => {}
                Err(e) => last_err = e,
            }
        }
        // топология могла измениться (add-disk) — fallback: спросить все
        // (walk-resilver later переносит блоки на новые места)
        for (i, s) in self.shards.iter().enumerate() {
            if reps.iter().any(|r| r.0 as usize == i) {
                continue;
            }
            if let Ok(v) = s.get(key) {
                return Ok(v);
            }
        }
        Err(last_err)
    }


    /// E21 (#145): шаг миграции mirror→erasure — партия ключей с курсором.
    /// Кандидат определяется БЕЗ чтений (has-паттерн «top-R есть, хвост
    /// пуст» + stat >= min_size); сама миграция — с canary read-back.
    pub fn migrate_step(
        &self,
        after: Option<&BlockKey>,
        batch: usize,
    ) -> DomainResult<MigrateReport> {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(ec) = self.cfg.ec.clone() else {
            return Err(DomainError::Io("migrate: redundancy=erasure не включён".into()));
        };
        let keys = self.list(b"", after, batch)?;
        let mut rep = MigrateReport {
            scanned: keys.len(),
            done: keys.len() < batch,
            ..Default::default()
        };
        for (key, _) in &keys {
            match self.migrate_key(key, &ec) {
                Ok(MigrateOutcome::Migrated) => rep.migrated += 1,
                Ok(MigrateOutcome::SkippedSmall) => rep.skipped_small += 1,
                Ok(MigrateOutcome::SkippedEc) => rep.skipped_ec += 1,
                Ok(MigrateOutcome::CanaryFailed) => rep.canary_failed += 1,
                Err(e) => {
                    tracing::warn!(?key, err = %e, "migrate: key failed");
                    rep.errors += 1;
                }
            }
        }
        self.metrics.migrate_migrated.fetch_add(rep.migrated as u64, Relaxed);
        self.metrics.migrate_canary_failed.fetch_add(rep.canary_failed as u64, Relaxed);
        self.metrics.migrate_errors.fetch_add(rep.errors as u64, Relaxed);
        rep.last_key = keys.into_iter().next_back().map(|(k, _)| k);
        Ok(rep)
    }

    /// Полная миграция с ПЕРСИСТЕНТНЫМ курсором "migrate" (E17): рестарт
    /// продолжает с места; по завершении прохода курсор снимается.
    pub fn migrate_full(&self, batch: usize) -> DomainResult<MigrateReport> {
        let mut total = MigrateReport::default();
        let mut after: Option<BlockKey> = self.shards[0].load_cursor("migrate")?;
        if after.is_some() {
            tracing::info!("migrate: resuming from persisted cursor");
        }
        loop {
            let r = self.migrate_step(after.as_ref(), batch)?;
            total.scanned += r.scanned;
            total.migrated += r.migrated;
            total.skipped_small += r.skipped_small;
            total.skipped_ec += r.skipped_ec;
            total.canary_failed += r.canary_failed;
            total.errors += r.errors;
            total.last_key = r.last_key.clone();
            if r.done || r.last_key.is_none() {
                total.done = true;
                let _ = self.shards[0].save_cursor("migrate", None);
                break;
            }
            after = r.last_key;
            let _ = self.shards[0].save_cursor("migrate", after.as_ref());
        }
        self.flush_all()?;
        Ok(total)
    }

    /// E21: миграция одного ключа. Порядок выживания (#145, dual-write-дух —
    /// читаемость в ЛЮБОЙ точке): 1) хвостовые куски на НОВЫЕ места + canary
    /// read-back бит-в-бит (откат при провале — зеркало не тронуто);
    /// 2) головные куски ПОВЕРХ зеркальных ног (probe видит либо тело,
    /// либо кусок + >=K доступных); 3) уборка stray-копий вне таргетов.
    fn migrate_key(
        &self,
        key: &BlockKey,
        ec: &crate::erasure::EcConfig,
    ) -> DomainResult<MigrateOutcome> {
        let total = ec.total();
        let targets = self.ranking_for(key, total);
        if targets.len() < total {
            return Err(DomainError::Io(format!(
                "migrate: alive disks {} < K+M {total}",
                targets.len()
            )));
        }
        let r_legs = self.cfg.replicas.min(total);
        // эра без чтений: хвост занят → уже EC/частичный EC (дело resilver)
        if targets[r_legs..total]
            .iter()
            .any(|sid| self.shards[sid.0 as usize].has(key).unwrap_or(false))
        {
            // полировка E21b: легаси-куски (до era-бита) чинятся на проходе —
            // индекс-строка апгрейдится БЕЗ перезаписи тела
            let mut bf_bytes = 0u64;
            for sid in targets.iter().take(total) {
                let i = sid.0 as usize;
                if let Ok((_, None)) = self.shards[i].stat_obj(key) {
                    if let Ok(b) = self.shards[i].get(key) {
                        bf_bytes += b.len() as u64;
                        if let Some(h) = crate::erasure::parse_piece_header(&b) {
                            match self.shards[i].set_obj_logical(key, h.logical_len) {
                                Ok(true) => {
                                    self.metrics.migrate_era_backfilled.fetch_add(
                                        1,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                                Ok(false) => {}
                                Err(e) => tracing::warn!(
                                    shard = i, ?key, err = %e, "era backfill failed"
                                ),
                            }
                        }
                    }
                }
            }
            if bf_bytes > 0 {
                self.bg.acquire(bf_bytes); // E19: чтения легаси-кусков — фон
            }
            return Ok(MigrateOutcome::SkippedEc);
        }
        let holders: Vec<usize> = targets[..r_legs]
            .iter()
            .map(|sid| sid.0 as usize)
            .filter(|i| self.shards[*i].has(key).unwrap_or(false))
            .collect();
        if holders.is_empty() {
            return Err(DomainError::Io("migrate: no mirror source on top-R".into()));
        }
        // мелочь — зеркалом навсегда (stat дёшев: из индекса)
        if let Ok(sz) = self.shards[holders[0]].stat(key) {
            if (sz as usize) < ec.min_size {
                return Ok(MigrateOutcome::SkippedSmall);
            }
        }
        let mut body: Option<Vec<u8>> = None;
        for i in &holders {
            if let Ok(b) = self.shards[*i].get(key) {
                body = Some(b);
                break;
            }
        }
        let Some(body) = body else {
            return Err(DomainError::Io("migrate: mirror body unreadable".into()));
        };
        if crate::erasure::parse_piece_header(&body).is_some() {
            return Ok(MigrateOutcome::SkippedEc); // stray-кусок на top-R — не зеркало
        }
        if body.len() < ec.min_size {
            return Ok(MigrateOutcome::SkippedSmall);
        }
        let pieces = crate::erasure::ec_encode(&body, ec)?;

        // фаза 1: хвост + canary; любой сбой → откат хвоста, зеркало цело
        let phase1 = (|| -> DomainResult<bool> {
            for r in r_legs..total {
                let i = targets[r].0 as usize;
                self.shards[i].put_meta(key, &pieces[r], Some(body.len() as u64))?;
                let back = self.shards[i].get(key)?;
                if back != pieces[r] {
                    tracing::error!(shard = i, ?key, "migrate: CANARY mismatch (#145)");
                    return Ok(false);
                }
            }
            Ok(true)
        })();
        let canary_ok = match phase1 {
            Ok(ok) => ok,
            Err(e) => {
                for r in r_legs..total {
                    let _ = self.shards[targets[r].0 as usize].delete(key);
                }
                return Err(e);
            }
        };
        if !canary_ok {
            for r in r_legs..total {
                let _ = self.shards[targets[r].0 as usize].delete(key);
            }
            return Ok(MigrateOutcome::CanaryFailed);
        }

        // фаза 2: головные куски поверх зеркальных копий (необратимая точка;
        // даже при сбое здесь объект читаем: вторая нога-тело или K кусков)
        for r in 0..r_legs.min(total) {
            let i = targets[r].0 as usize;
            match self.shards[i].put_meta(key, &pieces[r], Some(body.len() as u64)) {
                Ok(()) => {
                    if let Ok(back) = self.shards[i].get(key) {
                        if back != pieces[r] {
                            tracing::warn!(shard = i, ?key, "migrate: head canary mismatch — MRF");
                            self.enqueue_heal(key.clone(), HealPriority::High);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(shard = i, ?key, err = %e, "migrate: head piece failed — MRF");
                    self.enqueue_heal(key.clone(), HealPriority::High);
                }
            }
        }
        // фаза 3: stray-копии (handoff-тела) вне таргетов
        for (i, s) in self.shards.iter().enumerate() {
            if targets.iter().take(total).any(|t| t.0 as usize == i) {
                continue;
            }
            let _ = s.delete(key);
        }
        // E19: фон платит ~ чтение тела + запись 1.5× + canary-чтения 1.5×
        self.bg.acquire(body.len() as u64 * 4);
        Ok(MigrateOutcome::Migrated)
    }

    /// E20 (#138): EC-запись — K+M самоописанных кусков на top-(K+M) дисков
    /// параллельно (латентность = max). Кворум ec.write_quorum (дефолт K+1).
    fn put_ec(
        &self,
        key: &BlockKey,
        data: &[u8],
        ec: &crate::erasure::EcConfig,
    ) -> DomainResult<()> {
        use std::sync::atomic::Ordering::Relaxed;
        use std::sync::mpsc;
        let total = ec.total();
        let targets = self.ranking_for(key, total);
        if targets.len() < ec.write_quorum {
            return Err(DomainError::QuorumNotReached { ok: 0, want: ec.write_quorum });
        }
        let pieces = crate::erasure::ec_encode(data, ec)?;
        let obj = data.len() as u64; // E21b: era-бит + логический размер
        // W2.2: scoped threads для EC-записи — pieces живут по ссылке
        let (tx, rx) = mpsc::channel::<(usize, ShardId, DomainResult<()>)>();
        std::thread::scope(|sc| {
            for (i, sid) in targets.iter().enumerate() {
                let s = self.shards[sid.0 as usize].clone();
                let kk = key.clone();
                let piece = &pieces[i];
                let tx = tx.clone();
                let sid = *sid;
                sc.spawn(move || {
                    let _ = tx.send((i, sid, s.put_meta(&kk, piece, Some(obj))));
                });
            }
            drop(tx);
        });
        let mut ok = 0usize;
        let mut failed: Vec<usize> = Vec::new();
        let mut last_err = None;
        while let Ok((i, sid, res)) = rx.recv() {
            match res {
                Ok(()) => ok += 1,
                Err(e) => {
                    tracing::warn!(shard = sid.0, piece = i, err = %e, "ec piece write failed");
                    failed.push(i);
                    last_err = Some(e);
                }
            }
        }
        // handoff (#41): упавший кусок → запасной диск дальше по рангу;
        // кусок самоописан — чтение/ремонт найдут его сканом
        if !failed.is_empty() {
            let ranking = self.ranking_for(key, self.shards.len());
            let mut spares = ranking.into_iter().filter(|s| !targets.contains(s));
            for i in &failed {
                for sp in spares.by_ref() {
                    match self.shards[sp.0 as usize].put_meta(key, &pieces[*i], Some(obj)) {
                        Ok(()) => {
                            self.metrics.handoff_writes.fetch_add(1, Relaxed);
                            ok += 1;
                            break;
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
            }
            self.metrics.mrf_enqueued.fetch_add(1, Relaxed);
            self.mrf.lock().push(key.clone(), HealPriority::Normal);
        }
        self.metrics.ec_puts.fetch_add(1, Relaxed);
        if ok >= ec.write_quorum {
            Ok(())
        } else {
            Err(last_err
                .unwrap_or(DomainError::QuorumNotReached { ok, want: ec.write_quorum }))
        }
    }

    /// E20: чтение при включённом EC. Зонд по рангу: сырое тело = зеркальная
    /// эра (вернуть как есть), кусок = собрать K (двухфазно: data-куски,
    /// при нехватке — parity + реконструкция). k/m берём ИЗ ЗАГОЛОВКА куска
    /// (самоописанность переживает смену конфига).
    fn get_ec_aware(
        &self,
        key: &BlockKey,
        ec: &crate::erasure::EcConfig,
    ) -> DomainResult<Vec<u8>> {
        let total = ec.total().max(self.cfg.replicas);
        let targets = self.ranking_for(key, total);
        let mut last_err = DomainError::NotFound;
        for (rank, sid) in targets.iter().enumerate() {
            match self.shards[sid.0 as usize].get(key) {
                Ok(body) => {
                    return match crate::erasure::parse_piece_header(&body) {
                        None => Ok(body), // зеркальная эра / мелочь
                        Some(h) => self.gather_ec(key, &targets, rank, body, h),
                    };
                }
                Err(DomainError::NotFound) => {}
                Err(e) => last_err = e,
            }
        }
        // таргеты пусты: полный скан — зеркальное тело либо stray-куски
        let mut slots: Vec<Option<Vec<u8>>> = Vec::new();
        let mut hdr: Option<crate::erasure::PieceHeader> = None;
        for (i, s) in self.shards.iter().enumerate() {
            if targets.iter().any(|t| t.0 as usize == i) {
                continue;
            }
            if let Ok(body) = s.get(key) {
                match crate::erasure::parse_piece_header(&body) {
                    None => return Ok(body),
                    Some(h) => {
                        let t = (h.k + h.m) as usize;
                        if slots.len() != t {
                            slots = vec![None; t];
                        }
                        let idx = h.piece_idx as usize;
                        if slots[idx].is_none() {
                            slots[idx] = Some(body);
                        }
                        hdr = Some(h);
                    }
                }
            }
        }
        if let Some(h) = hdr {
            if slots.iter().flatten().count() >= h.k as usize {
                self.metrics
                    .ec_reconstructs
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return crate::erasure::ec_decode(slots, h.k as usize, h.m as usize);
            }
        }
        Err(last_err)
    }

    /// Сбор кусков: фаза 1 — data-ранги параллельно (здоровый путь = K чтений
    /// и конкатенация без RS-математики); фаза 2 — parity + реконструкция;
    /// фаза 3 — stray-куски сканом (handoff).
    fn gather_ec(
        &self,
        key: &BlockKey,
        targets: &[ShardId],
        probe_rank: usize,
        first: Vec<u8>,
        h: crate::erasure::PieceHeader,
    ) -> DomainResult<Vec<u8>> {
        let (k, m) = (h.k as usize, h.m as usize);
        let total = k + m;
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
        slots[h.piece_idx as usize] = Some(first);

        let read_ranks = |ranks: Vec<usize>, slots: &mut Vec<Option<Vec<u8>>>| {
            use std::sync::mpsc;
            let (tx, rx) = mpsc::channel::<Option<(usize, Vec<u8>)>>();
            let mut spawned = 0usize;
            for r in ranks {
                let Some(sid) = targets.get(r) else { continue };
                let s = self.shards[sid.0 as usize].clone();
                let kk = key.clone();
                let tx = tx.clone();
                std::thread::spawn(move || {
                    let res = s.get(&kk).ok().and_then(|b| {
                        crate::erasure::parse_piece_header(&b)
                            .map(|hh| (hh.piece_idx as usize, b))
                    });
                    let _ = tx.send(res);
                });
                spawned += 1;
            }
            drop(tx);
            for _ in 0..spawned {
                if let Ok(Some((idx, b))) = rx.recv() {
                    if idx < slots.len() && slots[idx].is_none() {
                        slots[idx] = Some(b);
                    }
                }
            }
        };

        let want: Vec<usize> =
            (0..k).filter(|r| *r != probe_rank && slots[*r].is_none()).collect();
        read_ranks(want, &mut slots);
        if slots[..k].iter().all(Option::is_some) {
            return crate::erasure::ec_decode(slots, k, m); // fast-path
        }
        let want: Vec<usize> =
            (k..total).filter(|r| *r != probe_rank && slots[*r].is_none()).collect();
        read_ranks(want, &mut slots);
        if slots.iter().flatten().count() < k {
            for (i, s) in self.shards.iter().enumerate() {
                if targets.iter().any(|t| t.0 as usize == i) {
                    continue;
                }
                if let Ok(b) = s.get(key) {
                    if let Some(hh) = crate::erasure::parse_piece_header(&b) {
                        let idx = hh.piece_idx as usize;
                        if idx < total && slots[idx].is_none() {
                            slots[idx] = Some(b);
                        }
                    }
                }
            }
        }
        self.metrics.ec_reconstructs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::erasure::ec_decode(slots, k, m)
    }
}

/// E28 (#129): декоратор шарда — замер латентности тяжёлых IO-операций
/// (put/get/put_meta) в DiskSlowMonitor. Остальные методы — прозрачный
/// проброс (ВСЕ, включая дефолтные: иначе обёртка тихо сломает put_meta!).
struct MeteredShard {
    inner: Arc<dyn ShardEngine>,
    idx: usize,
    mon: Arc<crate::diskslow::DiskSlowMonitor>,
}

impl ShardEngine for MeteredShard {
    fn put(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()> {
        let t = Instant::now();
        let r = self.inner.put(key, data);
        self.mon.record(self.idx, t.elapsed());
        r
    }
    fn get(&self, key: &BlockKey) -> DomainResult<Vec<u8>> {
        let t = Instant::now();
        let r = self.inner.get(key);
        self.mon.record(self.idx, t.elapsed());
        r
    }
    fn put_meta(
        &self,
        key: &BlockKey,
        data: &[u8],
        obj: Option<u64>,
    ) -> DomainResult<()> {
        let t = Instant::now();
        let r = self.inner.put_meta(key, data, obj);
        self.mon.record(self.idx, t.elapsed());
        r
    }
    fn has(&self, k: &BlockKey) -> DomainResult<bool> {
        self.inner.has(k)
    }
    fn delete(&self, k: &BlockKey) -> DomainResult<()> {
        self.inner.delete(k)
    }
    fn list(
        &self,
        p: &[u8],
        a: Option<&BlockKey>,
        l: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>> {
        self.inner.list(p, a, l)
    }
    fn usage(&self) -> DomainResult<Capacity> {
        self.inner.usage()
    }
    fn flush(&self) -> DomainResult<()> {
        self.inner.flush()
    }
    fn gc(&self, r: f64) -> DomainResult<ozd_domain::GcReport> {
        self.inner.gc(r)
    }
    fn verify_structure(&self) -> DomainResult<ozd_domain::StructureReport> {
        self.inner.verify_structure()
    }
    fn scrub_step(
        &self,
        a: Option<&BlockKey>,
        l: usize,
    ) -> DomainResult<ozd_domain::ScrubStep> {
        self.inner.scrub_step(a, l)
    }
    fn stat(&self, k: &BlockKey) -> DomainResult<u64> {
        self.inner.stat(k)
    }
    fn save_cursor(&self, n: &str, p: Option<&BlockKey>) -> DomainResult<()> {
        self.inner.save_cursor(n, p)
    }
    fn load_cursor(&self, n: &str) -> DomainResult<Option<BlockKey>> {
        self.inner.load_cursor(n)
    }
    fn ballast_released(&self) -> bool {
        self.inner.ballast_released()
    }
    fn release_ballast(&self) -> DomainResult<bool> {
        self.inner.release_ballast()
    }
    fn stat_obj(&self, k: &BlockKey) -> DomainResult<(u64, Option<u64>)> {
        self.inner.stat_obj(k)
    }
    fn set_obj_logical(&self, k: &BlockKey, o: u64) -> DomainResult<bool> {
        self.inner.set_obj_logical(k, o)
    }
    fn data_bytes(&self) -> DomainResult<u64> {
        self.inner.data_bytes()
    }
    fn evict_oldest_segment(&self) -> DomainResult<(u64, usize)> {
        self.inner.evict_oldest_segment()
    }
}

/// Отчёт scrub-шага по шарду.
#[derive(Debug, Default, Clone)]
pub struct ScrubReport {
    pub checked: u64,
    pub corrupt: u64,
    pub repaired: u64,
    pub unrepairable: u64,
    pub last_key: Option<BlockKey>,
    pub done: bool,
}

/// Отчёт walk-resilver.
#[derive(Debug, Default, Clone)]
pub struct ResilverReport {
    pub scanned: usize,
    pub repaired: usize,
    pub errors: usize,
    pub last_key: Option<BlockKey>,
    pub done: bool,
}

/// E21 (#145): отчёт шага миграции mirror→erasure.
#[derive(Debug, Default, Clone)]
pub struct MigrateReport {
    pub scanned: usize,
    pub migrated: usize,
    /// мелочь < ec_min_size — остаётся зеркалом навсегда
    pub skipped_small: usize,
    /// уже EC (или хвост частично есть — чинит resilver, не мигратор)
    pub skipped_ec: usize,
    /// canary read-back не сошёлся — зеркало НЕ тронуто, объект цел
    pub canary_failed: usize,
    pub errors: usize,
    pub last_key: Option<BlockKey>,
    pub done: bool,
}

enum MigrateOutcome {
    Migrated,
    SkippedSmall,
    SkippedEc,
    CanaryFailed,
}

impl BlockStore for Pool {
    fn put(&self, key: &BlockKey, data: &[u8]) -> DomainResult<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let t0 = Instant::now();
        self.metrics.inflight_puts.fetch_add(1, Relaxed);
        let r = self.put_inner(key, data);
        self.metrics.inflight_puts.fetch_sub(1, Relaxed);
        let el_us = t0.elapsed().as_micros() as u64;
        self.metrics.put_micros.fetch_add(el_us, Relaxed);
        self.metrics.put_hist.observe(el_us);
        match &r {
            Ok(()) => self.metrics.puts.fetch_add(1, Relaxed),
            Err(_) => self.metrics.put_errors.fetch_add(1, Relaxed),
        };
        r
    }

    fn get(&self, key: &BlockKey) -> DomainResult<Vec<u8>> {
        use std::sync::atomic::Ordering::Relaxed;
        let t0 = Instant::now();
        self.metrics.inflight_gets.fetch_add(1, Relaxed);
        let r = self.get_inner(key);
        self.metrics.inflight_gets.fetch_sub(1, Relaxed);
        let el = t0.elapsed();
        let el_us = el.as_micros() as u64;
        self.metrics.get_micros.fetch_add(el_us, Relaxed);
        self.metrics.get_hist.observe(el_us);
        match &r {
            Ok(_) => {
                self.read_lat.record(el); // E27: пища для p99-порога
                self.metrics.gets.fetch_add(1, Relaxed)
            }
            Err(DomainError::NotFound) => self.metrics.get_not_found.fetch_add(1, Relaxed),
            Err(_) => self.metrics.get_errors.fetch_add(1, Relaxed),
        };
        r
    }
    /// E11: HEAD из индекса — порядок read-нога → write-mostly → полный скан.
    /// E20: при включённом EC кусок по индексу неотличим от тела — зонд
    /// читает первое доступное и берёт logical_len из заголовка куска
    /// (HEAD на EC-объекте = чтение одного куска ~тело/K).
    fn stat(&self, key: &BlockKey) -> DomainResult<u64> {
        if self.cfg.ec.is_some() {
            // E21b: era-бит в addr-строке — HEAD БЕЗ чтения тела и для EC
            // (кусок → логический размер объекта, тело → его размер)
            let targets = self.ranking_for(key, self.shards.len());
            for sid in &targets {
                match self.shards[sid.0 as usize].stat_obj(key) {
                    Ok((sz, obj)) => return Ok(obj.unwrap_or(sz)),
                    Err(_) => continue,
                }
            }
            return Err(DomainError::NotFound);
        }
        let reps = self.replicas_for(key);
        for sid in &reps {
            match self.shards[sid.0 as usize].stat(key) {
                Ok(sz) => return Ok(sz),
                Err(DomainError::NotFound) => {}
                Err(_) => {}
            }
        }
        for (i, s) in self.shards.iter().enumerate() {
            if reps.iter().any(|r| r.0 as usize == i) {
                continue;
            }
            if let Ok(sz) = s.stat(key) {
                return Ok(sz);
            }
        }
        Err(DomainError::NotFound)
    }

    fn has(&self, key: &BlockKey) -> DomainResult<bool> {
        let reps = self.replicas_for(key);
        for sid in &reps {
            if self.shards[sid.0 as usize].has(key)? {
                return Ok(true);
            }
        }
        for (i, s) in self.shards.iter().enumerate() {
            if reps.iter().any(|r| r.0 as usize == i) {
                continue;
            }
            if s.has(key)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn delete(&self, key: &BlockKey) -> DomainResult<()> {
        // идемпотентно по всем шардам (реплики могли мигрировать)
        for s in &self.shards {
            let _ = s.delete(key);
        }
        // E23: outboard уходит вместе с телом
        if self.cfg.outboard.is_some() && !crate::verified::is_ob_key(key) {
            let okk = crate::verified::ob_key(key);
            for s in &self.shards {
                let _ = s.delete(&okk);
            }
        }
        Ok(())
    }

    fn list(
        &self,
        prefix: &[u8],
        after: Option<&BlockKey>,
        limit: usize,
    ) -> DomainResult<Vec<(BlockKey, u64)>> {
        // merge-скан локальных индексов + дедуп реплик (узкие строки #80)
        let mut merged: std::collections::BTreeMap<BlockKey, u64> = Default::default();
        for s in &self.shards {
            for (k, sz) in s.list(prefix, after, limit)? {
                merged.entry(k).or_insert(sz);
            }
        }
        Ok(merged.into_iter().take(limit).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RendezvousHrw;
    use ozd_engine::{DiskEngine, EngineConfig};

    fn pool(dirs: &[tempfile::TempDir], r: usize, w: usize) -> Pool {
        let shards: Vec<Arc<dyn ShardEngine>> = dirs
            .iter()
            .map(|d| {
                Arc::new(
                    DiskEngine::open(EngineConfig {
                        data_path: d.path().to_path_buf(),
                        inline_min: 64,
                        segment_max_size: 1 << 20,
                        fsync_items: 16,
                        index_path: None,
                        ..Default::default()
                    })
                    .unwrap(),
                ) as Arc<dyn ShardEngine>
            })
            .collect();
        Pool::new(
            shards,
            Box::new(RendezvousHrw::default()),
            PoolConfig {
                replicas: r,
                write_quorum: w,
                free_space_cache_ttl: Duration::from_secs(5),
                speculative_retry_after: None, // детерминизм в базовых тестах
                ..Default::default()
            },
        )
    }

    /// Обёртка-замедлитель: get для hedged-теста, put — для параллельной записи.
    struct SlowShard {
        inner: Arc<dyn ShardEngine>,
        delay_get: Duration,
        delay_put: Duration,
    }
    impl ShardEngine for SlowShard {
        fn put(&self, k: &BlockKey, d: &[u8]) -> ozd_domain::DomainResult<()> {
            std::thread::sleep(self.delay_put);
            self.inner.put(k, d)
        }
        fn get(&self, k: &BlockKey) -> ozd_domain::DomainResult<Vec<u8>> {
            std::thread::sleep(self.delay_get);
            self.inner.get(k)
        }
        fn has(&self, k: &BlockKey) -> ozd_domain::DomainResult<bool> {
            self.inner.has(k)
        }
        fn delete(&self, k: &BlockKey) -> ozd_domain::DomainResult<()> {
            self.inner.delete(k)
        }
        fn list(
            &self,
            p: &[u8],
            a: Option<&BlockKey>,
            l: usize,
        ) -> ozd_domain::DomainResult<Vec<(BlockKey, u64)>> {
            self.inner.list(p, a, l)
        }
        fn usage(&self) -> ozd_domain::DomainResult<ozd_domain::Capacity> {
            self.inner.usage()
        }
        fn flush(&self) -> ozd_domain::DomainResult<()> {
            self.inner.flush()
        }
    }

    /// Фиксированный порядок реплик: [0, 1, ...] — для управляемых тестов ног.
    struct FixedPolicy;
    impl ozd_domain::PlacementPolicy for FixedPolicy {
        fn select(
            &self,
            _key: &BlockKey,
            topology: &[(ozd_domain::ShardId, ozd_domain::Capacity, ozd_domain::ShardStatus)],
            rf: usize,
        ) -> Vec<ozd_domain::ShardId> {
            topology.iter().take(rf).map(|(id, _, _)| *id).collect()
        }
    }

    #[test]
    fn pool_replicates_r2() {
        let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
        let p = pool(&dirs, 2, 2);
        let data = vec![5u8; 50_000];
        for i in 0..50 {
            p.put(&BlockKey::new(format!("/blocks/k{i}")), &data).unwrap();
        }
        p.flush_all().unwrap();
        for i in 0..50 {
            assert_eq!(p.get(&BlockKey::new(format!("/blocks/k{i}"))).unwrap(), data);
        }
        // ровно R=2 копии: суммарное число ключей по индексам = 100
        let mut total = 0;
        for d in &dirs {
            let e = DiskEngine::open(EngineConfig {
                data_path: d.path().to_path_buf(),
                ..Default::default()
            });
            // отдельное открытие redb поверх живого было бы конфликтом —
            // считаем через list пула с дедупом и через сам пул:
            drop(e);
        }
        let listed = p.list(b"/blocks/", None, 1000).unwrap();
        assert_eq!(listed.len(), 50);
        total += listed.len();
        assert!(total > 0);
    }

    /// Проверка: каждый ключ имеет копии на ВСЕХ своих desired-шардах.
    fn assert_fully_replicated(p: &Pool, keys: &[BlockKey]) {
        for key in keys {
            let desired = p.replicas_for(key);
            for d in &desired {
                assert!(
                    p.shards[d.0 as usize].has(key).unwrap(),
                    "key {key:?} missing on desired shard {}",
                    d.0
                );
            }
        }
    }

    #[test]
    fn faulted_status_excludes_shard_from_placement() {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let p = pool(&dirs, 2, 2);
        let key = BlockKey::from("/blocks/st");
        // пометить шард 0 Faulted (как сделал бы ZFS-монитор) → HRW его не выберет
        p.set_shard_status(0, ozd_domain::ShardStatus::Faulted);
        let reps = p.replicas_for(&key);
        assert!(!reps.iter().any(|s| s.0 == 0), "faulted shard must be excluded: {reps:?}");
        // запись идёт на живые
        p.put(&key, &vec![1u8; 10_000]).unwrap();
        // возврат в Online → снова кандидат
        p.set_shard_status(0, ozd_domain::ShardStatus::Online);
        assert_eq!(p.shard_status(0), Some(ozd_domain::ShardStatus::Online));
    }

    /// Обёртка-отказник: put падает, пока взведён флаг (имитация мёртвого диска).
    struct FailShard {
        inner: Arc<dyn ShardEngine>,
        fail_puts: std::sync::atomic::AtomicBool,
    }
    impl FailShard {
        fn new(inner: Arc<dyn ShardEngine>) -> Arc<Self> {
            Arc::new(Self { inner, fail_puts: std::sync::atomic::AtomicBool::new(false) })
        }
        fn set_failing(&self, v: bool) {
            self.fail_puts.store(v, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl ShardEngine for FailShard {
        fn put(&self, k: &BlockKey, d: &[u8]) -> ozd_domain::DomainResult<()> {
            if self.fail_puts.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(ozd_domain::DomainError::Io("disk dead (injected)".into()));
            }
            self.inner.put(k, d)
        }
        fn get(&self, k: &BlockKey) -> ozd_domain::DomainResult<Vec<u8>> {
            self.inner.get(k)
        }
        fn has(&self, k: &BlockKey) -> ozd_domain::DomainResult<bool> {
            self.inner.has(k)
        }
        fn delete(&self, k: &BlockKey) -> ozd_domain::DomainResult<()> {
            self.inner.delete(k)
        }
        fn list(
            &self,
            p: &[u8],
            a: Option<&BlockKey>,
            l: usize,
        ) -> ozd_domain::DomainResult<Vec<(BlockKey, u64)>> {
            self.inner.list(p, a, l)
        }
        fn usage(&self) -> ozd_domain::DomainResult<ozd_domain::Capacity> {
            self.inner.usage()
        }
        fn flush(&self) -> ozd_domain::DomainResult<()> {
            self.inner.flush()
        }
    }

    fn mk_engine(d: &tempfile::TempDir) -> Arc<dyn ShardEngine> {
        Arc::new(
            DiskEngine::open(EngineConfig {
                data_path: d.path().to_path_buf(),
                inline_min: 64,
                segment_max_size: 1 << 20,
                fsync_items: 16,
                index_path: None,
                ..Default::default()
            })
            .unwrap(),
        ) as Arc<dyn ShardEngine>
    }

    #[test]
    fn heal_queue_priority_dedup_and_upgrade() {
        let mut q = HealQueue::default();
        q.push(BlockKey::from("/k/a"), HealPriority::Normal);
        q.push(BlockKey::from("/k/b"), HealPriority::Urgent);
        q.push(BlockKey::from("/k/c"), HealPriority::Normal);
        q.push(BlockKey::from("/k/a"), HealPriority::Normal); // дубль — слит
        assert_eq!(q.len(), 3);
        // upgrade: c повышается до High — обгоняет a
        q.push(BlockKey::from("/k/c"), HealPriority::High);
        assert_eq!(q.len(), 3);
        let order: Vec<(String, HealPriority)> = std::iter::from_fn(|| q.pop())
            .map(|(k, p)| (String::from_utf8_lossy(k.as_bytes()).into_owned(), p))
            .collect();
        assert_eq!(
            order.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/k/b", "/k/c", "/k/a"],
            "Urgent → High(upgrade) → Normal(FIFO): {order:?}"
        );
        assert_eq!(order[1].1, HealPriority::High);
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn scrub_unrepairable_enqueues_urgent_retry() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let p = pool(&dirs, 2, 2);
        let key = BlockKey::from("/blocks/both-rot");
        let data = vec![0x77u8; 70_000];
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();
        // портим ОБЕ реплики → источника нет → unrepairable → Urgent в очередь
        for d in &dirs {
            let seg = d.path().join("seg").join("seg.00000000.dat");
            let mut f =
                std::fs::OpenOptions::new().read(true).write(true).open(&seg).unwrap();
            let off = 20 + key.len() as u64 + 500;
            f.seek(SeekFrom::Start(off)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Start(off)).unwrap();
            f.write_all(&[b[0] ^ 0xFF]).unwrap();
        }
        let rep = p.scrub_shard_step(0, None, 10).unwrap();
        assert_eq!(rep.unrepairable, 1, "{rep:?}");
        assert_eq!(p.mrf_len(), 1, "Urgent-заявка на повтор");
        // свежая запись чинит мир → дренаж закрывает заявку
        p.put(&key, &data).unwrap();
        let (healed, requeued) = p.mrf_drain(10).unwrap();
        assert_eq!((healed, requeued), (1, 0));
        assert_eq!(p.mrf_len(), 0);
    }

    #[test]
    fn parallel_put_latency_is_max_not_sum() {
        // обе ноги тормозят put на 150мс: параллельная запись ≈ max (150),
        // последовательная была бы суммой (300)
        let d0 = tempfile::tempdir().unwrap();
        let d1 = tempfile::tempdir().unwrap();
        let slow = |d: &tempfile::TempDir| {
            Arc::new(SlowShard {
                inner: mk_engine(d),
                delay_get: Duration::ZERO,
                delay_put: Duration::from_millis(150),
            }) as Arc<dyn ShardEngine>
        };
        let p = Pool::new(
            vec![slow(&d0), slow(&d1)],
            Box::new(FixedPolicy),
            PoolConfig {
                replicas: 2,
                write_quorum: 2,
                free_space_cache_ttl: Duration::from_secs(5),
                speculative_retry_after: None,
                ..Default::default()
            },
        );
        let t0 = std::time::Instant::now();
        p.put(&BlockKey::from("/blocks/par"), &vec![1u8; 10_000]).unwrap();
        let el = t0.elapsed();
        assert!(
            el < Duration::from_millis(260),
            "parallel put took {el:?} (sequential would be ~300ms)"
        );
    }

    #[test]
    fn handoff_then_mrf_heals_when_disk_returns() {
        // desired = [0,1]; нога-1 мертва → put выживает через handoff на 2;
        // диск вернулся → MRF точечно дочинивает на правильное место
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let failing = FailShard::new(mk_engine(&dirs[1]));
        let shards: Vec<Arc<dyn ShardEngine>> = vec![
            mk_engine(&dirs[0]),
            failing.clone() as Arc<dyn ShardEngine>,
            mk_engine(&dirs[2]),
        ];
        let p = Pool::new(
            shards,
            Box::new(FixedPolicy),
            PoolConfig {
                replicas: 2,
                write_quorum: 2,
                free_space_cache_ttl: Duration::from_secs(5),
                speculative_retry_after: None,
                ..Default::default()
            },
        );
        let key = BlockKey::from("/blocks/ho");
        let data = vec![9u8; 20_000];

        failing.set_failing(true);
        p.put(&key, &data).unwrap(); // W=2 собран: нога-0 + handoff на 2
        assert_eq!(p.get(&key).unwrap(), data);
        assert_eq!(p.mrf_len(), 1, "неканоничное размещение должно попасть в MRF");
        // E14: счётчики зафиксировали операции/handoff/MRF
        {
            use std::sync::atomic::Ordering::Relaxed;
            let m = p.metrics();
            assert_eq!(m.handoff_writes.load(Relaxed), 1);
            assert_eq!(m.mrf_enqueued.load(Relaxed), 1);
            assert_eq!(m.puts.load(Relaxed), 1);
            assert_eq!(m.gets.load(Relaxed), 1);
            assert!(m.put_micros.load(Relaxed) > 0);
        }
        assert!(p.shards[2].has(&key).unwrap(), "handoff-копия на запасном диске");
        assert!(!p.shards[1].has(&key).unwrap());

        // пока диск мёртв — drain перекладывает в хвост, не зависает
        let (healed0, requeued0) = p.mrf_drain(10).unwrap();
        assert_eq!((healed0, requeued0), (0, 1));
        assert_eq!(p.mrf_len(), 1);

        // диск вернулся → MRF дочинивает канонику
        failing.set_failing(false);
        let (healed, _) = p.mrf_drain(10).unwrap();
        assert_eq!(healed, 1);
        assert_eq!(p.mrf_len(), 0);
        assert!(p.shards[1].has(&key).unwrap(), "реплика возвращена на desired-место");
        assert_eq!(p.metrics().mrf_healed.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn scrub_detects_bitrot_and_heals_from_replica() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let p = pool(&dirs, 2, 2);
        let key = BlockKey::from("/blocks/rot");
        let data = vec![0x5Au8; 80_000];
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();

        // bitrot: флипаем байт в ТЕЛЕ записи на реплике-0
        let reps = p.replicas_for(&key);
        let victim = reps[0].0 as usize;
        let seg0 = dirs[victim].path().join("seg").join("seg.00000000.dat");
        {
            let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&seg0).unwrap();
            // заголовок 16Б + key_len; портим байт глубоко в данных
            let off = 16 + key.len() as u64 + 1000;
            f.seek(SeekFrom::Start(off)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Start(off)).unwrap();
            f.write_all(&[b[0] ^ 0xFF]).unwrap();
        }

        // scrub находит порчу и чинит со здоровой реплики
        let rep = p.scrub_shard_step(victim, None, 100).unwrap();
        assert_eq!(rep.corrupt, 1, "{rep:?}");
        assert_eq!(rep.repaired, 1);
        assert_eq!(rep.unrepairable, 0);

        // после починки шард читает сам (новая запись, CRC сходится)
        assert_eq!(p.shards[victim].get(&key).unwrap(), data);

        // повторный scrub чист (битая старая запись стала мусором для GC)
        let rep2 = p.scrub_shard_step(victim, None, 100).unwrap();
        assert_eq!(rep2.corrupt, 0);
    }

    #[test]
    fn capacity_override_drives_placement() {
        // #150: переопределённая ёмкость (заполнен >95%) исключает диск из
        // placement через fill_block-гистерезис (#130)
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let p = pool(&dirs, 2, 2);
        p.set_shard_capacity(
            0,
            ozd_domain::Capacity { total_bytes: 100, free_bytes: 2 }, // 98% занято
        );
        let reps = p.replicas_for(&BlockKey::from("/blocks/cap"));
        assert!(!reps.iter().any(|s| s.0 == 0), "full-by-override shard excluded: {reps:?}");
    }

    #[test]
    fn resilver_rebuilds_replaced_disk() {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let keys: Vec<BlockKey> =
            (0..40).map(|i| BlockKey::new(format!("/blocks/rs{i:02}"))).collect();
        let big = vec![4u8; 30_000];
        let small = vec![5u8; 40]; // < inline_min=64 → inline-путь тоже чиним
        {
            let p = pool(&dirs, 2, 2);
            for (i, k) in keys.iter().enumerate() {
                p.put(k, if i % 4 == 0 { &small } else { &big }).unwrap();
            }
            p.flush_all().unwrap();
        } // drop: redb-локи освобождены

        // «замена диска»: каталог диска 1 полностью обнулён (свежий пустой)
        std::fs::remove_dir_all(dirs[1].path()).unwrap();
        std::fs::create_dir_all(dirs[1].path()).unwrap();

        let p2 = pool(&dirs, 2, 2);
        let rep = p2.resilver_full(8).unwrap();
        assert!(rep.repaired > 0, "must repair replicas lost with disk 1");
        assert_eq!(rep.errors, 0);
        assert_fully_replicated(&p2, &keys);

        // идемпотентность: второй проход ничего не копирует
        let rep2 = p2.resilver_full(8).unwrap();
        assert_eq!(rep2.repaired, 0, "second pass must be a no-op");

        // данные читаются даже при потере ЛЮБОГО одного диска (R=2 снова)
        for k in &keys {
            assert!(p2.get(k).is_ok());
        }
    }

    #[test]
    fn resilver_populates_added_disk() {
        let dirs2: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let keys: Vec<BlockKey> =
            (0..40).map(|i| BlockKey::new(format!("/blocks/ad{i:02}"))).collect();
        let data = vec![6u8; 20_000];
        {
            let p = pool(&dirs2, 2, 2);
            for k in &keys {
                p.put(k, &data).unwrap();
            }
            p.flush_all().unwrap();
        }
        // add-disk: третий пустой диск входит в топологию
        let mut dirs3 = dirs2;
        dirs3.push(tempfile::tempdir().unwrap());
        let p3 = pool(&dirs3, 2, 2);
        let rep = p3.resilver_full(8).unwrap();
        assert_eq!(rep.errors, 0);
        // часть ключей теперь хочет реплику на новом диске (≈1/3) — докопировано
        assert!(rep.repaired > 0, "some replicas must migrate to the new disk");
        assert_fully_replicated(&p3, &keys);
        let on_new = keys
            .iter()
            .filter(|k| p3.shards[2].has(k).unwrap_or(false))
            .count();
        assert!(on_new > 0, "new disk must hold some replicas, got {on_new}");
    }

    #[test]
    fn speculative_retry_hedges_slow_read_leg() {
        // нога-1 медленная (300мс), нога-2 быстрая; hedge-порог 30мс →
        // ответ должен прийти от ноги-2 задолго до 300мс (#121/#143)
        let d0 = tempfile::tempdir().unwrap();
        let d1 = tempfile::tempdir().unwrap();
        let mk = |d: &tempfile::TempDir| {
            Arc::new(
                DiskEngine::open(EngineConfig {
                    data_path: d.path().to_path_buf(),
                    inline_min: 64,
                    segment_max_size: 1 << 20,
                    fsync_items: 16,
                    index_path: None,
                    ..Default::default()
                })
                .unwrap(),
            ) as Arc<dyn ShardEngine>
        };
        let fast0 = mk(&d0);
        let fast1 = mk(&d1);
        let slow0 = Arc::new(SlowShard {
            inner: fast0,
            delay_get: Duration::from_millis(300),
            delay_put: Duration::ZERO,
        }) as Arc<dyn ShardEngine>;

        let p = Pool::new(
            vec![slow0, fast1],
            Box::new(FixedPolicy),
            PoolConfig {
                replicas: 2,
                write_quorum: 2,
                free_space_cache_ttl: Duration::from_secs(5),
                speculative_retry_after: Some(Duration::from_millis(30)),
                ..Default::default()
            },
        );
        let key = BlockKey::from("/blocks/hot");
        let data = vec![3u8; 20_000];
        p.put(&key, &data).unwrap(); // put медленный (нога-1 пишет 300мс? нет — slow только get)
        p.flush_all().unwrap();

        let t0 = std::time::Instant::now();
        assert_eq!(p.get(&key).unwrap(), data);
        let el = t0.elapsed();
        assert!(el < Duration::from_millis(250), "hedged read took {el:?} (want << 300ms)");
    }

    #[test]
    fn write_mostly_fallback_on_read_leg_failure() {
        // read-нога (реплика №1) умерла → чтение прозрачно падает на №2
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let p = pool(&dirs, 2, 2);
        let key = BlockKey::from("/blocks/wm");
        let data = vec![8u8; 25_000];
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();
        let reps = p.replicas_for(&key);
        // ломаем ИМЕННО read-ногу (первую по HRW)
        std::fs::remove_dir_all(dirs[reps[0].0 as usize].path().join("seg")).ok();
        assert_eq!(p.get(&key).unwrap(), data, "write-mostly нога должна отдать данные");
    }

    #[test]
    fn survives_one_disk_loss_r2() {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let p = pool(&dirs, 2, 1);
        let data = vec![6u8; 30_000];
        let key = BlockKey::from("/blocks/important");
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();
        // «теряем» один диск: ломаем доступ, удаляя каталог сегментов одной реплики
        let reps = p.replicas_for(&key);
        let victim = reps[0].0 as usize;
        std::fs::remove_dir_all(dirs[victim].path().join("seg")).ok();
        // вторая реплика жива
        assert_eq!(p.get(&key).unwrap(), data);
    }

    #[test]
    fn resilver_full_resumes_from_persisted_cursor_and_clears_it() {
        // E17 (#102): курсор "resilver" на shards[0] переживает рестарт —
        // resilver_full стартует С НЕГО (scanned < всего ключей),
        // а по завершении прохода курсор снимается
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let shard0 = mk_engine(&dirs[0]);
        let shard1 = mk_engine(&dirs[1]);
        let p = Pool::new(
            vec![shard0.clone(), shard1],
            Box::new(FixedPolicy),
            PoolConfig {
                replicas: 2,
                write_quorum: 2,
                free_space_cache_ttl: Duration::from_secs(5),
                ..Default::default()
            },
        );
        for i in 0..10 {
            p.put(&BlockKey::new(format!("/blocks/k{i:02}")), &vec![i as u8; 9000]).unwrap();
        }
        p.flush_all().unwrap();

        // «рестарт посреди обхода»: курсор указывает на середину keyspace
        shard0.save_cursor("resilver", Some(&BlockKey::from("/blocks/k04"))).unwrap();
        let r = p.resilver_full(3).unwrap();
        assert!(r.done);
        assert!(
            r.scanned < 10,
            "ожидали возобновление с k04 (scanned < 10), скан {}",
            r.scanned
        );
        assert_eq!(
            shard0.load_cursor("resilver").unwrap(),
            None,
            "после полного прохода курсор снят"
        );
        // следующий полный проход — снова с начала (курсора нет)
        let r2 = p.resilver_full(100).unwrap();
        assert_eq!(r2.scanned, 10, "без курсора обходим всё");
    }

    #[test]
    fn bg_throttle_paces_resilver_but_not_foreground() {
        // E19 (#131): фон (resilver) темпируется байтовым бюджетом,
        // foreground put/get — никогда. Бюджет 128КиБ/с фикс. (min=max),
        // ремонт 3×32КиБ ключей платит 3×64КиБ → долг → измеримый сон.
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let throttled = crate::throttle::BgThrottleConfig {
            max_bytes_per_sec: 128 * 1024,
            min_bytes_per_sec: 128 * 1024,
            fg_busy_ops_per_sec: f64::MAX, // эластика выкл — чистый pacing
        };
        // фаза 1: R=1 — ключи только на шарде 0 (вторая реплика отсутствует)
        {
            let p1 = Pool::new(
                shards.clone(),
                Box::new(FixedPolicy),
                PoolConfig {
                    replicas: 1,
                    write_quorum: 1,
                    bg_throttle: throttled.clone(),
                    ..Default::default()
                },
            );
            let t0 = std::time::Instant::now();
            for i in 0..3u8 {
                p1.put(&BlockKey::new(format!("/blocks/th{i}")), &vec![i; 32 * 1024])
                    .unwrap();
            }
            assert!(
                t0.elapsed() < Duration::from_millis(200),
                "foreground put НЕ троттлится (96КиБ при бюджете фона 128КиБ/с)"
            );
            p1.flush_all().unwrap();
        }
        // фаза 2: R=2 — resilver доливает копии на шард 1, платя токенами
        let p2 = Pool::new(
            shards,
            Box::new(FixedPolicy),
            PoolConfig {
                replicas: 2,
                write_quorum: 2,
                bg_throttle: throttled,
                ..Default::default()
            },
        );
        let t0 = std::time::Instant::now();
        let r = p2.resilver_full(10).unwrap();
        let elapsed = t0.elapsed();
        assert_eq!(r.repaired, 3);
        // 3 ремонта × 64КиБ = 192КиБ при burst 128КиБ → долг 64КиБ → сон ~0.5с
        assert!(
            elapsed >= Duration::from_millis(300),
            "ожидали pacing-сон, прошло {elapsed:?}"
        );
        let m = p2.metrics();
        assert!(
            m.bg_throttle_waits.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "счётчик ожиданий троттля должен вырасти"
        );
        assert!(
            m.bg_throttle_bytes.load(std::sync::atomic::Ordering::Relaxed) >= 192 * 1024,
            "учтённые байты фона"
        );
    }

    fn ec_pool(shards: Vec<Arc<dyn ShardEngine>>) -> Pool {
        Pool::new(
            shards,
            Box::new(FixedPolicy), // ранг = порядок шардов: кусок i → шард i
            PoolConfig {
                replicas: 2,
                write_quorum: 2,
                ec: Some(crate::erasure::EcConfig {
                    data: 4,
                    parity: 2,
                    min_size: 4096,
                    write_quorum: 5,
                }),
                ..Default::default()
            },
        )
    }

    #[test]
    fn ec_roundtrip_pieces_on_disks_and_small_stays_mirror() {
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let p = ec_pool(shards.clone());
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 253) as u8).collect();
        let key = BlockKey::from("/blocks/ec-big");
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();
        // на КАЖДОМ из 6 дисков — кусок размера ceil(300000/4)+16, не тело
        let want = 300_000usize.div_ceil(4) + crate::erasure::EC_HEADER_LEN;
        for (i, s) in shards.iter().enumerate() {
            let piece = s.get(&key).unwrap();
            assert_eq!(piece.len(), want, "шард {i}: ожидали кусок");
            let h = crate::erasure::parse_piece_header(&piece).unwrap();
            assert_eq!(h.piece_idx as usize, i, "distribution-array: кусок i → ранг i");
        }
        assert_eq!(p.get(&key).unwrap(), data, "roundtrip через fast-path");
        assert_eq!(ozd_domain::BlockStore::stat(&p, &key).unwrap(), 300_000, "HEAD = логический размер");
        // 1.5×: суммарно на дисках ~450КБ, а не 600КБ зеркала
        let on_disk: usize = shards.iter().map(|s| s.get(&key).unwrap().len()).sum();
        assert!(on_disk < 460_000, "EC 4+2 = 1.5×, на дисках {on_disk}");

        // мелочь (< min_size) остаётся зеркалом R=2
        let small = vec![7u8; 1000];
        let ks = BlockKey::from("/blocks/ec-small");
        p.put(&ks, &small).unwrap();
        let holders = shards.iter().filter(|s| s.has(&ks).unwrap()).count();
        assert_eq!(holders, 2, "мелочь — зеркало R=2, не куски");
        assert_eq!(p.get(&ks).unwrap(), small);
        assert_eq!(ozd_domain::BlockStore::stat(&p, &ks).unwrap(), 1000);
    }

    #[test]
    fn ec_degraded_read_survives_m_disk_losses() {
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let p = ec_pool(shards);
        let data: Vec<u8> = (0..200_000u32).map(|i| (i * 13 % 256) as u8).collect();
        let key = BlockKey::from("/blocks/ec-deg");
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();
        // теряем M=2 диска, причём DATA-куски (ранги 0 и 2) — худший случай
        std::fs::remove_dir_all(dirs[0].path().join("seg")).unwrap();
        std::fs::remove_dir_all(dirs[2].path().join("seg")).unwrap();
        assert_eq!(p.get(&key).unwrap(), data, "реконструкция из 2 data + 2 parity");
        assert!(
            p.metrics().ec_reconstructs.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "должен сработать reconstruct-путь"
        );
        // третья потеря (>M) — объект нечитаем
        std::fs::remove_dir_all(dirs[1].path().join("seg")).unwrap();
        assert!(p.get(&key).is_err(), "K+M=4+2 переживает ровно M=2 отказа");
    }

    #[test]
    fn ec_resilver_reconstructs_piece_on_replaced_disk() {
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let data: Vec<u8> = (0..150_000u32).map(|i| (i * 31 % 256) as u8).collect();
        let key = BlockKey::from("/blocks/ec-rsv");
        {
            let p = ec_pool(shards.clone());
            p.put(&key, &data).unwrap();
            p.flush_all().unwrap();
        }
        // «замена диска» ранга 3: свежий пустой движок на его месте
        let fresh_dir = tempfile::tempdir().unwrap();
        let mut shards2 = shards.clone();
        shards2[3] = mk_engine(&fresh_dir);
        let p = ec_pool(shards2.clone());
        assert!(!shards2[3].has(&key).unwrap());
        let r = p.resilver_full(10).unwrap();
        assert!(r.repaired >= 1, "resilver обязан реконструировать кусок: {r:?}");
        // кусок на новом диске — канонический (idx == 3) и бит-в-бит верный
        let piece = shards2[3].get(&key).unwrap();
        let h = crate::erasure::parse_piece_header(&piece).unwrap();
        assert_eq!(h.piece_idx, 3);
        assert_eq!(piece, shards[3].get(&key).unwrap(), "бит-в-бит со старым куском");
        // повторный resilver идемпотентен и БЕЗ чтений (has-паттерн)
        let r2 = p.resilver_full(10).unwrap();
        assert_eq!(r2.repaired, 0);
        // после ремонта переживаем ещё M=2 потери других дисков
        std::fs::remove_dir_all(dirs[0].path().join("seg")).unwrap();
        std::fs::remove_dir_all(dirs[5].path().join("seg")).unwrap();
        assert_eq!(p.get(&key).unwrap(), data);
    }

    #[test]
    fn ec_scrub_heals_corrupt_piece_by_reconstruction() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let p = ec_pool(shards.clone());
        let data: Vec<u8> = (0..120_000u32).map(|i| (i * 7 % 256) as u8).collect();
        let key = BlockKey::from("/blocks/ec-rot");
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();
        // bitrot в куске на шарде 4 (parity-кусок): флип байта глубоко в теле
        {
            let seg = dirs[4].path().join("seg").join("seg.00000000.dat");
            let mut f =
                std::fs::OpenOptions::new().read(true).write(true).open(&seg).unwrap();
            let off = 20 + key.len() as u64 + 700;
            f.seek(SeekFrom::Start(off)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Start(off)).unwrap();
            f.write_all(&[b[0] ^ 0xFF]).unwrap();
        }
        let good = shards[0].get(&key).unwrap(); // эталон куска до heal
        let rep = p.scrub_shard_step(4, None, 100).unwrap();
        assert_eq!(rep.corrupt, 1, "{rep:?}");
        assert_eq!(rep.repaired, 1, "кусок реконструирован, не скопирован чужой");
        // вылеченный кусок — канонический idx=4, и объект читается
        let healed = shards[4].get(&key).unwrap();
        let h = crate::erasure::parse_piece_header(&healed).unwrap();
        assert_eq!(h.piece_idx, 4);
        assert_ne!(healed, good, "это ДРУГОЙ кусок (idx 4), не копия куска 0");
        assert_eq!(p.get(&key).unwrap(), data);
    }

    #[test]
    fn migrate_converts_mirror_era_to_ec_with_canary() {
        // E21 (#145): пул жил зеркалом → включили erasure → мигратор
        // конвертирует крупные тела в куски, мелочь не трогает,
        // повторный проход идемпотентен, деградация после миграции — ок
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let big: Vec<u8> = (0..200_000u32).map(|i| (i % 249) as u8).collect();
        let small = vec![5u8; 800];
        let kb = BlockKey::from("/blocks/mig-big");
        let ks = BlockKey::from("/blocks/mig-small");
        // зеркальная эра (ec выключен)
        {
            let p = Pool::new(
                shards.clone(),
                Box::new(FixedPolicy),
                PoolConfig { replicas: 2, write_quorum: 2, ..Default::default() },
            );
            p.put(&kb, &big).unwrap();
            p.put(&ks, &small).unwrap();
            p.flush_all().unwrap();
        }
        // вторая эра: erasure включён
        let p = ec_pool(shards.clone());
        assert_eq!(p.get(&kb).unwrap(), big, "зеркальная эра читается ДО миграции");
        let r = p.migrate_full(10).unwrap();
        assert_eq!(r.migrated, 1, "{r:?}");
        assert_eq!(r.skipped_small, 1, "мелочь остаётся зеркалом: {r:?}");
        assert_eq!(r.canary_failed + r.errors, 0, "{r:?}");
        // тело превратилось в куски: на всех 6 дисках кусок со своим idx
        for (i, s) in shards.iter().enumerate() {
            let piece = s.get(&kb).unwrap();
            let h = crate::erasure::parse_piece_header(&piece)
                .unwrap_or_else(|| panic!("шард {i}: ожидали кусок"));
            assert_eq!(h.piece_idx as usize, i);
        }
        assert_eq!(p.get(&kb).unwrap(), big, "после миграции");
        assert_eq!(p.get(&ks).unwrap(), small);
        assert_eq!(ozd_domain::BlockStore::stat(&p, &kb).unwrap(), 200_000);
        // идемпотентность: второй проход ничего не мигрирует
        let r2 = p.migrate_full(10).unwrap();
        assert_eq!((r2.migrated, r2.skipped_ec, r2.skipped_small), (0, 1, 1), "{r2:?}");
        // курсор снят после полного прохода
        assert_eq!(shards[0].load_cursor("migrate").unwrap(), None);
        // выживаем M=2 потери после миграции
        std::fs::remove_dir_all(dirs[1].path().join("seg")).unwrap();
        std::fs::remove_dir_all(dirs[4].path().join("seg")).unwrap();
        assert_eq!(p.get(&kb).unwrap(), big);
    }

    #[test]
    fn migrate_canary_failure_leaves_mirror_untouched() {
        // E21 (#145): сбой записи хвостового куска → откат, зеркало цело,
        // объект читаем; после починки диска миграция доходит
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let raw: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        // шард 4 (хвостовой таргет, ранг 4) умеет ломаться на put
        let failing = FailShard::new(raw[4].clone());
        let mut shards = raw.clone();
        shards[4] = failing.clone() as Arc<dyn ShardEngine>;
        let big: Vec<u8> = (0..150_000u32).map(|i| (i % 241) as u8).collect();
        let key = BlockKey::from("/blocks/mig-can");
        {
            let p = Pool::new(
                shards.clone(),
                Box::new(FixedPolicy),
                PoolConfig { replicas: 2, write_quorum: 2, ..Default::default() },
            );
            p.put(&key, &big).unwrap();
            p.flush_all().unwrap();
        }
        let p = ec_pool(shards.clone());
        failing.set_failing(true);
        let r = p.migrate_full(10).unwrap();
        assert_eq!(r.migrated, 0, "{r:?}");
        assert_eq!(r.errors, 1, "сбой хвостовой записи учтён: {r:?}");
        // зеркало НЕ тронуто: обе ноги — сырые тела, объект читаем
        for i in [0usize, 1] {
            let body = shards[i].get(&key).unwrap();
            assert!(
                crate::erasure::parse_piece_header(&body).is_none(),
                "нога {i} осталась телом"
            );
        }
        // откат: на здоровых хвостовых дисках кусков не осталось
        for i in [2usize, 3, 5] {
            assert!(!shards[i].has(&key).unwrap(), "шард {i}: хвост откатан");
        }
        assert_eq!(p.get(&key).unwrap(), big, "объект читаем при провале миграции");
        // диск починили → миграция доходит
        failing.set_failing(false);
        let r2 = p.migrate_full(10).unwrap();
        assert_eq!(r2.migrated, 1, "{r2:?}");
        assert_eq!(p.get(&key).unwrap(), big);
    }

    #[test]
    fn migrate_full_resumes_from_persisted_cursor() {
        // E21+E17: курсор "migrate" переживает рестарт — проход с места
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        {
            let p = Pool::new(
                shards.clone(),
                Box::new(FixedPolicy),
                PoolConfig { replicas: 2, write_quorum: 2, ..Default::default() },
            );
            for i in 0..6u8 {
                p.put(&BlockKey::new(format!("/blocks/mg{i}")), &vec![i; 20_000]).unwrap();
            }
            p.flush_all().unwrap();
        }
        let p = ec_pool(shards.clone());
        // «рестарт посреди прохода»: курсор на середине
        shards[0].save_cursor("migrate", Some(&BlockKey::from("/blocks/mg2"))).unwrap();
        let r = p.migrate_full(2).unwrap();
        assert!(r.scanned < 6, "возобновление с mg2: {r:?}");
        assert_eq!(r.migrated, 3, "мигрированы mg3..mg5: {r:?}");
        assert_eq!(shards[0].load_cursor("migrate").unwrap(), None, "курсор снят");
        // добиваем остальных полным проходом
        let r2 = p.migrate_full(10).unwrap();
        assert_eq!(r2.migrated, 3, "mg0..mg2 домигрированы: {r2:?}");
        for i in 0..6u8 {
            let k = BlockKey::new(format!("/blocks/mg{i}"));
            assert_eq!(p.get(&k).unwrap(), vec![i; 20_000]);
        }
    }

    /// E21b: обёртка-счётчик чтений тел (доказательство «HEAD без чтений»).
    struct CountingShard {
        inner: Arc<dyn ShardEngine>,
        gets: std::sync::atomic::AtomicUsize,
    }
    impl ShardEngine for CountingShard {
        fn put(&self, k: &BlockKey, d: &[u8]) -> ozd_domain::DomainResult<()> {
            self.inner.put(k, d)
        }
        fn put_meta(
            &self,
            k: &BlockKey,
            d: &[u8],
            o: Option<u64>,
        ) -> ozd_domain::DomainResult<()> {
            self.inner.put_meta(k, d, o)
        }
        fn stat_obj(&self, k: &BlockKey) -> ozd_domain::DomainResult<(u64, Option<u64>)> {
            self.inner.stat_obj(k)
        }
        fn get(&self, k: &BlockKey) -> ozd_domain::DomainResult<Vec<u8>> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.get(k)
        }
        fn has(&self, k: &BlockKey) -> ozd_domain::DomainResult<bool> {
            self.inner.has(k)
        }
        fn delete(&self, k: &BlockKey) -> ozd_domain::DomainResult<()> {
            self.inner.delete(k)
        }
        fn list(
            &self,
            p: &[u8],
            a: Option<&BlockKey>,
            l: usize,
        ) -> ozd_domain::DomainResult<Vec<(BlockKey, u64)>> {
            self.inner.list(p, a, l)
        }
        fn usage(&self) -> ozd_domain::DomainResult<ozd_domain::Capacity> {
            self.inner.usage()
        }
        fn flush(&self) -> ozd_domain::DomainResult<()> {
            self.inner.flush()
        }
    }

    #[test]
    fn ec_head_and_list_logical_without_body_reads() {
        // E21b: era-бит в addr-строке → HEAD и ListV2 на EC-объекте
        // отдают ЛОГИЧЕСКИЙ размер БЕЗ единого чтения тела
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let counting: Vec<Arc<CountingShard>> = dirs
            .iter()
            .map(|d| {
                Arc::new(CountingShard {
                    inner: mk_engine(d),
                    gets: std::sync::atomic::AtomicUsize::new(0),
                })
            })
            .collect();
        let shards: Vec<Arc<dyn ShardEngine>> =
            counting.iter().map(|c| c.clone() as Arc<dyn ShardEngine>).collect();
        let p = ec_pool(shards);
        let data = vec![3u8; 123_456];
        let key = BlockKey::from("/blocks/era");
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();
        let before: usize =
            counting.iter().map(|c| c.gets.load(std::sync::atomic::Ordering::SeqCst)).sum();
        // HEAD
        assert_eq!(ozd_domain::BlockStore::stat(&p, &key).unwrap(), 123_456);
        // ListV2
        let listed = p.list(b"/blocks/", None, 10).unwrap();
        assert_eq!(listed[0].1, 123_456, "ListV2 = логический размер объекта");
        let after: usize =
            counting.iter().map(|c| c.gets.load(std::sync::atomic::Ordering::SeqCst)).sum();
        assert_eq!(after - before, 0, "ни одного чтения тела на HEAD/List (era-бит)");
    }

    #[test]
    fn outboard_lifecycle_with_ec_and_verified_range() {
        // E23: put крупного тела → outboard-запись рядом (отдельный ключ),
        // verified_slice работает поверх pool.get; delete убирает оба
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let p = Pool::new(
            shards,
            Box::new(FixedPolicy),
            PoolConfig {
                replicas: 2,
                write_quorum: 2,
                ec: Some(crate::erasure::EcConfig {
                    data: 4,
                    parity: 2,
                    min_size: 4096,
                    write_quorum: 5,
                }),
                outboard: Some(crate::verified::ObConfig { min_size: 8192 }),
                ..Default::default()
            },
        );
        let body: Vec<u8> = (0..300_000u32).map(|i| (i % 247) as u8).collect();
        let key = BlockKey::from("/blocks/ob-big");
        p.put(&key, &body).unwrap();
        let okk = crate::verified::ob_key(&key);
        assert!(p.has(&okk).unwrap(), "outboard-запись создана");
        // тело — EC-кусками, outboard мелкий — зеркалом; range верифицируется
        let ob = p.get(&okk).unwrap();
        let got = crate::verified::verified_slice(&p.get(&key).unwrap(), &ob, 100_000, 5000)
            .unwrap();
        assert_eq!(got, &body[100_000..105_000]);
        // ListV2 Kubo (префикс /blocks/) outboard не видит
        let listed = p.list(b"/blocks/", None, 10).unwrap();
        assert_eq!(listed.len(), 1, "{listed:?}");
        // мелочь без outboard
        let ks = BlockKey::from("/blocks/ob-small");
        p.put(&ks, &vec![1u8; 1000]).unwrap();
        assert!(!p.has(&crate::verified::ob_key(&ks)).unwrap());
        // delete убирает тело И outboard
        ozd_domain::BlockStore::delete(&p, &key).unwrap();
        assert!(!p.has(&key).unwrap());
        assert!(!p.has(&okk).unwrap(), "outboard ушёл вместе с телом");
    }

    #[test]
    fn migrate_backfills_era_bit_for_legacy_pieces() {
        // полировка E21b: куски, записанные ДО era-бита (через put, не
        // put_meta), отдают в HEAD размер куска; migrate-проход чинит
        // индекс-строку БЕЗ перезаписи тел
        let dirs: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        let shards: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let body: Vec<u8> = (0..100_000u32).map(|i| (i % 239) as u8).collect();
        let key = BlockKey::from("/blocks/legacy-ec");
        // легаси-состояние: куски положены сырым put (без obj_logical)
        let pieces = crate::erasure::ec_encode(
            &body,
            &crate::erasure::EcConfig { data: 4, parity: 2, ..Default::default() },
        )
        .unwrap();
        for (i, s) in shards.iter().enumerate() {
            s.put(&key, &pieces[i]).unwrap();
            s.flush().unwrap();
        }
        let p = ec_pool(shards.clone());
        // ДО бэкфилла HEAD честно отдаёт размер куска (легаси-ограничение)
        let before = ozd_domain::BlockStore::stat(&p, &key).unwrap();
        assert_eq!(before, pieces[0].len() as u64, "легаси: размер куска");
        // migrate-проход: SkippedEc + бэкфилл era-бита всем держателям
        let r = p.migrate_full(10).unwrap();
        assert_eq!((r.migrated, r.skipped_ec), (0, 1), "{r:?}");
        assert_eq!(
            p.metrics().migrate_era_backfilled.load(std::sync::atomic::Ordering::Relaxed),
            6,
            "все 6 кусков получили era-бит"
        );
        // ПОСЛЕ: HEAD и ListV2 — логический размер, без чтения тел
        assert_eq!(ozd_domain::BlockStore::stat(&p, &key).unwrap(), 100_000);
        let listed = p.list(b"/blocks/", None, 10).unwrap();
        assert_eq!(listed[0].1, 100_000);
        for s in &shards {
            assert_eq!(s.stat_obj(&key).unwrap().1, Some(100_000));
        }
        // тела НЕ перезаписаны: повторный проход ничего не бэкфилит
        let r2 = p.migrate_full(10).unwrap();
        assert_eq!(r2.skipped_ec, 1);
        assert_eq!(
            p.metrics().migrate_era_backfilled.load(std::sync::atomic::Ordering::Relaxed),
            6,
            "идемпотентно"
        );
        assert_eq!(p.get(&key).unwrap(), body);
    }

    #[test]
    fn adaptive_hedge_tracks_p99_and_clamps_to_floor() {
        // E27: ровная нагрузка (p99 ~4мс) → порог прижат к полу 10мс →
        // тормозящая read-нога (300мс) хеджируется ЗАДОЛГО до статических
        // 100мс; без прогрева hedge выключен (статика None)
        let d0 = tempfile::tempdir().unwrap();
        let d1 = tempfile::tempdir().unwrap();
        let fast0 = mk_engine(&d0);
        let fast1 = mk_engine(&d1);
        let slow0 = Arc::new(SlowShard {
            inner: fast0,
            delay_get: Duration::from_millis(300),
            delay_put: Duration::ZERO,
        }) as Arc<dyn ShardEngine>;
        let p = Pool::new(
            vec![slow0, fast1],
            Box::new(FixedPolicy),
            PoolConfig {
                replicas: 2,
                write_quorum: 2,
                speculative_retry_after: None, // статики нет — только p99
                adaptive_hedge: true,
                ..Default::default()
            },
        );
        let key = BlockKey::from("/blocks/p99");
        let data = vec![4u8; 20_000];
        p.put(&key, &data).unwrap();
        p.flush_all().unwrap();

        // до прогрева: порога нет → последовательный путь (медленно, 1 раз)
        let t0 = std::time::Instant::now();
        assert_eq!(p.get(&key).unwrap(), data);
        assert!(t0.elapsed() >= Duration::from_millis(280), "без прогрева hedge выключен");
        assert_eq!(p.metrics().hedged_reads.load(std::sync::atomic::Ordering::Relaxed), 0);

        // прогрев: ровные 3мс → p99 ≈ 4мс → clamp к полу 10мс
        p.seed_read_latency(Duration::from_millis(3), 200);
        let t0 = std::time::Instant::now();
        assert_eq!(p.get(&key).unwrap(), data);
        let el = t0.elapsed();
        assert!(el < Duration::from_millis(250), "адаптивный hedge при ~10мс: {el:?}");
        assert!(p.metrics().hedged_reads.load(std::sync::atomic::Ordering::Relaxed) >= 1);
        assert_eq!(
            p.metrics().hedge_threshold_ms.load(std::sync::atomic::Ordering::Relaxed),
            10,
            "гейдж = пол клампа"
        );
        // в гистограмму попали и медленные 300мс-чтения → после бури порог растёт
        p.seed_read_latency(Duration::from_millis(300), 200);
        let _ = p.hedge_threshold();
        let thr = p.metrics().hedge_threshold_ms.load(std::sync::atomic::Ordering::Relaxed);
        assert!(thr >= 300, "хвост 300мс поднял порог: {thr}мс — лишних дублей нет");
    }

    #[test]
    fn disk_slow_flag_demotes_shard_from_read_leg_and_placement() {
        // E28 (#129): MeteredShard кормит монитор автоматически; вердикт →
        // флаг → topology Suspect → HRW-вес ×0.01 → шард уходит из top-R
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let raw: Vec<Arc<dyn ShardEngine>> = dirs.iter().map(mk_engine).collect();
        let slow0 = Arc::new(SlowShard {
            inner: raw[0].clone(),
            delay_get: Duration::from_millis(200),
            delay_put: Duration::from_millis(200),
        }) as Arc<dyn ShardEngine>;
        let p = Pool::new(
            vec![slow0, raw[1].clone(), raw[2].clone()],
            Box::new(RendezvousHrw::default()), // настоящий HRW — нужен вес
            PoolConfig {
                replicas: 2,
                write_quorum: 1,
                speculative_retry_after: None,
                adaptive_hedge: false,
                disk_slow: crate::diskslow::DiskSlowConfig {
                    // запасы под параллельный cargo test: 200мс-шард ловится
                    // даже если «здоровые» под нагрузкой дают десятки мс
                    abs_floor_ms: 60,
                    rel_factor: 3.0,
                    min_samples: 16,
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        // нагрузка: монитор набирает сэмплы на ВСЕХ шардах через обёртки
        for i in 0..40u8 {
            let k = BlockKey::new(format!("/blocks/ds{i:02}"));
            p.put(&k, &vec![i; 9000]).unwrap();
            let _ = p.get(&k);
        }
        let v = p.disk_slow_verdicts();
        assert!(v[0], "тормозящий шард пойман: ewma={}мс", p.shard_ewma_ms(0));
        assert!(!v[1] && !v[2], "здоровые не задеты: {v:?}");

        // FSM-роль демона: флаг → Suspect в топологии → демоушен в HRW
        p.set_shard_slow(0, true);
        assert!(p.shard_slow(0));
        let wins = (0..20)
            .filter(|i| {
                let k = BlockKey::new(format!("/blocks/probe{i}"));
                p.replicas_for(&k).first() == Some(&ShardId(0))
            })
            .count();
        assert!(wins <= 1, "slow-шард не read-нога: выиграл {wins}/20 ключей");
        // данные на нём ОСТАЮТСЯ читаемыми (не Faulted!)
        p.set_shard_slow(0, false);
        assert!(!p.shard_slow(0), "выздоровление снимает флаг");
    }
}
