# OpenZFS Daemon

> Кодовое имя проекта. По сути это **IPFS-демон с пуловым (ZFS-подобным) sharded-блокстором**:
> много физических дисков объединяются в один логический blockstore, как vdev'ы в zpool.

**Целевой деплой:** один сервер, **60 × HDD**, один IPFS-демон (~3,8 млрд блоков, ~480 ТБ
полезных при R=2). Специфика HDD/масштаба вынесена в [ARCHITECTURE §8](Wiki/ARCHITECTURE.md#8-целевой-масштаб-60--hdd-на-одном-сервере).

## Архитектура крейтов

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  ozd — OpenZFS Daemon  ·  IPFS object storage on 60 HDD  ·  Rust / tokio / axum    ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

                        ┌─────────────────────────┐
                        │   Kubo (IPFS daemon)     │
                        │   go-ds-s3 S3 plugin     │
                        └────────────┬────────────┘
                                     │ HTTP S3 API + SigV4
╔════════════════════════╗            ▼           ╔═════════════════════════╗
║ ozd-ipfs               ║◄───────────────────────►║ ozd-admin               ║
║ S3 gateway (axum)      ║  ozd-daemon (binary)   ║ REST /admin/*           ║
║ SigV4 auth (E13)       ║  tokio runtime         ║ GC · Scrub · Resilver   ║
║ Range GET / suffix     ║  config.toml           ║ CAR import/export       ║
║ BAO outboard (E23)     ║  graceful shutdown     ║ healthz · /metrics      ║
╚════════════════════════╝                        ╚═════════════════════════╝
            │                                               │
            └──────────────────────┬────────────────────────┘
                                   ▼
╔══════════════════════════════════════════════════════════════════════╗
║  ozd-app  — application layer                                        ║
║                                                                      ║
║  ┌────────────────────────────┐  ┌─────────────────────────────┐    ║
║  │ Pool                       │  │ CacheTier — SuperDisk (E25)  │    ║
║  │ HRW placement (free-weight)│  │ NVMe read-leg (Discord-style)│    ║
║  │ R=2 mirror / erasure 4+2   │  │ write-through, FIFO eviction │    ║
║  │ hedged read (E27 p99-adapt)│  │ single-flight coalescing     │    ║
║  │ handoff · MRF · speculative│  │ bitrot self-heal from pool   │    ║
║  └────────────────────────────┘  └─────────────────────────────┘    ║
║                                                                      ║
║  ┌──────────────────────────────────────────────────────────────┐    ║
║  │ GC (discard-ratio, CAS-move)  · Scrub (deep-CRC, cursor)     │    ║
║  │ Resilver (walk add-only, R)   · HealQueue (priority+bulkhead)│    ║
║  │ BgThrottle (AIMD leaky-bucket)· DiskSlowMonitor (EWMA E28)   │    ║
║  │ Erasure 4+2 (Reed-Solomon)    · Migration mirror→erasure      │    ║
║  │ BLAKE3 outboard (abao E23)    · CAR import/export (E22)      │    ║
║  │ OpsMetrics 30+ atomics        · RollingP99 (22 log2-buckets) │    ║
║  └──────────────────────────────────────────────────────────────┘    ║
╚══════════════════════════════════════════════════════════════════════╝
          │                         │                      │
          ▼                         ▼                      ▼
╔══════════════════╗  ╔═══════════════════════╗  ╔══════════════════════╗
║ ozd-engine        ║  ║ ozd-zfs               ║  ║ ozd-domain           ║
║ DiskEngine        ║  ║ Runner (Local/Sudo)   ║  ║ traits:              ║
║ pack-segs ≤2GB    ║  ║ HealthFsm 4-state     ║  ║ BlockStore           ║
║ redb index NVMe   ║  ║ Properties+Source     ║  ║ ShardEngine          ║
║ CRC32 / zstd      ║  ║ drift-audit 60 pools  ║  ║ PlacementPolicy      ║
║ addr v3 (36B)     ║  ║ user-props ozd:*      ║  ║ piece (EC envelope)  ║
║ ballast / WAL-f/o ║  ║ freeing→eff_free      ║  ║ DomainError          ║
║ fadvise DONTNEED  ║  ║ sentinel errors       ║  ║                      ║
╚════════╤══════════╝  ╚═══════════════════════╝  ╚══════════════════════╝
         │
         ▼
╔══════════════════════════════════════════════════════════════════════╗
║  Physical Storage                                                    ║
║                                                                      ║
║  ┌─────────────────────────┐    ┌──────────────────────────────┐    ║
║  │  NVMe SSD               │    │  60 × HDD  (JBOD)            │    ║
║  │  redb — CID index       │    │  XFS per disk                │    ║
║  │  CacheTier segments     │    │  pack-segments ≤2GB          │    ║
║  │  T_CURSOR (checkpoints) │    │  per-disk ZFS pool (ozd-zfs) │    ║
║  │  ballast.bin (E18)      │    │  ~480TB полезных при R=2     │    ║
║  └─────────────────────────┘    └──────────────────────────────┘    ║
╚══════════════════════════════════════════════════════════════════════╝
```

## Идея (Часть 1)

```
                 ┌───────────────────────────────────────────────┐
   IPFS clients  │              ОДИН IPFS-демон                  │
  (Bitswap/HTTP) │   libp2p · Bitswap · DHT · UnixFS · RPC API   │
  ───────────────►                                               │
                 │            BlockStore (port/trait)            │
                 └───────────────────────┬───────────────────────┘
                                         │  get/put/has/delete(CID)
                         ┌───────────────▼─────────────────┐
                         │   SHARDED BLOCKSTORE (Pool)     │   ← наш домен
                         │   CID → hash(CID) → выбор vdev  │
                         └──┬────────┬────────┬────────┬───┘
                            │        │        │        │
                         ┌──▼──┐  ┌──▼──┐  ┌──▼──┐  ┌──▼──┐
                         │disk0│  │disk1│  │disk2│  │diskN│ ← physical shards vdev)
                         └─────┘  └─────┘  └─────┘  └─────┘
```

Демон видит **единый** blockstore. Физически блоки детерминированно распределены по дискам,
с **репликацией** (R копий на R разных дисках — как mirror-vdev в ZFS).
Логический путь блока (replication factor R):

```
CID ──► placement(CID, topology) ──► [ShardId₁ .. ShardId_R]   (top-R по HRW)
                                       │        │
                                  put на каждый из R дисков (write-quorum W из R)
```

## Чем это отличается от того, что уже есть на Rust

| Готовое решение | Хранилище | Чего не хватает для нашей цели |
|---|---|---|
| rust-ipfs / ipfs-embed | один blockstore (fs/sled) | нет распределения по нескольким дискам |
| iroh-blobs | redb + файлы, один store | не классические IPFS CID; один store |
| ipfs-sqlite-block-store | один SQLite | один файл/диск |

Готового «один IPFS-демон → много дисков как один blockstore» нет.
**Наш промежуточный слой (Pool) — оригинальная часть проекта.**

## Стек

- **Язык:** Rust (async, `tokio`)
- **IPFS-хост:** `rust-ipfs` (форк `dariusc93`) — даёт сеть и trait `BlockStore`, который мы реализуем
- **Носитель/ФС:** **XFS на каждом диске (JBOD)**, app владеет избыточностью — консенсус
  RustFS/MinIO. **Индекс CID — на NVMe** (app-level «special vdev»), тела блоков — на XFS-HDD.
  Не ZFS-пул. См. [ADR 0001](Wiki/adr/0001-storage-substrate.md)
- **Движок `ShardEngine`:** data-tier (XFS-HDD, **append-only pack-сегменты ≤2ГБ** + write-буфер +
  flushOffset) + index-tier (redb/NVMe, `CID→(seg,offset,len)`). Формат: TON `.pack` + geth freezer
- **Каталог:** **центрального нет** — расположение через placement; индекс у каждого диска свой
- **Хэш для placement:** Rendezvous (HRW), взвешенный по free space, top-R копий, domain-aware
- **Репликация:** R=2 (mirror), write-quorum, walk-based resilver/self-heal

## Документы

- [Wiki/ARCHITECTURE.md](Wiki/ARCHITECTURE.md) — **Variant A** (выбран): XFS + app-репликация. DDD: contexts, агрегаты, порты/адаптеры
- [Wiki/ARCHITECTURE-ZFS.md](Wiki/ARCHITECTURE-ZFS.md) — **Variant B** (альтернатива): ZFS владеет субстратом, тонкий демон, redb-on-ZFS
- [Wiki/PLAN.md](Wiki/PLAN.md) — пошаговый план реализации по фазам (для Variant A)
- [Wiki/PLAN-A-vs-B.md](Wiki/PLAN-A-vs-B.md) — пофазная дельта A↔B: что меняется при переключении на ZFS
- [Wiki/adr/0001-storage-substrate.md](Wiki/adr/0001-storage-substrate.md) — выбор ФС/носителя: A (XFS) vs B (ZFS), факты и решение
- [Wiki/Arch_DDD/HDD_SDD/TON-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/TON-storage-hdd-ssd.md) — разбор исходников TON: как он работает с HDD/SSD + извлечённые идеи
- [Wiki/Arch_DDD/HDD_SDD/go-ethereum-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/go-ethereum-storage-hdd-ssd.md) — разбор исходников go-ethereum: freezer/pebble/pathdb, HDD/SSD-тиринг + идеи
- [Wiki/Arch_DDD/HDD_SDD/quorum-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/quorum-storage-hdd-ssd.md) — разбор GoQuorum: off-chain content-addressed payload (это наша модель!), неймспейсинг, наследие geth 1.10.3
- [Wiki/Arch_DDD/HDD_SDD/pebble-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/pebble-storage-hdd-ssd.md) — разбор Pebble (LSM-движок): value separation = наши pack-сегменты, GC через liveness-битмапы, disk-slow/WAL-failover
- [Wiki/Arch_DDD/HDD_SDD/rocksdb-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/rocksdb-storage-hdd-ssd.md) — разбор RocksDB (каноничный LSM): tiered storage по temperature, NVMe L2-кэш тел, rate limiter, BlobDB
- [Wiki/Arch_DDD/HDD_SDD/oceanbase-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/oceanbase-storage-hdd-ssd.md) — разбор OceanBase (распределённая SQL-БД): macro/micro-блоки, fixed-block аллокатор+mark-sweep, nested-packing, IO-QoS, macro_cache
- [Wiki/Arch_DDD/HDD_SDD/ydb-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/ydb-storage-hdd-ssd.md) — разбор YDB (ближайший прототип!): PDisk/VDisk, Forseti cost-IO-scheduler, erasure block-4-2, fail-домены, handoff
- [Wiki/Arch_DDD/Network/YDB-Interconnect.md](Wiki/Arch_DDD/Network/YDB-Interconnect.md) — разбор YDB Interconnect (сетевой транспорт actor-системы): proxy/session, serial/confirm-надёжность, channel-scheduler, XDC/zero-copy, handshake/TLS, liveness
- [Wiki/Arch_DDD/HDD_SDD/polardb-pg-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/polardb-pg-storage-hdd-ssd.md) — разбор PolarDB-PG (compute/storage separation): PolarVFS (pluggable bio/dio/PFS), shared storage, consistent-LSN/copy-buffer/LogIndex (и почему нам они НЕ нужны)
- [Wiki/Arch_DDD/HDD_SDD/scylladb-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/scylladb-storage-hdd-ssd.md) — разбор ScyllaDB (Seastar): iotune (мерить диск!), ICS-фрагменты, backlog-controller, scheduling-groups, O_DIRECT+own-cache, scylla-manager (repair/backup)
- [Wiki/Arch_DDD/HDD_SDD/druid-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/druid-storage-hdd-ssd.md) — разбор Apache Druid: deep-storage + локальный кэш сегментов, декларативные load/drop-rules (тиры+реплики), спред по дискам (least-bytes-used), Smoosh, иммутабельные сегменты
- [Wiki/Arch_DDD/HDD_SDD/ignite-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/ignite-storage-hdd-ssd.md) — разбор Apache Ignite: page-memory + WAL + checkpoint (recovery дельтой), WAL-режимы, write-throttling, historical (WAL-delta) rebalance
- [Wiki/Arch_DDD/HDD_SDD/redis-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/redis-storage-hdd-ssd.md) — разбор Redis: RDB/AOF durability, сброс page-cache write-once (DONTNEED), неблокирующий writeback (sync_file_range), offload fsync/close в bio-потоки, манифест мультифайла, durable rename, diskless-репликация
- [Wiki/Arch_DDD/HDD_SDD/dragonfly-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/dragonfly-storage-hdd-ssd.md) — **★ самый близкий по задаче** разбор Dragonfly: tiered storage (тела на SSD, индекс в RAM), свой disk-аллокатор (256МБ-сегменты/size-class), SmallBins packing мелочи в 4КБ, O_DIRECT+io_uring registered buffers, read-coalescing, fork-less snapshot (версии бакетов), DFS-бэкап по файлу на шард
- [Wiki/Arch_DDD/HDD_SDD/iroh-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/iroh-storage-hdd-ssd.md) — **★ архитектурный близнец** разбор iroh-blobs (Rust, redb-индекс + файлы, content-addressed — наш стек): BLAKE3 verified streaming (outboard), inline-split в отдельные redb-таблицы, sparse bitfield+sizes для докачки, memory→disk spillover, per-key entity-актор, двухфазный delete-set+protect; single-store — наш Pool добавляет шардинг/HRW/R=2
- [Wiki/Arch_DDD/HDD_SDD/influxdb-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/influxdb-storage-hdd-ssd.md) — разбор InfluxDB 3 (Rust, Arrow/Parquet, object-store-centric — другая модель): WAL→buffer→иммутабельные Parquet в object-store + кэш; новое для нас — time-bucketed сегменты + drop-whole-file retention, checkpoint-rollup каталога (быстрый старт на масштабе), object-store-гигиена; колоночность/last-value — мимо (блоки непрозрачны)
- [Wiki/Arch_DDD/HDD_SDD/tarantool-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/tarantool-storage-hdd-ssd.md) — разбор Tarantool (vinyl LSM): зрелый LSM (конвергенция с RocksDB/Pebble/Scylla), но 2 ценных новых — vylog (метадата-лог-как-каталог + 2-фазные циклы PREPARE/CREATE, DROP/FORGET) и regulator (write-throttle по ИЗМЕРЕННОЙ bandwidth: гистограмма p10 + 0.75 headroom); slice ⚠️огранич (наши ключи — случайные CID), group-commit WAL
- [Wiki/Arch_DDD/HDD_SDD/hadoop-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/hadoop-storage-hdd-ssd.md) — **★ валидация дизайна** разбор HDFS (DataNode-с-многими-дисками = наш узел на 60 HDD): volume-choosing≈HRW, block-scanner≈scrub, JBOD+app-репликация≈ADR 0001, storage-types/lazy-persist≈тиринг; новое — tolerated-failed-volumes + live hot-swap, intra-node disk-balancer, scrub-приёмы (cursor/suspect/throttle), short-circuit read; контраст — NameNode central catalog (у нас нет) и directory-hashing (избегаем pack-сегментами)
- [Wiki/Arch_DDD/HDD_SDD/hive-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/hive-storage-hdd-ssd.md) — разбор Apache Hive (DWH поверх HDFS): тяжёлая конвергенция (ORC-колоночность непрозрачным блокам мимо, base+delta+compaction = LSM); берём 3 более острых приёма компакции/GC — splice-merge (копировать живые регионы байт-в-байт без перехэша), minor/major по порогам, reader-watermark Cleaner (MVCC-safe реклейм)
- [Wiki/Arch_DDD/HDD_SDD/flink-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/flink-storage-hdd-ssd.md) — разбор Apache Flink (state-backend = RocksDB → много конвергенции; file-merging≈pack-сегменты, multi-disk≈HRW, local-recovery≈deep-storage+кэш); берём оркестрацию чекпойнтов — changelog/DSTL (durable дельта-лог → RPO независим от объёма), инкрементальный backup + shared-segment refcount, TTL через compaction-filter
- [Wiki/Arch_DDD/HDD_SDD/kafka-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/kafka-storage-hdd-ssd.md) — **★ каноничная валидация pack-сегментов** разбор Apache Kafka (сегментный append-only лог + sparse mmap-индекс + page-cache + whole-segment retention + per-batch CRC + recovery-point — всё совпадает); новое — zero-copy sendfile (отдача без копий), durability через репликацию (ослабить per-write fsync), LazyIndex + warm-tail (быстрый старт на масштабе)
- [Wiki/Arch_DDD/HDD_SDD/nats-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/nats-storage-hdd-ssd.md) — разбор NATS JetStream filestore (ещё один сегментный append-only msg-store → тяжёлая конвергенция с Kafka, повторная валидация pack-сегментов); новое — dios (глобальный disk-I/O семафор поверх per-disk пулов), block-cache с idle-eviction + weak-ref (RAM без явного LRU), psim (вторичный индекс по атрибуту, ⚠️ не для CID)
- [Wiki/Arch_DDD/HDD_SDD/clickhouse-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/clickhouse-storage-hdd-ssd.md) — разбор ClickHouse (MergeTree, колоночная OLAP → тяжёлая конвергенция: parts≈сегменты, merge≈компакция, sparse+skip-индекс, IDisk≈PolarVFS; колоночность мимо); новое — declarative storage policies (volumes + move_factor + size-gate + TTL-move — единый каркас тиринга), hardlink instant FREEZE (мгновенный снимок→ленивый бэкап), zero-copy shared-object refcount + last-deletes (Ч3)
- [Wiki/Arch_DDD/HDD_SDD/cassandra-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/cassandra-storage-hdd-ssd.md) — разбор Apache Cassandra (распределённый LSM → очень сильная конвергенция со ScyllaDB: SSTable+memtable+compaction+summary+bloom+multi-disk; token-ring/vnodes — контраст с нашим HRW); новое **со снипетами кода** — merkle-tree anti-entropy repair (сверка реплик хэш-деревом, стрим только diff), tombstone + gc_grace (distributed-delete без «воскрешения»), speculative retry (tail-latency дубль-read)
- [Wiki/Arch_DDD/HDD_SDD/dgraph-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/dgraph-storage-hdd-ssd.md) — **★ архитектурный двойник** разбор Dgraph + его движка **Badger** (WiscKey value-log separation): `valuePointer{Fid,Offset,Len}` в LSM → тело в `.vlog` = БУКВАЛЬНО наш индекс `CID→(seg,off,len)` + pack-сегмент (прямая валидация ADR-0001); новое **со снипетами кода** — value-log GC по discard-счётчику+discardRatio (write-amp≈2×, #122), StreamWriter/bulk-loader (внешняя сортировка→сегменты напрямую, #123), raftwal pre-zeroed recovery-layout (#124), MoveTs read-fence на ребалансе (#125), ⚠️group-varint delta-pack только для сортированных uint64 (#126)
- [Wiki/Arch_DDD/HDD_SDD/cockroach-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/cockroach-storage-hdd-ssd.md) — разбор CockroachDB (на движке **Pebble** → сильная конвергенция: LSM/value-sep/компакция/WAL/bloom/disk-slow уже у нас; range-split по 512МБ — **контраст** с нашим content-addressed+HRW); cockroach-уровневое **со снипетами кода** — ★ballast (graceful full-disk recovery, #127), ★WAL failover на запасной диск (#128), ★/proc/diskstats-монитор+stall-trace+градация unavailable→fatal (#129), ★2-уровневый порог заполнения с гистерезисом 0.95/0.925 анти-пинг-понг (#130), ★admission elastic disk-bandwidth-токены, foreground не душить (#131), ★IngestAndExcise+range-tombstone (#132)
- [Wiki/Arch_DDD/HDD_SDD/qdrant-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/qdrant-storage-hdd-ssd.md) — разбор Qdrant (векторная БД на **Rust**; вектор-специфика quantization/HNSW — ⚠️ **неприменима**, мы content-addressed); ценное — **`gridstore`** (кастомный on-disk blob-store на Rust = референс нашего слоя) **со снипетами кода**: ★bitmask-аллокатор + per-region gap-summary (точечный re-use без компакции, #133), ★crash-safety «течь, но не портить» (порядок flush, #134), ★madvise-дисциплина POPULATE_READ+WILLNEED+low-memory (#135), ★SeqLock lock-free чтение (#136), ★TTL-кэш free-space (#137); конвергенция: WAL CRC-цепочка=recovery-point, atomic-save=durable-swap, vacuum/merge=компакция
- [Wiki/Arch_DDD/HDD_SDD/rustfs-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/rustfs-storage-hdd-ssd.md) — **★ самый близкий аналог** разбор RustFS (S3-объектное хранилище на **Rust**, MinIO-rewrite: erasure поверх JBOD, heal, scanner, bitrot, **no-central-catalog** — буквально наш домен); мощная валидация (JBOD+app-EC=ADR-0001, bitrot=CRC+scrub, fsync-ignored+temp→rename=#111, DONTNEED=#63, tiering=#116) + новое **со снипетами кода**: ★erasure-set + per-object distribution-array (чертёж Части-2, #138), ★self-describing xl.meta + quorum-pick-latest (no-central-catalog конкретно, #139), ★heal priority-queue + dedup + bulkhead + MRF (#140), ★scanner cycle-budget + jitter + deep/normal-каденс (#141), ★disk-health 4-state FSM Online/Suspect/Offline/Returning (#142); distributed-lock ⚠️ Часть 3
- [Wiki/Arch_DDD/HDD_SDD/discord-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/discord-storage-hdd-ssd.md) — разбор инженерных статей Discord (блог, не код): **★ super-disk** — асимметричное зеркало md RAID1: read-нога NVMe-RAID0 + durable-нога `write-mostly` вне read-балансировки, self-heal битого сектора кэша (⚠️ урок: dm-cache/bcache отвергнуты) (#143); **★ request coalescing + consistent-hash routing по ключу** (без роутинга коалесинг не срабатывает, #144); **★ миграция dual-write + свой мигратор 3.2М строк/с + canary-чтения** (#145); валидация: Rust data-services слой = архитектура ozd, Cassandra→ScyllaDB p99 40–125мс→15мс
- [Wiki/Arch_DDD/HDD_SDD/go-zfs-storage-hdd-ssd.md](Wiki/Arch_DDD/HDD_SDD/go-zfs-storage-hdd-ssd.md) — разбор krystal/go-zfs (библиотека-обвязка zfs/zpool CLI; прицельно для нашего крейта ozd-zfs) **со снипетами кода**: ★подключаемый command-runner → тесты без zfs-бинаря (#146), ★CLI-дисциплина -H -p + cleanUpStderr + stderr→sentinel-ошибки (#147), ★типизированный Property-слой + Source-трекинг → дрифт-аудит 60 пулов (#148), ★user-properties `ozd:*` = метаданные НА датасете, самоописанный диск (#149), ★метрики freeing/fragmentation: эффективный free = free+freeing для HRW (#150)
- [Wiki/Arch_DDD/Network/scylladb-networking.md](Wiki/Arch_DDD/Network/scylladb-networking.md) — разбор сетевого слоя ScyllaDB: verb-RPC (Seastar) vs «Interconnect», gossip+φ-детектор, streaming, row-level repair, обучаемый словарь сжатия RPC
- [Wiki/Arch_DDD/Network/oceanbase-networking.md](Wiki/Arch_DDD/Network/oceanbase-networking.md) — разбор сетевого слоя OceanBase: obrpc (pcode-RPC) на pnio/libeasy, PALF (Paxos-лог), keepalive/election, stream-сжатие с ring-buffer
- [Wiki/Arch_DDD/Network/ton-networking.md](Wiki/Arch_DDD/Network/ton-networking.md) — **★ самый близкий нам** P2P-стек TON: ADNL (id=hash(pubkey)) / DHT (Kademlia) / Overlay (broadcast+FEC) / **RLDP+FEC** (надёжная передача крупного); маппинг на libp2p/Bitswap
- [Wiki/Arch_DDD/Network/NETWORKING-SYNTHESIS.md](Wiki/Arch_DDD/Network/NETWORKING-SYNTHESIS.md) — синтез 4 сетевых разборов: что берём в наш libp2p/Bitswap-слой (FEC-bulk, сжатие потока, twostep, приоритеты, φ-liveness) и в какую фазу
- [Wiki/Arch_DDD/HDD_SDD/STORAGE-IDEAS-SYNTHESIS.md](Wiki/Arch_DDD/HDD_SDD/STORAGE-IDEAS-SYNTHESIS.md) — синтез TON+geth: что берём в дизайн и в какую фазу плана
- [Wiki/Feynman/](Wiki/Feynman/README.md) — объяснения «методом Фейнмана» (простыми словами): geth, …

## Ubiquitous Language (термины ZFS как метафора)

| Термин проекта | ZFS-аналог | Значение |
|---|---|---|
| **Pool** | zpool | агрегат всех дисков = единый логический blockstore |
| **Shard / Vdev** | vdev | один физический диск в пуле |
| **Placement** | — | детерминированная функция `CID → Shard` |
| **Rebalance** | resilver | перенос блоков при изменении топологии |
| **Scrub** | scrub | проверка целостности (re-hash, сверка с CID) |
| **Pin** | — | блок, защищённый от GC |
| **Resilver** | resilver | walk-based восстановление R копий после смены топологии |
