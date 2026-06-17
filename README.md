# OpenZFS Daemon

> **Делаем аналог [Discord SuperDisk](https://discord.com/blog/how-discord-supercharges-network-disks-for-extreme-low-latency)** —
> NVMe read-leg поверх 60 HDD: все чтения по скорости NVMe, durability через репликацию на HDD.
> Discord: md RAID1 (RAID0×4 NVMe + Persistent Disk `write-mostly`) → iowait ÷2, p99 чтений 15мс.
> Наш подход: app-уровень (`CacheTier` на Rust) — write-through на NVMe + single-flight coalescing +
> self-heal с пула при битом секторе кэша. Бенч: EC 4+2 с кэшем = 0.08мс p50 (без кэша = 0.40мс).

> Кодовое имя проекта. По сути это **IPFS-демон с пуловым (ZFS-подобным) sharded-блокстором**:
> много физических дисков объединяются в один логический blockstore, как vdev'ы в zpool.

**Целевой деплой:** один сервер, **60 × HDD**, один IPFS-демон (~3,8 млрд блоков, ~480 ТБ
полезных при R=2). Специфика HDD/масштаба вынесена в [ARCHITECTURE §8](Wiki/ARCHITECTURE.md#8-целевой-масштаб-60--hdd-на-одном-сервере).

## Quick Start

```bash
# 1. Сборка
cargo build --release

# 2. Запуск (3 диска в /tmp для теста)
mkdir -p /tmp/ozd-d{0,1,2}
cat > /tmp/ozd-test.toml << 'EOF'
listen = "127.0.0.1:9100"
replicas = 2
write_quorum = 2
[[disks]]
data_path = "/tmp/ozd-d0"
[[disks]]
data_path = "/tmp/ozd-d1"
[[disks]]
data_path = "/tmp/ozd-d2"
EOF
./target/release/ozd-daemon --config /tmp/ozd-test.toml &

# 3. Проверка
curl -s http://localhost:9100/healthz        # → "ok"

# 4. PUT блок
curl -X PUT -d "hello ipfs" http://localhost:9100/kubo/blocks/test1

# 5. GET блок
curl http://localhost:9100/kubo/blocks/test1  # → "hello ipfs"

# 6. Метрики (Prometheus)
curl http://localhost:9100/metrics | head -20

# 7. Полный smoke-тест
./scripts/kubo_smoke.sh http://localhost:9100
```

### Docker (с Kubo)

```bash
cd deployments/docker
docker compose up --build
# ozd: localhost:9100, Kubo API: localhost:5001
ipfs --api=/ip4/127.0.0.1/tcp/5001 add README.md
```

## Архитектура крейтов

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  ozd — OpenZFS Daemon  ·  IPFS object storage on 60 HDD  ·  Rust / tokio / axum      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

                        ┌─────────────────────────┐
                        │   Kubo (IPFS daemon)    │
                        │   go-ds-s3 S3 plugin    │
                        └────────────┬────────────┘
                                     │ HTTP S3 API + SigV4
╔════════════════════════╗           ▼            ╔═════════════════════════╗
║ ozd-ipfs               ║◄──────────────────────►║ ozd-admin               ║
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
║  ┌────────────────────────────┐  ┌──────────────────────────────┐    ║
║  │ Pool                       │  │ CacheTier — SuperDisk (E25)  │    ║
║  │ HRW placement (free-weight)│  │ NVMe read-leg (Discord-style)│    ║
║  │ R=2 mirror / erasure 4+2   │  │ write-through, FIFO eviction │    ║
║  │ hedged read (E27 p99-adapt)│  │ single-flight coalescing     │    ║
║  │ handoff · MRF · speculative│  │ bitrot self-heal from pool   │    ║
║  └────────────────────────────┘  └──────────────────────────────┘    ║
║                                                                      ║
║  ┌──────────────────────────────────────────────────────────────┐    ║
║  │ GC (discard-ratio, CAS-move)  · Scrub (deep-CRC, cursor)     │    ║
║  │ Resilver (walk add-only, R)   · HealQueue (priority+bulkhead)│    ║
║  │ BgThrottle (AIMD leaky-bucket)· DiskSlowMonitor (EWMA E28)   │    ║
║  │ Erasure 4+2 (Reed-Solomon)    · Migration mirror→erasure     │    ║
║  │ BLAKE3 outboard (abao E23)    · CAR import/export (E22)      │    ║
║  │ OpsMetrics 30+ atomics        · RollingP99 (22 log2-buckets) │    ║
║  └──────────────────────────────────────────────────────────────┘    ║
╚══════════════════════════════════════════════════════════════════════╝
          │                         │                      │
          ▼                         ▼                      ▼
╔═══════════════════╗  ╔═══════════════════════╗  ╔══════════════════════╗
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
║  ┌─────────────────────────┐    ┌──────────────────────────────┐     ║
║  │  NVMe SSD               │    │  60 × HDD  (JBOD)            │     ║
║  │  redb — CID index       │    │  XFS per disk                │     ║
║  │  CacheTier segments     │    │  pack-segments ≤2GB          │     ║
║  │  T_CURSOR (checkpoints) │    │  per-disk ZFS pool (ozd-zfs) │     ║
║  │  ballast.bin (E18)      │    │  ~480TB полезных при R=2     │     ║
║  └─────────────────────────┘    └──────────────────────────────┘     ║
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

Архитектура, планы, ADR, разборы 29 систем, как они работают с файловой системой (TON, geth, YDB, RustFS, Discord, Kafka и др.)
и 130+ объяснений методом Фейнмана — в **[Wiki/](Wiki/)**.

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

## Статистика проекта (LOC)

### Код (Rust — `crates/`)

| Крейт | Строк (.rs) | Назначение |
|--------|------------|------------|
| `ozd-app` | ~5 254 | Бизнес-логика (pool, cache, CAR, erasure, placement…) |
| `ozd-engine` | ~2 335 | Движок сегментов + property-тесты + criterion bench |
| `ozd-ipfs` | ~1 322 | IPFS-слой + SigV4 + async-адаптер + e2e-тест |
| `ozd-zfs` | ~952 | Обёртка над OpenZFS CLI |
| `ozd-daemon` | ~795 | Точка входа демона |
| `ozd-admin` | ~634 | Админ-API (typed responses, capacity planning) |
| `ozd-bench` | ~335 | Бенчмарки |
| `ozd-domain` | ~331 | Доменные типы |
| **Итого .rs** | **~11 958** | |

### Документация и проектирование (`Wiki/`)

| Раздел | Строк |
|--------|-------|
| Архитектура, планы, ADR | ~2 857 |
| Arch_DDD (анализ 30+ систем) | ~12 747 |
| Feynman-карточки (96 концептов) | ~3 814 |
| Прочее (ROADMAP, KUBO, BENCH, WEEKLY-ARCS, GRAFANA…) | ~1 132 |
| **Итого Wiki** | **~20 550** |

### Инфраструктура (scripts + deployments)

| Категория | Строк |
|-----------|-------|
| Docker (compose, Dockerfile×2, конфиги) | ~230 |
| Scripts (gen_config, smoke) | ~198 |
| systemd + example.toml | ~131 |
| **Итого infra** | **~559** |

### Общий итог

| Категория | Строк |
|-----------|-------|
| Rust-код | ~11 958 |
| Документация (Wiki + README) | ~20 550 |
| Инфраструктура (Docker, scripts, deploy) | ~559 |
| **Всего по проекту** | **~33 067** |

## Лицензия

Этот проект распространяется на условиях **GNU Affero General Public License v3.0 (AGPL-3.0)**.

Подробнее см. файл [LICENSE](LICENSE).
