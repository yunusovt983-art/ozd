# ADR 0001 — Файловая система и носитель под blockstore

- **Статус:** Принято (2026-06-08)
- **Контекст-домены:** Block Storage (core), Storage Pool
- **Связано:** [ARCHITECTURE §8](../ARCHITECTURE.md#8-целевой-масштаб-60--hdd-на-одном-сервере)

## Контекст

Целевой деплой: **один сервер, 60 × HDD**, один IPFS-демон + S3, ~3,8 млрд immutable
content-addressed блоков (~256 KiB), ~480 ТБ полезных при R=2. Главный риск
производительности — **random seek на HDD при доступе к мелким блокам**.

Вопрос: на чём держать per-disk blockstore — **OpenZFS** или **XFS**, и как решить
проблему seeks.

Наш демон **уже владеет** репликацией (HRW top-R, R=2), целостностью (CID = hash, scrub) и
self-heal (walk-based resilver). Значит избыточность/целостность на уровне ФС была бы **вторым
слоем** поверх app-слоя — это антипаттерн (двойная работа, потеря перфа, tech debt).

## Проверенные факты (2026)

| Тема | Findings | Источник |
|---|---|---|
| **TON** | NVMe + **64k+ IOPS обязательно**; скорость диска критична; archive ~12 ТБ | [TON docs](https://docs.ton.org/ecosystem/nodes/cpp/run-validator) |
| **Ethereum** | **ext4/XFS > ZFS** по перфу; «ZFS subpar write на NVMe для blockchain»; ext4 ~6× IOPS vs btrfs; ZFS — ради compression/snapshots | [yorickdowne](https://gist.github.com/yorickdowne/f3a3e79a573bf35767cd002cc977b038), [ETH on ZFS](https://gist.github.com/pryce-turner/bc14b70ff36ec11e417ef341361b2c5f) |
| **RustFS** (Rust S3) | **строго XFS на всех дисках**; ext4/BTRFS/**ZFS избегать**; **JBOD**, redundancy делает app; `mkfs.xfs -i size=512 -n ftype=1` | [RustFS docs](https://docs.rustfs.com/installation/linux/single-node-single-disk.html) |
| **MinIO** | то же: **XFS + JBOD, без RAID/ZFS/LVM** — erasure coding в app | [MinIO reqs](https://docs.min.io/enterprise/aistor-object-store/reference/aistor-server/requirements/storage/) |
| **ZFS special vdev** | metadata — всегда; small blocks — опц. `special_small_blocks` (**дефолт 0=off**); **постоянное хранилище, не кэш**; только stripe/mirror; при заполнении спилит на HDD; **потеря неизбыточного special = потеря пула** | [openzfs #14542](https://github.com/openzfs/zfs/discussions/14542) |
| **OpenZFS версия** | **2.3** (13.01.2025) — RAIDZ expansion, Fast Dedup, **Direct I/O** (bypass ARC для NVMe), Linux ≤6.12 | [Phoronix](https://www.phoronix.com/news/OpenZFS-2.3-Released) |
| **XFS мелкие файлы** | до млрд файлов; `-i size=512 -n ftype=1`, mount `inode64,logbsize=256k,noatime` | [RHEL XFS tuning](https://oneuptime.com/blog/post/2026-03-04-tune-xfs-file-system-performance-mount-options-rhel-9/view) |
| **ZFS overhead** | ~10–30 % на крупных трансферах; RAM-голод; CoW-фрагментация при in-place записи | [openzfs #14346](https://github.com/openzfs/zfs/issues/14346), [Klara](https://klarasystems.com/articles/using-object-storage-with-openzfs-and-seaweedfs/) |

## Рассмотренные варианты

- **A. ZFS владеет пулом** (mirror + special vdev): демон тонкий, ZFS даёт пулинг/репликацию/
  целостность/кэш. Минус: дублирует то, что мы строим; overhead на запись; RAM; ZFS «не
  рекомендован» эко S3 (MinIO/RustFS); special vdev — риск потери пула.
- **B. ZFS single-disk, copies=1 + app-репликация**: ZFS как детектор bit-rot. Минус: общий
  special vdev невозможен на 60 пулах; overhead остаётся.
- **C. XFS + JBOD + app владеет всем + индекс/метаданные на NVMe** ← **выбрано**.

Ключевой разворот относительно первой интуиции: **преимущество ZFS special vdev (мелочь на SSD)
воспроизводимо на уровне приложения** — индекс на NVMe, блоки на XFS-HDD. Значит «убойная фича»
ZFS достижима на XFS проще и без overhead, а консенсус Rust-эко (RustFS) и blockchain-перф
указывают на XFS.

## Решение

**Вариант C: XFS, по одной ФС на диск (JBOD). Демон владеет репликацией, целостностью и
размещением. Горячие метаданные/индекс — на отдельном NVMe (app-level «special vdev»),
bulk-блоки — на XFS-HDD.**

- Формат: `mkfs.xfs -i size=512 -n ftype=1`; mount `inode64,logbsize=256k,noatime`.
- **Block data** → XFS-HDD, запись **write-once immutable** (flatfs-стиль `next-to-last/2`,
  `tmp→fsync→rename`) — нет in-place правок, нет фрагментации.
- **Per-disk local index** (CID → наличие/смещение) → **NVMe** (redb per shard). ~3,8 млрд
  записей ≈ 150–250 ГБ на NVMe — там это дёшево и быстро (random lookup без HDD-seek).
- Источник правды о расположении — детерминированный HRW `placement` (без центрального каталога).
- NVMe-индекс — это **производная** (восстановим из `ShardEngine.iter()` по данным на HDD), но
  для durability держим его на зеркале/с бэкапом, чтобы не пересобирать после краха.

## Последствия

**Плюсы:** совпадает с консенсусом RustFS/MinIO; max сырая скорость, низкий RAM; HDD-seeks
решены на app-уровне (индекс на NVMe); нет двойного слоя избыточности; immutable-запись дружит
с XFS (нет CoW-фрагментации); путь к multi-host (app-репликация по сети).

**Минусы / что принимаем:** нет FS-checksum (bit-rot ловим сами: CID-verify на чтении + scrub +
self-heal с реплики — обязательны); нет компрессии/снапшотов на уровне ФС; NVMe-индекс надо
делать durable (зеркало NVMe) и уметь пересобирать.

**Влияние на дизайн:** движок `ShardEngine` разделяется на **data-tier (XFS-HDD, write-once)** и
**index-tier (NVMe, redb)**. См. обновлённые ARCHITECTURE §4.2 и §8.

## Когда пересмотреть

Если приоритетом станут интегрированная целостность + компрессия + снапшоты ценой перфа/RAM —
перейти на **Variant B (ZFS владеет субстратом)**, отдав ZFS пулинг/избыточность/целостность и
упростив демон. Полная альтернативная архитектура уже проработана:
[ARCHITECTURE-ZFS.md](../ARCHITECTURE-ZFS.md) (mirror-пул + зеркальный special vdev,
`compression=lz4`, `dedup=off`, per-dataset `recordsize`, redb-индекс на NVMe). Переход
оформить сменой статуса этого ADR.
