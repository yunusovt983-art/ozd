# Feynman — объяснения «методом Фейнмана»

Простые объяснения сложных вещей: на аналогиях, без жаргона, как будто слышишь впервые.
Каждое заканчивается «проверкой понимания» — пересказом одним абзацем.
**Принцип папки: одна сущность — один файл.**

Покрывает сущности из [../Arch_DDD/HDD_SDD/STORAGE-IDEAS-SYNTHESIS.md](../Arch_DDD/HDD_SDD/STORAGE-IDEAS-SYNTHESIS.md)
(90 идей, 14 прототипов) и сетевые из [../Arch_DDD/Network/NETWORKING-SYNTHESIS.md](../Arch_DDD/Network/NETWORKING-SYNTHESIS.md)
(близкие/тривиальные сгруппированы в один файл).

## Обзор системы

- [geth.md](geth.md) — что такое go-ethereum (общая картина)

## Детали geth

- [geth-freezer.md](geth-freezer.md) — freezer-формат: полка + опись + бирка
- [geth-pathdb.md](geth-pathdb.md) — pathdb: картотека по адресу, а не по отпечатку пальца
- [geth-write-buffer.md](geth-write-buffer.md) — write-буфер: список покупок

## Ядро нашего дизайна

- [jbod-vs-raid.md](jbod-vs-raid.md) — ADR-0001: 60 банок вместо цистерны + лотерея равномерности
- [two-tier.md](two-tier.md) — data(HDD) + index(NVMe): склад + картотека у входа
- [pack-segments.md](pack-segments.md) — pack-сегменты: переплетённые тома + каталожная карточка
- [hrw-placement.md](hrw-placement.md) — HRW-placement: лотерея блок↔диск (вместо «mod N»)
- [replication-quorum.md](replication-quorum.md) — R=2 / W: документ в двух сейфах
- [walk-resilver.md](walk-resilver.md) — walk-resilver: библиотека без главного каталога
- [segment-gc.md](segment-gc.md) — GC сегментами: перепаковка живых, выброс старого тома целиком
- [inline-small.md](inline-small.md) — inline мелочи: держим в картотеке, не ходим на склад
- [namespacing.md](namespacing.md) — неймспейсинг индекса: один ящик с цветными разделителями
- [bloom-filter.md](bloom-filter.md) — Bloom/Ribbon: табличка «точно нет» на двери
- [nvme-cache.md](nvme-cache.md) — NVMe L2-кэш тел + раздельные пулы
- [forseti.md](forseti.md) — Forseti: умный диспетчер у единственной кассы
- [ephemeral-ttl.md](ephemeral-ttl.md) — pinned vs временные (TTL/FIFO)
- [ops-scheduler.md](ops-scheduler.md) — ops-планировщик: «завхоз с расписанием»
- [sparse-summary.md](sparse-summary.md) — sparse Summary: корешки-разделители в словаре

## Самоадаптация и тиринг

- [device-adaptation.md](device-adaptation.md) — disk-slow / WAL-failover / readahead: рефлексы под диск
- [scylladb-iotune.md](scylladb-iotune.md) — iotune: тест-драйв диска, а не вера в буклет
- [rocksdb-temperature.md](rocksdb-temperature.md) — температура: холодильник/кладовка/морозилка

## Прототип-специфичные приёмы

- [ton-pack.md](ton-pack.md) — TON `.pack`: фотоальбомы + слайсы + LRU
- [macro-micro.md](macro-micro.md) — OceanBase macro/micro: том → главы
- [fixed-block-allocator.md](fixed-block-allocator.md) — OceanBase: диск как камера хранения
- [ydb-erasure.md](ydb-erasure.md) — YDB erasure 4+2: «любые 4 ключа из 6 открывают сейф»
- [failure-domains.md](failure-domains.md) — realm/domain: не клади оба парашюта в один шкаф
- [handoff.md](handoff.md) — handoff: попросить соседа подержать посылку
- [off-chain-cas.md](off-chain-cas.md) — Quorum: номерок из гардероба (= наша модель)
- [polarvfs.md](polarvfs.md) — PolarVFS: универсальная розетка с переходниками
- [immutability.md](immutability.md) — контр-урок: фотография vs классная доска
- [compute-storage-separation.md](compute-storage-separation.md) — один склад, много читальных залов

## Свежие приёмы (Redis / Ignite / Dragonfly / iroh-blobs)

- [wal-checkpoint.md](wal-checkpoint.md) — Ignite: журнал + снимок → recovery индекса за секунды
- [historical-rebalance.md](historical-rebalance.md) — Ignite: догнать дельтой, не полным walk
- [page-cache-hygiene.md](page-cache-hygiene.md) — Redis: убрал со стола сразу + мусор порциями
- [segment-manifest.md](segment-manifest.md) — Redis: опись набора + durable rename
- [smallbins-packing.md](smallbins-packing.md) — Dragonfly: мелочь в одну коробку-страницу
- [disk-allocator.md](disk-allocator.md) — Dragonfly: malloc для диска (секции→полки→ячейки)
- [odirect-iouring.md](odirect-iouring.md) — Dragonfly: грузить фуру напрямую (O_DIRECT+io_uring)
- [read-coalescing.md](read-coalescing.md) — Dragonfly: один поход к колодцу для всех
- [cooling-tier.md](cooling-tier.md) — Dragonfly: стол → ящик → архив (hot/cool/cold)
- [dfs-backup.md](dfs-backup.md) — Dragonfly: каждый отдел сдаёт папку + оглавление
- [verified-streaming.md](verified-streaming.md) — iroh: пломбы на каждой секции (BLAKE3 outboard)
- [inline-split.md](inline-split.md) — iroh: тонкий каталог + отдельный сейф для тел
- [partial-bitfield.md](partial-bitfield.md) — iroh: чек-лист докачки + стол не безразмерный
- [entity-actor.md](entity-actor.md) — iroh: свой кассир на каждый занятый прилавок (per-CID)
- [two-phase-delete.md](two-phase-delete.md) — iroh: вычеркнуть из реестра, потом сжечь
- [chunk-range-fetch.md](chunk-range-fetch.md) — iroh: список «чего не хватает», а не «всё заново»
- [multi-source-fetch.md](multi-source-fetch.md) — iroh: заказать дефицит у нескольких поставщиков
- [observer-availability.md](observer-availability.md) — iroh: подписка на трек-номер (diff-only)
- [manifest-as-log.md](manifest-as-log.md) — Tarantool: вахтенный журнал склада + 2-фазные циклы
- [regulator.md](regulator.md) — Tarantool: пускать на мост по реальной скорости разбора пробки
- [slice-window.md](slice-window.md) — Tarantool: закладка-диапазон вместо ксерокопии главы
- [group-commit.md](group-commit.md) — Tarantool: один автобус вместо такси каждому + eof-маркер
- [tolerated-volumes.md](tolerated-volumes.md) — HDFS: гирлянда, где перегоревшая лампочка не гасит цепь
- [disk-balancer.md](disk-balancer.md) — HDFS: грузчик перекладывает коробки между полками одного склада
- [scrub-cursor.md](scrub-cursor.md) — HDFS: обходчик путей с маршрутным листом и отметкой «где остановился»
- [short-circuit-read.md](short-circuit-read.md) — HDFS: выдать ключ от склада вместо доставки курьером
- [splice-merge.md](splice-merge.md) — Hive: переплести готовые главы в новый том, не перепечатывая
- [minor-major-compaction.md](minor-major-compaction.md) — Hive: лёгкая уборка стола vs генеральная
- [reader-watermark-cleaner.md](reader-watermark-cleaner.md) — Hive: не сносить здание, пока внутри люди
- [incremental-backup.md](incremental-backup.md) — Flink: «Машина времени» вместо полной копии каждый день
- [changelog-dstl.md](changelog-dstl.md) — Flink: слать дневник изменений курьером, а не фотать весь архив
- [ttl-compaction-filter.md](ttl-compaction-filter.md) — Flink: выбрасывать просрочку во время переборки холодильника
- [zero-copy-sendfile.md](zero-copy-sendfile.md) — Kafka: трубопровод напрямую, а не «ведром через комнату»
- [durability-via-replication.md](durability-via-replication.md) — Kafka: документ в нескольких сейфах, а не нотариус каждой странице
- [lazy-index.md](lazy-index.md) — Kafka: не доставать все справочники с полок, пока не спросили
- [storage-policies.md](storage-policies.md) — ClickHouse: склад с зонами + правила зонирования (move-factor)
- [hardlink-freeze.md](hardlink-freeze.md) — ClickHouse: заложить закладки во все книги за секунду, вынести потом
- [shared-object-refcount.md](shared-object-refcount.md) — ClickHouse: общая комната, последний выключает свет

## Dgraph / Badger

- [vlog-discard-gc.md](vlog-discard-gc.md) — Badger: табличка «протухло, кг» на ящике + порог окупаемости перепаковки
- [bulk-stream-writer.md](bulk-stream-writer.md) — Dgraph: переезд библиотеки — рассортировать на полу, ставить на полки начисто
- [wal-prezero-slots.md](wal-prezero-slots.md) — Dgraph: тетрадь в клеточку, заранее стёртая ластиком (ноль = конец)
- [move-ts-fence.md](move-ts-fence.md) — Dgraph: переезд офиса с табличкой «работаем с такого-то числа»

## CockroachDB

- [ballast-recovery.md](ballast-recovery.md) — CockroachDB: спасательный балласт воздушного шара (сбросить → расклинить диск)
- [wal-failover.md](wal-failover.md) — CockroachDB: вторая касса, когда у первой завис терминал
- [disk-fullness-hysteresis.md](disk-fullness-hysteresis.md) — CockroachDB: термостат с «мёртвой зоной» (анти-пинг-понг)
- [elastic-io-tokens.md](elastic-io-tokens.md) — CockroachDB: полоса для скорой, грузовики едут по остатку

## Qdrant

- [bitmask-allocator.md](bitmask-allocator.md) — Qdrant: гардероб с лампочками-номерками + доска «где свободно»
- [leak-not-corrupt.md](leak-not-corrupt.md) — Qdrant: кладовщик сначала вешает бирку «занято» (утечка, не порча)
- [madvise-discipline.md](madvise-discipline.md) — Qdrant: записка библиотекарю, как подавать книги
- [seqlock.md](seqlock.md) — Qdrant: табло вылетов — все смотрят, никто не держит в руках
- [capacity-ttl-cache.md](capacity-ttl-cache.md) — Qdrant: табло «свободно» у парковки, раз в N секунд

## RustFS

- [erasure-set-distribution.md](erasure-set-distribution.md) — RustFS: страницы книги по сейфам + запасные (erasure), перестановка на книгу
- [self-describing-meta.md](self-describing-meta.md) — RustFS: у каждого экземпляра вшита выходная страница (каталог не нужен, версия — кворумом)
- [heal-priority-queue.md](heal-priority-queue.md) — RustFS: приёмное отделение больницы (триаж heal: приоритет, дедуп, лимит, MRF)
- [scanner-cycle-budget.md](scanner-cycle-budget.md) — RustFS: обход охранника с лимитом времени + выборочный досмотр
- [disk-health-fsm.md](disk-health-fsm.md) — RustFS: светофор здоровья диска (4 состояния с гистерезисом)

## Discord (блог)

- [write-mostly-mirror.md](write-mostly-mirror.md) — Discord: читальный зал + архив-сейф (read-нога и write-mostly копия)
- [coalesce-routing.md](coalesce-routing.md) — Discord: все вопросы об одной книге — к одному библиотекарю
- [store-migration.md](store-migration.md) — Discord: переезд библиотеки без закрытия (dual-write + свой грузовик + сверка)

## go-zfs (ZFS-обвязка)

- [command-runner-port.md](command-runner-port.md) — go-zfs: окошко снабжения на кухне (exec через порт + sentinel-ошибки)
- [property-source-drift.md](property-source-drift.md) — go-zfs: этикетка говорит, КТО наполнил банку (Source → дрифт-аудит)
- [zfs-user-props-identity.md](zfs-user-props-identity.md) — ZFS: гравировка на собаке, а не бирка на будке (ozd:shard_id)
- [freeing-effective-free.md](freeing-effective-free.md) — ZFS: деньги «в пути» на счёте (free + freeing = честный вес)

## Сетевой слой

- [fec-transfer.md](fec-transfer.md) — FEC: «волшебный порошок» вместо страниц
- [ton-rldp.md](ton-rldp.md) — TON RLDP: курьерская служба поверх FEC
- [dictionary-compression.md](dictionary-compression.md) — словарь сокращений для телеграмм

> Технические разборы тех же систем — в [../Arch_DDD/](../Arch_DDD/) (storage и networking).
