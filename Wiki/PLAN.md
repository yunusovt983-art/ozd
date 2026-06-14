# План реализации — OpenZFS Daemon (Часть 1)

Инкрементально: каждая фаза даёт работающий артефакт. Сначала ядро домена (placement +
sharded blockstore), потом подключение к IPFS, потом эксплуатационные фичи (resilver,
scrub, gc).

Легенда: 🎯 цель фазы · ✅ Definition of Done.

> Это план **Variant A (XFS)**. Дельта при переключении на Variant B (ZFS) — по фазам в
> [PLAN-A-vs-B.md](PLAN-A-vs-B.md). Точка невозврата — старт Фазы 2 (Фазы 0–1 общие).

> **Целевой деплой:** один сервер, **60 × HDD**. HDD-специфика (per-disk worker pool с малой
> глубиной, degraded start, domain-aware placement, параллельный heal, пул хэш-воркеров, отказ
> от центрального per-CID каталога) описана в [ARCHITECTURE §8](ARCHITECTURE.md#8-целевой-масштаб-60--hdd-на-одном-сервере)
> и вплетена в фазы ниже как HDD-задачи.

---

## Фаза 0 — Каркас воркспейса
🎯 Пустой, но компилируемый гексагон.

- [ ] `cargo new` workspace, 6 крейтов из ARCHITECTURE §6 (`ozd-engine` объединяет tier'ы).
- [ ] `ozd-domain`: типы-заглушки `Cid`, `Block`, `ShardId`, `BlockData`, `Capacity`,
      `ShardStatus`; traits `BlockStore`, `ShardEngine` (data-tier + index-tier), `PlacementPolicy`.
- [ ] Конфиг диска: `{ data_path (XFS-HDD), index_path (NVMe), cold_path?, domain, realm,
      segment_max_size=2ГБ, micro_block_size≈16КБ, fsync_items, fsync_interval,
      fsync_policy: per-write|on-seal|periodic, lazy_index: bool, sendfile_serve: bool,
      dios_max_concurrent_io, block_cache_idle_evict,
      storage_policy (tiers + move_factor + max_segment_size_per_tier + move_ttl), compress: none|zstd,
      delete_grace, speculative_retry: never|fixed|p99|hybrid, speculative_retry_ms≈100,
      read_preferred_replica: bool (реплика №1 = read-нога, №2+ = write-mostly, #143), merkle_repair: bool,
      vlog_gc_discard_ratio≈0.5, bulk_loader: bool, wal_prezero_slots: bool, move_ts_fence: bool,
      zfs_pool?, zfs_dataset?, zfs_health_interval_secs≈30, drive_suspect_threshold≈3,
      drive_recover_threshold≈2, scrub_interval_secs≈600, scrub_keys_per_cycle≈5000,
      ballast_size (1ГБ|%), wal_failover: off|among-disks|explicit-path, wal_failover_path?,
      max_sync_duration≈20s, disk_stall_fatal: bool, diskstats_poll≈100ms,
      fill_block≈0.95, fill_rebalance_to≈0.925, elastic_disk_max_util≈0.8, clear_range_threshold≈512КБ,
      inline_min, direct_io_index, bg_rate_limit, secondary_cache_path?,
      segment_alloc: append-only|fixed-block|segmented|bitmask, io_backend: std|uring|direct,
      bitmask_block_size≈128, bitmask_region_blocks≈8192, mmap_advice: random|sequential|normal,
      mmap_populate: bool, low_memory_mode: off|no-resident|no-populate, free_space_cache_ttl≈5s,
      small_bins: bool, device_type: auto|rot|ssd|nvme,
      max_size, free_space_percent, selector: hrw|least-bytes-used|round-robin,
      failed_disks_tolerated, scrub_period_days, scrub_bytes_per_sec, disk_balancer_bandwidth,
      scan_max_duration, scan_max_objects, scan_cycle_jitter, deep_scan_every_n_cycles,
      drive_suspect_threshold, drive_returning_threshold, drive_offline_grace, drive_long_offline,
      heal_max_concurrent_per_set, heal_mrf: bool,
      erasure_set_size?, erasure_parity? (Часть 2),
      compaction_garbage_ratio, compaction_delta_num_threshold,
      changelog_remote: off|cold_path|s3, changelog_persist_delay, changelog_persist_size, incremental_backup: bool,
      index_wal_mode: fsync|log_only|background|none,
      fadvise_dontneed: bool, writeback_chunk≈4МБ, diskless_resilver: bool,
      ephemeral_time_bucket?, manifest_checkpoint_interval, cold_store_max_inflight }`.
- [ ] **Device-type авто-профиль** (из YDB) + **iotune-калибровка** (из ScyllaDB): определить
      ROT/SSD/NVMe И **измерить** реальные IOPS/bandwidth каждого из 60 HDD (короткий бенчмарк на
      старте) → drive-model по измерениям, не по типу. Профиль: in-flight HDD 4 / NVMe 128,
      reorder 50мс/1мс, **inline_min по носителю** HDD 512КБ / SSD 64КБ.
- [ ] `ShardEngine`-адрес блока — `(segment_id, offset, len)`, а не «путь к файлу».
- [ ] **`ShardEngine` — подключаемый порт** (урок Quorum о «замороженном субстрате»): движок и
      формат сегментов сменяемы без переписывания домена.
- [ ] **PolarVFS-style бэкенды носителя** (из PolarDB): за `ShardEngine` — `xfs-file` /
      `raw-O_DIRECT` / `remote`, выбор по пути (`data_path`/`index_path`/`cold_path`). В Части 1
      реализуем `xfs-file`; raw/remote — заглушки за тем же портом.
- [ ] CI: `cargo fmt --check`, `clippy -D warnings`, `cargo test`.
- [ ] `error.rs`: доменные ошибки (`ShardUnavailable`, `IntegrityViolation`, `NotFound`...).

✅ `cargo build --workspace` зелёный; правило зависимостей соблюдено (`ozd-domain` без IO-крейтов).

---

## Фаза 1 — Один диск, два tier'а: pack-сегменты + индекс (вертикальный срез)
🎯 `ShardEngine` (pack-сегменты на XFS-HDD + redb-индекс на NVMe) работает end-to-end.
Формат принят из TON `.pack` + geth freezer — см. [SYNTHESIS](Arch_DDD/HDD_SDD/STORAGE-IDEAS-SYNTHESIS.md).

- [ ] **Data-tier (pack-сегменты, XFS-HDD):** append тел блоков в `seg.NNNN.dat`, ротация по
      `segment_max_size` (2ГБ). **Write-буфер**: копить в RAM → батч-flush по порогу буфера/fsync.
      `meta.flushOffset` на сегмент. Компрессия по политике (тела опц. zstd; CID/хэши — никогда).
      ✅ *Сжатие в ozd (E10/E11, формат v2): header 20Б (+logical_len, flags.zstd), CRC по stored,
      несжимаемое — как есть, GC переносит без перепаковки (splice-дух #104); `stat()` из индекса —
      HEAD/ListV2 без чтения тела; e2e 519КБ→4КБ.*
- [ ] **Лимиты диска (StorageLocation, из Druid):** per-диск `max_size` + резерв свободного места
      (`free_space_percent`); при превышении — авто-вытеснение не-pinned (reclaim), не переполнять диск.
- [ ] **★ Ballast-файл: graceful full-disk recovery (из CockroachDB):** на каждом диске держать
      **резервный файл** `~ballast_size` (дефолт `min(1ГБ, 1% ёмкости)`). Диск считать **«полон»**, когда
      `avail < ballast/2` — **ранний стоп приёма ДО реального нуля** (на нуле HDD «вешает» и GC/recovery).
      Оператор/демон **удаляет ballast** → мгновенно освобождает место расклинить диск и запустить GC.
      Резервировать **grow-only-if-safe**: растить ballast, только если `extend ≤ avail/4` ИЛИ останется
      `>10ГБ` (резерв не должен сам добить диск). Дополняет лимиты Druid и reclaim.
- [ ] **Macro/Micro split** (из OceanBase): сегмент = последовательность **микроблоков ~16КБ** —
      единиц **IO, сжатия и checksum**; адрес блока `(segment_id, micro_off, len)` указывает внутрь
      микро. Даёт частичное чтение одного микро без расжатия всего сегмента + точечную целостность
      (checksum per-micro, а не на весь блок/сегмент).
- [ ] (опц.) **Общий per-disk write-лог с коалесингом** (из YDB): один WAL на диск; записи разных
      сегментов/реплик коалесятся в один батч → один seek/батч на HDD (поверх write-буфера).
      **O_DSYNC** (data-sync без metadata) + **recycle сегментов** после flush (из ScyllaDB).
- [ ] **★ Group-commit + eof-маркер** (из Tarantool WAL): WAL/write-лог в **отдельном потоке**;
      батчить записи многих параллельных `put` в **один `fsync`** (durability возвращается всем разом)
      → амортизация fsync под нагрузкой. В конце файла/батча — **eof-маркер**: при старте отсутствие
      маркера = torn tail → отбросить неполный хвост (второй детектор рядом с `flushOffset`).
- [ ] **★ Pre-zeroed фикс-слоты в WAL/манифесте → zero = детектор хвоста (из Dgraph raftwal):**
      раскладка лог-файла = **регион фикс-размерных слотов** (напр. `term|index|dataOffset|type`),
      **занулённый при создании**, + переменные данные после смещения. Тогда (1) адресация записи —
      **O(1) арифметикой** `idx·slot_size` (не парсинг); (2) при рестарте идём по слотам **пока поле
      ≠ 0** — **зануление само маркирует конец** валидных записей (третий детектор хвоста рядом с
      `flushOffset` и eof-маркером, дёшево). Полагаться на **mmap (переживает крах процесса)**, `msync`
      — лишь выборочно для hard-reboot (как Badger `SyncWrites=false`, наш дефолт #57/#72).
- [ ] **★ WAL failover на запасной носитель (из CockroachDB):** при **стопе primary-диска** index-tier
      WAL (`MaxSyncDuration`-порог, #129) — **прозрачно переключать запись WAL на запасной путь**
      (другой NVMe / диск / явный `wal_failover_path`) → **latency коммита изолирована от одного
      тормозящего носителя** (критично на 60-HDD-узле). Режимы `wal_failover: off | among-disks |
      explicit-path`. При восстановлении primary — вернуться; `MaxSyncDuration` поднять, чтобы дать
      окно failover до fatal. Дополняет group-commit и pre-zeroed-слоты.
- [ ] (опц.) ⚠️ **Group-varint delta-pack отсортированных uint64 (из Dgraph codec):** offset-таблицы
      внутри сегмента / списки CID в манифесте, если **отсортированы**, паковать дельтами по 4
      (group-varint) — компактнее. **⚠️ Не для CID-тел** (случайные хэши → дельты не сжимаются), только
      для упорядоченных метаданных (как slice #98 / psim #115 — берём с оговоркой).
- [ ] (опц.) **Sparse in-RAM Summary** (из ScyllaDB): разрежённый индекс в RAM поверх redb
      («в какую область сегмента»), прореживаемый (downsampling) под давление памяти.
- [ ] **★ LazyIndex (отложенный mmap) + warm-tail** (из Kafka): per-segment индекс/Summary
      **не загружать (не mmap'ить) до первого доступа** → быстрый старт при тысячах сегментов
      (3,8 млрд блоков); бинарный поиск держит **горячий хвост** индекса резидентно (cache-friendly,
      меньше page-fault). LRU открытых per-segment индексов (лимит FD/памяти).
- [ ] **★ madvise-дисциплина для mmap (из Qdrant):** (1) **глобально `MADV_RANDOM`** для индекса/Summary
      (lookup'ы случайны); (2) **`MADV_POPULATE_READ`** — на загрузке **горячего** индекса prefault'нуть
      страницы (тёплый старт без серии page-fault; fallback — прочитать каждый 512-й байт); (3)
      **`MADV_WILLNEED`** — когда блок/значение **пересекает границы страниц** mmap, префетчить весь
      регион **одним syscall** (а не ловить fault постранично → на HDD это серия seek'ов); (4) парно к
      **`MADV_DONTNEED`** для write-once тел (#63). Зеркально low-memory-режимам ниже.
- [ ] **★ Low-memory режимы (из Qdrant `LowMemoryMode`):** один тумблер деградации под нехватку RAM:
      `no-resident` — компоненты грузить как **mmap-варианты** (индекс/Summary on-disk вместо резидентных);
      `no-populate` — то же + **пропустить prefault** (`POPULATE_READ`), всё грузится лениво по обращению.
      Байтовый формат тот же → возврат без rebuild. Дополняет LazyIndex и тиринг RAM↔mmap.
- [ ] **Index-tier (NVMe, redb):** `CID → (segment_id, offset, len)`; `put` пишет тело в сегмент,
      адрес — в индекс; `get/has` — через индекс; `iter` — обход индекса; `usage` — по сегментам.
      **Неймспейсинг префиксами** (из Quorum): `b|cid` (адрес), `p|cid` (пины), `s|seg` (мета).
- [ ] **★ Self-describing метаданные + quorum-pick-latest (из RustFS `xl.meta`):** каждый диск держит
      **самодостаточное** описание своих блоков (CID → адрес + checksum [+ erasure-конфиг/distribution в
      Части 2]) — **центральный каталог не нужен** (наш ADR): состояние пула восстановимо обходом дисков.
      При расхождении R копий (версия/mod-time после краша/handoff) — выбирать **актуальную кворумом**
      (`read_all` метаданных реплик → consensus по mod_time/etag → pick-latest), а не «первую живую».
      Конкретизирует «центрального каталога нет» и read-путь при неконсистентных репликах.
- [ ] **★ Inline-split в отдельную таблицу** (из iroh-blobs): inline-тела хранить **НЕ в строке
      адреса** `b|cid`, а в отдельном неймспейсе `i|cid` — основная таблица адресов остаётся
      **узкой** (фикс. размер `(seg,off,len)`), её `iter`/скан не тащит тела → быстрый обход 3,8 млрд.
- [ ] **Inline мелких блоков** (из Pebble): тела меньше `inline_min` хранить прямо в redb (в `i|cid`)
      (а не в сегменте) → минус seek на HDD для мелочи.
- [ ] (опц.) **External-reference mode** (из iroh-blobs `DataLocation::External`): индекс может
      ссылаться на **уже лежащий на диске файл без копирования байтов** (zero-copy импорт/дедуп) —
      адрес-вариант «внешний путь» вместо `(seg,off,len)`. Нишево (миграция/импорт существующих данных).
- [ ] **★ SmallBins packing мелких тел** (из Dragonfly): тела между `inline_min` и ~½ страницы
      паковать **по нескольку в одну выровненную страницу** (~4–16КБ): формат `N | (cid,len)×N |
      value×N`; страница освобождается при refcount живых → 0; при заполнении **<50%** — пометить на
      **дефраг** (перечитать живые + пересобрать). Между inline (крошки) и micro/сегментом (крупное).
- [ ] **★ O_DIRECT + io_uring для тел** (из Dragonfly): HDD-сегменты опц. через **O_DIRECT** +
      **io_uring** с пулом **pre-registered буферов** (без пере-pinning на каждый I/O); весь body-I/O
      **page-aligned (4КБ)**. Профиль по `device_type`; `io_backend: std|uring|direct`.
- [ ] **★ Read-coalescing тел** (из Dragonfly OpManager): дедуп одновременных чтений одной страницы/
      микроблока (`pending_reads` по offset) → один HDD-seek обслуживает N параллельных `get`.
- [ ] (опц.) **Cooling-слой записи** (из Dragonfly, адаптировано): свежезаписанные тела держать
      недолго в RAM-буфере (cool) до seal'а сегмента → быстрый повторный `get` без диска. У нас тела
      **иммутабельны** — без promotion обратно в RAM (это делает NVMe L2 read-кэш, Ф4).
- [ ] **Async pre-grow сегментов** (из Dragonfly): расширять/преаллоцировать следующий сегмент
      **заранее в фоне** (при <15% свободного места в активном) + backoff при ошибке → запись не
      упирается в рост файла.
- [ ] **Direct-I/O политика** (из RocksDB): NVMe-индекс — direct; HDD-сегменты — через page-cache.
- [ ] **★ Сброс page-cache для write-once тел** (из Redis, `POSIX_FADV_DONTNEED`): после flush+fsync
      участка сегмента — выкинуть его из page-cache. Холодные тела (random-read раз) **не вытесняют**
      горячий индекс/Summary. (Дополняет direct-I/O-политику: HDD пишем через cache, но сразу чистим.)
- [ ] **★ Неблокирующий инкрементальный writeback** (из Redis, `sync_file_range`): write-буфер
      сбрасывать чанками ~4МБ с `SYNC_FILE_RANGE_WRITE` + `WAIT_BEFORE` на 2× окне → грязные
      страницы ограничены (~8МБ) → **нет fsync-столла** в конце записи сегмента (fallback — обычный fsync).
- [ ] **Offload fsync/close в фоновый пул** (из Redis bio): `fsync`/`close` сегментов и WAL —
      в per-disk фоновый пул (FIFO, порядок сохранён) → горячий путь не блокируется на медленном диске.
- [ ] **Манифест сегментов per-disk** (из Redis AOF-manifest): опись набора `seg.NNNN` со статусом
      `active|sealed|pending-delete` — обновлять **атомарной перезаписью манифеста**, а не rename-на-файл;
      отработавшие (`pending-delete`) удаляются фоном. Источник правды о наборе сегментов диска.
- [ ] **Durable atomic swap** (из Redis): финализация сегмента/манифеста — `temp → fsync →
      rename → fsync(каталога)`, чтобы rename пережил краш (иначе rename может «откатиться»).
- [ ] **★ Checkpoint-rollup манифеста/каталога** (из InfluxDB): манифест сегментов вести как
      **дельты + периодический checkpoint** (свёртка дельт в агрегат). Старт/recovery = загрузить
      **последний checkpoint + свежие дельты**, а не обходить все сегменты/всю историю манифеста →
      O(checkpoint + recent) на 3,8 млрд. Сворачивает «много мелких файлов манифеста» (как table-index
      merge). Поверх WAL+checkpoint индекса (#59).
- [ ] **★ Changelog/DSTL: durable-remote дельта-лог (из Flink):** index/manifest-WAL **непрерывно
      стримить в `cold_path`/S3** (батч по size/time, backpressure по in-flight-байтам; опц.
      dual-write local+remote). Точка восстановления (backup) = указатель на дельта-лог → **RPO до
      секунд НЕЗАВИСИМО от 480ТБ** (грузим дельту, не весь стор). Периодически **материализовать**
      лог в checkpoint-rollup (см. выше) → старый лог удалить. Расширяет WAL+checkpoint #59 до
      durable-remote (RPO/durability на отдельном носителе, переживает потерю узла).
- [ ] **★ Манифест = append-only лог структурных событий + 2-фазные жизненные циклы** (из Tarantool
      vylog): не переписывать манифест, а **дописывать события** (`segment-prepare/create`,
      `segment-drop/forget`). **`prepare`→`create`**: если краш между ними → `prepare` без `create` =
      **осиротевший сегмент** (запись/компакция оборвалась) → удалить при recovery. **`drop`→удалить
      файл→`forget`**: крах-безопасное удаление (нет «удалили файл, но числится»). Это уточняет манифест
      (#66) и двухфазный delete (Ф5) — единая модель каталога-как-лога (наш «нет central catalog»).
- [ ] (опц.) **dict-компрессия тел** (zstd dictionary) для похожих мелких блоков.
- [ ] (опц.) **durability index-tier на запасной NVMe** при стопе primary (Pebble WAL-failover).
- [ ] **WAL + checkpoint для index-tier (из Ignite):** индекс-операции писать в WAL (sequential,
      по `index_wal_mode`: fsync/log_only/background) + периодический checkpoint-маркер. Recovery =
      **replay WAL с последнего checkpoint** (быстро), а **полный обход 3,8 млрд сегментов** — лишь
      фолбэк при потере и WAL, и индекса.
- [ ] **Crash-recovery:** на старте хвост активного сегмента за `flushOffset` отбрасывается;
      индекс восстанавливается WAL-replay'ем с checkpoint (фолбэк — обход сегментов, он производный).
- [ ] **★ Durability через репликацию, НЕ fsync-на-запись (из Kafka):** не fsync'ить каждый блок
      (дорого на HDD) — durability обеспечивают **R=2 реплики (W=2) + recovery-point + per-micro CRC +
      torn-tail** (flushOffset/eof). fsync — **на seal сегмента / периодически** (page-cache writeback
      сам сбросит). Конфиг `fsync_policy: per-write | on-seal | periodic`; **clean-shutdown marker** →
      при чистом стопе recovery почти не нужен; recovery re-validate **только грязный хвост** после
      recovery-point (не все 3,8 млрд). Главный выигрыш — throughput записи на 60 HDD.
- [ ] **★ Per-CID сериализация (entity-актор, из iroh-blobs):** операции на одном CID
      сериализуются через **один логический handle на хэш** (без глобальных локов) → дедуп
      одновременных `put` одного CID + согласование с read-coalescing (#73); пул акторов
      **idle-recycle** (не плодить состояние на 3,8 млрд ключей — актор живёт, пока есть операции).
      **★ Дополнение из Discord (#144):** коалесинг срабатывает, только если дубли одного ключа
      **встречаются в одном воркере** — внутри демона это даёт сам актор (роутинг по CID на handle);
      при нескольких gateway (Часть 3) нужен **consistent-hash routing запросов по CID на инстанс**,
      иначе дубли размазаны и коалесинг мёртв.
- [ ] **★ Crash-safety «течь, но не портить» — порядок записи (из Qdrant gridstore):** упорядочить
      фазы flush так, чтобы **любой крах давал безопасную утечку места, а не порчу/потерю данных**.
      Порядок: **сначала разметить занятость (allocator/bitmap/манифест) → записать тело в сегмент →
      обновить индекс `CID→addr` → освободить старые блоки**. Крах в середине → блоки помечены «занято»,
      но индекс на них не ссылается → **осиротевшее место** (не переиспользуем, чинит фон-GC/scrub),
      данные целых блоков **не повреждены**. Без отдельного recovery-лога. Дизайн-принцип поверх
      `flushOffset`, манифеста (#66) и two-phase-delete (#84): предпочесть утечку потере.
- [ ] Property-тесты: `put(x); get == x`; `delete` идемпотентен; рандомные CID; **краш в середине
      батча → recovery по `flushOffset`**; краш между data и index → консистентность.
- [ ] Бенч: запись/чтение 100k мелких + N крупных; проверить sequential-append на HDD, что
      lookup идёт по NVMe без HDD-seek, и что чтение одного блока тянет **один микроблок** (а не
      весь сегмент); per-micro checksum ловит повреждение.

✅ Блок кладётся/достаётся; тело в сегменте на XFS-HDD, адрес в redb на NVMe; recovery и бенч проходят.

---

## Фаза 2 — Pool + Placement + Репликация (СЕРДЦЕ проекта)
🎯 Много дисков как один логический blockstore, с R копиями (детерминированно).

- [ ] `PlacementPolicy` — **подключаемая стратегия** (паттерн Druid StorageLocationSelector):
      `Modulo` (baseline), `RendezvousHrw` (взвешенный по `free` — наш дефолт, ≈ least-bytes-used),
      опц. round-robin/random; `select(cid, topology, rf) -> Vec<ShardId>` (top-R различных дисков).
- [ ] **★ 2-уровневый порог заполнения с гистерезисом (из CockroachDB allocator):** два порога —
      `fill_block≈0.95` (диск **не цель** размещения + активно **сбрасывать** на него не-pinned) и
      `fill_rebalance_to≈0.925` (диск **не цель ребаланса**, строже). Буфер 0.925–0.95 = **гистерезис
      против пинг-понга** блоков между дисками (без него реплики «скачут» туда-обратно). **Compare-cascade**
      выбора кандидата: `valid > disk-health(не full/не Faulted) > diversity(домен) > io-overload >
      ровность(free) > число блоков` — **здоровье диска важнее ровности**. Применять в HRW-by-free
      (#2) и disk-balancer (#101).
- [ ] **★ TTL-кэш ёмкости/free-space (из Qdrant `disk_usage`):** HRW-by-free и allocator опрашивают
      `free`/`avail` **на каждом** `put`/ребалансе → `statvfs` на 60 дисков = шторм сисколлов.
      Кэшировать результат на **~5с** (TTL) per-disk → опрос гасится, веса HRW стабильны в окне.
      Обновляется фоном/по событию (flush сегмента, GC). Дешёвая телеметрия ёмкости для placement.
- [ ] **★ SeqLock для горячего разделяемого состояния (из Qdrant `trififo`):** топология/веса
      free-space/счётчики-кэша/горячий хвост индекса читаются на горячем пути **очень часто** многими
      воркерами. Использовать **seqlock** (читатели **не блокируются**: читают, сверяют seq, ретрай при
      конкурентной записи; писатель редок) → нет contention на 60-wide параллелизме. Применимо к
      snapshot топологии для placement и кэш-статистике (не для самих данных блоков).
- [ ] `Pool` (aggregate root): `attach/detach`, `put/get/has/delete`, `locate`; параметры R, W.
- [ ] `StoreBlock`: запись на R дисков параллельно, успех при ≥W; `FetchBlock`: чтение с
      первой живой реплики; `DeleteBlock`: удаление со всех R.
      ✅ *Параллельная запись в ozd v0.1: потоки на каждую ногу, латентность = max (тест
      `parallel_put_latency_is_max_not_sum`: 2×150мс-ноги → put <260мс, не 300).*
- [ ] **★ Асимметричные реплики: read-нога + write-mostly (из Discord super-disk):** порядок HRW
      детерминирован → **реплика №1 = стабильная READ-нога** (все чтения идут ей — её page-cache
      греется, cache-affinity), **реплики №2+ = write-mostly** (пишутся всегда, читаются **только при
      отказе/таймауте** read-ноги — как md-флаг `write-mostly`: «исключена из read-балансировки»).
      Связка со speculative retry (#121): таймаут read-ноги → hedged-дубль write-mostly-ноге, берём
      первый ответ. ⚠️ Урок Discord: НЕ block-кэш-прослойки (dm-cache/bcache — битый сектор кэша валит
      чтение); битая read-нога → чтение с write-mostly + heal (наш CRC+heal это уже даёт). На
      ZFS-деплое тот же паттерн ярусом ниже: **L2ARC/special-vdev на NVMe** (промах кэша → чтение с
      пула, безопасно как write-mostly). ✅ *Реализовано в ozd v0.1: `Pool::get` reps[0]-first +
      hedged read `speculative_retry_ms` (тесты `speculative_retry_hedges_slow_read_leg`,
      `write_mostly_fallback_on_read_leg_failure`).*
- [ ] **HDD:** per-disk worker pool, глубина inflight 1–4 (параллелизм шириной 60, не глубиной).
- [ ] **HDD:** пул хэш-воркеров (sha2-256 многоядерно), domain-aware выбор 2-й реплики.
- [ ] Тест распределения: 1M CID, R=2, по N дискам → равномерность ±X% и **ровно R копий**
      на разных дисках; при `N→N+1` «переезжает» ≈`1/(N+1)` (HRW vs modulo).
- [ ] Тест устойчивости: убрать 1 диск из топологии → все блоки ещё читаются (вторая реплика).
- [ ] (опц.) **Ribbon/Bloom CID-фильтр per-disk** (из RocksDB): компактный фильтр «есть ли CID на
      диске» перед опросом redb R дисков-кандидатов → меньше лишних lookup'ов.
- [ ] (опц.) **Per-segment bloom** (из OceanBase): фильтр на сегмент — пропустить чтение сегмента,
      если CID точно не в нём (гранулярнее per-disk).
- [ ] `ozd-app`: use cases `StoreBlock`, `FetchBlock`, `HasBlock`, `DeleteBlock`.

✅ Pool из ≥3 «дисков» ведёт себя как единый blockstore; держит R копий; переживает потерю диска.

---

## Фаза 3 — Walk-based resilver (изменение топологии без каталога)
🎯 Добавление/удаление/отказ диска восстанавливает R копий без центрального каталога.

- [ ] `ResilverService` (объединяет rebalance + heal): walk по `ShardEngine.iter()` уцелевших
      дисков → для каждого CID пересчитать `placement(cid, current_topology, R)` → докопировать
      недостающие реплики; идемпотентно, возобновляемо (чекпойнт позиции walk).
      ✅ *Ядро реализовано в ozd v0.1: `Pool::resilver_step/full` (merged-walk индексов с курсором
      `after`, add-only докопирование до R, источник = desired-держатели → любой шард; идемпотентно);
      `POST /admin/resilver?batch=`; тесты `resilver_rebuilds_replaced_disk` (replace-диск, 2-й проход
      = 0 копий) и `resilver_populates_added_disk` (add-disk миграция ≈1/N); e2e: смерть d2 → replace →
      13 реплик восстановлено, 20/20 читаются. TODO: persist-чекпойнт, триггеры по событиям, удаление
      лишних копий (balancer), throttle под Forseti.*
- [ ] Триггеры: `add-disk`, `remove-disk`, `ShardFaulted`, расписание. События
      `ResilverStarted/Progress/Completed`.
- [ ] **HDD:** параллельный resilver — читать дельту со всех уцелевших дисков, писать на много
      целевых (размазать нагрузку, сжать окно rebuild при R=2).
- [ ] **HDD:** degraded start — не блокировать запуск на всех 60 дисках; недоступные → `Faulted`,
      resilver добьёт по возвращении/замене.
- [ ] **★ Tolerated-failed-volumes + live hot-swap диска (из HDFS):** конфиг `failed_disks_tolerated`
      — сколько **одновременно** мёртвых дисков терпим, продолжая обслуживать (свыше — алерт/стоп
      приёма, не падение). Диск **добавить/убрать вживую** без рестарта демона (структура дисков
      переживает изменение под чтениями) → горячая замена на 60-HDD узле как штатная операция;
      при возврате/замене — resilver/disk-balancer заполняет.
- [ ] **Historical (WAL-delta) rebalance (из Ignite):** диск вернулся после **короткого** отсутствия →
      догнать **дельтой** (change-log того, что записалось в его HRW-зону, пока отсутствовал), а полный
      walk-resilver — только при долгом отсутствии/замене или если change-log не покрывает.
- [ ] **Readahead** на последовательных проходах сегментов (из Pebble): 64КБ→max → быстрее walk на HDD.
- [ ] **Diskless stream-копирование при resilver/handoff** (из Redis `repl-diskless-sync`): копии
      блоков стримить с источника на цель **потоком, минуя temp-файл на диске** (тот же sink-интерфейс
      движка, цель=соединение/fd, а не файл) → меньше disk-I/O и места при восстановлении R.
- [ ] **★ Bitfield + sizes-сайдкар для частичной реплики** (из iroh-blobs): при докачке крупного
      блока/набора держать карту «какие чанки уже получены» (`.bitfield`) + известные размеры
      (`.sizes`) → **resumable resilver/fetch** (возобновить с места обрыва, не качать сначала);
      bitfield — производная (реконструируема из данных), как индекс.
- [ ] **★ Merkle-tree anti-entropy: обнаружение РАСХОЖДЕНИЙ копий** (из Cassandra): walk-resilver
      находит **недостающие** копии; merkle-дерево дополнительно ловит **разошедшиеся** копии (тихая
      порча/пропущенная запись). Построить **хэш-дерево над CID-диапазоном** на обеих репликах диска,
      сравнить корни (совпали → 0 трафика), рекурсивно спуститься до **минимальных diff-диапазонов** и
      синхронизировать **только их** (через chunk-range #86 + multi-source #89). Запускать в scrub/периодике.
- [ ] **★ Memory→disk spillover незавершённых передач** (из iroh-blobs): in-flight копии держать в
      RAM до порога, при превышении — **persist sparse на диск** (записанные чанки + bitfield) →
      не держать незавершённые resilver/fetch в RAM сверх лимита (важно при массовом rebuild).
- [ ] **★ Chunk-range request-протокол** (из iroh-blobs `RangeSpec`/`ChunkRangesSeq`): при resilver
      запрашивать у источника **только недостающие чанки** (`local.missing()` по bitfield), а не весь
      блок/набор — компактно (delta-кодировка границ), resumable. Применимо и к крупным наборам сегментов.
- [ ] **★ Incremental verified-streaming decode** (из iroh-blobs): копию **проверять по ходу приёма**
      (`cid == hash` по мере прихода чанков), а corrupt-источник **обрывать сразу** (fail-fast) — не
      «скопировали блок, потом не сошёлся CID». Для крупных — verified по merkle-сайдкару (#79, Ч2/3).
- [ ] **★ Multi-source resilver** (из iroh-blobs downloader): недостающие реплики тянуть из
      **нескольких источников параллельно** (discovery живых дисков-держателей → пул соединений →
      запрос только `missing()` у каждого → fallback при отказе → split по блокам). Дедуп по bitfield,
      без повторной работы. Сжимает окно rebuild при R=2 сильнее одиночного источника.
- [ ] **Handoff** (из YDB): если целевой по HRW диск недоступен при записи — писать реплику на
      **запасной** диск (транзиентно, помечено handoff), позже вернуть на основной при возвращении →
      запись не блокируется отказом, дополняет walk-resilver.
      ✅ *Ядро в ozd v0.1: упавшая нога → следующий кандидат по полному HRW-рангу + ключ в MRF;
      возврат диска → MRF точечно дочинивает канонику (тест `handoff_then_mrf_heals_when_disk_returns`).*
- [ ] **★ MoveTs read-fence при миграции (из Dgraph predicate-move):** когда блоки **переезжают**
      между дисками (after add-disk / disk-balancer / смена HRW-владельца), целевой диск штампует
      **`MoveTs`** (момент завершения переезда зоны). Чтение/locate с **`ts < MoveTs`** для этой зоны
      **отклоняется** (данных тут ещё не было на тот epoch) → во время ребаланса нет «дыр» и чтения
      устаревшей раскладки (**epoch-fencing**, родственно манифест-fencing #94). Сам перенос — copy →
      verify → fence(MoveTs) → удаление исходного (двухфазно, delete-set).
- [ ] **★ Atomic ingest-and-excise при миграции (из CockroachDB IngestAndExcise):** при переезде зоны
      применять перенос **одной атомарной операцией** — «**влить новый набор сегментов + вырезать старый
      диапазон**» — чтобы **не было окна** «старое уже удалили, новое ещё не влили» (consistency на
      целевом диске). Сочетается с MoveTs-fence (#125) и bulk-StreamWriter (#123: новые сегменты
      собираются отсортированно, затем atomic-link). Снапшот replica-move у CockroachDB = ровно так.
- [ ] **★ Heal priority-queue: dedup + per-set bulkhead + MRF (из RustFS):** заявки на heal/resilver —
      в **приоритетную очередь** (BinaryHeap): **dedup** (слить повторные заявки на тот же CID/диск, если
      не `force`), **приоритеты** (срочный reconstruct при нехватке кворума > metadata-repair > обычный
      heal > фоновый), FIFO внутри приоритета. **Per-set/per-disk bulkhead** — не запускать сверх
      `max_concurrent_heal` на один набор/диск (отложить заявку), чтобы heal не забивал IO целевого
      диска. **★ MRF (most-recent-failures):** вести лог **недавно-сбойных записей** (put, где реплика
      не записалась) → **быстрый точечный heal** этих CID, не дожидаясь полного scrub-обхода 3,8 млрд.
      ✅ *MRF-часть в ozd v0.1: bounded dedup-очередь (cap 100K) в Pool, push при упавшей ноге/handoff,
      фоновый дренаж раз в 5с (`heal_mrf`), `repair_key` — общее ядро с resilver; gauge `ozd_mrf_queue`.
      TODO: приоритеты/bulkhead из полного #140.*
- [ ] Тест: убить диск → walk восстанавливает все его блоки до R на других дисках; данные доступны
      весь процесс (вторая реплика). Тест add-disk → часть блоков мигрирует на новый (≈доля диска).

✅ Потеря/добавление диска корректно обрабатывается walk-резилвером; R восстанавливается; даунтайма нет.

---

## Фаза 4 — Интеграция с IPFS-демоном
🎯 Настоящий `один IPFS-демон` поверх sharded-пула.

- [ ] `ozd-ipfs`: реализовать `BlockStore`/`Repo` из `rust-ipfs` через ACL над `Pool`.
- [ ] `ozd-daemon`: composition root — конфиг (список дисков, движок, placement), запуск
      `rust-ipfs` (libp2p, Bitswap, DHT, UnixFS, HTTP API) с нашим стором.
- [ ] E2E: `ipfs add <file>` → блоки распределены по дискам; `ipfs cat <CID>` собирает обратно;
      внешний go-ipfs/kubo достаёт по Bitswap.
- [ ] **LRU fetch-кэш горячих блоков + retry** (из Quorum/Tessera): кэш недавно прочитанных тел
      (TTL/размер); при удалённом fetch (Bitswap) — ретраи с backoff.
- [ ] **★ Read-coalescing локальных чтений** (из Dragonfly): несколько одновременных `get` одного
      CID (или одной страницы/микроблока) на промахе кэша делят **один** HDD-seek (in-flight map по
      адресу, все ждут одного чтения) → меньше дисковой нагрузки на горячих блоках.
- [ ] **★ Speculative retry: дубль-read медленной реплики** (из Cassandra): при чтении блока с реплики
      №1 — если не ответила за **порог** (fixed / 99-й перцентиль латентности диска / hybrid), послать
      **дубль-read реплике №2** (другой диск), взять ответ быстрейшего. Срезает хвостовую латентность
      при disk-slow/seek-storm одного диска. Торгуем лишний read на tail-latency (только при R≥2).
- [ ] (опц.) **Short-circuit local read (из HDFS):** для локального клиента — отдавать тело **по fd/
      mmap** региона сегмента (минуя лишние копии); для **уже верифицированного** (scrub'нутого/
      mlocked) блока — **skip повторной проверки checksum** на чтении. Локальное zero-copy чтение.
- [ ] **★ Serve verified-range off-disk** (из iroh-blobs `export_bao`): при отдаче блока по Bitswap
      читать со store **поток** (`Size|Parent|Leaf|Done`) и писать в сетевой sink; **flow-control
      транспорта = естественный backpressure** (медленный получатель не заваливает диск). Для крупных
      объектов — отдавать только запрошенный **диапазон** (offset>0 → обход hashseq/UnixFS-DAG).
- [ ] **★ Zero-copy sendfile отдачи** (из Kafka `FileRecords.transferTo`): для **непрозрачного, уже
      верифицированного** диапазона сегмента — `sendfile`/`splice` **page-cache → сокет** без копии в
      user-space (DMA). Быстрый путь для несжатого/без-verify-decode тела; verified-streaming (#90) —
      когда нужна потоковая проверка. Меньше CPU/копий → выше throughput отдачи на 60 HDD.
- [ ] **★ Multi-source fetch недостающих блоков** (из iroh-blobs downloader): при удалённом `get`
      (Bitswap) тянуть блок из **нескольких пиров параллельно** (пул соединений → запрос только
      `missing()` → fallback при отказе → split по under-блокам крупного объекта). Усиливает LRU-кэш+retry.
- [ ] **★ Observer: реактивная доступность блока** (из iroh-blobs `ObserveRequest`): подписка «какие
      чанки/реплики блока уже есть» с **diff-only** обновлениями (только новые диапазоны) → дешёвый
      progress для `ipfs cat`/докачки и координация с resilver/GC (не удалять то, что кто-то тянет).
- [ ] (опц.) **NVMe как L2 read-кэш ТЕЛ блоков** (из RocksDB SecondaryCache): промах RAM-кэша тел
      → NVMe-кэш → HDD-сегмент (индекс уже на NVMe; здесь — кэш самих тел).
- [ ] **Multi-cache раздельно** (из OceanBase): отдельные пулы под индекс / тела блоков / bloom —
      тела не вытесняют горячий индекс.
- [ ] **★ Block-cache: idle-timer eviction + weak-ref (из NATS):** кэш тел/страниц сегмента — **загрузка
      по доступу**, выселение **по таймеру простоя** (не по жёсткому LRU-учёту), GC-friendly **weak-ref**
      (под давлением памяти GC сам освободит, таймеры доберут); **метаданные сегмента** (Summary/bloom)
      кэшировать **отдельным, более длинным таймером** — данные тела ушли, метаданные ещё живут. Тонкое
      RAM-управление на 60 дисках без явного LRU. Поверх NVMe L2 / cooling (#74).

---

## Фаза 5 — Эксплуатация пула (Pool/Topology + Admin)
🎯 Управление дисками вживую.

> `ResilverService` (resilver+heal) реализован в Фазе 3. Здесь — остальная эксплуатация.

- [ ] `ScrubService` (scrub): фоновая сверка `hash(data)==cid` по всем дискам; повреждённую
      реплику восстанавливает со здоровой (через resilver одного CID).
- [ ] **★ Scrub-приёмы (из HDFS Volume/BlockScanner):** (1) **throttle байт/с на диск** (под Forseti,
      класс scrub < клиент); (2) **период** (полный обход диска за N дней, напр. ~21д); (3)
      **suspect-приоритизация** — блоки с подозрением (CRC-warn/медленное чтение) проверять первыми,
      с TTL «недавно проверенных»; (4) **cursor-checkpoint** — сохранять позицию обхода → scrub
      **возобновляется** после рестарта (важно на 3,8 млрд, не начинать сначала); (5)
      **skip-recently-read** — недавно прочитанные не пересканировать.
- [ ] **★ Scanner cycle-budget + jitter + normal/deep-каденс (из RustFS):** у scrub-цикла — **бюджет**:
      `max_duration` (async-timeout обрывает цикл), `max_objects`, `max_directories` — любой превышенный
      лимит **завершает цикл досрочно** (load-shedding), **причина обрыва логируется**. Межцикловая пауза
      с **рандомным джиттером ±10%** (анти-thundering-herd на 60 дисках). Два режима: **Normal** (дёшево:
      обход + сверка адресов/usage-stats) и **Deep/bitrot** (полный `hash==cid` + heal на выборке) —
      Deep запускать **раз в N циклов / T времени** (не каждый раз), низкой конкуренцией. На старте —
      **skip начальной задержки**, если usage-кэш «холодный». Уточняет HDFS-scrub конкретными лимитами.
      ✅ *Ядро в ozd v0.1: `scrub_step` (engine: партия ключей CRC-чтением) + `Pool::scrub_shard_step`
      (self-heal битых с реплики; тест с реальным bitrot-флипом байта) + фоновый цикл (бюджет
      `scrub_keys_per_cycle`, джиттер ±10%, курсор на шард) + `POST /admin/scrub`; делегирование
      нижнему ярусу — `POST /admin/zfs/scrub`. TODO: suspect-приоритет, persist-курсор, throttle.*
- [ ] **★ Intra-node disk-balancer (из HDFS):** выровнять **заполнение дисков внутри узла** (mixed-size
      партии / после add-disk), **отдельно** от topology-resilver: **offline-план** (откуда→куда,
      сколько) → перенос блоков под **лимитом bandwidth** (фон-класс Forseti). В пределах одного диска
      перемещение дёшево; между дисками — копия + verify + удаление исходного (двухфазно, см. delete-set).
- [ ] `GarbageCollector` — **сегментами по чертежу Pebble blob-rewrite**: per-block **liveness-битмап**
      живых блоков + **refcount** сегмента; rewrite запускать **age-gated** (сегмент старше порога) и
      при garbage-ratio выше порога — копировать только живые в новый сегмент, старый удалить целиком;
      адреса стабильны (индекс обновляется пачкой, не поблочно). Pin/refcount — **merge-дельты** в redb.
- [ ] **★ Persistent discard-счётчик сегмента + `discardRatio` (из Badger value-log GC):** держать
      **mmap-таблицу «мёртвых байт» на каждый сегмент** (инкремент при удалении/перезаписи/компакции,
      как badger `discardStats`: 16Б на файл) → выбор жертвы GC = **O(1) `MaxDiscard()`** (самый
      мусорный сегмент), без сканирования. Rewrite запускать **только если** `discard ≥ discardRatio ×
      size` (`discardRatio≈0.5` → пожизненный **write-amp ≈ 2×**: 1 + 0.5 + 0.25 + …). Это **дешёвый
      выбор + жёсткая граница write-amplification** поверх liveness-битмапа выше. Критерий «живое» при
      rewrite: указатель индекса всё ещё == `(этот seg, этот offset)` (иначе блок мёртв).
      ✅ *E12: discard-bump свёрнут в транзакции put/delete (нет второй txn на операцию);
      `sweep_orphans` в каждом gc-проходе убирает сегменты без единой ссылки — штатный уборщик
      leak-not-corrupt (#134): крах GC между move и unlink, вымершие сегменты, чужой мусор.*
      ✅ *Реализовано в ozd v0.1: `DiskEngine::gc_once` (discard-счётчики `discard.{seg}` в redb-meta,
      max-discard жертва, ratio-порог, CAS-перенос живых в активный сегмент, flush→unlink (#134),
      retry-lookup в `get` при гонке с переездом); фоновый цикл `gc_interval_secs` + `POST /admin/gc`;
      тесты `gc_reclaims_dead_segments_and_keeps_live`, `gc_respects_discard_ratio`,
      `gc_overwrite_counts_as_discard` + e2e (5480КБ→3704КБ).*
- [ ] **★ Bulk-loader / StreamWriter: прямая сборка сегментов для импорта/restore (из Dgraph):**
      путь **массового импорта / восстановления из бэкапа / offline-rebuild** — **внешняя сортировка**
      входа на диске (map-файлы) → запись **СРАЗУ в готовые pack-сегменты + индекс**, минуя write-буфер/
      WAL/компакцию (как `db.NewStreamWriter`). Параллельно **по диску на поток** (shared-nothing). Не
      платим write-amplification обычного пути → быстрый restore 480ТБ и первичная заливка. Дополняет
      инкрементальный restore (#107) и DFS-бэкап (#76).
- [ ] **★ Splice-merge компакция (из Hive OrcFileStripeMerge):** при rewrite живые блоки/регионы
      **копировать байт-в-байт БЕЗ перечтения и перехэша** (блоки иммутабельны и content-addressed →
      уже валидны; checksum уже в индексе/micro). Это **дешёвый minor-режим** компакции (splice живых
      участков сегмента + пересборка манифеста/индекса) против полного rewrite с верификацией.
- [ ] **★ Minor-vs-major компакция по порогам (из Hive Initiator):** два режима — **minor** (слить
      мелкие/дельта-сегменты splice-merge'ем, базовые не трогать) и **major** (полная перепаковка
      базового сегмента). Решение по порогам: `garbage-ratio` (delta/base) и **число мелких/дельта-
      сегментов** (`delta_num_threshold`) — дополняет age-gated триггер чёткими условиями запуска.
- [ ] **★ Двухфазный delete-set + protect-handle (из iroh-blobs) + reader-watermark Cleaner (из Hive):**
      физическое удаление сегмента/файла — **только ПОСЛЕ commit'а транзакции индекса** (удаления копятся
      в delete-set) → блок не осиротеет при краше между «удалить файл» и «обновить индекс». **protect**:
      сегменты, что сейчас пишет/компактит фон, исключены из delete-set. **★ Reader-watermark**: сегмент,
      ставший obsolete после компакции, удаляется **только когда все читатели ниже min-open-read-watermark
      закончили** (watermark берём из MVCC-снимка redb) → scrub/backup/long-read не «выдернут» сегмент
      из-под себя. Усиливает манифест (#66), vylog-2-фазы (#96) и Pebble-GC.
- [ ] **★ Tombstone + grace для распределённого удаления** (из Cassandra gc_grace): delete блока пишет
      **надгробие** (delete-marker), которое держится **≥ grace-периода** перед физическим purge — чтобы
      удаление дошло до **всех R реплик** (отставшая реплика, не увидевшая delete, **не воскресит** блок
      при resilver). two-phase-delete (#84) + reader-watermark (#106) — это **локальная** безопасность
      (краш/читатель); gc_grace — **распределённая**. При R=2 `delete_grace` ≈ макс. время восстановления
      реплики; на возврате реплика получает надгробие (через handoff/resilver) и сама чистит блок.
- [ ] **ICS-фрагменты для GC** (из ScyllaDB): компактить **фрагментами фикс. размера (~1ГБ)** →
      temp-space ограничен фрагментом, а не размером всего тира/сегмента.
- [ ] **Backlog-controller** (из ScyllaDB): пропорциональный темп GC/компакции по «долгу»
      (`bytes_uncompacted × log4(total)`); растёт долг → больше IO компакции, падает → меньше.
- [ ] (опц.) **Periodic major-merge** (из OceanBase): по расписанию — полная компакция сегментов
      шарда в чистый read-оптимизированный набор (амортизация write-amp), совместить со scrub.
- [ ] (опц., альт.) **Fixed-block аллокатор** (из OceanBase) вместо append-only: диск = файл 2МБ-слотов
      + bitmap + mark-sweep GC + pending-free — оценить vs компакция (нет фрагментации «дырами»).
- [ ] (опц., альт.) **★ Segmented disk-аллокатор** (из Dragonfly `external_alloc`, mimalloc-стиль):
      backing-файл = **сегменты 256МБ → страницы по size-class → битмап блоков** (29 бинов, рост ~1.25×).
      Точечный re-use освободившихся блоков **без полной компакции** сегмента. Третий вариант
      `segment_alloc: append-only | fixed-block | segmented` — сравнить write-amp vs фрагментацию.
- [ ] (опц., альт.) **★ Bitmask-аллокатор + per-region gap-summary** (из Qdrant `gridstore`, Rust-референс):
      free-space = **битмаска 1 бит/блок** (напр. блок 128Б), а поверх неё — **сводка на регион**
      `RegionGaps{max, leading, trailing}` (длиннейший свободный прогон + свободные блоки с краёв). Поиск
      места под N блоков: найти регион с `max ≥ N` (O(числа регионов), **без скана всей битмаски**), затем
      скан **только этого региона**; `leading/trailing` сшивают прогоны на границах. Точечный re-use дырок
      от удалённых блоков **без полной компакции**. Адрес = `(page, block_offset, len)` ≡ наш `(seg,off,len)`.
      Четвёртый вариант `segment_alloc: …| bitmask` — у Qdrant это реальная Rust-реализация (можно изучить код).
- [ ] (опц.) **Slice: ref-counted окно в сегменте** (из Tarantool vinyl): при GC/реорганизации не
      переписывать живые блоки в новый сегмент, а **ссылаться на живой регион** старого через
      ref-counted slice (сегмент удаляется при refs=0) → меньше write-amp. ⚠️ Наши ключи — случайные
      CID (не диапазоны), поэтому это **не range-split**, а адресация «живой подучасток сегмента»;
      оценить vs обычный rewrite-GC.
- [ ] **disk-slow детекция** (из Pebble, порог ~5с): per-disk таймер на write/sync → событие →
      шард `Degraded/Faulted` → `ResilverService`.
- [ ] **★ Per-disk монитор `/proc/diskstats` + stall-trace + градуированная реакция (из CockroachDB):**
      (1) фоновый сборщик `/proc/diskstats` каждые **~100мс** на все 60 дисков → **ring-buffer истории**
      latency/IOPS (tracer) — независимый от движка живой сигнал; (2) при превышении `MaxSyncDuration`
      (порог ~20с) — **сначала `make-process-unavailable`** (стоп приёма), **дамп истории latency**
      (`LogTrace`) для диагностики, **затем fatal** (флаг `disk_stall_fatal`); не-fatal-режим даёт окно
      для **WAL-failover** (#128). Дополняет порог 5с (disk-slow), iotune (#49) и delayed-fsync (Redis).
- [ ] **Метрика `delayed_fsync` + write-deferral** (из Redis): если фоновый fsync ещё идёт —
      отложить следующий flush (до порога), считать «отложенные fsync» как **дешёвый сигнал
      «диск не успевает»** → backpressure/`Degraded` (дополняет disk-slow и iotune-модель).
- [ ] **★ ZFS-адаптер `ozd-zfs` (из Go zfspool + krystal/go-zfs, #146–150):** на ZFS-деплое нижний
      ярус отдаёт телеметрию и берёт часть работы: (1) **runner-порт** (#146: Local/Sudo/Fake — тесты
      без zfs-бинаря); (2) **sentinel-ошибки + stderr-гигиена** (#147); (3) **Property-слой с Source**
      (#148: дрифт-аудит recordsize/lz4/atime по 60 пулам — `source=default` значит тюнинг не
      применён); (4) **identity через user-props `ozd:shard_id`** (#149: Mismatch на старте =
      отказ стартовать — диски перепутаны); (5) **`freeing` в вес HRW** (#150: эффективный free =
      free+freeing — вес не прыгает после GC-волн) + fragmentation/compressratio; (6) **делегирование
      checksum-проверки `zpool scrub`'у** (свой deep-scrub — реже). ✅ *Реализовано в ozd v0.1:
      крейт ozd-zfs (status/capacity/metrics/scrub/identity/drift), монитор → FSM → ShardStatus,
      `/admin/zfs` + `/admin/zfs/scrub`; 11 тестов через FakeRunner.*
- [ ] **★ Disk-health 4-state FSM с гистерезисом (из RustFS):** статус диска — конечный автомат
      `Online → Suspect → Offline → Returning` вместо бинарного up/down: **`Suspect`** после **N сбоев
      подряд** (transient-глюк не валит диск сразу), `Offline` при I/O-timeout/недоступности, **`Returning`**
      при возврате — **probe-проверками**, и лишь после **N успехов подряд** → `Online`. **Recovery-class
      по длительности offline**: `Short` (< grace, напр. 30мин) — ждать и догнать **дельтой**
      (historical-WAL #..); `Long` (> порога, напр. 24ч) — **full rebuild/resilver**. Политика
      **`ignore-scanner-timeouts`**: тайм-ауты фонового scanner'а **не** считать в счётчик сбоев (фон не
      latency-critical). Гистерезис убирает «дёрганье» Faulted↔Online; дополняет disk-slow/tolerated-volumes.
      ✅ *Ядро в ozd v0.1: `HealthFsm` (Online/Suspect/Faulted/Returning, suspect_after/recover_after,
      probe-возврат с рецидивом) поверх ZFS-монитора → `set_shard_status`; 4 теста переходов.
      TODO: recovery-class по длительности offline (short=дельта/long=rebuild), ignore-scanner.*
- [ ] **★ Cost-based IO-scheduler «Forseti»** (из YDB) — вместо простого rate-limiter:
      `cost = seek_ns + bytes·1e9/speed_bps` (**drive-model измерена iotune**, #49) +
      **scheduling-groups** (таксономия классов из ScyllaDB: commit/query > flush > compaction/resilver)
      + fair-share по weight + reorder-окно (HDD 50мс). Точное честное IO на 60 HDD; rate-limiter — частный случай.
- [ ] **★ Admission: elastic disk-bandwidth токены (из CockroachDB):** разделить трафик на **foreground**
      (клиентские put/get) и **elastic** (фон: GC/компакция/resilver/scrub/бэкап). Каждый интервал
      (~15с) мерить реальную bandwidth диска; **резервировать чтения наперёд** (сглаживание α=0.5,
      пессимистично max) и выдавать elastic-работе write-токенов = `elastic_max_util(≈0.8) × provisioned
      − reads`. **Foreground НИКОГДА не душить** (может превысить util). Конкретная формула токенов фона
      поверх Forseti (#... scheduling-groups) и regulator (#97): Forseti — приоритеты/честность, regulator
      — рантайм-bandwidth, **этот пункт — явный elastic/foreground-сплит с резервом под чтения**.
- [ ] **★ Глобальный disk-I/O семафор `dios` (из NATS):** поверх per-disk пулов (inflight 1–4) и
      Forseti — **процесс-wide лимит суммарного числа одновременных blocking disk-операций** (read/
      write/sync/rename/delete), дешёвый backstop против исчерпания OS-потоков/горутин при I/O-шторме
      (массовый resilver/scrub/GC на 60 дисках). Простой буф-семафор; нет слота → ждать (backpressure).
- [ ] **Ops-планировщик** (прообраз scylla-manager): отдельный cron-планировщик фоновых задач —
      `scrub` / периодический `resilver`-проверка / бэкап в `cold_path`/S3 — вне горячего пути.
- [ ] **★ Hardlink instant FREEZE → ленивый бэкап (из ClickHouse):** снимок = снять список active-
      сегментов (точка снимка) + **`createHardLink` всех в `snapshot/<id>/`** — O(числа файлов), **0
      копий байт** (hardlink = указатель ФС); исходники пометить read-only. Запись блокируется лишь на
      снятие списка. Затем DFS-/инкрементальный бэкап **лениво** копирует hardlink'и в `cold_path`/S3
      (фоном); после выгрузки `snapshot/<id>/` удаляется. Мгновенный консистентный снимок перед бэкапом.
- [ ] **★ Параллельный DFS-бэкап** (из Dragonfly): бэкап/дамп — **по файлу на диск** (60 потоков
      параллельно, shared-nothing) + **summary-манифест последним** (фиксирует набор файлов и
      топологию). Backend pluggable: локальный `cold_path` / S3 / GCS / Azure. *(копирует hardlink'и из
      snapshot/<id>/ от FREEZE выше.)*
- [ ] **★ Инкрементальный backup + shared-segment refcount (из Flink incremental-checkpoint):** бэкап
      грузит в `cold_path`/S3 **только новые/изменённые сегменты** с прошлого бэкапа (не весь датасет;
      сегменты иммутабельны → дедуп «по имени»/CID бесплатно). **Registry с refcount**: сегмент, общий
      для нескольких бэкап-точек, удаляется из cold-store лишь когда **ни одна точка его не держит**
      (reader-watermark #106 на уровне бэкапов). Полный DFS-бэкап (#76) = частный случай (первый/база).
- [ ] **★ Консистентный scrub/backup под записью** (из Dragonfly fork-less snapshot): обход для
      scrub/backup идёт **поверх MVCC-снапшота redb** (аналог версионирования бакетов — каждый
      элемент фиксируется ровно раз, конкурентная запись не рвёт обход), **без fork и удвоения RAM**.
- [ ] **Write-throttling по прогрессу flush** (из Ignite): если клиент пишет быстрее, чем сегменты
      сбрасываются на HDD — **тормозить писателя** (speed-based); **checkpoint-buffer** (CoW буфера/
      страниц на время flush), чтобы писатели не ждали сброс. Поверх Forseti.
- [ ] **★ Backpressure по in-flight байтам + pacing фон-walk** (из Dragonfly): троттлить запись по
      **объёму байт в полёте на диск** (`pending_write_bytes ≥ max` → клиенту Future, не по числу ops);
      фоновый walk (GC/resilver/offload) — **time-sliced** (бюджет ~100µs/тик), обход **в порядке
      сегментов** (локальность), пропуск недавно тронутого. Уточняет write-throttling/Forseti метрикой.
- [ ] **★ Regulator: write-throttle по ИЗМЕРЕННОЙ bandwidth** (из Tarantool): раз в ~1с мерить
      реальную bandwidth фона (компакция/GC/resilver) — **гистограмма**, брать **10-й перцентиль**
      (консервативно, worst-case); лимит писателя = **0.75 × измеренной bandwidth** (25% headroom,
      чтобы фон не отстал). Точнее статичных порогов: iotune (#49) даёт стартовую drive-model, regulator
      — **рантайм-обратную связь**. Кормит Forseti/backpressure (#78) реальным числом, а не догадкой.
- [ ] **LRU открытых сегментов** per-disk: ленивое `open`, лимит одновременно открытых FD.
- [ ] (опц.) флаги движка `index-in-ram` / `index-preload` (срез латентности hot-пути, из TON).
- [ ] **★ Declarative storage policies: тиры (volumes) + move_factor + size-gate + TTL-move** (из
      ClickHouse): **единый каркас тиринга** — упорядоченные тиры `[hot (NVMe), warm (SSD), cold
      (HDD/cold_path)]`, каждый = набор дисков (выбор least-used/round-robin). Авто-перенос сегмента в
      следующий тир по: **(а) заполнению** (`move_factor`: диск тира заполнен > порога → переносить
      пока не освободит), **(б) размеру** (`max_segment_size_per_tier`: крупное сразу в cold), **(в)
      возрасту** (`move_ttl`). Перенос: clone→swap→async-delete (как в #101 disk-balancer). Объединяет
      температурный тиринг (RocksDB), disk-balancer (#101) и декларативные правила (Druid) в один конфиг.
- [ ] **Температурный тиринг сегментов → `cold_path`** (из RocksDB): тег сегмента (hot/cold по
      pin-статусу/частоте, возраст — вспом.); холодные мигрируют на дешёвый/remote носитель.
      *(теперь — частный случай storage-policy выше: тег = `move_ttl`/частотный критерий тира.)*
- [ ] **Декларативные load/drop-rules** (из Druid): политику тиринга/репликации/срока задавать
      **правилами по классам** («класс A → 2 копии hot; B → 1 cold; X → cold_path/drop») — единый
      механизм поверх температуры, переменной R и FIFO=TTL, вместо жёсткого R=2.
      *(drop-rules = «куда/сколько/насколько», storage-policy = «как кочуют сегменты между тирами».)*
- [ ] (опц.) **FIFO=TTL** для ephemeral (не-pinned) блоков: удалять старейшие по TTL/лимиту.
- [ ] **★ TTL через compaction-filter (из Flink):** для ephemeral-блоков, которые **нельзя**
      сгруппировать в отдельный сегмент-окно, выкидывать истёкшие **инлайн на проходе компакции/
      splice-GC** (по embedded-ts блока), **без отдельного скана/прохода удаления**. Дополняет
      time-bucketed drop-whole-file (ниже) и FIFO=TTL: окно целиком — когда можно, инлайн-фильтр —
      для перемешанных TTL.
- [ ] **★ Time-bucketed сегменты + drop-whole-file retention** (из InfluxDB gen1): для **ephemeral**
      блоков класть тела в сегменты, нарезанные по **окну ingest-времени** («остывшее» окно
      запечатываем, активное не трогаем). Retention/TTL тогда = **удалить сегмент-окно целиком** по
      возрасту (один unlink, без компакции/переписывания живых). Резко дешевле age-gated GC для
      потоков с истечением. Pinned/долгоживущие — в обычные сегменты.
- [ ] **★ Range-tombstone с порогом point/range-delete (из CockroachDB ClearRange):** массовое удаление
      **диапазона/префикса индекса** (напр. снос целого namespace, пина-группы, мигрированной зоны) —
      **одним range-tombstone** (логически O(1), физический реклейм — на следующей компакции/GC), а не
      поштучно. Порог `clear_range_threshold` (~512КБ): **мелочь → поштучный delete** (range-tombstone
      для пары ключей расточителен), **крупное → range-tombstone**. Дополняет two-phase-delete (#84) и
      time-bucketed drop (выше) для удаления **по диапазону ключей**, а не по сегменту.
- [ ] (опц., ограниченно) **min/max-статистика по сегменту** (из InfluxDB Parquet stats): хранить в
      манифесте диапазон **вторичного/временного** атрибута сегмента (напр. ingest-время) → scrub/GC/
      листинг-по-времени **пропускают** сегменты вне диапазона. NB: для основного lookup по CID
      (случайный хэш) бесполезно — там [Bloom/Ribbon](#) (#19); поэтому только для time-сканов.
- [ ] (опц., ограниченно) **★ Вторичный индекс по атрибуту (psim, из NATS):** карта `attr →
      (сегменты, счётчик)` для вторичных атрибутов (`pin-owner`, `namespace`, `ingest-окно`) →
      **таргетный GC/scrub/листинг** без обхода 3,8 млрд (напр. «удалить всё, что пинил X» / «scrub
      namespace N»). NB: по CID (случайный хэш) неприменимо (там redb-primary); это **точный
      match-индекс** в дополнение к min/max-skip (#91) и неймспейсингу (#16).
- [ ] **Object-store tier-гигиена для `cold_path`/S3** (из InfluxDB): **лимит одновременных
      запросов** к бэкенду (не залить S3) + **retry** транзиентных ошибок + **adaptive-multipart** для
      крупных сегментов. Поверх Forseti (фон-класс «миграция в cold»).
- [ ] `domain-aware placement` (+ **2-уровневые fail-домены** из YDB): `realm` (DC/полка) +
      `domain` (контроллер/хост) — реплики в разные домены/realm; по умолчанию «диск = домен».
- [ ] **Backpressure по самому медленному потребителю** (из PolarDB): троттлить ingest/репликацию
      по отстающему потребителю (реплика/будущий gateway), поверх Forseti-планировщика.
- [ ] `ozd-admin`: CLI/RPC — `pool status|add-disk|remove-disk|resilver|scrub|gc`,
      показ числа недо-реплицированных блоков (оценка через walk).
- [ ] `ozd-metrics`: Prometheus (per-shard used/free, IO, hit/miss, resilver progress,
      под-репликация, очереди per-disk; **disk-slow события, фон-rate-limit, hit/miss кэша тел,
      hot/cold распределение сегментов**).

✅ scrub чинит подсунутое повреждение; GC удаляет все копии; админ-команды и метрики работают.

---

## Фаза 6 — Закалка
🎯 Готовность к нагрузке и сбоям.

- [ ] Краш-тесты: kill во время записи/rebalance/heal → нет «полублоков»; **recovery по
      `flushOffset`** отбрасывает недописанный хвост сегмента; индекс пересобираем.
- [ ] **Тест recovery index-tier через WAL-replay** (Ignite): kill после checkpoint → старт
      восстанавливает индекс **replay'ем WAL с checkpoint** (а не полным обходом сегментов);
      проверить все WAL-режимы (`fsync`/`log_only`/`background`) на корректность хвоста после краша.
- [ ] **Тест historical WAL-delta rebalance** (Ignite): диск кратко отсутствовал → возврат
      догоняется дельтой из change-log (трафик ≪ полного walk-resilver); долгое отсутствие → фолбэк на walk.
- [ ] **Тест durable swap + манифест** (Redis): kill между `rename` сегмента и `fsync(dir)` → после
      рестарта набор сегментов консистентен (манифест = источник правды, нет «висячих»/потерянных файлов).
- [ ] **Тест page-cache hygiene** (Redis): под длительной записью RSS page-cache от тел не растёт
      неограниченно (`DONTNEED` работает); горячий индекс/Summary не вытесняется; нет fsync-столла (writeback).
- [ ] Нагрузочное на **компакцию сегментов** (GC) под параллельной записью.
- [ ] Faulted-диск: чтение уходит на живую реплику, демон не падает; heal восстанавливает R.
- [ ] Тест долговечности: убить диск физически (удалить mount) → данные доступны со второй копии.
- [ ] **Тест disk-slow**: искусственно замедлить диск → шард `Faulted`, чтение с реплики, resilver чинит.
- [ ] **Тест rate-limiter / IO-QoS**: фон (resilver/GC) под нагрузкой не вызывает write-stall и
      деградацию latency клиентских записей (приоритет клиент > фон); проверить миграцию сегмента
      hot↔cold и NVMe-кэш тел под нагрузкой.
- [ ] **Тест iotune + backlog + ICS** (Scylla): измеренная drive-model корректна; backlog-controller
      ускоряет/замедляет компакцию по «долгу»; ICS-фрагменты держат temp-space в пределах фрагмента.
- [ ] **Тест dios-семафора** (NATS): I/O-шторм (массовый resilver+scrub+GC) → число одновременных
      disk-операций ≤ `dios_max_concurrent_io` (нет всплеска OS-потоков/горутин); клиентский IO не голодает.
- [ ] **Тест idle-evict block-cache** (NATS): после простоя кэш тел блока выселяется по таймеру (RSS
      падает), метаданные сегмента живут дольше; под давлением памяти weak-ref освобождается GC.
- [ ] **Тест SmallBins packing** (Dragonfly): мелкие тела пакуются по нескольку в страницу
      (утилизация >90%); удаление → free страницы при refcount 0; дефраг при <50% — без потери данных.
- [ ] **Тест read-coalescing** (Dragonfly): N параллельных `get` одного CID/страницы дают **один**
      диск-I/O (счётчик seek'ов), результат идентичен; нет гонок при delete-after-read.
- [ ] **Тест параллельного DFS-бэкапа + консистентности** (Dragonfly): бэкап 60 дисков параллельно
      под активной записью → restore из (shard-файлы + summary) даёт консистентный набор (MVCC-обход).
- [ ] **Тест hardlink-freeze** (ClickHouse): FREEZE снимает консистентный набор сегментов **мгновенно**
      (время ≈ O(числа файлов), не O(байт); запись не блокируется на копирование); ленивый бэкап из
      `snapshot/<id>/` корректен; на не-CoW FS hardlink не дублирует байты.
- [ ] **Тест storage-policy / move_factor** (ClickHouse): диск тира заполнен > `move_factor` → фон
      переносит сегменты в следующий тир (clone→swap→delete), горячее остаётся на NVMe; крупный сегмент
      сразу в cold (size-gate); `move_ttl` переносит по возрасту. Данные целы, чтение не падает.
- [ ] **Тест инкрементального backup + shared-refcount** (Flink): второй бэкап грузит **только новые
      сегменты** (объём upload ≈ дельта, не весь стор); сегмент, общий двум точкам, удаляется лишь
      после удаления обеих; restore из (база + инкременты) идентичен.
- [ ] **Тест changelog/DSTL** (Flink): поток записи → дельта-лог непрерывно уходит в cold_path (RPO ≈
      changelog_persist_delay, не зависит от объёма); kill узла → recovery из (последняя материализация
      + хвост дельта-лога) консистентен; материализация сворачивает лог и чистит старое.
- [ ] **Тест zero-copy sendfile** (Kafka): отдача проверенного диапазона использует `transferTo`
      (счётчик user-space копий = 0; CPU/throughput vs обычный read+write); данные идентичны.
- [ ] **Тест durability-via-replication** (Kafka): `fsync_policy=on-seal/periodic` + kill узла без
      fsync хвоста → блок доступен со **второй реплики**; локально recovery обрезает torn-tail по CRC;
      нет потери подтверждённых (W=2) блоков.
- [ ] **Тест LazyIndex + быстрый старт** (Kafka): старт при тысячах сегментов не грузит все индексы
      (время старта ~const); clean-shutdown → re-scan только грязного хвоста (не все сегменты).
- [ ] **Тест двухфазного delete-set** (iroh-blobs): kill между «удалить файл сегмента» и «commit
      индекса» → после рестарта нет осиротевших данных и нет «удалённых, но числящихся» блоков;
      protect не даёт удалить сегмент, который компактится.
- [ ] **Тест reader-watermark Cleaner** (Hive): запустить долгий read/scrub поверх MVCC-снимка →
      параллельная компакция делает старый сегмент obsolete → Cleaner **не удаляет** его, пока читатель
      не закончил (read не падает); после — удаляет.
- [ ] **Тест splice-merge + minor/major** (Hive): minor-компакция сливает дельта-сегменты
      **splice'ом байт-в-байт** (нет перехэша, скорость ≫ rewrite), данные идентичны; пороги
      `garbage-ratio`/`delta_num` корректно выбирают minor vs major.
- [ ] **Тест orphan-detect (prepare/create)** (Tarantool vylog): kill во время записи/компакции
      сегмента (есть `prepare`, нет `create`) → recovery находит и **удаляет осиротевший** `.seg`;
      манифест-как-лог проигрывается, набор сегментов консистентен.
- [ ] **Тест regulator** (Tarantool): искусственно замедлить фон → измеренная bandwidth падает →
      лимит писателя снижается до ~0.75× реальной (LSM/GC не отстаёт); ускорить фон → лимит растёт.
- [ ] **Тест group-commit + eof-маркер** (Tarantool): N параллельных `put` → один `fsync` (счётчик
      fsync ≪ N); kill с недописанным хвостом → нет eof-маркера → хвост отброшен, данные целы.
- [ ] **Тест tolerated-volumes + hot-swap** (HDFS): убить `failed_disks_tolerated` дисков → демон
      продолжает обслуживать (read с реплик, приём на живые); add/remove диска вживую без рестарта;
      превышение порога → алерт/стоп приёма (не падение).
- [ ] **Тест scrub cursor + suspect** (HDFS): рестарт во время scrub → возобновление **с курсора**
      (не сначала); suspect-блок проверяется раньше очереди; throttle держит заданные байт/с.
- [ ] **Тест disk-balancer** (HDFS): перекос заполнения (mixed-size/после add-disk) → балансировщик
      выравнивает под bandwidth-лимитом, не мешая клиенту; данные целы (copy+verify+delete).
- [ ] **Тест resumable resilver** (iroh-blobs): прервать массовый rebuild на середине → возобновление
      идёт **с места обрыва** по bitfield (не сначала); spillover на диск не превышает RAM-лимит.
- [ ] **Тест merkle anti-entropy** (Cassandra): подсунуть **расхождение** двух копий (тихая порча 1
      блока) → merkle-сверка находит **минимальный diff-диапазон**, стримит только его (трафик ≪ полной
      копии); идентичные реплики → 0 трафика (корни совпали).
- [ ] **Тест tombstone + gc_grace** (Cassandra): delete блока при down-реплике → реплика возвращается
      в пределах `delete_grace` → получает надгробие, блок **не воскресает**; после grace надгробие
      выпиливается; purge раньше grace → проверить, что не происходит (защита от zombie).
- [ ] **Тест speculative retry** (Cassandra): искусственно замедлить реплику-1 → после порога идёт
      дубль-read реплике-2; latency p99 не деградирует; лишний read не шлётся, когда реплика-1 быстра.
      ✅ *ozd v0.1: `speculative_retry_hedges_slow_read_leg` (300мс-нога → ответ < 250мс от ноги-2).*
- [ ] **Тест read-нога / write-mostly** (Discord): при живой read-ноге реплика №2 **не читается**
      (счётчик reads ноги-2 ≈ 0 — page-cache ноги-1 греется); смерть read-ноги → чтение прозрачно
      уходит на write-mostly-ногу. ✅ *ozd v0.1: `write_mostly_fallback_on_read_leg_failure`.*
- [ ] **(Часть 2) Тест миграции dual-write + canary** (Discord): включить dual-write → мигратор
      переносит историю с checkpoint'ом (kill/resume — с места); canary-процент чтений в оба формата
      даёт 0 расхождений; cutover без потери блоков.
- [ ] **Тест value-log GC discard-ratio** (Badger): сегмент с мусором < `discard_ratio×size` → GC
      **не переписывает** (ErrNoRewrite, IO сэкономлено); с мусором ≥ ratio → переписывает лишь живое
      (указатель индекса == seg,off), мёртвое выкинуто; `MaxDiscard` выбирает самый мусорный за O(1).
- [ ] **Тест bulk-loader / StreamWriter** (Dgraph): restore 10М блоков из бэкапа через внешнюю
      сортировку → запись прямо в сегменты+индекс (минуя write-буфер/компакцию) **быстрее** обычного
      пути (нет write-amp), результат идентичен; параллельно по дискам без гонок.
- [ ] **Тест pre-zeroed WAL-слоты** (Dgraph raftwal): kill с недописанным хвостом → старт находит
      конец **по первому занулённому слоту** (zero=tail), хвост отброшен; адресация записи O(1);
      mmap-файл без `msync` переживает крах процесса (данные до последнего слота целы).
- [ ] **Тест MoveTs read-fence** (Dgraph): во время миграции зоны между дисками чтение с `ts<MoveTs`
      **отклоняется** (нет чтения старой раскладки); после переезда (copy+verify+fence) исходник удалён,
      чтение с `ts≥MoveTs` идёт с нового диска; нет «дыр» под конкурентной миграцией.
- [ ] **Тест ballast full-disk recovery** (CockroachDB): забить диск до `avail < ballast/2` → демон
      помечает диск «полон» и **стопит приём** на него (не падает, читает с реплик); **удаление
      ballast** освобождает место → GC/reclaim запускается, приём возобновляется; ballast не растёт,
      когда останется < запаса.
- [ ] **Тест WAL failover** (CockroachDB): искусственно застопить primary-носитель index-WAL → WAL
      **переключается на запасной путь**, коммиты не висят (latency в норме); восстановление primary →
      возврат; без failover тот же стоп → деградация latency коммитов (контроль).
- [ ] **Тест diskstats-монитор + градация stall** (CockroachDB): замедлить диск > `max_sync_duration`
      → сначала `process-unavailable` (стоп приёма) + дамп истории latency, затем (если `disk_stall_fatal`)
      fatal; ниже порога — только счётчик disk-slow; /proc/diskstats опрашивается ~100мс.
- [ ] **Тест гистерезис заполнения** (CockroachDB): диск между 0.925 и 0.95 → **не цель ребаланса**, но
      ещё принимает необходимые реплики; >0.95 → сброс не-pinned; проверить **отсутствие пинг-понга**
      блоков между двумя почти-полными дисками (без буфера он есть — контроль).
- [ ] **Тест admission elastic/foreground** (CockroachDB): насыщение диска фоном (GC+resilver) →
      elastic-токены = `0.8×bw − reads`, фон тормозится; **клиентский put/get не деградирует**
      (foreground не душится); reads зарезервированы наперёд.
- [ ] **Тест ingest-and-excise / range-tombstone** (CockroachDB): миграция зоны — atomic «влить+вырезать»
      без окна «удалили/не влили» (конкурентное чтение всегда видит полный набор); снос целого префикса
      крупнее порога — одним range-tombstone (быстро), мельче — поштучно; реклейм на компакции.
- [ ] **Тест bitmask-аллокатор** (Qdrant): put/delete множества блоков → освободившиеся дырки
      **переиспользуются точечно** (без компакции); `RegionGaps` находит место под N блоков **без скана
      всей битмаски**; нет «потерянных» свободных прогонов на границах регионов.
- [ ] **Тест crash-safety leak-not-corrupt** (Qdrant): kill в каждой фазе записи (между разметкой,
      записью тела, обновлением индекса) → после рестарта **данные целых блоков не повреждены**; в худшем
      случае — осиротевшее (помечено-занято-но-не-в-индексе) место, которое **фон-GC/scrub освобождает**;
      потерь подтверждённых блоков нет.
- [ ] **Тест madvise + low-memory** (Qdrant): с `mmap_populate` старт **prefault'ит** горячий индекс
      (меньше page-fault на первых запросах); многостраничное значение читается с одним `WILLNEED`-префетчем
      (счётчик page-fault падает); `low_memory_mode=no-populate` → prefault пропущен, формат тот же (возврат
      без rebuild).
- [ ] **Тест TTL-кэш free-space + seqlock** (Qdrant): шторм `put` под HRW-by-free → число `statvfs`
      ≪ числа операций (кэш TTL ~5с); горячее чтение топологии/весов **не блокирует** писателя обновления
      (seqlock: читатели ретраят, не ждут на локе); веса HRW стабильны в TTL-окне.
- [ ] **Тест self-describing meta + quorum-pick-latest** (RustFS): рассинхронизировать версии/mod-time
      R копий (краш во время handoff) → read **выбирает актуальную кворумом** (не «первую живую»);
      состояние пула **полностью восстановимо обходом дисков** без центрального каталога.
- [ ] **Тест heal priority-queue** (RustFS): заявки на тот же CID **сливаются** (dedup); срочный
      reconstruct (нехватка кворума) идёт раньше фонового; **per-set bulkhead** не пускает сверх
      `heal_max_concurrent_per_set`; **MRF** чинит недавно-сбойную запись быстро, не дожидаясь scrub.
- [ ] **Тест scanner cycle-budget + deep-каденс** (RustFS): превышение `scan_max_objects`/`max_duration`
      **обрывает цикл** (причина в логах), джиттер паузы виден; **Deep-bitrot** запускается раз в N
      циклов (не каждый), Normal — чаще и дешевле; холодный usage-кэш → старт без начальной задержки.
- [ ] **Тест disk-health FSM** (RustFS): единичный тайм-аут → `Suspect` (не Faulted); N сбоев → `Offline`;
      возврат → `Returning` с probe → после N успехов `Online`; короткий offline → догон **дельтой**,
      долгий → **full rebuild**; тайм-ауты scanner'а **не** валят диск (ignore-scanner).
- [ ] **(Часть 2) Тест erasure-set + distribution** (RustFS): объект→набор по sipHash; шарды
      раскладываются перестановкой (нет горячего диска); потеря ≤M дисков набора → объект **читается
      reconstruct'ом** (≥K шардов); write-кворум соблюдён.
- [ ] **Тест inline-split + per-CID актор** (iroh-blobs): `iter` индекса не читает тела (узкая
      строка); конкурентные `put` одного CID дедуплицируются (один write на диск), без гонок.
- [ ] **Тест chunk-range + multi-source resilver** (iroh-blobs): rebuild тянет у источников **только
      `missing()`** (трафик ≈ объём недостающего, не блока целиком); параллельно у нескольких дисков,
      fallback при отказе источника на лету → R восстановлен, окно rebuild сжато.
- [ ] **Тест verified-streaming decode** (iroh-blobs): источник с повреждённым телом → приём
      **обрывается на первом несошедшемся чанке** (fail-fast), битые данные не доходят до store;
      переключение на другой источник.
- [ ] **Тест serve-off-disk + observer** (iroh-blobs): медленный получатель → flow-control тормозит
      чтение с диска (нет переполнения буфера); observer шлёт **только diff** доступности при докачке.
- [ ] **Тест checkpoint-rollup манифеста** (InfluxDB): после N снапшотов старт грузит **checkpoint +
      свежие дельты** (время старта ≈ const, не растёт с историей); восстановленный каталог совпадает
      с полным обходом.
- [ ] **Тест time-bucketed retention** (InfluxDB): эфемерные блоки в окнах-сегментах → по возрасту
      окно **удаляется целиком** (один unlink, без компакции); живые/pinned не задеты.
- [ ] **Тест cold-store гигиены** (InfluxDB): миграция в `cold_path`/S3 под лимитом inflight (бэкенд
      не перегружен), retry транзиентных ошибок, крупный сегмент уходит multipart'ом.
- [ ] Нагрузочное: профиль латентности под параллельной нагрузкой; tuning семафоров и W.
- [ ] Документация эксплуатации + пример `config.toml` (диски, движок, R, W, placement,
      inline_min, cold_path, bg_rate_limit, direct_io, index_wal_mode, fadvise_dontneed, writeback_chunk,
      segment_alloc, io_backend, small_bins, ephemeral_time_bucket, manifest_checkpoint_interval,
      failed_disks_tolerated, scrub_period_days, scrub_bytes_per_sec, disk_balancer_bandwidth).

✅ Демон переживает потерю диска (читает с реплики) и краш процесса без потери целостных блоков.

---

## Матрица решений (зафиксировать перед стартом)

| Вопрос | Зафиксировано | Альтернатива |
|---|---|---|
| IPFS-хост | **`rust-ipfs` (dariusc93)** | iroh (не классич. CID) |
| Placement | **`RendezvousHrw`** взвеш. по free (≈least-bytes-used), top-R; **подключаемая стратегия**; **★ 2-уровневый порог заполнения с гистерезисом 0.95/0.925 + compare-cascade (disk-health > diversity > ровность)** (CockroachDB) | `Modulo`/round-robin/random (Druid selector) |
| Носитель/ФС | **XFS на диск (JBOD)** + индекс на **NVMe**; не ZFS (ADR 0001) | ZFS (если нужны checksum/compression/snapshots ценой перфа) |
| Движок | data-tier **pack-сегменты ≤2ГБ** (XFS-HDD) + index-tier (redb/NVMe); write-буфер, flushOffset | (формат из TON+geth, см. SYNTHESIS) |
| Строение индекса | узкая строка адреса `b\|cid→(seg,off,len)`; **inline-тела в отдельной таблице** `i\|cid` (iroh-blobs) | inline в строке (отвергнут — раздувает скан) |
| Конкуренция на CID | **per-CID entity-актор + idle-recycle** (iroh-blobs): дедуп/сериализация операций без глоб. локов; **★ Ч3: consistent-hash routing по CID на gateway** (Discord — без роутинга коалесинг не срабатывает) | глобальный лок (отвергнут) |
| Гигиена page-cache | **`POSIX_FADV_DONTNEED` для write-once тел** + **неблокирующий writeback `sync_file_range`** (Redis) | direct-I/O для тел как альт. |
| Набор сегментов | **per-disk манифест** = append-only лог событий + **2-фазные циклы** prepare/create (orphan-detect) + drop/forget (Tarantool vylog); checkpoint-rollup (InfluxDB) | rename-на-файл / переписываемый MANIFEST (отвергнуты) |
| Финализация файлов | **durable swap**: temp→fsync→rename→fsync(dir) (Redis) | — |
| Каталог | **центрального нет**: placement + NVMe-индексы дисков; **★ self-describing per-disk meta + quorum-pick-latest** (RustFS) — версия восстановима кворумом с дисков | (отвергнут на масштабе 3,8 млрд) |
| Репликация | **в Части 1**: R=2 (mirror), W=2, walk-based resilver | **erasure block-4-2** (YDB) / **★ erasure-set + distribution-array + quorum** (RustFS, K+M, sipHash→набор) → Часть 2 |
| Топология | один сервер, **60 одинаковых HDD** | — |
| Failure domains | опция `shard.domain`, дефолт «диск=домен» | **2-уровневые realm/domain** (YDB) при known-карте |
| GC сегментов | liveness-битмап + age-gated rewrite + refcount; **★ persistent discard-счётчик (O(1) MaxDiscard) + `discardRatio≈0.5` → write-amp≈2×** (Badger); **ICS-фрагменты** + **backlog-controller** (Scylla); **двухфазный delete-set + protect** (iroh) + **reader-watermark Cleaner** (Hive); **minor/major по порогам** + **splice-merge без перехэша** (Hive) | fixed-block (OceanBase) / **segmented size-class** (Dragonfly) / **bitmask+region-gaps** (Qdrant) как альт. |
| Аллокатор места | append-only + компакция (дефолт); альт. **★ bitmask 1бит/блок + per-region gap-summary (max/leading/trailing) → точечный re-use без компакции** (Qdrant, Rust-референс) | fixed-block / segmented size-class |
| Crash-safety записи | **★ «течь, но не портить»: порядок разметка→тело→индекс→free → крах = утечка места (чинит фон), не порча** (Qdrant) | recovery-лог (избыточно) |
| mmap-политика | **★ `MADV_RANDOM` (lookup) + `POPULATE_READ` prefault горячего + `WILLNEED` многостраничного + `DONTNEED` write-once тел** (Qdrant + Redis) | дефолтный readahead (мимо) |
| Low-memory деградация | **★ `no-resident` (компоненты→mmap) / `no-populate` (+skip prefault)**, формат тот же → возврат без rebuild (Qdrant) | OOM / ручной тюнинг |
| Чтение горячего состояния | **★ SeqLock (lock-free, читатели ретраят) + TTL-кэш free-space ~5с** (Qdrant) | RwLock + statvfs на каждый put (contention/шторм) |
| Packing мелочи | inline (крошки) + **★ SmallBins** (несколько тел в 4КБ-страницу, free по refcount, дефраг <50%) (Dragonfly) | micro-block ~16КБ (OceanBase) |
| Body-I/O | page-cache + DONTNEED по умолч.; опц. **O_DIRECT + io_uring registered buffers** (Dragonfly) | std read/write |
| Read-coalescing | **дедуп параллельных чтений** одной страницы/CID → один seek (Dragonfly) | — |
| IO-планировщик | **cost-based «Forseti»** (YDB) + **scheduling-groups** (Scylla); **drive-model измерена iotune** (Scylla) + **regulator: рантайм-bandwidth p10 → писатель ≤0.75×** (Tarantool) + **глобальный `dios`-семафор concurrent I/O** (NATS) | rate-limiter/QoS — частный случай |
| Самоадаптация | disk-slow→`Faulted`; device-type + **iotune-измерение**; handoff; **write-throttling по flush** (Ignite); **delayed-fsync метрика** (Redis); **tolerated-failed-volumes + live hot-swap** (HDFS) | — |
| Recovery index-tier | **WAL + checkpoint → replay дельты** (Ignite); WAL-режимы `fsync/log_only/background/none`; **★ pre-zeroed фикс-слоты → zero=детектор хвоста + O(1)-адресация** (Dgraph raftwal) | обход 3,8 млрд сегментов — лишь фолбэк |
| Bulk-загрузка / restore | **★ StreamWriter: внешняя сортировка → запись прямо в сегменты+индекс**, минуя hot-path (Dgraph) — быстрый restore/первичная заливка без write-amp | обычный put-путь (медленно на 480ТБ) |
| Миграция-fencing | **★ MoveTs read-fence**: переезд зоны штампует epoch, чтение с `ts<MoveTs` отклоняется (Dgraph); **★ atomic ingest-and-excise** (CockroachDB) — влить+вырезать без окна | без fence (риск чтения старой раскладки) |
| Durability записи | **репликация R=2 + recovery-point + CRC + torn-tail**, fsync на seal/периодически (`fsync_policy`, Kafka) | per-write fsync (дорого на HDD) |
| Старт / индекс | **LazyIndex (отложенный mmap) + warm-tail** (Kafka): не грузить индексы всех сегментов; clean-shutdown → re-scan только грязного хвоста | загрузить всё при старте (медленно на масштабе) |
| Recovery каталога | **checkpoint-rollup манифеста** (старт = checkpoint + свежие дельты, не обход всего) (InfluxDB) | проигрывать всю историю манифеста (отвергнут) |
| Rebalance | walk-resilver (полный) + handoff; **historical WAL-delta** (Ignite); **bitfield+spillover resumable** + **chunk-range missing-only** + **multi-source** (iroh-blobs) | дельта вместо полного walk |
| Anti-entropy | walk-resilver = **нет копии**; **★ merkle-tree** = копии **разошлись** (тихая порча/пропуск) → стрим только diff (Cassandra) | full-сравнение (отвергнуто) |
| Распред. удаление | two-phase delete-set (#84) + reader-watermark (#106) — локально; **★ tombstone + gc_grace** (Cassandra) — реплика не воскресит | сразу purge (риск zombie) |
| Tail-latency чтения | **★ speculative retry**: медленная реплика → дубль-read второй (Cassandra); **★ read-нога + write-mostly №2** (Discord super-disk) — стабильная нога греет page-cache, вторая вне read-балансировки ✅ *в ozd v0.1* | ждать одну реплику |
| Миграция формата/стора | **★ dual-write + свой быстрый мигратор (диапазоны+checkpoint) + canary-сравнение чтений** (Discord, 3.2М/с, 9 дней) — для mirror→erasure Ч2 | generic-мигратор (медленно), стоп-мир |
| Transfer (fetch/serve) | **chunk-range запрос** (только missing) + **verified-streaming decode** (fail-fast) + **multi-source** + **serve-off-disk поток (flow-control)** + **observer diff-only** (iroh-blobs) + **★ zero-copy sendfile** для проверенного диапазона (Kafka) | тянуть/отдавать блок целиком (грубее) |
| Ops | вынесенный cron-планировщик scrub/resilver/backup (прообраз scylla-manager) | — |
| Scrub | `hash==cid` по дискам; **throttle байт/с + suspect-приоритет + cursor-checkpoint + skip-recent** (HDFS); **★ cycle-budget (duration/objects/dirs обрыв) + jitter + normal/deep-bitrot каденс** (RustFS) | период ~21д, возобновляемо |
| Heal-планирование | **★ priority-queue: dedup + per-set bulkhead + MRF (recent-failures) + типы-приоритеты** (RustFS) | прямой запуск heal (дубли/перегрузка набора) |
| Балансировка дисков | **intra-node disk-balancer** (offline-план + bandwidth cap, HDFS) — выровнять заполнение; отдельно от topology-resilver | при mixed-size / после add-disk |
| Бэкап | **★ hardlink instant FREEZE** (мгновенный снимок, ClickHouse) → **DFS параллельно** (файл на диск + summary, Dragonfly) + **инкрементальный** (delta + shared-refcount, Flink) | под MVCC-обходом; полный = база |
| RPO / durable-лог | **★ changelog/DSTL** — index/manifest-WAL непрерывно в cold_path → RPO до секунд **независимо от объёма** (Flink) | локальный WAL+checkpoint (#59) — без durable-remote |
| Backpressure | по in-flight **байтам** (Dragonfly) + write-throttling (Ignite) + delayed-fsync (Redis), поверх Forseti; **★ admission elastic disk-bandwidth токены** `0.8×bw−reads`, foreground не душить (CockroachDB) | по числу ops (грубее) |
| Disk-health | disk-slow 5с→Faulted (Pebble); **★ /proc/diskstats монитор 100мс + stall-trace + градация unavailable→fatal** `max_sync_duration` (CockroachDB); **★ 4-state FSM Online/Suspect/Offline/Returning + recovery-class (short=дельта/long=rebuild) + ignore-scanner-timeouts** (RustFS) ✅ *FSM-ядро в ozd* | один порог без диагностики |
| ZFS-адаптер | **★ ozd-zfs (#146–150): runner-порт (тесты без бинаря) + sentinel-ошибки + Property/Source дрифт-аудит + identity `ozd:*` (bail при Mismatch) + effective free=free+freeing → HRW + делегирование zpool scrub** ✅ *в ozd v0.1* | парсить статус ad-hoc / statvfs-only |
| Тиринг/кэш | **★ declarative storage policies: тиры (volumes) + move_factor + size-gate + TTL-move** (ClickHouse) — единый каркас; температура→`cold_path`; NVMe L2-кэш; multi-cache; **idle-evict block-cache + weak-ref** (NATS); на ZFS-деплое — **★ L2ARC/special-vdev NVMe** (безопасен как write-mostly; ⚠️ урок Discord: НЕ dm-cache/bcache — битый сектор кэша валит чтение) | ручные тиринг-решения (отвергнуты) |
| Политика данных | **декларативные load/drop-rules по классам** (тир+реплики+срок, Druid) | вместо жёсткого R=2 |
| Retention эфемерных | **★ time-bucketed сегменты → drop целого окна по возрасту** (InfluxDB gen1) | age-gated GC живых (для долгоживущих) |
| Cold-store гигиена | **лимит inflight + retry + adaptive-multipart** к `cold_path`/S3 (InfluxDB) | без лимита — заливаем бэкенд |
| Лимиты диска | per-диск `max_size` + резерв free-space + reclaim (Druid StorageLocation); **★ ballast-файл: full-disk graceful-recovery** (CockroachDB) — full = avail<ballast/2, удалить → расклинить | не переполнять диск |
| Full-disk recovery | **★ ballast-резерв ~1ГБ; удаление расклинивает забитый диск** (CockroachDB); grow-only-if-safe | без резерва — нуль вешает HDD |
| WAL failover | **★ при стопе primary-носителя WAL → запасной путь** (CockroachDB), latency коммита изолирована | без failover — стоп одного диска тормозит коммиты |
| Range-delete | **★ range-tombstone с порогом point/range-delete** (CockroachDB) — снести префикс/зону одним маркером, реклейм на компакции | поштучный delete (дорого на масштабе) |
| Строение блока | сегмент = **микроблоки ~16КБ** (IO/сжатие/**checksum** на micro) | OceanBase macro/micro |
| Backend-абстракция | **PolarVFS-style** pluggable `ShardEngine`: xfs/raw-O_DIRECT/remote по пути | PolarDB vfs_mgr |
| Масштабирование | Часть 1 — один демон; Часть 3 — compute/storage separation + deep-storage/кэш (gateway'и) | PolarDB/Druid; immutability → проще |

---

## Что НЕ входит в Часть 1 (зафиксировано как Часть 2+)

- Erasure coding / RAIDZ-аналог (паритет вместо полных копий) — главный кандидат **YDB block-4-2**
  (4+2, переживает 2 отказа, 1.5× overhead vs 3× у R=3). *Mirror-репликация R=2 — в Части 1.*
  **★ Конкретный чертёж — RustFS/MinIO (#138):** диски сгруппировать в **erasure-наборы** (напр. 16
  дисков/набор); объект (блок/группа) → набор детерминированно по **`sipHash(cid, set_count, pool_id)`**
  (≈ наш HRW, но на набор); внутри набора шарды раскладывать по дискам через **per-object
  distribution-array** (перестановку шард↔диск, хранится в метаданных) → **нет «горячего» диска**.
  `default_parity` по размеру набора (8+ дисков → EC:4 = K=12 data + M=4 parity). **Read-кворум = K**
  (любые K из K+M восстанавливают, `reconstruct_data` поверх parity), **write-кворум = data (+1 если
  data==parity)**. Reed-Solomon GF(2^8) (есть Rust-крейты `reed-solomon-erasure`/`-simd`). На 60 HDD —
  напр. наборы по 12–16 дисков, EC:4 → 1.33–1.5× overhead, переживает 4 отказа в наборе.
  **★ Процедура переезда mirror→erasure (из Discord, #145):** (1) **dual-write** — новые блоки писать
  сразу в оба формата; (2) **свой быстрый мигратор** (не generic): по диапазонам ключей/сегментам,
  **checkpoint в локальной БД** (возобновляемость) — у Discord свой Rust-мигратор дал 3.2М строк/с и
  9 дней вместо 3 месяцев; (3) **canary-валидация** — малый % чтений в **оба** формата со сравнением;
  (4) cutover после нуля расхождений. Урок: следить за «хвостом» миграции (стопор Discord на 99.9999%
  из-за tombstone-диапазонов — вылечила компакция).
- **Raw block device бэкенд** `ShardEngine` (мимо ФС, O_DIRECT — как PDisk YDB / PolarFS). В Части 1 — XFS.
- **BLAKE3-style verified streaming (outboard)** (паттерн **iroh-blobs**): merkle-сайдкар для
  **проверяемого random-access и докачки** диапазона крупного блока без перехэша целиком. В Части 1
  блоки ~256КБ (1–2 чанка, не нужно); кандидат для крупных UnixFS и verified/resumable Bitswap (Ч2/3).
- **Compute/storage separation** (Часть 3, паттерн PolarDB/**Druid**): stateless IPFS/S3-gateway'и
  над общим content-addressed blockstore; **deep-storage (durable источник правды, S3/cold_path) +
  локальный кэш** на узлах (потеря диска → перекачать). **Упрощение:** immutability снимает
  consistent-LSN/copy-buffer/LogIndex — gateway просто читает неизменный блок по CID (Druid это
  подтверждает на иммутабельных сегментах).
  **Fencing от split-brain** (паттерн InfluxDB): владелец диска/префикса берётся через **atomic-create**
  (`PutMode::Create`); `AlreadyExists` = есть другой владелец → не стартовать (анти-split-brain при
  нескольких gateway над общим стором). В Части 1 (один демон) не нужно.
  **★ Zero-copy shared-object + refcount + last-deletes** (паттерн ClickHouse): несколько gateway/реплик
  ссылаются на **один** сегмент в cold_path/S3 (не копируют); refcount в координаторе; **последний
  дропнувший — удаляет** объект. Без tombstone/lease. Расширяет shared-segment refcount (#107, бэкап-точки)
  на **живые** gateway. В Части 1 не нужно.
  **★ Consistent-hash routing запросов по CID на gateway-инстанс** (паттерн Discord, #144): при
  нескольких gateway дубли горячего CID должны попадать **в один инстанс**, иначе request-coalescing
  (#73/#83) не срабатывает (дубли размазаны и «не встречаются»). Роутинг по hash(CID) → инстанс —
  обязательная пара к коалесингу на масштабе. В Части 1 (один демон) даёт сам per-CID актор.
- Несколько IPFS-демонов поверх общего пула (кластер).
- Failure domains (группировка дисков по контроллеру/корзине).
- Авто-tiering между NVMe/HDD по частоте доступа (ручной `cold_path` — опц. в Фазе 5).
- Дедупликация на уровне пула (CID уже дедуплицирует блоки).
- Шифрование at-rest.

> Примечание: сжатие тел блоков на уровне движка (опц. zstd, не для CID/хэшей) — **входит**
> в Часть 1 (Фаза 1), см. [SYNTHESIS](Arch_DDD/HDD_SDD/STORAGE-IDEAS-SYNTHESIS.md). Исключено лишь
> сжатие/дедуп на уровне всего пула.
