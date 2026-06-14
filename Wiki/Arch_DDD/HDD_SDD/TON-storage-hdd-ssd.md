# TON Storage — как TON работает с HDD/SSD (DDD-разбор исходников)

> Исследование исходников **ton-blockchain/ton** (`Vendor/TON`, свежий слой, commit
> `8e6f0917…` от 2026-05-31) с целью вытащить идеи для нашего content-addressed blockstore
> на 60 HDD. Все факты — с ссылками `файл:строка` и проверены в коде.

TL;DR философии TON: **горячее, случайно-адресуемое состояние (cells) держим в RAM/SSD с
кэшем и refcount-GC; холодную историю (blocks) пишем append-only пакетами, дружелюбными к HDD,
и нарезаем на слайсы, которые открываются по требованию и тиксаются по LRU/TTL.** Это ровно
hot/cold-разделение, которое нам нужно.

---

## 1. Bounded Contexts хранения TON

```
┌───────────────────────────────────────────────────────────────────┐
│                         TON Node Storage                          │
│                                                                   │
│  ┌───────────────────────┐        ┌─────────────────────────────┐ │
│  │  CellDB CTX (STATE)   │        │   Archive CTX (HISTORY)     │ │
│  │ «bag of cells»        │        │  blocks/proofs во времени   │ │
│  │ hash→cell, refcount   │        │  append-only .pack + index  │ │
│  │ RAM / RocksDB + LRU   │        │  слайсы, per-slice RocksDB  │ │
│  │ СЛУЧАЙНЫЙ доступ→ SSD │        │  ПОСЛЕДОВАТЕЛЬНЫЙ → HDD     │ │
│  └───────────┬───────────┘        └───────────────┬─────────────┘ │
│              │                                    │               │
│        ┌─────▼────────────────────────────────────▼─────┐         │
│        │     KV Substrate CTX — td::RocksDb (LSM)       │         │
│        │  block cache · bloom · merge op · direct I/O   │         │
│        └────────────────────────────────────────────────┘         │
└───────────────────────────────────────────────────────────────────┘
```

| Контекст | Роль | Носитель по природе доступа |
|---|---|---|
| **CellDB** (состояние блокчейна) | core: «bag of cells», случайный доступ при исполнении | RAM / SSD (random) |
| **Archive** (исторические блоки) | холодная история, запись-один-раз | HDD (sequential) |
| **KV Substrate** (`td::RocksDb`) | generic LSM-движок под обоими | — |

> Ключ DDD-наблюдения: TON **физически разделяет два контекста по типу доступа к диску**.
> Состояние и история — разные модели хранения, не один «блокстор на всё».

---

## 2. Ubiquitous Language (термины TON)

| Термин | Значение | Где в коде |
|---|---|---|
| **Cell** | узел дерева состояния, адресуется 256-бит хэшем | `crypto/vm/cells/CellTraits.h:41` |
| **BoC** (bag of cells) | сериализованное дерево ячеек | `crypto/vm/db/CellStorage.cpp` |
| **refcount** | счётчик ссылок ячейки (для GC) | `DynamicBagOfCellsDb.cpp:48` |
| **CellDB** | хранилище ячеек поверх RocksDB | `validator/db/celldb.cpp` |
| **Archive slice** | диапазон seqno мастерчейна (20 000 блоков) | `archive-manager.hpp:92` |
| **Package (.pack)** | append-only файл с блоками + offset-индекс | `validator/db/package.cpp` |
| **temp / permanent / key** | три класса архива с разными путями и TTL | `archive-manager.cpp:31` |

---

## 3. CellDB — состояние (RAM/SSD, случайный доступ)

### 3.1 Модель: hash-addressed cells + refcount
Каждая ячейка хранится как `key = SHA256(cell)`, `value = tag(1B) + refcnt(4B) + data`
(`CellStorage.cpp:262`). Это **content-addressed KV с подсчётом ссылок** — прямой аналог
нашего CID→block, только с refcount для дедупликации поддеревьев.

### 3.2 Трёхуровневый tiering RAM ↔ disk (главная идея)
TON выбирает режим хранения ячеек флагом — это явная RAM/SSD-развилка
(`validator/db/celldb.cpp:220`, флаги в `validator-engine.cpp`):

| Режим | Что делает | Носитель | Флаг |
|---|---|---|---|
| **InMemory** | все ячейки в RAM при старте, RocksDB только для durability | RAM (+SSD persist) | `--celldb-in-memory` |
| **Dynamic V2** (default) | ленивая загрузка + **LRU-кэш с TTL** | SSD + RAM-кэш | (по умолчанию; `--celldb-v2` deprecated/on) |
| **Dynamic V1** | ленивая загрузка без кэша | SSD | legacy |

V2-кэш: `cache_size_max = 1 000 000` ячеек, `cache_ttl_max = 2000` мс, 8192 бакета с
per-bucket мьютексами; кэш сбрасывается **целиком** по достижении лимита/TTL
(`DynamicBagOfCellsDb.h:136`, `DynamicBagOfCellsDbV2.cpp:975`). Кэш — generational
(drop-all), а не пер-элементный LRU — дёшево и без фрагментации учёта.

Сопутствующие операторские «рычаги» под носитель:
- `--celldb-preload-all` — последовательно прогреть весь стор в кэш на старте
  (рекомендовано с большим кэшем и direct-io) — **HDD-friendly прогрев** (`validator-engine.cpp:5876`).
- `--celldb-cache-size` — размер RocksDB block cache (дефолт 1 ГиБ); TON **сам поднимает**
  его до ≥5 ГиБ в V2 и до ≥16 ГиБ при two-level index (`celldb.cpp:202–244`).
- `--celldb-in-memory` → write-path использует `VectorRepFactory` memtable и
  `no_reads/no_block_cache` для read-only загрузчика (`RocksDb.cpp:99`, `celldb.cpp:254`).

### 3.3 GC через refcount + RocksDB merge operator
Удаление не трассирующее, а **по refcount**: `inc/dec` от корня, при `refcnt==0` →
`storer.erase()` (`DynamicBagOfCellsDb.cpp:499,556`). Главная оптимизация записи:
**custom merge operator** `MergeOperatorAddCellRefcnt` (`celldb.cpp:84`) — изменение
счётчика пишется как merge-дельта (`CellStorage.cpp:270`), без read-modify-write всей
ячейки. RocksDB схлопывает дельты лениво при компакции.

`--permanent-celldb` отключает GC целиком (архивные ноды) — «pin всего», меньше
write-amplification от компакции (`validator-engine.cpp:5980`).

### 3.4 `--celldb-compress-depth` — батчинг поддеревьев (важно для HDD!)
Экспериментально: «store cells of depth X with whole subtrees» (`validator-engine.cpp:5826`).
То есть мелкие связанные ячейки сворачиваются в **одно значение** → меньше мелких случайных
чтений/seek’ов на HDD ценой CPU на десериализацию. Это паттерн «pack related small objects».

---

## 4. Archive — история (HDD, append-only)

### 4.1 Append-only packages + offset-индекс
Блоки пишутся в `.pack` последовательно: заголовок `magic 0xae8fdd01`, далее записи
`magic 0x1e8b | filename_len | filename | data` (`package.cpp:47,70`). `Package::append()`
делает `pwrite` в конец и **возвращает offset** (`package.cpp:41`). Offset кладётся в
RocksDB: `key = block_hash.hex()`, `value = offset` (`archive-slice.cpp:437`). Чтение —
`offset из KV → PackageReader` (`archive-slice.cpp:516`).

> Тела блоков — **последовательная запись на HDD без seek**; «карта» (hash→offset) — мелкий
> random в RocksDB (хочется на SSD). Это и есть наш «data на HDD + index на NVMe».

### 4.2 Нарезка на слайсы по seqno
Архив режется по мастерчейн-seqno: `archive_size = 20 000` блоков на слайс,
`key_archive_size = 200 000` для key-блоков (`archive-manager.hpp:92`). Привязка блока к
слайсу: `seqno - (seqno % 20000)` (`archive-manager.cpp:1226`). Внутри — под-слайсы по
`slice_size = 100` (`archive-slice.cpp:179`). Каждый слайс — **отдельный экземпляр RocksDB**
(`archive-slice.cpp:713`), открываемый по требованию.

### 4.3 Hot / Cold / Temp — три класса с разными путями и TTL
`PackageId::path()` (`archive-manager.cpp:31`):

| Класс | Путь | Назначение | TTL |
|---|---|---|---|
| **temp** | `/files/packages/` | живые/неподтверждённые, почасовая ротация | 3600 с (hard 14400 с) |
| **permanent** | `/archive/packages/arch…/` | старые блоки | `archive-ttl` (деф. 7 дней) |
| **key** | `/archive/packages/key…/` | только key-блоки/пруфы | как permanent |

Temp-пакеты — почасовые корзины `ts - ts%3600` (`archive-manager.cpp:1211`); GC удаляет
старше TTL и только если все шарды ушли вперёд на `seqno+16` (`archive-manager.cpp:1044`).

### 4.4 LRU открытых слайсов + «permanent pinning»
Открытых RocksDB-инстансов слишком много — TON закрывает их по **LRU числа файлов**:
`ArchiveLru` с `max_total_files`, `enforce_limit()` закрывает старейшие
(`archive-slice.hpp:242`, `archive-slice.cpp:1527`). Недавние слайсы **пиннятся открытыми**
по `archive_preload_period` (`archive-manager.cpp:1002`, `set_permanent_slices`).

> Идея: при тиринге на HDD холодные слайсы открываются лениво (async-actor скрывает
> HDD-латентность), а число одновременно открытых ограничено LRU — контроль FD/RAM.

### 4.5 Компрессии архива нет
В архивном пути сжатие не найдено — блоки лежат «как есть» (`archive-slice.cpp:375`).
Возможность: per-package zstd сократил бы HDD-footprint без вреда append-only паттерну.

---

## 5. KV Substrate — настройки RocksDB (проверено в `tddb/td/db/RocksDb.cpp`)

| Опция | Значение | Строка | Урок для HDD/SSD |
|---|---|---|---|
| LRU block cache | дефолт **1 ГиБ** (`1<<30`) | 77 | HDD: попадание в кэш критично; SSD: можно 16–32 ГиБ |
| Bloom filter | `NewBloomFilterPolicy(10,false)` | 89 | сокращает лишние seek’и; на HDD выигрыш ещё важнее |
| Two-level index/filter | при `state_ttl ≥ 30 дней` | 90 | партиционированные фильтры → меньше RAM на огромном сторе |
| Memtable `VectorRepFactory` | в in-memory режиме (`no_reads`) | 99 | write-only путь без конкуррентной memtable |
| WAL recovery | `kTolerateCorruptedTailRecords` | 105 | переживает обрыв записи на хвосте WAL |
| `use_direct_reads` | конфигурируемо | 106 | bypass page-cache — только при большом кэше/SSD |
| `manual_wal_flush` | true | 107 | приложение само решает, когда flush |
| `max_background_compactions` | **4** | 109 | HDD: меньше (2–3) — меньше IO-контеншена |
| `max_background_flushes` | **2** | 110 | HDD: держать низким |
| `bytes_per_sync` | **1 МиБ** | 111 | SSD: можно 4–8 МиБ (реже fsync) |
| `writable_file_max_buffer_size` | **32 КиБ** | 112 | HDD: крупнее буфер помогает батчингу |
| `max_log_file_size` / `keep_log_file_num` | 100 МиБ / 1 | 114 | минимальное хранение WAL |

Операторские флаги (носитель-зависимые), `validator-engine.cpp`:
`--celldb-cache-size` (5863), `--celldb-direct-io` («не применяется при кэше < 30G», 5872),
`--celldb-preload-all` (5876), `--celldb-in-memory` (5884), `--celldb-disable-bloom-filter`
(5889), `--celldb-compress-depth` (5826), `--permanent-celldb` (5980), `--state-ttl` (5706),
`--archive-ttl` (5729).

---

## 6. Философия TON по носителям (синтез)

1. **Разделяй по типу доступа, а не по «всё в один стор».** Состояние (random) и история
   (sequential) — разные подсистемы с разными движками и носителями.
2. **Случайный hot-доступ → RAM/SSD + кэш.** In-memory режим, preload, большой block cache,
   bloom, опц. direct-io при крупном кэше.
3. **История → append-only пакеты на HDD + мелкий offset-индекс отдельно.** Никаких
   in-place правок в телах; «карта» в RocksDB.
4. **Нарезка на слайсы** даёт гранулярность для тиринга, GC и ограничения открытых ресурсов
   (LRU открытых слайсов, pin недавних).
5. **GC по refcount + merge-дельты**, чтобы не делать read-modify-write на каждый счётчик.
6. **Меньше мелких seek’ов на HDD**: батчинг поддеревьев (`compress-depth`), крупные буферы,
   умеренная фоновая компакция.

---

## 6-bis. Снипеты кода (реальные выдержки + объяснение)

### CS1. `.pack`-append: header(magic+len) + данные (≈ pack-сегмент)

```cpp
// validator/db/package.cpp:41 — Package::append()
td::uint64 Package::append(std::string filename, td::Slice data, bool sync) {
  auto size = fd_.get_size().move_as_ok();           // текущий конец файла = offset
  td::uint32 header[2];
  header[0] = entry_header_magic() + (narrow_cast<uint32>(filename.size()) << 16);
  header[1] = narrow_cast<uint32>(data.size());
  fd_.pwrite(Slice((uint8*)header, 8), size);        // header, дальше — filename + data
```

**Объяснение:** append блоба в `.pack` по текущему offset: заголовок (magic+len имени, len данных) +
данные. → наш **pack-сегмент** (иммутабельный sequential append, адрес = offset).

### CS2. Refcount-GC через merge-дельты (≈ pin/refcount без RMW)

```cpp
// crypto/vm/db/CellStorage.cpp:270 — CellStorer::merge()
td::Status CellStorer::merge(td::Slice hash, td::int32 refcnt_diff) {
  return kv_.merge(hash, serialize_refcnt_diffs(refcnt_diff));    // +1/−1 как merge-операция
}
// merge-оператор: new_refcnt = left_refcnt + right_refcnt_diff;  (refcnt==0 → удалить ячейку)
```

**Объяснение:** pin/unpin = **merge-дельта** (+1/−1), а не read-modify-write всей записи; при refcnt=0
ячейка удаляется. → наш **refcount/pin через merge-дельты в redb** + segment-GC.

### CS3. Индекс hash→offset (опц. в RAM)

```cpp
// validator/db/archive-slice.cpp:446 — add_file_cont()
kv_->set(ref_id.hash().to_hex(), td::to_string(offset)).ensure();   // hash → offset в KV
// DynamicBagOfCellsDb.h:139 — CreateV2Options{ cache_size_max=1000000, cache_ttl_max=2000 } (in-RAM кэш)
```

**Объяснение:** при append в pack offset сразу пишется в KV `hash→offset`; V2 даёт in-RAM кэш ячеек.
→ наш **index-tier `CID→(seg,off,len)`** на NVMe + опц. `index-in-ram`.

---

## 7. Извлечённые идеи для OpenZFS Daemon

Маппинг механизмов TON на наш дизайн ([ARCHITECTURE.md](../../ARCHITECTURE.md) /
[ARCHITECTURE-ZFS.md](../../ARCHITECTURE-ZFS.md)):

| Идея из TON | Где у нас применить | Эффект |
|---|---|---|
| **Data (sequential) на HDD + index (random) отдельно** (`package.cpp` + offset-KV) | подтверждает наш **data-tier XFS-HDD + index-tier NVMe**; тела блоков — append-only | главный HDD-выигрыш, уже в ADR 0001 |
| **Append-only pack-файлы + offset-индекс** (вместо файла-на-блок) | усилить data-tier: писать блоки в **pack-сегменты** (как предлагали в Part 2), а не миллиарды inode | меньше inode-давления и seek’ов на 3,8 млрд блоков |
| **Слайсы + LRU открытых хэндлов** (`ArchiveLru`) | для холодного/архивного тиринга: открывать pack-сегменты по требованию, ограничивать число открытых FD | контроль RAM/FD на 60 дисках |
| **refcount-GC через merge-operator** | наш `GarbageCollector`/pin: счётчики пинов как merge-дельты в index-tier (redb) | GC без RMW; дешёвый inc/dec |
| **In-memory / preload режим индекса** (`--celldb-in-memory`, `--celldb-preload-all`) | опция «индекс целиком в RAM» или прогрев NVMe-индекса на старте | latency hot-пути ↓ |
| **`compress-depth` — батчинг мелких связанных объектов** | паковать связанные мелкие блоки в один сегмент-значение | срезает random-seek на HDD |
| **Hot/cold/temp с разными TTL и путями** | модель **pinned vs ephemeral** блоков: temp-блоки с TTL-ротацией, pinned — permanent | управление ёмкостью без ручного GC |
| **RocksDB-тюнинг** (bloom(10), two-level index при большом сторе, bytes_per_sync, direct-io при кэше ≥30G) | если index-tier/Variant B на RocksDB — взять параметры почти как есть | проверенные дефолты «из боя» |
| **WAL `kTolerateCorruptedTailRecords` + manual flush** | crash-safety нашего index-tier | переживание обрыва записи |
| **Авто-подъём cache до порога** (≥16 ГиБ при two-level index) | sanity-guard в нашем конфиге: не дать поставить кэш меньше рабочего набора | защита от мисконфига |

### Три вывода, меняющие наш план
1. **Перейти от «файл-на-блок» к append-only pack-сегментам** в data-tier (Фаза 1/Часть 2):
   TON доказывает, что на миллиардах объектов это правильный путь для HDD.
2. **Pin/GC сделать на merge-дельтах** в index-tier (redb поддерживает) — а не RMW.
3. **Добавить режимы `index-in-RAM` и `preload`** для NVMe-индекса — дешёвый способ убрать
   латентность hot-пути, как делает CellDB.

---

## 8. Источники в коде (для перепроверки)

- Cells/refcount: `crypto/vm/cells/CellTraits.h:41`, `crypto/vm/db/CellStorage.cpp:262,270`,
  `crypto/vm/db/DynamicBagOfCellsDb.cpp:48,499,556`, `…/DynamicBagOfCellsDb.h:136`,
  `…/DynamicBagOfCellsDbV2.cpp:975`.
- CellDB режимы/кэш/merge: `validator/db/celldb.cpp:84,121,202,220,242,254,291`.
- Archive: `validator/db/package.cpp:41,47,70`, `validator/db/archive-slice.cpp:179,375,437,
  516,713,1527`, `validator/db/archive-slice.hpp:242`, `validator/db/archive-manager.cpp:31,
  1002,1044,1211,1226`, `validator/db/archive-manager.hpp:92`.
- RocksDB substrate: `tddb/td/db/RocksDb.cpp:77,89,90,99,105–115`, `tddb/td/db/RocksDb.h:62`.
- Операторские флаги: `validator-engine/validator-engine.cpp:5706,5729,5826,5863,5872,5876,
  5884,5889,5980`.
