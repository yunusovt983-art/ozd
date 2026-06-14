# Pebble Storage — как Pebble работает с HDD/SSD (DDD-разбор исходников)

> Исследование исходников **cockroachdb/pebble** (`Vendor/pebble`, свежий слой, commit
> `b6588354…` от 2026-06-02). Все факты — с ссылками `файл:строка`, проверены в коде.

Pebble — это **сам LSM-движок** (на нём работает geth). Поэтому здесь — самые применимые к нам
механизмы. **Главное:** Pebble реализует **value separation (WiscKey)** — крупные значения в
отдельных **blob-файлах**, ключи + мелочь в SSTable. Это **буквально наши pack-сегменты +
index-tier**, причём с готовым решением самого сложного — **GC сегментов через liveness-битмапы и
rewrite без переписывания индекса**. Плюс: **disk-slow детекция**, **WAL failover на запасной
диск**, **readahead**, **deletion pacing**, **тиринг в remote storage**.

---

## 1. Где Pebble в нашей картине

```mermaid
flowchart LR
    subgraph US["OpenZFS Daemon (наш дизайн)"]
        IDX["index-tier (redb)<br/>CID → (seg, offset, len)"]
        SEG["data-tier (pack-сегменты)"]
    end
    subgraph PB["Pebble (прототип-движок)"]
        SST["SSTable: ключи + мелкие значения"]
        BLOB["blob-файлы: крупные значения"]
        SST -->|"blobHandle"| BLOB
    end
    IDX -. «как» SSTable-ссылка .-> SST
    SEG -. «как» blob-файл .-> BLOB
    note["value separation Pebble = наш two-tier:<br/>indexHandle≈(seg,offset,len), blob≈сегмент"]
```

Pebble мы можем и **прямо использовать** как index-tier (вместо redb) — но даже если оставим
redb, его механизмы value-separation/GC/disk-health **переносим как паттерны**.

---

## 2. Архитектурные диаграммы (Mermaid)

### P1. LSM путь записи (WAL → memtable → L0 → компакция)

```mermaid
flowchart TB
    PUT["Set(k,v)"] --> WAL["WAL (record/) — durable журнал<br/>BytesPerSync фоном"]
    WAL --> MEM["memtable (arena skiplist)<br/>default 4МБ, stop при 2×"]
    MEM --> FULL{"полна?"}
    FULL -->|да| FLUSH["flush → L0 SSTable"]
    FLUSH --> L0{"L0 sublevels ≥ 4?"}
    L0 -->|да| C["компакция L0→Lbase→…→L6<br/>multiplier ×10, файлы 2МБ→128МБ"]
    C --> GC["перекрытые SSTable удаляются (pacing)"]
```

### P2. Value separation = наши pack-сегменты (★)

```mermaid
flowchart LR
    W["запись значения"] --> SZ{"len ≥ MinimumSize?"}
    SZ -->|"нет (мелкое)"| INL["inline в SSTable<br/>(у нас: inline в redb-значение)"]
    SZ -->|"да (крупное)"| BLOB["в blob-файл (append)<br/>(у нас: в pack-сегмент)"]
    BLOB --> H["в SSTable — InlineHandle:<br/>(ReferenceID, ValueLen, BlockID, ValueID)<br/>(у нас: (segment_id, offset, len))"]
    H --> READ["read: SSTable → handle → blob[block][value]<br/>(у нас: redb → сегмент[offset])"]
```

### P3. GC blob-файла: liveness-битмап + rewrite (чертёж нашей компакции сегментов)

```mermaid
flowchart TB
    REF["per-SSTable liveness:<br/>RLE-битмап «какие значения живы»"] --> HEUR{"blob стар (RewriteMinimumAge)<br/>И garbage ≥ порога?"}
    HEUR -->|нет| KEEP["не трогать"]
    HEUR -->|да| RW["rewrite: скопировать ТОЛЬКО живые<br/>в новый blob; virtual-block remap"]
    RW --> REFC{"refcount blob → 0?"}
    REFC -->|да| DEL["удалить старый blob"]
    note["handle остаётся валидным<br/>через virtual-block indirection"]
```

### P4. Resilience: disk-slow детекция + WAL failover

```mermaid
flowchart LR
    OP["каждая file-op (write/sync)<br/>обёрнута таймером"] --> CHK{"длилась > DiskSlowThreshold<br/>(деф. 5с)?"}
    CHK -->|да| EVT["событие DiskSlow → callback"]
    EVT --> DEG["(у нас) пометить шард Degraded/Faulted → resilver"]
    WALW["WAL write latency"] --> WCHK{"> 100мс устойчиво?"}
    WCHK -->|да| FO["failover: писать WAL на ВТОРОЙ диск"]
    FO --> BACK["проба primary 1с; failback если <25мс×15с"]
```

### P5. Тиринг: objstorage + remote (disaggregated)

```mermaid
flowchart TB
    PROV["objstorage.Provider<br/>(Readable/Writable абстракция)"] --> LOC[("локальные SSTable/blob")]
    PROV --> REM["CreateOnShared: Lower/All"]
    REM --> S3[("remote/shared store (S3-like)<br/>L5/L6 — холодные")]
    note["горячие L0–L4 локально, холодные L5/L6 — в remote<br/>(у нас: cold_path-тир)"]
```

---

## 2-bis. Файловая система: раскладка и потоки (Mermaid)

### FS1. Реальная раскладка на диске (+ WAL failover на второй диск)

```mermaid
flowchart TB
    subgraph PRIM["primary dir — NVMe/SSD"]
        WAL["*.log — WAL (primary)"]
        SST["*.sst — L0..L6 SSTable"]
        BLOB["*.blob — value separation (крупные значения)"]
        MAN["MANIFEST / CURRENT / OPTIONS"]
    end
    subgraph SEC["secondary WAL dir — другой диск"]
        WAL2["*.log — WAL failover"]
    end
    WAL -. "при стопе primary (>100мс)" .-> WAL2
    classDef m fill:#eef,stroke:#557;
    class PRIM m;
```

### FS2. Запись на уровне файлов (WAL failover + value separation)

```mermaid
sequenceDiagram
    participant W as Set(k,v)
    participant WAL as WAL primary (*.log)
    participant WAL2 as WAL secondary (*.log)
    participant MT as memtable (4МБ)
    participant SST as L0 *.sst
    participant BLOB as *.blob
    W->>WAL: append
    alt latency WAL > 100мс устойчиво
        W->>WAL2: failover на secondary dir
    end
    W->>MT: вставка
    MT->>SST: flush → L0 .sst (ключи + мелкие значения)
    MT->>BLOB: значения ≥ MinimumSize → blob-файл
    Note over SST: компакция L0→L6 (multiplier ×10), bloom L0–L5
```

### FS3. Чтение: SSTable → blobHandle → blob-файл

```mermaid
flowchart LR
    GET["Get(k)"] --> BC{"block cache (DRAM)?"}
    BC -->|да| V["value"]
    BC -->|нет| RS["read блок *.sst"]
    RS --> H{"значение inline или blobHandle?"}
    H -->|inline| V
    H -->|"blobHandle (FileID, BlockID, ValueID)"| RB["read *.blob[block][value]"] --> V
```

### FS4. Disk-health: обёртка файловых операций таймером

```mermaid
flowchart LR
    OP["file op (Write/Sync) на *.log/*.sst/*.blob"] --> TIM["обёртка таймером (vfs)"]
    TIM --> CHK{"длилась > DiskSlowThreshold (5с)?"}
    CHK -->|да| EVT["событие DiskSlow → callback<br/>(у нас: шард → Faulted)"]
    CHK -->|нет| OK["норма"]
```

---

## 3. Ubiquitous Language (термины Pebble)

| Термин | Значение | Где в коде |
|---|---|---|
| **memtable** | in-memory skiplist-буфер записи | `mem_table.go` |
| **SSTable** | отсортированный иммутабельный файл уровня | `sstable/` |
| **blob file** | отдельный файл крупных значений (value separation) | `sstable/blob/` |
| **blobHandle** | адрес значения: `(BlobFileID, BlockID, ValueID, ValueLen)` | `sstable/blob/handle.go:34` |
| **liveness bitmap** | RLE-битмап живых значений в блоке (для GC) | `sstable/blob_reference_index.go` |
| **DiskSlowThreshold** | порог «медленной» дисковой операции (5с) | `options.go:1867` |
| **WAL failover** | переключение WAL на запасной диск при стопе | `wal/failover_manager.go` |

---

## 4. LSM-ядро и тюнинг (проверенные дефолты)

| Опция | Дефолт | Строка | Урок HDD vs SSD |
|---|---|---|---|
| `MemTableSize` | **4 МБ** (старт 256КБ, ×2) | `options.go:1768` | HDD: меньше → ниже пики латентности при flush |
| `MemTableStopWritesThreshold` | **2** | options | write-stall при 2× — поднять на HDD |
| `LevelMultiplier` | **10** | `options.go:49` | ниже → меньше write-amp, больше места |
| L0CompactionThreshold | **4** sublevels | options | HDD: 2–3 (агрессивнее, ниже read-amp) |
| L0StopWritesThreshold | **12** | options | HDD: 8–10 (ловить backlog раньше) |
| Block cache | **8 МБ** (деф.) | `options.go:48` | HDD: 256МБ–1ГБ критично (избегать seek) |
| Bloom (per-level) | L0 нет; L1–L3 16 бит; L6 8 бит | options | прогрессивно: крупные уровни — меньше бит |
| Компрессия | Snappy; профили до Zstd на L6 | options | HDD: Zstd на L1+ (меньше байт = меньше seek) |
| `BytesPerSync` | **512 КБ** | `options.go:1656` | сглаживает I/O, без storm dirty-страниц |
| `CompactionConcurrencyRange` | **[1,1]** | options | HDD: держать 1 (seek-контеншн); SSD: 2–4 |
| Deletion pacing `BaselineRate` | **0 (выкл)** | deletepacer | HDD: 1–10 МБ/с — сгладить удаление |

> Замечание: дефолты Pebble нейтральны; geth поверх них ставит свои (NoSync WAL, L0=2,
> seek-compaction off) — см. [go-ethereum doc](go-ethereum-storage-hdd-ssd.md).

---

## 5. Value separation — это наши pack-сегменты (★ детально)

`ValueSeparationPolicy` (`options.go:1299`): `MinimumSize` — **значения меньше порога пишутся
inline в SSTable, крупнее — выносятся в blob-файл** (в тестах дефолт ~512 байт).

- **blobHandle** (`sstable/blob/handle.go:34`): `{ BlobFileID, ValueLen, BlockID, ValueID }`. В
  SSTable хранится **InlineHandle** = `(ReferenceID, ValueLen, BlockID, ValueID)`, где
  `ReferenceID` — индекс в массиве `BlobReferences` таблицы (а не прямой FileID — это развязывает
  идентичность ссылки от файла, позволяя ремапить).
- **blob-файл append-only**: значения пишутся в блоки последовательно, индекс — в конце
  (`sstable/blob/blob.go`).
- **GC через rewrite** (`blob_rewrite.go`): blob переписывается **без переписывания SSTable** —
  копируются только живые значения, старые handle остаются валидны через **virtual-block remap**.
  Живость — **RLE-битмап per-block** (`sstable/blob_reference_index.go`), решение о rewrite — по
  `RewriteMinimumAge` + garbage-ratio порогам; `refcount` blob → 0 ⇒ удаление.

**Маппинг на наш дизайн (1:1):**

| Pebble | OpenZFS Daemon |
|---|---|
| blob-файл | **pack-сегмент** `seg.NNNN.dat` |
| blobHandle `(FileID, BlockID, ValueID, len)` | **`(segment_id, offset, len)`** в redb |
| InlineHandle/ReferenceID | ссылка в index-tier |
| MinimumSize (inline vs separate) | **порог inline-в-redb для крошечных блоков** |
| liveness bitmap + refcount | **liveness наших сегментов для GC** |
| blob rewrite (только живые) | **компакция сегмента** (перепаковка живых) |
| RewriteMinimumAge + garbage-ratio | **триггеры нашей компакции** |
| virtual-block remap | handle стабилен при перепаковке |

> Pebble закрывает самый сложный кусок нашей Фазы 5 (GC сегментами): **готовая схема liveness +
> age-gated rewrite + refcount**, причём перепаковка не требует трогать индекс на каждый блок.

---

## 6. Resilience: disk-slow, WAL failover, readahead, pacing

- **Disk-slow детекция** (`vfs/disk_health.go`, порог `5с` — `options.go:1867`): каждая
  write/sync обёрнута таймером; при превышении — callback `DiskSlow`. **Для нас:** вешаем на
  per-disk ops → при устойчивых стопах помечаем шард `Degraded/Faulted` → `ResilverService`.
- **WAL failover** (`wal/failover_manager.go`): при латентности WAL-записи **>100мс** устойчиво —
  переключение записи журнала на **второй диск**; проба primary каждую 1с, failback при <25мс×15с.
  **Для нас:** durability index-tier — при стопе NVMe писать на запасной NVMe.
- **Readahead** (`objstorage/.../readahead.go`): старт **64КБ**, экспоненциальный рост до max
  после ≥2 последовательных чтений; отдельный `ReadHandle` на контекст. **Для нас:** включать на
  последовательных проходах сегментов (resilver/scrub).
- **Deletion pacing** (`internal/deletepacer`): сглаживание удалений (`BaselineRate`, при
  нехватке места — ускорение). **Для нас:** пейсить удаление сегментов при GC, не устраивать
  I/O-storm на HDD.

---

## 7. Тиринг: objstorage + remote (disaggregated)

`objstorage.Provider` абстрагирует локальный файл vs remote (`objstorage/objstorage.go`).
`CreateOnShared` (`provider.go`): `Lower` → L5+ на remote, L0–L4 локально; `All` → всё на remote
(S3-like, `objstorage/remote/`). **Для нас:** `cold_path`-тир — редкие/не-pinned сегменты можно
выносить в shared/remote store, как Pebble выносит холодные уровни.

---

## 8. Философия и вывод XFS/ZFS

Pebble — движок, а не нода, поэтому медиа-совет тот же, что у geth (Pebble там и используется):
горячий LSM-индекс → **SSD/NVMe (XFS/ext4)**; холодные данные → дешевле/медленнее.
Ключевая идея Pebble для нас не «какая ФС», а **как движок сам адаптируется к носителю**:
disk-slow детекция, WAL failover, pacing, readahead, value separation — всё это мы переносим в
наш `ShardEngine`/`ResilverService` поверх XFS.

---

## 8-bis. Снипеты кода (реальные выдержки + объяснение)

### CS1. Value separation: handle (blob,block,value) ≈ наш (seg,off,len)

```go
// sstable/blob/handle.go:34
type Handle struct {
    BlobFileID base.BlobFileID
    ValueLen   uint32
    BlockID    BlockID        // блок внутри blob-файла
    ValueID    BlockValueID   // значение внутри блока
}
```

**Объяснение:** значение вынесено в blob-файл, адресуется `(файл, блок, value)`. → 1:1 наш адрес
`(segment_id, offset, len)` тела в pack-сегменте (value separation = inline/сегмент).

### CS2. Disk-slow детекция: порог по времени операции

```go
// vfs/disk_health.go:274
lastWriteDuration := now.Sub(d.createTime) - delta
if lastWriteDuration > d.diskSlowThreshold {       // дефолт ~5с
    d.onSlowDisk(op, writeSize, lastWriteDuration) // callback → деградация
}
```

**Объяснение:** таймер на write/sync; превышение порога (~5с) → callback. → наш **disk-slow → `Faulted`**
(callback помечает шард деградированным).

### CS3. GC по garbage-ratio (liveness blob-файла)

```go
// compaction_picker.go:1774
garbagePct := float64(aggregateStats.ValueSize-aggregateStats.ReferencedValueSize) /
              float64(aggregateStats.ValueSize)
if garbagePct <= policy.GarbageRatioHighPriority { return nil }   // мало мусора → не переписывать
```

**Объяснение:** доля неживых значений в blob-файле; ниже порога — rewrite не запускать (+ age-gate).
→ наш **liveness-битмап + garbage-ratio + age-gated rewrite** GC сегментов.

---

## 9. Извлечённые идеи для OpenZFS Daemon

| Идея из Pebble | Где применить | Эффект |
|---|---|---|
| **Value separation** (blob ≈ сегмент, handle ≈ `(seg,offset,len)`) | подтверждает наш data-tier+index-tier 1:1 | сильнейшая валидация формата |
| **★ GC сегментов: liveness-битмап + age-gated rewrite + refcount + virtual-remap** | **Фаза 5** — наш `GarbageCollector`/компакция | готовый чертёж самого сложного куска |
| **MinimumSize: inline мелочь / separate крупное** | **Фаза 1** — крошечные блоки inline в redb-значение, крупные — в сегмент | минус один seek на мелких блоках |
| **Disk-slow детекция (5с) → событие** | **Фаза 5/6** — per-disk латентный монитор → `ShardFaulted` | авто-деградация диска без ручного вмешательства |
| **WAL failover на запасной диск (100мс)** | **Фаза 1/5** — durability index-tier на запасной NVMe | переживание стопа NVMe |
| **Readahead 64КБ→max на последовательном** | **Фаза 3/5** — resilver/scrub проходы сегментов | быстрее обход на HDD |
| **Deletion pacing** | **Фаза 5** — пейсинг удаления сегментов при GC | без I/O-storm на HDD |
| **objstorage/remote тиринг (CreateOnShared)** | **Фаза 5 (опц.)** — `cold_path` в shared/remote | дешёвый холодный тир |
| **LSM-тюнинг (block cache, bloom, compaction)** | если index-tier на Pebble вместо redb | проверенные дефолты «из боя» |

### Главные заимствования (новое сверх TON/geth/quorum)
1. **GC сегментов по образцу blob-rewrite**: per-block **liveness-битмап** + **refcount** +
   **age-gated rewrite** + **virtual-block remap** (handle стабилен) — закрывает нашу Фазу 5.
2. **Inline мелких значений** (порог `MinimumSize`): крошечные блоки держать прямо в redb, не
   гоняя за ними на HDD — экономит seek.
3. **Самоадаптация к носителю**: disk-slow→`ShardFaulted`, WAL failover, readahead, pacing —
   перенести в `ShardEngine`/`ResilverService`.

---

## 10. Источники в коде (для перепроверки)

- LSM/опции: `options.go:48,49,1656,1768,1867`, `mem_table.go`, `compaction_picker.go`,
  `compaction.go`, `compaction_scheduler.go`.
- Value separation: `options.go:1297–1369`, `sstable/blob/handle.go:34`,
  `sstable/blob/blob.go`, `sstable/blob/doc.go`, `compaction_value_separation.go`,
  `blob_rewrite.go`, `sstable/blob_reference_index.go`,
  `internal/manifest/blob_metadata.go`.
- Resilience/тиринг: `vfs/disk_health.go`, `wal/failover_manager.go`, `wal/wal.go`,
  `objstorage/objstorage.go`, `objstorage/remote/storage.go`,
  `objstorage/objstorageprovider/readahead.go`, `internal/deletepacer/options.go`.
