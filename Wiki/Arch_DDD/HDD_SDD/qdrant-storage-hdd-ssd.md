# Qdrant Storage — как Qdrant работает с HDD/SSD (DDD-разбор исходников)

> Исследование исходников **qdrant/qdrant** (`Vendor/qdrant`, свежий слой, commit `44ad62f` от
> 2026-06-03). **Rust** (как наш демон!). Все факты — с ссылками `файл:строка`, проверены в коде;
> ключевые места — **с реальными снипетами** (см. §9-bis).

Qdrant — векторная БД на Rust. ⚠️ **Бóльшая часть — вектор-специфика и для нас НЕприменима**
(quantization f32→u8/bits, HNSW-граф, rescore) — мы храним **непрозрачные content-addressed блоки**, не
векторы. Но у Qdrant есть **`gridstore`** — собственный on-disk **blob-store** (pages + bitmask + tracker),
почти буквально наш слой, **на нашем же языке**. Копаем там, где по-настоящему полезно:

1. **★ `gridstore`: bitmask-аллокатор + per-region gap-summary** — свободное место как **битмаска**
   (1 бит/блок 128Б) + сводка на регион (`max/leading/trailing` свободный прогон) → быстрый best-fit
   без скана. Альтернатива append-only + GC (точечный re-use без полной компакции).
2. **★ Crash-safety без recovery-лога: «течь, но не портить»** — порядок flush (bitmask→pages→
   tracker→free) такой, что крах **переразмечает занятое** (утечка места, чинится позже), но **никогда
   не теряет/портит данные**.
3. **★ Дисциплина mmap/madvise** — `MADV_POPULATE_READ` (prefault горячего индекса на старте) +
   `MADV_WILLNEED` (префетч значения, лежащего через несколько страниц, одним syscall) + **low-memory
   режимы** (`NoResident` → mmap-варианты, `NoPopulate` → без prefault).
4. **★ SeqLock: lock-free чтение горячего состояния** — читатели **не блокируются** (ретрай при
   конкурентной записи) → дёшево читать free-space/ёмкость/статы-кэша под нагрузкой.
5. **★ Кэш ёмкости/free-space с TTL** — `statvfs` дорог; кэшировать на ~5с → нет «шторма» сисколлов
   при постоянном опросе free-space (HRW-by-free на 60 дисках).

> Контекст-конвергенция (НЕ новые строки): pages+block-pointer `(page,block_off,len)` = наш сегмент +
> индекс `(seg,off,len)`; WAL (CRC32C-цепочка per-entry + recovery до первого несовпадения + 8-байт
> паддинг) = Kafka recovery-point (#111) + eof-маркер (#99); atomic-save (temp→fsync→rename) = durable
> swap (#67); segment-build в TempDir→rename = наш манифест; vacuum/merge-optimizer (deleted_ratio +
> greedy-batch) = наша компакция/GC; LZ4-сжатие значений = опц. zstd; O_DIRECT+io_uring disk-cache =
> #72 (Dragonfly) + NVMe L2 (#...).
>
> ⚠️ **НЕ берём** (вектор-специфика): quantization (#... — блоки непрозрачны, нечего квантовать),
> HNSW-граф, rescore/oversampling, posting-list bitpacking (как #126 — только сортированные id).

---

## 1. Bounded Contexts

```mermaid
flowchart TB
    PUT["put(value)"] --> ALLOC["★ bitmask-аллокатор: найти free-прогон (region-gaps)"]
    ALLOC --> PAGES["pages: page_N.dat (32МБ), блоки 128Б"]
    PAGES --> TRACK["tracker: id → ValuePointer(page,block_off,len)"]
    TRACK --> FLUSH["★ flush-порядок: bitmask→pages→tracker→free (leak-not-corrupt)"]
    GET["get(id)"] --> TRACK
    TRACK --> READ["read_from_pages + LZ4-decompress"]
    READ --> MADV["★ madvise: WILLNEED многостраничного значения"]
    LOAD["load сегмента"] --> POP["★ POPULATE_READ prefault / low-memory: skip"]
    CAP["free-space / ёмкость"] --> TTL["★ TTL-кэш statvfs (5с)"]
    HOT["горячее состояние"] --> SEQ["★ SeqLock: lock-free чтение"]
    classDef m fill:#eef,stroke:#557;
    class ALLOC,FLUSH,MADV,POP,TTL,SEQ m;
```

| Контекст | Ответственность | Файлы |
|---|---|---|
| **★ Bitmask-аллокатор** | free-space битмаска + region-gaps best-fit | `gridstore/src/bitmask/mod.rs`, `bitmask/gaps.rs` |
| **Pages** | фикс-страницы 32МБ, блоки 128Б, value через страницы | `gridstore/src/pages.rs`, `config.rs` |
| **Tracker** | id → `ValuePointer(page,block_off,len)` (mmap) | `gridstore/src/tracker.rs` |
| **★ Flush/crash-safety** | порядок flush, leak-not-corrupt | `gridstore/src/gridstore/mod.rs` |
| **★ mmap/madvise** | POPULATE_READ / WILLNEED / low-memory | `common/src/mmap/advice.rs`, `low_memory.rs` |
| **WAL** | CRC32C-цепочка, recovery до mismatch | `lib/wal/src/segment.rs` |
| **★ SeqLock** | lock-free чтение горячего состояния | `lib/trififo/src/seqlock.rs` |
| **★ Capacity** | free-space с TTL-кэшем | `common/src/disk_usage.rs` |
| **Optimizers** | vacuum (deleted_ratio) + merge (greedy) | `collection_manager/optimizers/*` |
| ⚠️ Quantization/HNSW | вектор-специфика — **не для нас** | `lib/quantization/`, `index/hnsw_index/` |

---

## 2. Архитектурные диаграммы (Mermaid)

### Qd1. gridstore: bitmask-аллокатор + region-gaps (★)

```mermaid
flowchart TB
    REQ["нужно N блоков (128Б каждый)"] --> GAPS["regions_gaps: найти регион с max-gap ≥ N"]
    GAPS -->|нет| NEWPAGE["вырастить новую страницу 32МБ"]
    GAPS -->|есть| SCAN["сканировать битслайс региона → первый прогон нулей ≥ N"]
    SCAN --> MARK["mark_blocks(used=1) + обновить RegionGaps(max/leading/trailing)"]
    MARK --> PTR["ValuePointer(page_id, block_offset, length)"]
    FREE["delete/overwrite: старый pointer"] --> UNMARK["mark_blocks(used=0) → точечный re-use БЕЗ компакции"]
    note["RegionGaps = {max, leading, trailing} на регион → best-fit без полного скана битмаски"]
```

### Qd2. Crash-safety: «течь, но не портить» (★)

```mermaid
sequenceDiagram
    participant P as put_value
    participant B as Bitmask
    participant Pg as Pages
    participant T as Tracker
    P->>B: mark used (новые блоки)
    P->>Pg: записать данные
    P->>T: set pointer (pending)
    Note over P: flush-порядок: bitmask → pages → tracker → free-blocks
    alt краш в середине
        Note over B,T: блоки помечены used, но pointer не записан →\nутечка места (не переиспользуем), данные ЦЕЛЫ
    else flush завершён
        Note over T: старые pointer'ы вернулись → free их блоки
    end
```

### Qd3. mmap/madvise дисциплина (★)

```mermaid
flowchart TB
    OPEN["open_read_mmap(path)"] --> POPQ{"populate? (не low-memory NoPopulate)"}
    POPQ -->|да| POP["MADV_POPULATE_READ — prefault страниц (тёплый старт)"]
    POPQ -->|нет| LAZY["lazy: страницы грузятся по обращению"]
    POP --> ADV["madvise(Random) — глобальная политика"]
    LAZY --> ADV
    READV["read значения через N страниц"] --> WILL["MADV_WILLNEED — префетч региона одним syscall"]
    LM["low-memory mode"] --> NR["NoResident: quantization/payload/storage → mmap-варианты"]
    LM --> NP["NoPopulate: + пропустить prefault"]
```

### Qd4. SeqLock: lock-free чтение (★)

```mermaid
flowchart LR
    R["reader"] --> S1["seq1 = load (если нечётно → писатель, spin)"]
    S1 --> CB["прочитать данные (без лока)"]
    CB --> S2["seq2 = load"]
    S2 --> CHK{"seq1 == seq2?"}
    CHK -->|да| OK["консистентно — вернуть"]
    CHK -->|нет| R
    W["writer"] --> INC1["seq += 1 (нечётно = locked)"]
    INC1 --> MUT["мутировать"]
    MUT --> INC2["seq += 1 (чётно = unlocked)"]
```

### Qd5. Capacity TTL-кэш (★)

```mermaid
flowchart LR
    Q["disk_usage(path)"] --> C{"в кэше и age < 5с?"}
    C -->|да| HIT["вернуть из кэша (нет syscall)"]
    C -->|нет| SYS["statvfs: total/available"]
    SYS --> STORE["записать в кэш с меткой времени"]
    STORE --> RET["вернуть"]
    note["HRW-by-free опрашивает free-space часто → TTL гасит шторм statvfs на 60 дисках"]
```

### Qd6. ⚠️ Что НЕ берём (вектор-специфика)

```mermaid
flowchart TB
    Q["quantization f32→u8/bits + always_ram + rescore"] --> NA1["⚠️ блоки непрозрачны — нечего квантовать"]
    H["HNSW-граф on-disk/mmap"] --> NA2["⚠️ у нас нет ANN-поиска"]
    PL["posting-list bitpacking (delta+128-chunk)"] --> NA3["⚠️ как #126: только сортированные id, не CID-тела"]
```

---

## 2-bis. Файловая система: раскладка и потоки (Mermaid)

### FS1. Раскладка gridstore на диске

```mermaid
flowchart TB
    DIR["каталог gridstore"] --> P0["page_0.dat (32МБ mmap)"]
    DIR --> P1["page_1.dat (32МБ mmap)"]
    DIR --> BM["bitmask (1 бит/блок) + region-gaps"]
    DIR --> TR["tracker.dat (id → ValuePointer, mmap)"]
    DIR --> CFG["config.json (page/block/region, compression=LZ4)"]
    TR -. "(page,block_off,len)" .-> P0
```

### FS2. Запись значения (put)

```mermaid
sequenceDiagram
    participant W as put_value
    participant BM as bitmask
    participant PG as pages
    participant TR as tracker
    W->>BM: find_available_blocks(N) → (page, block_off)
    W->>BM: mark_blocks(used)
    W->>PG: write_to_pages (через границы страниц)
    W->>TR: set pointer (pending)
    Note over TR: flush позже: bitmask→pages→tracker→free
```

### FS3. WAL: append + recovery по CRC

```mermaid
flowchart TB
    APP["append(entry)"] --> LEN["u64 len + data + 0..7 padding (8-байт align)"]
    LEN --> CRC["CRC32C = append поверх предыдущего crc (цепочка)"]
    CRC --> MMAP["в mmap; flush = msync диапазона"]
    REC["recovery"] --> SCAN["скан записей, проверяя CRC"]
    SCAN --> STOP{"CRC совпал?"}
    STOP -->|да| NEXT["следующая"]
    STOP -->|нет| TRUNC["стоп — torn tail отброшен (как наш eof/flushOffset)"]
```

### FS4. mmap madvise по носителю/режиму

```mermaid
flowchart LR
    LOAD["load mmap"] --> HOT{"горячий индекс?"}
    HOT -->|да| POP["MADV_POPULATE_READ (prefault)"]
    HOT -->|нет / low-mem| SKIP["skip populate (lazy)"]
    GLOB["глобально"] --> RAND["MADV_RANDOM (дефолт для lookup)"]
    BODY["write-once тела"] --> DN["(наш) MADV_DONTNEED после записи (#63)"]
```

### FS5. Atomic save состояния

```mermaid
flowchart LR
    SAVE["save_state"] --> TMP["write → tmp-файл"]
    TMP --> FS["fsync"]
    FS --> REN["rename (атомарно) → финал"]
    BUILD["build сегмента"] --> TD["в TempDir"]
    TD --> RENS["rename каталога → collection/"]
```

### FS6. Bitmask-аллокатор: точечный re-use дырок (#133)

```mermaid
flowchart TB
    subgraph SEG["сегмент = блоки 128Б"]
      direction LR
      B0["■ used"] --- B1["□ free"] --- B2["□ free"] --- B3["■ used"] --- B4["□ free"]
    end
    DEL["delete блока"] --> CLR["mark_blocks(used=0) → дырка"]
    CLR --> UPD["обновить RegionGaps{max,leading,trailing}"]
    PUT["put N блоков"] --> FIND["regions_gaps: регион с max≥N"]
    FIND --> SCAN["скан ТОЛЬКО этого региона → первый прогон ≥ N"]
    SCAN --> REUSE["переиспользовать дырку (без компакции)"]
    UPD --> FIND
```

### FS7. Crash-safety «течь, но не портить»: порядок фаз (#134)

```mermaid
flowchart LR
    S1["1. разметить занятость (bitmask/манифест)"] --> S2["2. записать тело в сегмент"]
    S2 --> S3["3. обновить индекс CID→addr"]
    S3 --> S4["4. освободить старые блоки"]
    CRASH["💥 краш на 1–2 (до индекса)"] -.-> LEAK["блоки 'занято', индекс не ссылается → УТЕЧКА"]
    LEAK --> GCFIX["фон-GC/scrub: 'занято, но не в индексе' → освободить"]
    note["данные целых блоков НЕ повреждены ни на одной фазе; без recovery-лога"]
```

---

## 3. Ubiquitous Language (термины Qdrant → наши)

| Термин | Значение | Наш аналог |
|---|---|---|
| **gridstore** | кастомный on-disk blob-store (Rust) | наш data-tier (pack-сегменты) |
| **page (page_N.dat)** | фикс-файл 32МБ | сегмент |
| **block (128Б)** | мин. единица аллокации | (нет — у нас append; bitmask = альтернатива) |
| **region (8192 блока)** | группа блоков с gap-сводкой | (нет — новое, #133) |
| **bitmask + RegionGaps** | free-space индекс (max/leading/trailing) | **★ новое** (#133) |
| **tracker / ValuePointer** | id → (page, block_off, len) | индекс `CID→(seg,off,len)` |
| **flusher** | порядок flush компонентов | recovery-point + манифест |
| **MADV_POPULATE_READ / WILLNEED** | prefault / префетч | **★ новое** (#135); DONTNEED = #63 |
| **low_memory_mode** | NoResident / NoPopulate | тиринг RAM↔mmap |
| **SeqLock** | lock-free чтение состояния | **★ новое** (#136) |
| **vacuum / merge optimizer** | rebuild по deleted_ratio / greedy-merge | наша компакция/GC |
| ⚠️ quantization / HNSW | сжатие векторов / ANN-граф | **не для нас** (непрозрачные блоки) |

---

## 4. Что берём (★) и почему — кратко

| # | Идея | Откуда | Зачем нам |
|---|---|---|---|
| **133** | Bitmask-аллокатор + per-region gap-summary (max/leading/trailing) | `gridstore/bitmask` | точечный re-use освободившихся блоков **без полной компакции**; best-fit без скана |
| **134** | Crash-safety «течь, но не портить»: порядок flush, leak-not-corrupt | `gridstore/mod.rs` | крах переразмечает занятое (утечка, чинится позже), данные никогда не портятся; без recovery-лога |
| **135** | madvise-дисциплина: POPULATE_READ + WILLNEED + low-memory тиры | `mmap/advice.rs`, `low_memory.rs` | тёплый старт горячего индекса, префетч многостраничного значения, деградация под нехватку RAM |
| **136** | SeqLock: lock-free чтение горячего состояния | `trififo/seqlock.rs` | дёшево читать free-space/ёмкость/статы под нагрузкой (читатели не блокируются) |
| **137** | TTL-кэш ёмкости/free-space (~5с) | `disk_usage.rs` | HRW-by-free часто опрашивает free → гасим шторм `statvfs` на 60 дисках |

---

## 5. Конвергенция (Qdrant ≈ наш дизайн — повторная валидация)

- **gridstore pages + ValuePointer** = наш сегмент + индекс `(seg,off,len)` — **Rust-референс нашего слоя**.
- **WAL**: CRC32C-цепочка per-entry + recovery **до первого несовпадения** + 8-байт паддинг + random-seed
  на сегмент + mmap+msync — = Kafka recovery-point (#111), eof-маркер (#99), torn-tail по CRC.
- **atomic-save** (temp→fsync→rename), **segment-build в TempDir→rename** = durable swap (#67) + манифест.
- **vacuum-optimizer** (rebuild при `deleted_ratio` > порога И ≥ min-points) = наш age/garbage-gated GC;
  **merge-optimizer** (greedy-batch, гарантия снижения числа сегментов: batch ≥3 или ≥2 batches) =
  minor/major компакция (Hive #104/#105).
- **O_DIRECT + io_uring + user-space 16КБ block-cache** = #72 (Dragonfly) + NVMe L2-кэш.
- **LZ4-сжатие значений** = наша опц. zstd тел.
- ⚠️ **quantization / HNSW / rescore / posting-bitpacking** — вектор-специфика, **не берём** (блоки
  непрозрачны; posting-bitpacking = как #126, только сортированные id).

---

## 9-bis. Снипеты кода (реальные выдержки + объяснение)

### QD1. Конфиг gridstore: страницы/блоки/регионы (#133 контекст)

`gridstore/src/config.rs:1-10`:

```rust
pub const DEFAULT_BLOCK_SIZE_BYTES: usize = 128;
pub const DEFAULT_PAGE_SIZE_BYTES: usize = 32 * 1024 * 1024; // 32MB
pub const DEFAULT_REGION_SIZE_BLOCKS: usize = 8_192;
```

**Зачем нам:** иерархия page(32МБ)→region(8192 блока)→block(128Б). Блок = единица аллокации; регион =
единица сводки свободного места. У нас сегмент(2ГБ) мог бы получить такой же sub-блочный аллокатор.

### QD2. RegionGaps: сводка свободного места на регион (#133)

`gridstore/src/bitmask/gaps.rs:13-19`:

```rust
#[repr(C)]
pub struct RegionGaps {
    pub max: u16,       // самый длинный свободный прогон в регионе
    pub leading: u16,   // свободных блоков с начала
    pub trailing: u16,  // свободных блоков с конца
}
```

**Зачем:** чтобы найти место под N блоков, не сканируем всю битмаску — берём регион, где `max ≥ N`
(`leading/trailing` помогают сшивать прогоны на границах регионов). O(числа регионов), не O(блоков).

### QD3. find_available_blocks: best-fit через gap-сводку (#133)

`gridstore/src/bitmask/mod.rs:252-282`:

```rust
pub(crate) fn find_available_blocks(&self, num_blocks: u32)
    -> Result<Option<(PageId, BlockOffset)>> {
    let Some(region_id_range) = self.regions_gaps.find_fitting_gap(num_blocks)? else {
        return Ok(None);                 // нет подходящего региона → вырастить страницу
    };
    let all_bits = self.bitslice.read_all()?;
    let regions_bitslice = &all_bits[regions_start_offset..regions_end_offset];
    Ok(Self::find_available_blocks_in_slice(...))   // скан ТОЛЬКО внутри найденного региона
}
```

**Зачем нам:** альтернатива append-only + компакция — **точечно переиспользовать дырки** от удалённых
блоков (как Dragonfly segmented-alloc #..., но с быстрым free-space-индексом).

### QD4. ValuePointer: id → (page, block_off, len) (#133 контекст)

`gridstore/src/tracker.rs:82-103`:

```rust
#[repr(C)]
pub struct ValuePointer {
    pub page_id: PageId,            // u32: какая страница
    pub block_offset: BlockOffset, // u32: смещение в БЛОКАХ
    pub length: u32,               // длина значения в байтах
}
```

**Наш аналог:** ровно `CID→(seg, off, len)`. Tracker = sparse-массив в mmap `tracker.dat`. Прямая
Rust-валидация нашей адресной модели.

### QD5. Crash-safety: «течь, но не портить» (#134)

`gridstore/src/gridstore/mod.rs:232-278` (док-коммент к `put_value`):

```rust
// This function needs to NOT corrupt data in case of a crash.
// ... we don't want to flush on every write ...
// In case of crashing somewhere in the middle of this operation, the worst
// that should happen is that we mark more cells as used than they actually are,
// so will never reuse such space, but data will not be corrupted.
```

И порядок flush (`mod.rs:443-483`): **bitmask → pages → tracker → free-blocks**.
**Зачем нам:** дизайн-принцип — упорядочить запись так, чтобы крах давал **утечку места** (безопасно,
чинится фоном), а не порчу/потерю. Усиливает two-phase-delete (#84) и манифест.

### QD6. madvise: POPULATE_READ (prefault) + low-memory (#135)

`common/src/mmap/advice.rs:114-141`:

```rust
fn populate(&self) {
    if crate::low_memory::low_memory_mode().skip_populate() {
        return;                                  // low-memory: НЕ prefault
    }
    if *POPULATE_READ_IS_SUPPORTED {
        match self.advise_impl(memmap2::Advice::PopulateRead) {  // MADV_POPULATE_READ (5.14+)
            Ok(()) => return,
            Err(_) => { /* fallback: читать каждый 512-й байт */ }
        }
    }
    self.populate_simple_impl();
}
```

**Зачем нам:** на старте **прогреть горячий индекс/Summary** (prefault), а под нехватку RAM —
`NoPopulate` пропускает прогрев (lazy). Парный к нашему `DONTNEED` (#63) для write-once тел.

### QD7. madvise: WILLNEED для многостраничного значения (#135)

`common/src/mmap/advice.rs:276-302`:

```rust
pub fn will_need_multiple_pages(region: &[u8]) {
    // ... page-align addr ...
    let res = unsafe { nix::libc::madvise(addr as *mut _, length, nix::libc::MADV_WILLNEED) };
    // префетч всего региона одним syscall, когда значение пересекает границы страниц
}
```

**Зачем:** блок/значение, лежащее через несколько страниц mmap — **префетчить целиком одним
madvise**, а не ловить page-fault на каждой странице (на HDD это серия seek'ов).

### QD8. low-memory режимы: тиринг RAM↔mmap (#135)

`common/src/low_memory.rs:19-34`:

```rust
pub enum LowMemoryMode {
    Disabled,
    NoResident,   // quantization always_ram=false; payload index on_disk=true; storage = mmap
    NoPopulate,   // то же + пропустить prefault на load
}
```

**Зачем нам:** один тумблер «мало RAM» переводит компоненты с RAM-варианта на mmap-вариант (тот же
байтовый формат → можно вернуть назад без rebuild). Перекликается с нашим тирингом и LazyIndex (#112).

### QD9. SeqLock: lock-free чтение (#136)

`trififo/src/seqlock.rs` (read):

```rust
fn read<U, F: Fn(&T) -> U>(&self, callback: F) -> U {
    loop {
        let seq1 = self.seq.load(Ordering::Acquire);
        if seq1 & 1 == 1 { std::hint::spin_loop(); continue; } // нечётно → писатель
        let result = callback(unsafe { &*self.inner.get() });
        fence(Ordering::Acquire);
        if seq1 == self.seq.load(Ordering::Relaxed) { return result; } // seq не изменился → ок
    }
}
```

**Зачем нам:** читать **горячее разделяемое состояние** (free-space, ёмкость диска, статистику кэша,
горячий хвост индекса) **без блокировки читателей** — важно при широком параллелизме на 60 дисках.

### QD10. WAL recovery: скан до первого несовпадения CRC (конвергенция)

`lib/wal/src/segment.rs:242-306` (сокр.):

```rust
while offset + HEADER_LEN + CRC_LEN < capacity {
    let len = LittleEndian::read_u64(&segment[offset..]) as usize;
    let entry_crc = crc32c::crc32c_append(!crc.reverse_bits(), &segment[offset..offset+HEADER_LEN+padded_len]);
    let stored_crc = LittleEndian::read_u32(&segment[offset+HEADER_LEN+padded_len..]);
    if entry_crc != stored_crc { break; }   // несовпадение → torn tail, стоп
    crc = entry_crc;                         // CRC-ЦЕПОЧКА (seed + поверх предыдущего)
    index.push((offset + HEADER_LEN, len));
    offset += HEADER_LEN + padded_len + CRC_LEN;
}
```

**Конвергенция:** ровно наш torn-tail по CRC (Kafka recovery-point #111, eof-маркер #99). CRC-цепочка +
random-seed на сегмент — аккуратная деталь.

### QD11 (диаграмма). Где Qdrant полезен vs неприменим

```mermaid
flowchart LR
    GRID["gridstore (Rust blob-store)"] --> USE["★ #133-134 аллокатор + crash-safety"]
    MMAP["mmap/madvise"] --> USE2["★ #135 populate/willneed/low-mem"]
    SEQ["trififo seqlock"] --> USE3["★ #136 lock-free чтение"]
    CAP["disk_usage TTL"] --> USE4["★ #137 кэш free-space"]
    VEC["quantization / HNSW / rescore"] --> SKIP["⚠️ вектор-специфика — НЕ берём"]
```

---

## 10. Извлечённые идеи для OpenZFS Daemon

### Конвергенция (Qdrant на Rust — валидация нашей модели)
- gridstore pages+ValuePointer = сегмент+индекс; WAL CRC-цепочка+recovery = recovery-point/eof;
  atomic-save = durable swap; vacuum/merge = компакция/GC; O_DIRECT+io_uring = #72; LZ4 = опц. zstd.
- ⚠️ Не берём: quantization, HNSW, rescore, posting-bitpacking (вектор-специфика / только сортированные id).

### Главные новые заимствования
- **#133 ★** Bitmask-аллокатор + per-region gap-summary (max/leading/trailing) — точечный re-use
  дырок без полной компакции; best-fit без скана.
- **#134 ★** Crash-safety «течь, но не портить»: порядок flush bitmask→pages→tracker→free; крах =
  утечка места (чинится фоном), не порча данных; без recovery-лога.
- **#135 ★** madvise-дисциплина: POPULATE_READ (prefault горячего индекса) + WILLNEED (префетч
  многостраничного значения) + low-memory тиры (NoResident/NoPopulate).
- **#136 ★** SeqLock: lock-free чтение горячего состояния (free-space/ёмкость/статы/хвост индекса).
- **#137** TTL-кэш ёмкости/free-space (~5с) — гасит шторм `statvfs` при HRW-by-free на 60 дисках.

---

## 11. Источники в коде (для перепроверки)

- `gridstore/src/config.rs:1-67` page/block/region + LZ4; `pages.rs:20-428` раскладка/запись страниц
- `gridstore/src/bitmask/mod.rs:36-47,252-282,383-427`, `bitmask/gaps.rs:13-19` битмаска + region-gaps
- `gridstore/src/tracker.rs:35-103,355-367,506-625` tracker/ValuePointer/pending; `gridstore/mod.rs:232-483` crash-safety/flush; `gridstore/view.rs:93-110` get_value
- `common/src/mmap/advice.rs:10-141,276-302`, `mmap/ops.rs:86-100`, `low_memory.rs:6-48` madvise/low-mem
- `lib/wal/src/segment.rs:152-447` WAL append/flush/truncate/recovery
- `lib/trififo/src/seqlock.rs` SeqLock lock-free
- `common/src/disk_usage.rs:44-72` TTL-кэш free-space
- `common/src/save_on_disk.rs:151-159`, `segment_constructor/segment_builder.rs:759-762` atomic-save
- `collection_manager/optimizers/{vacuum,merge}_optimizer.rs` оптимайзеры
- `common/src/universal_io/{disk_cache/mod.rs,io_uring/mod.rs}` 16КБ block-cache + O_DIRECT/io_uring
- ⚠️ (не берём) `lib/quantization/src/encoded_vectors_{u8,pq,binary}.rs`, `index/hnsw_index/*`

---

*Связано: [pack-segments (Feynman)](../../Feynman/pack-segments.md), [STORAGE-IDEAS-SYNTHESIS.md](STORAGE-IDEAS-SYNTHESIS.md), [dragonfly (segmented-alloc, O_DIRECT)](dragonfly-storage-hdd-ssd.md), [kafka (recovery-point)](kafka-storage-hdd-ssd.md), [redis (DONTNEED, durable-swap)](redis-storage-hdd-ssd.md).*
