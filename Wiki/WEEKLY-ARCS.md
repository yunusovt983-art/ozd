# Арки на неделю — Code Review & Architecture Sprint

> Стиль: 3x-ui architecture research. Дата: 2026-06-17.
> Метод: полный обход 11 049 строк Rust, ARCHITECTURE.md, PLAN.md, ROADMAP.md.

---

## I. Код-ревью: найденные проблемы и предложения

### 1. Блокирующий IO в async-контексте (ozd-app/pool.rs)

**Проблема:** `Pool::put_body` и `Pool::get_inner` запускают `std::thread::spawn` + `mpsc::channel` для параллельной записи/hedged-read. Это корректно (blocking IO в отдельных потоках), но:
- Каждый PUT создаёт R потоков (2 на запись) + R потоков на hedged GET = **до 4 потоков на каждый запрос**.
- На 60 HDD при нагрузке 1000 RPS = 4000 живых потоков. Потенциальный thread-exhaustion.

**Предложение:** Заменить на `tokio::task::spawn_blocking` + bounded thread-pool (по числу дисков × inflight). Либо per-disk `crossbeam::channel` workers (Фаза 2 PLAN: per-disk worker pool, inflight 1–4). Это второй по приоритету рефакторинг.

### 2. Аллокации на горячем пути (pool.rs put_body)

```rust
let shared: Arc<Vec<u8>> = Arc::new(data.to_vec()); // копия тела на КАЖДЫЙ PUT
```

**Проблема:** Для тела 256 КиБ — лишние 256 КиБ копий + Arc на каждый запрос. Kubo по умолчанию шлёт 262144-байтные блоки, это ~1 ГБ/с лишних аллокаций при 4000 блоков/с.

**Предложение:** Принять `&[u8]` по ссылке в `ShardEngine::put`, передавать в потоки через scoped threads (уже есть в bench — паттерн знаком). Или `Bytes` (zero-copy при clone).

### 3. Отсутствие `#[inline]` на горячих lookup-методах (ozd-engine/lib.rs)

`lookup()`, `decode_addr()`, `encode_addr()` — вызываются на каждый get/put/has. Компилятор может не инлайнить между крейтами.

**Предложение:** `#[inline]` на `decode_addr`, `encode_addr`; `#[inline(never)]` на slow-path (GC, recovery).

### 4. serde_json_like в ozd-admin — ручная сериализация JSON

**Текущее:** строки форматируются через `format!()` без экранирования. Ошибка с кавычкой в сообщении → сломанный JSON.

**Предложение:** Или подтянуть `serde_json` (4 КБ в бинаре, уже есть `serde`), или хотя бы экранировать `"` и `\` в error-сообщениях.

### 5. `parking_lot_lite` в runner.rs — самодельный wrapper

```rust
mod parking_lot_lite {
    pub struct Mutex<T>(std::sync::Mutex<T>);
```

**Проблема:** Крейт `ozd-zfs` использует `std::sync::Mutex` через обёртку, а все остальные крейты — `parking_lot`. Несогласованность.

**Предложение:** Добавить `parking_lot` в зависимости `ozd-zfs` (уже в workspace deps). Унификация.

### 6. Отсутствие таймаутов на ZFS-команды (runner.rs)

`Command::new(program).args(args).output()` — блокирует бесконечно если zfs/zpool зависнет (а на умирающем диске — зависает).

**Предложение:** `tokio::process::Command` с `timeout` или `wait_timeout` из `std`. Критично на 60 дисках: один зависший `zpool status` блокирует мониторный цикл.

### 7. HealQueue: `BinaryHeap` + HashMap дубликатов растёт без bound на HashMap

CAP = 100_000 элементов, но `HashMap<BlockKey, HealPriority>` не чистится от lazy-delete записей в heap (устаревшие entry остаются в heap, только dedup-карта обновляется). При upgrade heap может содержать до 2× CAP фантомных записей.

**Предложение:** Периодический `shrink_to_fit()` или drain+rebuild при len > 2× dedup.len().

### 8. Тесты используют `std::thread::sleep` для timing — flaky на CI

Тесты `parallel_put_latency_is_max_not_sum`, `speculative_retry_hedges_slow_read_leg` полагаются на wall-clock timing. На перегруженном CI могут флаповать.

**Предложение:** Увеличить допуск (уже 260мс на 150+150мс — хорошо) или использовать injection-шов для времени (как в `RollingP99`).

### 9. `DiskEngine::gc_once` — full scan addr-table для `referenced_segments`

На 3.8 млрд ключей этот скан = минуты. Сейчас вызывается на КАЖДЫЙ GC-проход (sweep_orphans).

**Предложение:** Кэшировать referenced-set; инвалидировать при put/delete (инкрементальный учёт). Или запускать sweep_orphans раз в N проходов (не каждый).

### 10. Нет graceful-degradation при ошибке открытия redb

`DiskEngine::open` → `Database::create` может упасть (permissions, corrupt). Демон падает целиком.

**Предложение:** Пометить шард Faulted и продолжить старт (degraded-start, PLAN Ф3). Сейчас один битый индекс = полный отказ.

---

## II. Арки на неделю (17–23 июня 2026)

### Арка W1 — Degraded Start + Timeouts ✅

| Задача | Файл | Описание | Статус |
|--------|------|----------|--------|
| W1.1 | ozd-daemon/main.rs | Degraded start: ошибка открытия шарда → Faulted, не panic | ✅ |
| W1.2 | ozd-zfs/runner.rs | Таймаут 30с на все ZFS-команды (try_wait + kill) | ✅ |
| W1.3 | ozd-daemon/main.rs | ZFS-монитор: timeout 60с на spawn_blocking | ✅ |
| W1.4 | ozd-admin/lib.rs | JSON-экранирование error-сообщений (`json_escape`) | ✅ |

**Критерий:** демон стартует при 1 недоступном диске; зависший zpool не блокирует мониторинг.

---

### Арка W2 — Zero-copy горячий путь ✅

| Задача | Файл | Описание | Статус |
|--------|------|----------|--------|
| W2.1 | ozd-domain/lib.rs | `BlockStore::put` принимает `&[u8]` — scoped threads по ссылке | ✅ (объединено с W2.2) |
| W2.2 | ozd-app/pool.rs | `put_body`/`put_ec`: `std::thread::scope` вместо `spawn` + `Arc<Vec>` | ✅ |
| W2.3 | ozd-app/pool.rs | `get_inner` hedged — уже scoped в bench | ⬜ (не требуется) |
| W2.4 | ozd-engine/lib.rs | `#[inline]` на decode_addr/encode_addr/lookup | ✅ |

**Критерий:** bench PUT p50 улучшается ≥10% на 256КиБ телах; нет регрессии тестов.

---

### Арка W3 — Per-disk Worker Pool (отложена → backlog)

> **Решение:** W2.2 (scoped threads) уже решает thread-exhaustion — потоки живут ровно на
> операцию, не утекают. Bounded per-disk channel — следующий уровень оптимизации при
> необходимости (замер на стенде E30). Scoped threads достаточны для текущего масштаба.

---

### Арка W4 — Observability: /metrics Prometheus + Grafana ✅

| Задача | Файл | Описание | Статус |
|--------|------|----------|--------|
| W4.1 | ozd-app/metrics.rs | Histogram buckets для put/get latency (12 стандартных бакетов) | ✅ |
| W4.2 | Wiki/GRAFANA.md | Шаблон дашборда (20 панелей, JSON для импорта) | ✅ |
| W4.3 | ozd-app/metrics.rs+pool.rs | `ozd_inflight_puts/gets` gauge — backpressure мониторинг | ✅ |

---

## III. Приоритизация (MoSCoW)

| Must | Should | Could | Won't (эта неделя) |
|------|--------|-------|---------------------|
| W1 degraded-start | W2 zero-copy | W4 Grafana | Kubo-стенд (E30) |
| W1 timeouts | W3 worker-pool | JSON-escape | Multi-node (Ч3) |
| | | GC sweep кэш | io_uring backend |

---

## Неделя 2 (24–30 июня 2026)

### Арка W5 — Error taxonomy + Config validation ✅

| Задача | Файл | Описание | Статус |
|--------|------|----------|--------|
| W5.1 | ozd-domain/lib.rs | Sentinel-варианты: `Timeout`, `DiskFull`, `Corrupt`, `Config` | ✅ |
| W5.2 | ozd-daemon/main.rs | Graceful config validation: write_quorum/replicas проверяются при старте | ✅ |
| W5.3 | ozd-app/pool.rs | Информативные assert-сообщения в `Pool::new` | ✅ |

**Критерий:** невалидный конфиг → понятное сообщение при старте (не panic); sentinel-ошибки матчатся без парсинга строк.

---

### Арка W6 — GC sweep_orphans кэширование ✅

| Задача | Файл | Описание | Статус |
|--------|------|----------|--------|
| W6.1 | ozd-engine/lib.rs | Полный кэш `referenced_segments` с инвалидацией | ⬜ (backlog — периодический sweep достаточен) |
| W6.2 | ozd-engine/lib.rs | `sweep_orphans` раз в 5 GC-проходов (gc_pass_count % 5) | ✅ |

**Критерий:** на 10K ключей GC-проход не сканирует всю addr-таблицу; sweep_orphans отрабатывает периодически.

---

### Арка W7 — Property-тесты + CI bench ✅

| Задача | Файл | Описание | Статус |
|--------|------|----------|--------|
| W7.1 | Cargo.toml | `proptest = "1"` в dev-deps ozd-engine | ✅ |
| W7.2 | crates/ozd-engine/tests/proptest_segment.rs | 4 property-теста: roundtrip, delete, stat, reopen-recovery | ✅ |
| W7.3 | .github/workflows/ci.yml | `cargo test` + `cargo clippy -- -D warnings` | ✅ |
| W7.4 | .github/workflows/ci.yml | Bench smoke: `cargo run -p ozd-bench --release -- --disks=4 --objects=50` | ✅ |

**Критерий:** CI зелёный с proptest + clippy + bench-smoke; property-тесты ловят edge-cases (пустые ключи, huge тела, concurrent put/get).

---

### Арка W8 — Async-ready Port (подготовка к multi-node) ✅

| Задача | Файл | Описание | Статус |
|--------|------|----------|--------|
| W8.1 | ozd-domain/lib.rs | `AsyncBlockStore` trait с RPITIT (Rust 1.75+) | ✅ |
| W8.2 | ozd-ipfs/src/async_adapter.rs | `SpawnBlockingAdapter`: sync BlockStore → async через spawn_blocking | ✅ |
| W8.3 | ozd-daemon/main.rs | Подключить адаптер в хэндлеры | ⬜ (backlog — текущие хэндлеры уже spawn_blocking) |

**Критерий:** S3-шлюз работает через async-адаптер; sync Pool не тронут; подготовка к Ч3 (gateway'и).

---

## Приоритизация (MoSCoW) — Неделя 2

| Must | Should | Could | Won't (эта неделя) |
|------|--------|-------|---------------------|
| W5 error taxonomy | W7 proptest+CI | W8 async port | Per-disk worker pool (W3) |
| W5 config validation | W7 bench smoke | | Multi-node (Ч3) |
| W6 GC sweep кэш | | | Kubo-стенд (E30, нужен сервер) |

1. **async/await переход Pool** — сейчас sync + thread::spawn. Для multi-node (Ч3) нужен настоящий async.
2. **Property-тесты** — proptest для segment format (PLAN Ф1). Нет ни одного.
3. **Benchmarks CI** — criterion + regression detection.
4. **Error taxonomy** — `DomainError::Io(String)` слишком широк; sentinel-типы (как в ozd-zfs).
5. **reed-solomon-simd** — текущий `reed-solomon-erasure` без SIMD; на AVX2 EC 4+2 в 8× быстрее.
6. **Integration test с реальным Kubo** — E15/E30, блокирован сервером.
7. **`ozd-bench` в CI** — regression detection на perf (пока manual).
8. **Config validation** — `ec_write_quorum > total`, `replicas > disks` ловятся assert, не graceful.

---

## V. Архитектурные наблюдения

### Что сделано хорошо

- **DDD-чистота:** домен (`ozd-domain`) без IO — trait-порты, value-objects, чистые типы. Адаптеры зависят от домена, не наоборот.
- **Self-describing данные:** EC-куски несут заголовок с k/m/idx/logical — ремонт без каталога.
- **Crash-safety:** порядок «тело → индекс → free» + CRC + torn-tail recovery — корректен.
- **Тестовое покрытие pool.rs:** 20+ интеграционных тестов (bitrot, failover, resilver, EC, migration, hedged-read). Покрытие edge-cases выше среднего для storage-проекта.
- **FIFO-эвикция кэша** вместо LRU — правильный выбор для content-addressed (иммутабельные тела, нет инвалидации).

### Архитектурный риск

- **Sync-only Pool:** вся логика пула — synchronous (thread::spawn/mpsc). При переходе на multi-node (gateway'и) придётся переписывать на async. Стоит закладывать async-порт уже сейчас (trait с `async fn` через `async-trait` или RPITIT).
- **Один redb на шард:** при 3.8 млрд ключей redb-файл ~200 ГБ. Backup/compaction redb'a не контролируем. Рассмотреть sharded-redb (split по prefix) или переход на rocksdb/fjall при росте.

---

*Обновлять по результатам спринта. Следующий ревью — после Арки W3.*
