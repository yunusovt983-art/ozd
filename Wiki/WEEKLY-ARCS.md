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

### Арка W1 — Degraded Start + Timeouts (2 дня)

| Задача | Файл | Описание |
|--------|------|----------|
| W1.1 | ozd-daemon/main.rs | Degraded start: ошибка открытия шарда → Faulted, не panic. Логировать, продолжить с N-1 дисками |
| W1.2 | ozd-zfs/runner.rs | Таймаут 30с на все ZFS-команды (Command + wait_timeout) |
| W1.3 | ozd-daemon/main.rs | ZFS-монитор: timeout на spawn_blocking = 60с, при таймауте → Observation::Down |
| W1.4 | ozd-admin/lib.rs | JSON-экранирование error-сообщений (минимум `"` и `\`) |

**Критерий:** демон стартует при 1 недоступном диске; зависший zpool не блокирует мониторинг.

---

### Арка W2 — Zero-copy горячий путь (2 дня)

| Задача | Файл | Описание |
|--------|------|----------|
| W2.1 | ozd-domain/lib.rs | `BlockStore::put(&self, key, data: &[u8])` → scoped-threads в Pool (убрать Arc<Vec>) |
| W2.2 | ozd-app/pool.rs | `put_body`: `std::thread::scope` вместо `std::thread::spawn` (нет аллокации Arc<Vec>) |
| W2.3 | ozd-app/pool.rs | `get_inner` hedged: то же — scoped threads, без Arc на результат |
| W2.4 | ozd-engine/lib.rs | `#[inline]` на decode_addr/encode_addr/lookup |

**Критерий:** bench PUT p50 улучшается ≥10% на 256КиБ телах; нет регрессии тестов.

---

### Арка W3 — Per-disk Worker Pool (2 дня)

| Задача | Файл | Описание |
|--------|------|----------|
| W3.1 | ozd-app/pool.rs | `DiskWorkerPool`: bounded crossbeam-channel per shard (capacity = inflight 4) |
| W3.2 | ozd-app/pool.rs | put_body/get_inner отправляют задачи в воркер-пул, не spawn |
| W3.3 | ozd-app/pool.rs | Конфиг `disk_inflight: usize` (дефолт 4 HDD, 32 NVMe) |
| W3.4 | Cargo.toml | `crossbeam-channel` в workspace deps |

**Критерий:** при 1000 конкурентных PUT/GET живых потоков ≤ shards × inflight (не тысячи); thread-count стабилен.

---

### Арка W4 — Observability: /metrics Prometheus + Grafana (1 день)

| Задача | Файл | Описание |
|--------|------|----------|
| W4.1 | ozd-admin/lib.rs | Histogram buckets для put/get latency (вместо только sum) |
| W4.2 | Wiki/ | `GRAFANA.md` — шаблон дашборда (JSON) для 30+ метрик |
| W4.3 | ozd-app/metrics.rs | `ozd_pool_threads_active` gauge — текущие живые IO-потоки |
| W4.4 | ozd-daemon/main.rs | `/metrics` — добавить go-runtime-стиль метрики (heap, goroutines → threads) |

**Критерий:** Grafana-дашборд импортируется и показывает capacity/latency/heal/ec на синтетической нагрузке.

---

## III. Приоритизация (MoSCoW)

| Must | Should | Could | Won't (эта неделя) |
|------|--------|-------|---------------------|
| W1 degraded-start | W2 zero-copy | W4 Grafana | Kubo-стенд (E30) |
| W1 timeouts | W3 worker-pool | JSON-escape | Multi-node (Ч3) |
| | | GC sweep кэш | io_uring backend |

---

## IV. Технический долг (backlog, не на эту неделю)

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
