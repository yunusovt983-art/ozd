# NEXT STEPS

**Дата:** 2026-06-19
**Источники истины:** [Wiki/ROADMAP.md](Wiki/ROADMAP.md), [Wiki/WEEKLY-ARCS.md](Wiki/WEEKLY-ARCS.md)

> ⚠️ Предыдущая версия этого файла ошибочно планировала W10–W31 как будущую
> работу. На деле **все они уже закрыты** (см. WEEKLY-ARCS.md). Этот файл
> переписан под реальное состояние.

---

## TL;DR

**Вся работа, не требующая железа, завершена.** Готово:
- **Арки 1–7** (ROADMAP): каркас, sharding+packing, самовосстановление, формат
  данных (zstd/EC 4+2/CAR/BLAKE3), СуперДиск (CacheTier), доводка p99.
- **Weekly W1–W31** (WEEKLY-ARCS): degraded-start, таймауты, zero-copy, метрики,
  Grafana, error-taxonomy, proptest, CI, async-порт, Docker+Kubo, **W10
  gen_config+systemd**, hardening, typed admin API, JSON-логи, graceful
  shutdown v2, rate-limiter, snapshots/backup, healthz v2, SIGHUP, retention.

**Следующая настоящая работа требует сервера** (Арка 8). До его появления
остаются только: (1) проверка W9 Phase 2 локально под Docker и (2) опциональный
backlog.

---

## Что осталось без железа

### 1. Верификация W9 Phase 2 (Kubo + go-ds-s3) — ✅ ЗАКРЫТО (2026-06-20, commit 4621d87)

Стек собран и прогнан под Docker/colima. `ipfs add`→`ipfs cat` roundtrip через
ozd S3 работает; блоки лежат как `index.redb` + pack-сегменты на шардах (R=2),
SigV4 ВКЛ, 0 отказов от Kubo. По ходу найдено и исправлено 3 runtime-бага
(glibc-mismatch билдера, рассинхрон `datastore_spec`, ключ `regionEndpoint` вместо
`endpoint`). go-ds-s3 v0.11.0 ↔ Kubo v0.32.1 — совместимы.

- [x] `docker compose build` — Kubo c go-ds-s3 собирается (s3ds вшит в бинарь).
- [x] `docker compose up` → ozd healthy → Kubo `Daemon is ready`.
- [x] `ipfs add` → блок улетел в ozd (проверено на дисках).
- [x] `ipfs cat` → бит-в-бит совпадает (повторяемо).
- [ ] `bash scripts/kubo_smoke.sh` зелёный против auth-стека — **остаток на W32.1**
      (скрипту нужен SigV4; см. [PLAN-WEEK.md](PLAN-WEEK.md)).

**Дальнейшее упрочнение (без железа):** см. [PLAN-WEEK.md](PLAN-WEEK.md) — multi-block,
pin/GC, durable-прогон без tmpfs, авто-e2e в CI, проводка ozd-zfs health.

### 2. Backlog (всё помечено «не требуется сейчас» — делать только по необходимости)

| ID | Что | Почему отложено | Когда вернуться |
|----|-----|-----------------|-----------------|
| W3 | Per-disk worker pool (bounded channel) | scoped threads уже сняли thread-exhaustion | если замер E32 покажет contention |
| W6.1 | Полный кэш `referenced_segments` с инвалидацией | периодический sweep (раз в 5 проходов) достаточен | если GC-скан станет узким местом на масштабе |
| W8.3 | Async-адаптер в хэндлерах | хэндлеры уже `spawn_blocking` | при переходе на multi-node (Арка 9) |
| W11 | reed-solomon-simd (GF 2^16) | breaking-change формата (сейчас GF 2^8) | при переходе на формат v2 |

---

## Арка 8 — требует сервера (E30 → E31 → E32)

> Порядок ROADMAP: E30 первым, как только есть железо.

### E30 — Kubo-стенд (= E15)
Реальный Kubo+go-ds-s3 → ozd по [KUBO-INTEGRATION.md](Wiki/KUBO-INTEGRATION.md):
SigV4-канонизация, `ipfs add/cat/pin/gc`, первый реальный hit-rate СуперДиска.
**Критерий:** `ipfs add`→`ipfs cat` бит-в-бит; блоки в /metrics; SigV4 — 0 отказов.

> Заметка: верификация W9 Phase 2 (выше) — это E30 «в миниатюре» на tmpfs под
> Docker. Полный E30 = то же на реальном Kubo-трафике на сервере.

### E31 — Деплой на полку
gen_config.sh (✅ готов) на 60 дисков → systemd (✅ готов) → runbook (zpool
create/tuning из шапки ozd.example.toml) → Grafana-дашборд (✅ GRAFANA.md).
**Критерий:** демон стартует на полке с identity-чеком #149; дашборд живой.

### E32 — Нагрузка на полке
Профиль реального трафика → тюнинг (ec_min_size, cache max_bytes, bg-бюджеты,
scrub-каденс) + хаос-смоук (выдернуть диск под нагрузкой → resilver при живом
трафике). Здесь же закрываются «→ E32 на железе» хвосты: RSS-замер #64
sync_file_range (E26), /proc/diskstats (E28).
**Критерий:** p99 чтения и время ребилда в docs/BENCH.md; throttle держит foreground.

---

## Арка 9 — Часть 3: мультиузел / P2P 🧊 (после Арки 8)

E33 Merkle anti-entropy · E34 Tombstone+gc_grace · E35 Fencing+мульти-шлюз ·
E36 P2P verified fetch (фундамент готов: x-ozd-bao + verify_bao_slice).
Заморожено до стабилизации одной ноды.

---

## Архитектурные долги (из WEEKLY-ARCS §V — следить, не срочно)

1. **Sync-only Pool** — вся логика пула синхронна (thread::scope). Multi-node
   (Арка 9) потребует настоящего async. Задел есть: `AsyncBlockStore` +
   `SpawnBlockingAdapter` (W8) готовы, но не подключены (W8.3 backlog).
2. **Один redb на шард** — при 3.8 млрд ключей redb-файл ~200 ГБ; backup/compaction
   не контролируем. Рассмотреть sharded-redb (split по prefix) или rocksdb/fjall
   при росте. Не блокер до реального масштаба.

---

## Рекомендация

Без сервера осмысленный шаг ровно один: **собрать и прогнать W9 Phase 2 под
Docker** (раздел 1). Это либо подтвердит интеграцию Kubo↔ozd, либо вскроет
проблему совместимости go-ds-s3 заранее — до того, как появится железо для E30.
Всё остальное — либо сделано, либо ждёт полки, либо backlog «по необходимости».

*Обновлять при закрытии E30/E31/E32. История решений — memory/ozd-implementation.*
