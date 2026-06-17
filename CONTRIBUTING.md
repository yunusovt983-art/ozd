# Contributing to OpenZFS Daemon (ozd)

## Быстрый старт

```bash
git clone https://github.com/yunusovt983-art/ozd.git
cd ozd
cargo build
cargo test
```

## Структура крейтов

```
crates/
├── ozd-domain/    # Ядро домена: traits, VO, ошибки. БЕЗ IO.
├── ozd-engine/    # ShardEngine: pack-сегменты + redb-индекс (один диск)
├── ozd-app/       # Use-cases: Pool, placement, cache, erasure, health
├── ozd-ipfs/      # S3-шлюз (axum) + SigV4 + async-адаптер
├── ozd-admin/     # Admin API: /metrics, /admin/gc, /admin/resilver…
├── ozd-zfs/       # ZFS-адаптер: runner, parser, properties, FSM
├── ozd-daemon/    # Binary: конфиг, wiring, фоновые сервисы
└── ozd-bench/     # Нагрузочный харнесс (in-process)
```

## Правило зависимостей (гексагон)

```
ozd-domain ← ни от кого не зависит (чистый домен)
ozd-engine, ozd-zfs ← зависят от ozd-domain
ozd-app ← от ozd-domain
ozd-ipfs, ozd-admin ← от ozd-app + ozd-domain
ozd-daemon ← собирает всё (composition root)
```

**Нарушение этого правила = PR не принимается.**

## DDD-принципы

1. **Домен без IO** — `ozd-domain` содержит traits (`BlockStore`, `ShardEngine`, `PlacementPolicy`),
   value-objects (`BlockKey`, `ShardId`, `Capacity`), ошибки. Никаких файловых/сетевых операций.

2. **Порты и адаптеры** — домен определяет порты (traits), адаптеры реализуют их:
   - `DiskEngine` реализует `ShardEngine`
   - `Pool` реализует `BlockStore`
   - `RendezvousHrw` реализует `PlacementPolicy`

3. **Агрегат Pool** — гарантирует инварианты: R копий на R разных дисках,
   write-quorum, placement детерминирован.

## Стандарты кода

- **Идентификаторы** — английские
- **Комментарии** — по-русски
- **Panic** — только в тестах (`unwrap()`). Горячий путь — `Result`
- **Зависимости** — минимум. Не добавлять без крайней необходимости
- **Форматирование** — `cargo fmt`
- **Линтер** — `cargo clippy -- -D warnings` (0 warnings в CI)

## Тестирование

```bash
# Все тесты
cargo test

# Конкретный крейт
cargo test -p ozd-engine --lib

# Property-тесты (proptest, ~10с)
cargo test -p ozd-engine --test proptest_segment

# Bench smoke
cargo run -p ozd-bench --release -- --disks=4 --objects=100 --reads=200

# Smoke-тест S3 API (нужен запущенный ozd на :9100)
./scripts/kubo_smoke.sh
```

## Коммиты

- Сообщения по-русски, краткие
- Формат: `модуль: описание` или `W<N>.<M>: описание` (для арок)
- Маленькие атомарные коммиты (одна задача = один коммит)

## Ветки

- `main` — стабильная, после ревью
- `feature/night` — рабочая ветка автономных прогонов
- PR в main — после `cargo test` зелёного

## Документация

- `Wiki/ARCHITECTURE.md` — полный дизайн (DDD, гексагон)
- `Wiki/PLAN.md` — план реализации (6 фаз)
- `Wiki/ROADMAP.md` — арки и эпики с статусами
- `Wiki/WEEKLY-ARCS.md` — текущий спринт
- `Wiki/KUBO-INTEGRATION.md` — как подключить Kubo
- `Wiki/GRAFANA.md` — шаблон дашборда

## Лицензия

AGPL-3.0-or-later. SPDX-заголовок в каждом `.rs` файле.
