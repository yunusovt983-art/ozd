# План: Variant A (XFS) vs Variant B (ZFS) — что меняется по фазам

Дельта реализации между [PLAN.md](PLAN.md) (Variant A, выбран) и переключением на
[ARCHITECTURE-ZFS.md](ARCHITECTURE-ZFS.md) (Variant B). Решение по умолчанию — A
([ADR 0001](adr/0001-storage-substrate.md)); эта таблица показывает цену/выгоду смены.

Легенда дельты: 🟢 проще/меньше кода · 🔴 сложнее/новая работа · ⚪ без изменений ·
🔧 код→конфиг (сложность уезжает в ops).

---

## Сводка одной строкой

> **Variant B удаляет две самые тяжёлые и баг-опасные фазы (placement+репликация и
> walk-resilver), но переносит их в конфигурацию ZFS и эксплуатацию.** Меньше Rust-кода —
> больше ops-ответственности и ZFS-тюнинга. Фаза интеграции с IPFS и GC — общие.

| Метрика | Variant A | Variant B |
|---|---|---|
| Объём нашего кода | больше (placement, R=2, resilver) | **меньше** (тонкий blockstore) |
| Сложность в коде vs ops | в коде | **в ops/конфиге** |
| Самые рискованные части | HRW-распределение, resilver-консистентность | recordsize-тюнинг, заполнение special vdev |
| Multi-host в будущем | ✅ есть путь (сетевая репликация) | ❌ нет (ZFS = один сервер) |

---

## Пофазная матрица

### Фаза 0 — Каркас
| | Variant A | Variant B | Δ |
|---|---|---|---|
| Крейты | `ozd-engine` (data+index tier) | `ozd-store-zfs` (redb+файлы); **нет** placement-крейта | 🟢 |
| Traits | `BlockStore`, `ShardEngine`, `PlacementPolicy` | только `BlockStore` (+GC) | 🟢 |
| Пререквизит | — | **провижининг ZFS-пула** (30× mirror + special NVMe) — ops, до кода | 🔴🔧 |
| Конфиг | `data_path/index_path/domain` на диск | `zfs_mountpoint`, `index_path` | 🟢 |

### Фаза 1 — Один диск / один стор
| | Variant A | Variant B | Δ |
|---|---|---|---|
| Суть | `ShardEngine`: data-tier XFS-HDD + index-tier NVMe | `ZfsBlockStore`: redb-индекс + файлы на ZFS-маунте | 🟢 |
| Запись | flatfs write-once + redb на NVMe вручную | файл на `tank/blocks` + redb (ZFS даёт durability) | 🟢 |
| Новый труд | — | **datasets + recordsize-тюнинг** (index 16K / blocks 1M), бенч двойного CoW | 🔴🔧 |
| Целостность | verify on write/read (наш) | verify on write; on read **опц.** (доверяем ZFS-checksum) | 🟢 |

### Фаза 2 — Pool + Placement + Репликация
| | Variant A | Variant B | Δ |
|---|---|---|---|
| Код | **HRW top-R, `Pool`, R=2, write-quorum, тесты распределения** | **ОТСУТСТВУЕТ** — striping и mirror делает ZFS | 🟢🟢 |
| Замена | — | настройка/валидация mirror-пула: проверить, что отказ половины зеркала не теряет данные | 🔧 |
| Тесты | равномерность 1M CID, N→N+1 ≈1/(N+1) | `zpool offline` диска → данные доступны (ZFS) | 🟢 |

> Это фаза, ради которой существует Variant A. В B она схлопывается в `zpool create` + проверку.

### Фаза 3 — Resilver
| | Variant A | Variant B | Δ |
|---|---|---|---|
| Код | **walk-based `ResilverService`** (проход индексов, пересчёт placement, докопирование) | **ОТСУТСТВУЕТ** — `zpool replace` + mirror-resilver | 🟢🟢 |
| Замена | — | **runbook**: замена диска, мониторинг resilver, alert на degraded | 🔧 |
| degraded start | наш код | ZFS импортирует degraded-пул сам | 🟢 |

### Фаза 4 — Интеграция с IPFS-демоном
| | Variant A | Variant B | Δ |
|---|---|---|---|
| `ozd-ipfs` impl `rust-ipfs BlockStore` | да | да | ⚪ |
| E2E `ipfs add/cat`, Bitswap с kubo | да | да | ⚪ |
| Отличие | блоки на 60 XFS-дисках | блоки на одном ZFS-маунте | ⚪ |

> Практически идентична. Точка подключения (наш `BlockStore`) одна и та же.

### Фаза 5 — Эксплуатация
| | Variant A | Variant B | Δ |
|---|---|---|---|
| Scrub | `ScrubService` (наш код) | **`zpool scrub`** (cron) | 🟢🔧 |
| Resilver/heal | наш (из Фазы 3) | ZFS | 🟢 |
| GC (pin mark-sweep) | наш | **наш** (ZFS не знает IPFS-семантику) | ⚪ |
| Admin CLI | `pool status/add/remove/resilver/scrub/gc` | `store status/gc` + проксирование `zpool status`/`arcstat` | 🟢 |
| Метрики | per-shard used/free, resilver progress | **+ ZFS-exporter** (ARC hit, фрагментация, special vdev fill, scrub) | 🔴 |
| domain-aware | наш выбор 2-й реплики | не наше (топологию знает ZFS-админ) | 🟢 |

### Фаза 6 — Закалка
| | Variant A | Variant B | Δ |
|---|---|---|---|
| Краш записи | tmp→rename + redb recovery | ZFS-транзакционность + redb recovery | 🟢 |
| Отказ диска | наш resilver под нагрузкой | `zpool replace` + resilver под нагрузкой | 🟢 |
| Новые риски | seek-латентность, баланс | **двойной CoW redb (фрагментация во времени), переполнение special vdev, RAM/ARC под нагрузкой** | 🔴 |
| Бэкап | — (Часть 2) | **ZFS snapshots + `zfs send`** офсайт | 🟢 |

---

## Что исчезает и что добавляется при переходе A → B

**Удаляется (наш код):**
- `PlacementPolicy` (HRW), агрегат `Pool`/`Shard`, write-quorum — вся Фаза 2.
- `ResilverService` (walk-based) — вся Фаза 3.
- domain-aware размещение, capacity-балансировка, app-level two-tier логика.

**Добавляется (ops + тюнинг, не Rust):**
- Провижининг и сопровождение ZFS-пула (vdev-раскладка, special vdev, ARC, scrub-cron).
- Per-dataset `recordsize`-тюнинг и контроль фрагментации redb-on-ZFS.
- Мониторинг ZFS (exporter), runbook замены диска, дисциплина snapshots/бэкапов.
- Требование к железу: 128–256 ГБ+ RAM, зеркальный NVMe под special vdev.

**Остаётся общим:** `ozd-domain` (Cid/Block/Pin), `ozd-app` (StoreBlock/FetchBlock/GC),
`ozd-ipfs`, `ozd-admin`-каркас, `ozd-daemon` — Фазы 0(частично), 1(частично), 4, 5(GC), 6.

---

## Точка невозврата

Переключение **дёшево до конца Фазы 1** (общий домен и blockstore-порт). После Фазы 2–3
(когда написаны HRW и resilver) переход на B означает **выбросить этот код**. Поэтому решение
A vs B стоит подтвердить **до старта Фазы 2** — Фазы 0–1 совместимы с обоими вариантами.
