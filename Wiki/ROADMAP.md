# ozd — Роадмап: Арки и Эпики

> Дисциплина: один эпик = один заход (дизайн → код → тесты → e2e → ✅ в PLAN → память).
> Источники требований: docs/PLAN.md (150 идей), docs/GO-MIGRATION.md, постановка «IPFS 03.06».
> Статусы: ✅ готово · 🔧 в работе · 🔜 следующий · ⬜ запланирован · 🧊 заморожен (Часть 2/3).

## Арка 1 — Вертикальный срез (sharding + packing) ✅

| Эпик | Содержание | Статус |
|---|---|---|
| E1 Каркас | workspace 6 крейтов, домен без IO, порты | ✅ |
| E2 Engine | pack-сегменты + CRC + torn-tail recovery; redb addr/inline; discard-счётчики | ✅ |
| E3 Pool | HRW-by-free + гистерезис 0.95, R=2/W=2, TTL-кэш ёмкости | ✅ |
| E4 S3-шлюз | Put/Get/Head/Delete/ListV2 для Kubo go-ds-s3; graceful shutdown | ✅ |

## Арка 2 — Самовосстановление ✅

| Эпик | Содержание | Статус |
|---|---|---|
| E5 GC | #122 discard-ratio victim, CAS-перенос живых, flush→unlink; фон+admin | ✅ |
| E6 Resilver | walk add-only до R, курсор, идемпотентность; admin | ✅ |
| E7 Scrub | #102/#141 deep-CRC партиями + self-heal с реплик; джиттер; zpool-делегир. | ✅ |
| E8 ZFS+FSM | ozd-zfs (#146–150: runner/sentinel/Source-drift/identity/freeing) + HealthFsm #142 | ✅ |
| E9 Write-path v2 | параллельный put (max≠сумма) + handoff #41 + MRF #140-lite | ✅ |

## Арка 3 — Формат и эффективность данных 🔧

| Эпик | Содержание | Критерий приёмки | Статус |
|---|---|---|---|
| **E10 Сжатие тел (zstd)** | формат записи v2 (header 20Б: +logical_len, flags.bit0=zstd); `compress: none\|zstd`; CRC по stored-байтам; GC переносит без перепаковки; CID/ключи не сжимаются | roundtrip сжатого==оригинал; несжимаемое хранится как есть; GC/scrub/recovery работают по сжатым | ✅ 2026-06-10 (e2e: 519КБ→4КБ/реплика) |
| **E11 HEAD из индекса** | `stat()` в портах: logical_len из addr-строки/inline — HEAD без чтения тела (go-ds-s3 зовёт GetSize часто!) | HEAD отдаёт верный Content-Length без disk-read тела | ✅ 2026-06-10 (HEAD/ListV2 = логический размер) |
| E12 GC-полировка | батч discard-bump (1 txn/проход), очистка orphan-сегментов из verify_structure | нет txn-на-запись в GC; orphan удаляется фоном | ✅ 2026-06-10 (bump в txn put/delete; sweep_orphans в каждом gc_once) |

## Арка 4 — Продакшен-шлюз ⬜

| Эпик | Содержание | Статус |
|---|---|---|
| E13 SigV4 | проверка подписи AWS SigV4 (ключи в конфиге) | ✅ 2026-06-10 (middleware: payload-hash чек + skew ±15м; healthz открыт; Kubo-совместимость подтвердить на E15) |
| E14 Метрики v2 | счётчики put/get/err/латентности, gc/scrub/resilver totals | ✅ 2026-06-10 (OpsMetrics: 22 счётчика + seconds_sum; handoff/hedged/MRF/scrub/GC) |
| E15 Kubo-стенд | смоук с реальным Kubo+go-ds-s3 по KUBO-INTEGRATION (на сервере юзера) | ⬜ |

## Арка 5 — Операции на масштабе ⬜

| Эпик | Содержание | Статус |
|---|---|---|
| E16 Heal-queue полный | #140: приоритеты/bulkhead/типы поверх MRF | ✅ 2026-06-10 (BinaryHeap+dedup-upgrade; параллельный дренаж + per-shard bulkhead; scrub-unrepairable→Urgent) |
| E17 Persist-чекпойнты | курсоры scrub/resilver в redb-meta (рестарт с места, #102) | ✅ 2026-06-10 (T_CURSOR в redb; resilver_full resume+clear; scrub-луп демона персистит после каждого шага) |
| E18 Ballast+WAL-failover | #127 ballast-файл; #128 запасной путь WAL | ✅ 2026-06-10 (балласт несжимаемый + авто-сброс на ENOSPC + /admin/ballast/release + гейдж; failover-ротация сегментов на запасной путь, чтения из обоих, автоfailback) |
| E19 Throttle фона | elastic-токены #131 / простой rate-limit на GC/scrub/resilver | ✅ 2026-06-10 (BgThrottle: leaky-bucket байт/с + AIMD по fg-нагрузке; платят repair_key/scrub/GC; гейдж ozd_bg_rate_bps) |

## Арка 6 — Часть 2 🔧

| Эпик | Содержание | Статус |
|---|---|---|
| E20 Erasure-set | #138: K+M кусков (Reed-Solomon), distribution-array = HRW-ранг, самоописанные куски, эры сосуществуют | ✅ 2026-06-10 (4+2=1.5×; degraded-read/resilver-реконструкция/scrub-heal куска; e2e: чтение при 2 выбитых) |
| E21 Миграция mirror→erasure | #145: фоновый мигратор + canary read-back + persist-курсор; dual-write не нужен (эры сосуществуют по E20) | ✅ 2026-06-10 (canary-откат при сбое — зеркало цело; admin POST /admin/migrate; daemon migrate_interval_secs; e2e: зеркало→куски→чтение при 2 выбитых) |
| E21b Era-бит в индексе | put_meta/stat_obj + addr v3 (+obj_logical u64); конверт куска в ozd-domain::piece; recovery восстанавливает era-бит парсом конверта; GC переносит | ✅ 2026-06-10 (HEAD/ListV2 на EC = логический размер БЕЗ чтения тел — закрыты ограничения E20) |
| E22 CAR import/export | bulk-залив #123 (StreamWriter-дух): CARv1-парсер без deps, Kubo-ключи CIQ…, verify sha2, воркеры+backpressure; экспорт CIDv1+raw | ✅ 2026-06-10 (admin /admin/car/{import,export}; e2e: 3 блока → EC → GET по Kubo-ключу → roundtrip бит-в-бит; CARv2 отклоняется) |
| E23 BLAKE3 outboard | #79 verified streaming: abao (16КБ chunk-группы, ~0.4%), outboard отдельным ключом /ozd/ob3*, Range GET с верификацией против write-time root | ✅ 2026-06-10 (206+x-ozd-verified:blake3; бит-флип в диапазоне → 500, вне группы — читается; e2e на EC-пуле) |
| E24 Микроблоки 16КБ | #15 — если профиль покажет пользу частичного чтения | 🧊 ВЕРДИКТ E29 (данными): Kubo читает блоки только целиком — Range-трафика нет; пересмотр при появлении S3-Range в профиле стенда (docs/BENCH.md) |
| E25 СуперДиск | #143 Discord NVMe read-leg (CacheTier = свой DiskEngine на NVMe, write-through, FIFO-эвикция сегментами #92/#110, self-heal с пула) + coalescing #144 (single-flight) | ✅ 2026-06-11 (5+1 тестов: хиты мимо HDD, bitrot-self-heal, бюджет с гистерезисом, 8 GET=1 чтение; e2e: hits=3/miss=0 на EC-пуле) |

Полировка 2026-06-10 ✅: суффикс-Range `bytes=-n`; bao-слайс наружу (x-ozd-bao: 1 → application/vnd.ozd.bao-slice + x-ozd-bao-root, клиент верифицирует verify_bao_slice — P2P-фундамент Ч3); era-бэкфилл легаси-кускам на migrate-проходе (set_obj_logical правит ТОЛЬКО индекс-строку, тела не перезаписываются; метрика ozd_migrate_era_backfilled_total).

Оставшееся ограничение: старые зеркальные тела после миграции — мёртвые байты в сегментах до GC-прохода (discard уже учтён).

## Арка 7 — СуперДиск-доводка: путь чтения и IO-гигиена ✅

> Цель: довести p99 чтения до «дискордовских» цифр на нагрузке. Все эпики измеримы
> метриками /metrics; железо не требуется (dev-машина ок).

| Эпик | Содержание | Критерий приёмки | Статус |
|---|---|---|---|
| E26 Page-cache гигиена | #63 (Redis DONTNEED): `posix_fadvise(DONTNEED)` на сегмент после flush/GC-переноса/CAR-импорта — write-once байты не вымывают горячие чтения из RAM; #64 (неблокирующий writeback) — Linux-only, cfg-gated, на macOS no-op | записи не растят page-cache RSS на нагрузочном смоуке; конфиг `fadvise_dontneed=true`; нет регрессии тестов | ✅ 2026-06-11 (3 точки: инкрементально при flush минус 8МБ горячего хвоста / sealed целиком при ротации+failover / после холодных GC-eviction-сканов; NVMe-кэш не трогаем; daemon-дефолт true; RSS-замер и #64 sync_file_range → E32 на железе) |
| E27 p99-адаптивный hedge | порог hedged-read из СКОЛЬЗЯЩЕГО p99 get-латентности (кольцевая гистограмма в OpsMetrics), clamp [10мс..2с]; статический `speculative_retry_ms` остаётся как override | hedge-rate падает на ровной нагрузке и растёт при тормозящей ноге (тест с SlowShard); гейдж `ozd_hedge_threshold_ms` | ✅ 2026-06-11 (RollingP99: 22 log2-бакета × 2 эпохи, lock-free, прогрев 64 сэмпла → статика-fallback; пол 10мс ловит 300мс-ногу, буря 300мс поднимает порог — лишних дублей нет) |
| E28 Disk-slow монитор | #129 (CRDB): EWMA-латентность put/get ПО ШАРДУ → второй вход HealthFsm помимо ZFS (диск «жив, но умирает» ловится ДО zpool-ошибок); Linux: /proc/diskstats опционально | искусственно медленный шард → Suspect → HRW его обходит; гейдж per-shard latency | ✅ 2026-06-11 (MeteredShard-декоратор кормит EWMA α=1/8; вердикт = выброс vs медиана ПАРКА + абс-пол 250мс; FSM-гистерезис у демона; Suspect-вес ×0.01 в HRW — read-leg/записи уходят, чтение/ремонт остаются; /proc/diskstats → E32 на железе) |
| E29 Бенч-харнесс | нагрузочный профиль (put/get mix, hot-set, размеры тел из Kubo-статистики) + отчёт: p50/p99/p999, hit-rate СуперДиска, IOPS на ногу; решение по E24 (микроблоки) данными | воспроизводимый `cargo run -p ozd-bench` отчёт; вердикт по E24 в ROADMAP | ✅ 2026-06-11 (crates/ozd-bench: Kubo-микс 75/15/10, hot-set, честные перцентили, 3 профиля → docs/BENCH.md; EC-GET ×5.7 медленнее зеркала БЕЗ кэша и неотличим С кэшем) |

## Арка 8 — Реальное железо: стенд и продакшен ⬜ (нужен сервер)

| Эпик | Содержание | Критерий приёмки | Статус |
|---|---|---|---|
| E30 Kubo-стенд (= E15) | реальный Kubo+go-ds-s3 → ozd по KUBO-INTEGRATION: SigV4-канонизация, ipfs add/cat/pin/gc, первый реальный hit-rate СуперДиска | `ipfs add` файла → `ipfs cat` бит-в-бит; блоки видны в /metrics; sigv4 0 отказов | ⬜ |
| E31 Деплой на полку | генератор конфига на 60 дисков (скрипт: zpool list → [[disks]]), systemd-unit, runbook (zpool create/tuning из ozd.example.toml шапки), Grafana-дашборд на наши метрики | демон стартует на полке с identity-чеком #149; дашборд: capacity/hit-rate/heal/латентности | ⬜ |
| E32 Нагрузка на полке | профиль реального трафика → тюнинг (ec_min_size, cache max_bytes, bg-бюджеты, scrub-каденс #141 deep/normal); хаос-смоук: выдернуть диск под нагрузкой → resilver при живом трафике | p99 чтения и время ребилда зафиксированы в docs/BENCH.md; throttle держит foreground | ⬜ |

## Арка 9 — Часть 3: несколько узлов / P2P 🧊 (после Арки 8)

| Эпик | Содержание | Статус |
|---|---|---|
| E33 Merkle anti-entropy | #119 (Cassandra): сверка реплик/кусков хэш-деревом по диапазонам ключей → стримить только diff (замена полного resilver-walk для меж-узловой сверки) | 🧊 |
| E34 Tombstone + gc_grace | #120: distributed delete без воскрешения при нескольких писателях/шлюзах | 🧊 |
| E35 Fencing + мульти-шлюз | #94 (atomic-create fencing) + #118 (zero-copy refcount): два ozd-узла над общей полкой/двумя полками без затирания | 🧊 |
| E36 P2P verified fetch | межузловая отдача bao-слайсов (фундамент готов: x-ozd-bao + verify_bao_slice) + zero-copy sendfile #110 на отдаче сегментов | 🧊 |

Порядок по умолчанию: Арка 7 целиком (E26→E29, без железа) → Арка 8 как только есть сервер (E30 первым) → Арка 9 после стабилизации одной ноды.

---
*Обновлять статусы при закрытии эпика. История решений — memory/ozd-implementation.*
