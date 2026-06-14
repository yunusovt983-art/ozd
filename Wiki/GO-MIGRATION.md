# Go (Vendor/OpenZFS-main) → Rust (ozd): что переносить, что нет

> Проиндексирован прежний Go-слой `kubo-zfs-integration` (91 файл; Вариант-1-архитектура:
> контейнерный Kubo на диск + object-packing + ZFS-шифрование с Vault + Docker-оркестрация).
> Сравнение с Rust-демоном ozd v0.1 и план переноса. Дата: 2026-06-10.

## 1. Packing-слой: НЕ переносить формат (наш сильнее), взять 2 фичи

Сравнение `internal/objectstore` ↔ `ozd-engine`:

| Аспект | Go objectstore | Rust ozd-engine | Вердикт |
|---|---|---|---|
| Формат pack | **без заголовков записей** (offset/size только во внешнем индексе) | record-header MAGIC+len+**CRC32** (self-describing → scan/recovery возможны) | Rust ✓ |
| Размер пака | 64МБ | 2ГБ (меньше файлов на 3,8 млрд блоков) | Rust ✓ |
| Durability | **fsync на КАЖДЫЙ Append + pebble.Sync на каждый Put** (медленно на HDD) | fsync раз в `fsync_items` — durability via replication (#111) | Rust ✓ |
| Индекс-строка | Pebble: string-key → **JSON** (`{key,pack_id,offset,size,hash-hex,stored_at}`) — разбухает | redb: бинарные **22 байта** `(seg,off,len,klen,crc)` + inline-split (#80) | Rust ✓ |
| Целостность | SHA-256 **всего объекта на каждый Get** (CPU-дорого) | CRC32 записи verify-on-read (микро-чек дешевле; CID-проверка опц.) | Rust ✓ |
| Overwrite | `Put` → ошибка "key already exists" (адаптер делает Delete+Put) | идемпотентный overwrite + discard-учёт | Rust ✓ |
| Компакция | вся жертва **в RAM** списком, через двойной Get/hash | стриминг `scan_segment` O(записи) + discard-ratio (#122) | Rust ✓ |
| Recovery | DetectMissingPacks (индекс↔файлы) + **fetch пака у peer по HTTP + verify** | torn-tail по CRC + resilver по репликам | **взять у Go** ↓ |
| CAR | **export/import CAR v1** (go-car) | нет | **взять у Go** ↓ |

**Переносим из Go:**
1. **Структурный health-check** (`recovery.go DetectMissingPacks`): дёшевая сверка «каждый
   `seg_id` из addr-таблицы существует как файл» (без чтения тел!) — быстрый старт-чек и
   admin-эндпоинт. → `ozd-engine::verify_structure()` + `GET /admin/health/structure`.
2. **CAR import/export** (`car.go`): мост к стандартному IPFS-тулингу. Импорт CAR →
   прямо через bulk-StreamWriter (#123) в финальные сегменты; экспорт сегментов → CAR для
   обмена/бэкапа. → Часть 2, крейт `ozd-ipfs` (есть Rust-крейты `iroh-car`/`rust-car`).
3. (Часть 3) **Отдача целого сегмента peer'у** (`/api/v1/packs/{id}` + verify по индексу) —
   segment-level resilver быстрее поключевого для полного rebuild диска.

**Уроки-антипаттерны Go-кода** (уже учтены в ozd, не повторять): fsync-на-каждую-запись;
JSON+hex-хэш в индекс-строке; компакция в RAM; не-идемпотентный Put; `Query` без стриминга.

## 2. zfspool: ПЕРЕНОСИТЬ В ПЕРВУЮ ОЧЕРЕДЬ (самое ценное)

`internal/zfspool/pool.go` (1128 строк) — готовый, обкатанный ops-слой для **наших же 60
ZFS-пулов**, которого в ozd нет совсем:

- **`zpool status -p` парсер** → `State` (ONLINE/DEGRADED/FAULTED/...) + per-device
  read/write/**checksum** errors + ScrubInfo. Это идеальный вход для **disk-health FSM
  (#142)**: checksum-errors растут → `Suspect`; FAULTED → `Faulted` + resilver.
- **`zfs get used,available,...`** → точная Capacity пула (statvfs на ZFS может врать при
  квотах/резервах) → веса HRW честнее.
- **Scrub-управление** (`zpool scrub` / `-s` / `-p` + прогресс) → наш `ScrubService` на
  ZFS-деплое **делегирует** проверку контрольных сумм нижнему ярусу (у ZFS свой checksum),
  оставляя себе только сверку индекс↔сегмент (структурный чек) — огромная экономия.
- Свойства пула при создании (lz4/sha256/recordsize=1M/atime=off) — совпадают с нашим
  KUBO-INTEGRATION.md (валидация догадок боевым конфигом!).

→ Новый крейт **`ozd-zfs`** (адаптер за портом, как PolarVFS #17): `PoolHealth`,
`pool_capacity()`, `scrub_{start,stop,status}()`, парсеры `zpool status -p`/`zfs get -Hp`.
Подключение: `ShardStatus` из health, `usage()` через `zfs get`, admin `/admin/zfs`.

## 3. Vault + ZFS-шифрование: НЕ переносить — переиспользовать Go-бинарь

`cmd/kubo-zfs-keyloader` — самодостаточный systemd-бинарь (boot: Vault AppRole → список
ключей → `zfs load-key` через stdin). Он **не на горячем пути** и не зависит от остального
Go-кода → **оставить как есть** рядом с ozd. Перенос на Rust — только если появится
требование (Часть 2+). Ценные паттерны в каталог: key-cache TTL 5мин + **fallback на
просроченный кэш при недоступном Vault** (доступность важнее свежести ключа), ротация с
версионированием + audit-лог.

## 4. discovery: перенести как admin-CLI (средний приоритет)

`lsblk -J -b -d` (+ обогащение `smartctl -i -j`; фильтры loop/ram/sr/dm-; system-disk по
mountpoint; IsUsedByZFS по `zpool status`) → `ozd disks discover` — авто-инвентаризация
60 дисков при вводе в строй; SMART-данные → suspect-приоритизация scrub (#102).

## 5. НЕ переносить совсем

- **Docker-оркестратор Kubo-на-диск** (`orchestrator/`) — это Вариант 1, отвергнут самой
  постановкой (ozd = один Kubo поверх пула). Утечка памяти Kubo ×60 контейнеров — то, от
  чего ушли.
- **YDB statestore** — противоречит no-central-catalog (#139: self-describing meta + кворум).
- **go-datastore адаптер** — у нас S3-протокол (go-ds-s3), слой не нужен.

Полезная семантика, которую сверили: Delete несуществующего ключа = OK (у нас так),
двухступенчатый health container→API (healthy/degraded/unhealthy) = конвергенция #142.

## 6. Порядок работ

| # | Что | Куда | Приоритет |
|---|---|---|---|
| 1 | `ozd-zfs`: zpool status парсер → health/Capacity/scrub | новый крейт + admin | **P1 ✅ (2026-06-10)** |
| 2 | Структурный health-check (addr ↔ seg-файлы) | ozd-engine + admin | **P1 ✅ (2026-06-10)** |
| 3 | Метрики Prometheus (список из Go-monitor как чек-лист) | ozd-admin | P2 ✅ базовый `/metrics` (2026-06-10); расширение счётчиками — TODO |
| 4 | CAR import (через bulk #123) / export | ozd-ipfs | P2 (Часть 2) — отложено |
| 5 | `ozd disks discover` (lsblk+smartctl) | ozd-admin CLI | P2 |
| 6 | Keyloader — переиспользовать Go-бинарь без изменений | deployments | P3 |
| 7 | Segment-level peer-fetch для resilver | transfer | P3 (Часть 3) |
