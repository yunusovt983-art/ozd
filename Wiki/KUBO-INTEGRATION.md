# Интеграция: Kubo ⇄ ozd ⇄ 60 HDD (ZFS)

```
IPFS Kubo (go-ds-s3 plugin) ──S3──> ozd (этот демон) ──HRW R=2──> 60 × ZFS-датасет HDD
                                       │                              (pack-сегменты)
                                       └── redb-индексы (NVMe)
```

Демон решает обе проблемы из постановки (IPFS 03.06): **Часть 1 sharding**
(`key → HRW → top-R дисков`) и **Часть 2 packing** (мелкие random write →
append в 2ГБ pack-сегменты, sequential для HDD). Kubo не патчится — он видит
обычный S3-datastore.

## 1. Подготовка дисков (ZFS, ADR-0001-аддендум)

Один диск = **один zpool** (JBOD-философия: без RAIDZ/mirror на уровне ZFS —
durability даёт репликация демона R=2; RAID-слой лишь удвоил бы потерю ёмкости):

```sh
for i in $(seq -w 1 60); do
  zpool create -o ashift=12 disk$i /dev/disk-by-id-...   # свой id диска
  zfs set recordsize=1M atime=off compression=lz4 xattr=sa \
      logbias=throughput primarycache=metadata disk$i
  zfs create disk$i/ozd
done
```

Пояснения:
- `recordsize=1M` — крупные записи pack-сегментов sequential, меньше IOPS;
- `compression=lz4` — почти бесплатно (тела блоков IPFS часто несжимаемы — lz4
  early-abort это учует); можно `off`;
- `primarycache=metadata` — ARC не дублирует page-cache телами write-once
  блоков (аналог нашего DONTNEED #63);
- `atime=off`, `xattr=sa`, `logbias=throughput` — стандартная гигиена.
- Индексы redb — на **NVMe** (отдельный pool/датасет), `index_path` в конфиге.

## 2. Запуск демона

```sh
cp ozd.example.toml ozd.toml   # вписать 60 disks[]
cargo build --release
./target/release/ozd --config ozd.toml
# слушает 127.0.0.1:9100 (S3-subset + /healthz + /admin/usage)
```

## 3. Конфигурация Kubo (go-ds-s3)

Собрать Kubo с плагином s3ds (или взять сборку с ним), затем `~/.ipfs/config`:

```json
"Datastore": {
  "Spec": {
    "type": "mount",
    "mounts": [
      {
        "mountpoint": "/blocks",
        "prefix": "s3.datastore",
        "type": "measure",
        "child": {
          "type": "s3ds",
          "region": "us-east-1",
          "bucket": "kubo",
          "rootDirectory": "",
          "regionEndpoint": "http://127.0.0.1:9100",
          "accessKey": "ozd",
          "secretKey": "ozd"
        }
      },
      {
        "mountpoint": "/",
        "prefix": "leveldb.datastore",
        "type": "measure",
        "child": { "type": "levelds", "path": "datastore", "compression": "none" }
      }
    ]
  }
}
```

- В `/blocks` (тела блоков — 99.9% объёма) идёт в ozd; метаданные/пины Kubo —
  локальный leveldb.
- `accessKey/secretKey` — те же, что в `[auth]`-секции ozd.toml: демон **проверяет
  SigV4** (E13): подпись, фактический SHA-256 тела, допуск часов ±15 мин. Без
  `[auth]`-секции — dev-режим без проверки (только loopback!).
- После правки `Datastore.Spec` нужно синхронизировать `datastore_spec`.

## 4. Что уже работает / что дальше (по PLAN.md)

| Готово (v0.1) | Дальше |
|---|---|
| pack-сегменты + CRC записи + recovery torn-tail (#99/#111) | микроблоки 16КБ + zstd (#15) |
| redb-индекс: addr/inline-split (#80), inline-мелочь (#44) | LazyIndex (#112), WAL-режимы (#59) |
| HRW top-R по free + гистерезис 0.95 (#2/#130) | full compare-cascade, ballast (#127) |
| Pool R=2/W=2, чтение с живой реплики, fallback-скан | speculative retry (#121), handoff (#41) |
| TTL-кэш free-space (#137), discard-счётчики (#122 задел) | GC сегментов (Pebble/#122), scrub (#102/#141) |
| S3-subset для go-ds-s3 + ListObjectsV2 | walk-resilver (Фаза 3), heal-queue (#140) |
| graceful shutdown с flush (recovery-point) | disk-health FSM (#142), Forseti (#47) |
