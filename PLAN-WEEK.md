# PLAN — Завтра (W32.1) и неделя W32

**Создан:** 2026-06-20 · **Старт:** 2026-06-21
**Контекст:** стек Kubo→go-ds-s3→ozd S3 поднят и проверен e2e под Docker/colima
(commit `4621d87`). NEXT-STEPS §1 (верификация W9 Phase 2) фактически закрыт.
**Тема недели:** локальное упрочнение стека Kubo↔ozd без железа + готовность к E30/E31.
**Источники истины:** [Wiki/ROADMAP.md](Wiki/ROADMAP.md) · [Wiki/WEEKLY-ARCS.md](Wiki/WEEKLY-ARCS.md) · [NEXT-STEPS.md](NEXT-STEPS.md)

> Гейт: E30/E31/E32 требуют полки (60 HDD). Всё ниже — то, что реально можно
> сделать на dev-машине, чтобы к приезду железа деплой был «включил и поехал».

---

## ЗАВТРА — W32.1: добить W9 Phase 2 и упрочнить e2e-roundtrip

Цель дня: превратить «однажды прошло add/cat» в **воспроизводимый, покрывающий
pin/GC/multi-block** прогон, и закрыть документацию.

Стек уже запущен (`docker compose ... ps` → ozd healthy, kubo ready). Если нет:
`docker compose -f deployments/docker/docker-compose.yml up -d`.

- [ ] **T1. Smoke зелёный против auth-стека.** `scripts/kubo_smoke.sh` сейчас бьёт
      ozd S3 **без SigV4** (обычный curl) → против боевого compose (auth ВКЛ) даст 403.
      Решение: добавить в скрипт SigV4-подпись (переиспользовать рабочий
      `curl --aws-sigv4 "aws:amz:us-east-1:s3" -u minioadmin:minioadmin`).
      *Приёмка:* `bash scripts/kubo_smoke.sh http://localhost:9100` — все шаги OK
      против compose с auth.
- [ ] **T2. Multi-block roundtrip.** Залить файл >1 МБ (UnixFS порежет на много
      блоков + DAG): `head -c 5000000 /dev/urandom > /tmp/big.bin`,
      `ipfs add -q`, `ipfs cat <cid> | sha256sum` — сверить с оригиналом.
      *Приёмка:* бит-в-бит совпадение; `ipfs repo stat` NumObjects заметно вырос;
      на дисках ozd видно несколько pack-сегментов.
- [ ] **T3. Pin + GC через s3ds.** Запинить CID, добавить «мусорный» блок,
      `ipfs repo gc` → убедиться: запиненное живо (`ipfs cat` ОК), незапиненное
      реально удалено из ozd (DeleteObject дошёл, сегмент-GC отработал).
      *Приёмка:* pinned survives gc; unpinned исчез из ozd.
- [ ] **T4. Документация + закрытие.** Обновить
      [deployments/docker/README-W9.md](deployments/docker/README-W9.md): проверенные
      команды + 3 пофикшенных runtime-бага (glibc / datastore_spec / regionEndpoint,
      уже в git). Во [NEXT-STEPS.md](NEXT-STEPS.md) пометить §1 как ✅ done.
      *Приёмка:* commit; NEXT-STEPS §1 закрыт.

**Definition of done (день):** воспроизводимый add/cat/pin/gc + multi-block через
ozd S3 с auth; smoke зелёный; README-W9 и NEXT-STEPS актуальны.

---

## НЕДЕЛЯ W32 — день за днём

### W32.2 — Автоматизация e2e + CI
- [ ] `scripts/e2e_kubo_ozd.sh`: up → ждать healthz ozd → add/cat/pin/gc/smoke
      ассерты → teardown; **ненулевой exit при любом провале**.
- [ ] Завести job в `.github/workflows/ci.yml` (guarded: пропуск, если нет docker;
      как минимум `docker compose config` + build образов). 
- *Приёмка:* скрипт самодостаточен и зелёный локально; CI-job добавлен.

### W32.3 — Долговечный локальный прогон (убрать tmpfs) + crash-recovery
> Сейчас 3 диска ozd на **tmpfs** → данные исчезают при рестарте контейнера.
- [ ] Compose-override с bind-mount каталогов вместо tmpfs (persistent).
- [ ] Рестарт ozd → переоткрытие redb + recovery pack-сегментов → `ipfs cat` всё ещё ОК.
- [ ] Chaos-lite: убить ozd в середине записи (`docker kill`) → рестарт →
      recovery-point держит, нет порчи (CRC/redb консистентны).
- *Приёмка:* данные переживают рестарт ozd; путь восстановления реально пройден; задокументировано.

### W32.4 — Прочность S3-шлюза и покрытие тестами
- [ ] Починить косметический `curl --aws-sigv4` ListObjects (GET + query-строка) —
      мой диагностический запрос давал `SignatureDoesNotMatch` (канонизация query);
      сам Kubo листит корректно.
- [ ] Интеграционный тест ozd-ipfs на пути aws-sdk-go: реальный signed-payload и
      UNSIGNED-PAYLOAD (регресс-гард на логику `to_bytes`/`verify`).
- [ ] Гард/лог на `endpoint` vs `regionEndpoint` в kubo-init (или явная проверка),
      чтобы баг №3 больше не вернулся незаметно.
- *Приёмка:* list работает; новые тесты зелёные; гард на regionEndpoint есть.

### W32.5 — Подготовка ZFS-интеграции (без полки) + готовность к E30/E31
> `ozd-zfs` — это парсер (`zpool status`→`PoolHealth`/FSM, `zfs get`→`Properties`/
> drift) с абстракцией `Runner` (`Local`/`Sudo`/`Fake`). Реальный zpool нужен
> ZFS-хост; но проводку и фикстуры можно сделать сейчас.
- [ ] Пробросить `ozd-zfs` `PoolHealth`/FSM в `healthz`/admin демона
      (поверхность статуса пула), питая её **захваченными реальными** выводами
      `zpool status`/`zfs get` через `FakeRunner`.
- [ ] Drift-audit ожидаемых `ozd:*` user-props (идея #149).
- [ ] *Stretch:* файловый zpool в привилегированной ZFS-capable Linux-VM
      (проверить выполнимость на colima/отдельной VM, задокументировать).
- [ ] **Runbook E30/E31** в Wiki: `gen_config.sh` на 60 дисков → systemd →
      `zpool create`/tuning → Grafana-дашборд — чтобы деплой был turnkey.
- *Приёмка:* healthz показывает здоровье пула из фикстур; черновик runbook готов.

---

## Бэклог недели (по времени/желанию)
- `ozd-bench` dry-run на локальном ozd: снять baseline p99 add/cat (не полка, но цифра).
- Перепроверить `smoke-local.toml` (no-auth профиль) — нужен ли ещё после SigV4 в smoke.
- Привести `docs/`-симлинки/индексы в порядок (верхний `docs/` пуст, всё в `Wiki/`).

## Риски / заметки
- **tmpfs ⇒ эфемерно**: «персистентность» сегодня — только в пределах жизни процесса
  ozd. Реальная durability проверяется в W32.3 (bind-mount).
- **ZFS на macOS нет**: реальный zpool — только в Linux-VM с zfs-модулем или на полке.
  Поэтому W32.5 опирается на фикстуры + FakeRunner, а не на живой пул.
- **colima монтирует только `/Volumes/Kingston`** — все bind-mount-источники держать
  под этим путём (см. memory `ozd-docker-build-env`).

## Что НЕ трогаем (ждёт железа — Арка 8)
E30 (реальный Kubo-трафик, hit-rate СуперДиска) · E31 (деплой на полку) ·
E32 (нагрузка/хаос на полке) · Арка 9 (мультиузел/P2P) — заморожены до полки.

*Обновлять по ходу W32. История решений — memory/ozd-implementation, ozd-next-steps.*
