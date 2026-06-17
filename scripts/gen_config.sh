#!/usr/bin/env bash
# W10.1: Генератор ozd.toml из списка ZFS-пулов.
#
# Использование:
#   ./scripts/gen_config.sh > /etc/ozd/ozd.toml
#   ./scripts/gen_config.sh --listen 0.0.0.0:9100 --redundancy erasure
#
# Находит все ZFS-пулы с именем disk* (или заданным паттерном),
# генерирует секции [[disks]] для каждого.

set -euo pipefail

# Параметры (переопределяются env или аргументами)
LISTEN="${OZD_LISTEN:-127.0.0.1:9100}"
REPLICAS="${OZD_REPLICAS:-2}"
WRITE_QUORUM="${OZD_WRITE_QUORUM:-2}"
REDUNDANCY="${OZD_REDUNDANCY:-mirror}"
POOL_PATTERN="${OZD_POOL_PATTERN:-disk}"
INDEX_PATH="${OZD_INDEX_PATH:-}"  # пусто = на том же диске
AUTH_KEY="${OZD_AUTH_KEY:-minioadmin}"
AUTH_SECRET="${OZD_AUTH_SECRET:-minioadmin}"

# Аргументы командной строки
while [[ $# -gt 0 ]]; do
  case "$1" in
    --listen) LISTEN="$2"; shift 2;;
    --replicas) REPLICAS="$2"; shift 2;;
    --write-quorum) WRITE_QUORUM="$2"; shift 2;;
    --redundancy) REDUNDANCY="$2"; shift 2;;
    --pattern) POOL_PATTERN="$2"; shift 2;;
    --index-path) INDEX_PATH="$2"; shift 2;;
    --auth-key) AUTH_KEY="$2"; shift 2;;
    --auth-secret) AUTH_SECRET="$2"; shift 2;;
    *) echo "неизвестный аргумент: $1" >&2; exit 1;;
  esac
done

# Найти пулы
POOLS=$(zpool list -H -o name 2>/dev/null | grep "^${POOL_PATTERN}" || true)

if [ -z "$POOLS" ]; then
  echo "# ОШИБКА: не найдено ZFS-пулов с паттерном '${POOL_PATTERN}*'" >&2
  echo "# Пример: zpool create disk01 /dev/sda" >&2
  exit 1
fi

POOL_COUNT=$(echo "$POOLS" | wc -l | tr -d ' ')

cat << EOF
# ozd.toml — сгенерирован $(date -Iseconds)
# Пулы: ${POOL_COUNT} дисков (паттерн: ${POOL_PATTERN}*)

listen = "${LISTEN}"
replicas = ${REPLICAS}
write_quorum = ${WRITE_QUORUM}
redundancy = "${REDUNDANCY}"

gc_interval_secs = 300
gc_discard_ratio = 0.5
scrub_interval_secs = 600
scrub_keys_per_cycle = 5000
zfs_health_interval_secs = 30
heal_mrf = true
adaptive_hedge = true
speculative_retry_ms = 100

[auth]
access_key = "${AUTH_KEY}"
secret_key = "${AUTH_SECRET}"

[engine]
segment_max_size = 2147483648  # 2GB
inline_min = 4096
fsync_items = 256
compress = "zstd"
ballast_bytes = 1073741824  # 1GB
fadvise_dontneed = true

EOF

# Генерация [[disks]] секций
INDEX=0
for POOL in $POOLS; do
  DATASET="${POOL}/ozd"
  MOUNTPOINT=$(zfs get -H -o value mountpoint "${DATASET}" 2>/dev/null || echo "/${POOL}/ozd")

  echo "[[disks]]"
  echo "data_path = \"${MOUNTPOINT}\""
  if [ -n "$INDEX_PATH" ]; then
    echo "index_path = \"${INDEX_PATH}/${POOL}\""
  fi
  echo "zfs_pool = \"${POOL}\""
  echo "zfs_dataset = \"${DATASET}\""
  echo ""

  INDEX=$((INDEX + 1))
done

echo "# Итого: ${INDEX} дисков" >&2
