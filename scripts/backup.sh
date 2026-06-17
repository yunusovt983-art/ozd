#!/usr/bin/env bash
# W23: Backup script — snapshot + tar + upload (rsync/S3).
# Использование:
#   ./scripts/backup.sh http://localhost:9100 /backups/ozd
#
# 1. POST /admin/snapshot → hardlinks запечатанных сегментов
# 2. tar.zst каждого шарда
# 3. upload (rsync/S3) — оператор настраивает BACKUP_DEST
#
# Требования: curl, jq, tar, zstd (или pigz).

set -euo pipefail

OZD_URL="${1:-http://localhost:9100}"
BACKUP_DIR="${2:-/var/backups/ozd}"
BACKUP_DEST="${BACKUP_DEST:-}"  # rsync://host/path или s3://bucket/prefix

echo "=== ozd backup: creating snapshot ==="
RESP=$(curl -sf -X POST "${OZD_URL}/admin/snapshot")
SNAP_ID=$(echo "$RESP" | jq -r '.id')
SEGMENTS=$(echo "$RESP" | jq -r '.segments')
BYTES=$(echo "$RESP" | jq -r '.bytes')

echo "  snapshot: ${SNAP_ID}"
echo "  segments: ${SEGMENTS}, bytes: ${BYTES}"

if [ "$SEGMENTS" -eq 0 ]; then
    echo "  (no sealed segments — nothing to backup)"
    exit 0
fi

mkdir -p "${BACKUP_DIR}"
ARCHIVE="${BACKUP_DIR}/ozd-${SNAP_ID}.tar.zst"

echo "=== archiving snapshot ==="
# Конфиг ozd: data_paths — где лежат snapshots/<id>/
# Для простоты: обходим все шарды через /admin/usage (число) и конфиг.
# Оператор задаёт DATA_PATHS или скрипт вычитывает из конфига.
if [ -z "${DATA_PATHS:-}" ]; then
    echo "  DATA_PATHS not set — using config heuristic"
    # Fallback: ищем snapshots/<id> в типичных /tmp/ozd-d* или /data/ozd-*
    DATA_PATHS=$(find /tmp /data /mnt -maxdepth 2 -type d -name "$SNAP_ID" 2>/dev/null \
        | sed 's|/snapshots/.*||' | sort -u | tr '\n' ':')
fi

IFS=':' read -ra PATHS <<< "${DATA_PATHS}"
TAR_ARGS=()
for dp in "${PATHS[@]}"; do
    SNAP_PATH="${dp}/snapshots/${SNAP_ID}"
    if [ -d "$SNAP_PATH" ]; then
        TAR_ARGS+=("$SNAP_PATH")
    fi
done

if [ ${#TAR_ARGS[@]} -eq 0 ]; then
    echo "ERROR: no snapshot dirs found for ${SNAP_ID}"
    exit 1
fi

tar -cf - "${TAR_ARGS[@]}" | zstd -3 -T0 > "$ARCHIVE"
ARCHIVE_SIZE=$(stat -f%z "$ARCHIVE" 2>/dev/null || stat -c%s "$ARCHIVE" 2>/dev/null)
echo "  archive: ${ARCHIVE} (${ARCHIVE_SIZE} bytes)"

# Upload (если настроен BACKUP_DEST)
if [ -n "$BACKUP_DEST" ]; then
    echo "=== uploading to ${BACKUP_DEST} ==="
    if [[ "$BACKUP_DEST" == s3://* ]]; then
        aws s3 cp "$ARCHIVE" "${BACKUP_DEST}/$(basename "$ARCHIVE")"
    elif [[ "$BACKUP_DEST" == rsync://* ]]; then
        rsync -avz "$ARCHIVE" "${BACKUP_DEST}/"
    else
        rsync -avz "$ARCHIVE" "${BACKUP_DEST}/"
    fi
    echo "  upload complete"
fi

echo "=== backup done: ${SNAP_ID} ==="
