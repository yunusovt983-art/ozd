#!/bin/sh
# W9 Phase 2: инициализация Kubo с go-ds-s3 → ozd.
#
# Конфиг datastore:
#   /blocks → s3ds (ozd S3 API на порту 9100)
#   /       → levelds (метаданные, pins, локальное состояние Kubo)

set -e

export IPFS_PATH="${IPFS_PATH:-/data/ipfs}"

# Инициализация, если ещё нет
if [ ! -f "${IPFS_PATH}/config" ]; then
  ipfs init --profile=server
  echo "Kubo initialized with server profile"
fi

# Инъекция Datastore.Spec: /blocks → s3ds (ozd), / → levelds
# Формат: JSON-спецификация хранилищ Kubo (see docs/datastores.md)
S3_ENDPOINT="${OZD_S3_ENDPOINT:-http://ozd:9100}"
S3_BUCKET="${OZD_S3_BUCKET:-kubo}"
S3_REGION="${OZD_S3_REGION:-us-east-1}"
S3_ACCESS_KEY="${OZD_S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${OZD_S3_SECRET_KEY:-minioadmin}"

SPEC=$(cat <<EOF
{
  "mounts": [
    {
      "child": {
        "type": "s3ds",
        "region": "${S3_REGION}",
        "bucket": "${S3_BUCKET}",
        "endpoint": "${S3_ENDPOINT}",
        "rootDirectory": "",
        "accessKey": "${S3_ACCESS_KEY}",
        "secretKey": "${S3_SECRET_KEY}",
        "workers": 100
      },
      "mountpoint": "/blocks",
      "prefix": "s3.datastore",
      "type": "measure"
    },
    {
      "child": {
        "compression": "none",
        "path": "datastore",
        "type": "levelds"
      },
      "mountpoint": "/",
      "prefix": "leveldb.datastore",
      "type": "measure"
    }
  ],
  "type": "mount"
}
EOF
)

# Применяем конфиг через jq (атомарная перезапись)
if command -v jq >/dev/null 2>&1; then
  jq --argjson spec "$SPEC" '.Datastore.Spec = $spec' "${IPFS_PATH}/config" > "${IPFS_PATH}/config.tmp"
  mv "${IPFS_PATH}/config.tmp" "${IPFS_PATH}/config"
  echo "Datastore.Spec injected: /blocks → s3ds (${S3_ENDPOINT})"
else
  echo "WARNING: jq not found — Datastore.Spec NOT injected (install jq)"
fi

# Разрешить API доступ извне контейнера
ipfs config Addresses.API "/ip4/0.0.0.0/tcp/5001"
ipfs config Addresses.Gateway "/ip4/0.0.0.0/tcp/8080"

echo "Starting Kubo daemon..."
exec ipfs daemon --migrate
