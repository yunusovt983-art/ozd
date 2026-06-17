#!/bin/sh
# W9.1: инициализация Kubo с go-ds-s3 плагином, указывающим на ozd.
# Вызывается перед `ipfs daemon`.
#
# ПРИМЕЧАНИЕ: стандартный образ ipfs/kubo НЕ включает go-ds-s3 плагин.
# Для реального smoke нужен кастомный образ Kubo с плагином.
# Этот скрипт пока инициализирует Kubo со стандартным flatfs-сторе,
# а smoke-тест проверяет ozd напрямую через curl (S3 API).
# TODO: собрать Kubo+go-ds-s3 образ (W9 Phase 2).

set -e

# Инициализация, если ещё нет
if [ ! -f /data/ipfs/config ]; then
  IPFS_PATH=/data/ipfs ipfs init --profile=server
fi

export IPFS_PATH=/data/ipfs

echo "Kubo initialized. go-ds-s3 integration requires custom build (see KUBO-INTEGRATION.md)."
echo "Smoke-тест пойдёт через curl → ozd S3 API напрямую."
