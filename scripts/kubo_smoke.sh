#!/usr/bin/env bash
# W9.2: Smoke-тест ozd S3 API (без реального Kubo+go-ds-s3 — через curl).
#
# Проверяет: PutObject → GetObject == тело → HeadObject → ListV2 →
#            DeleteObject → 404. Эмулирует то, что делает go-ds-s3.
#
# Использование:
#   docker compose -f deployments/docker/docker-compose.yml up -d ozd
#   ./scripts/kubo_smoke.sh
#
# Или без Docker (ozd слушает localhost:9100):
#   ./scripts/kubo_smoke.sh http://localhost:9100

set -euo pipefail

OZD_URL="${1:-http://localhost:9100}"
BUCKET="kubo"
KEY="blocks/CIQTEST$(date +%s)"
BODY="hello-from-smoke-test-$(date +%s)"

echo "=== ozd smoke-тест: ${OZD_URL} ==="

# 1. healthz
echo -n "healthz... "
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${OZD_URL}/healthz")
[ "$STATUS" = "200" ] && echo "OK" || { echo "FAIL ($STATUS)"; exit 1; }

# 2. PUT
echo -n "PUT /${BUCKET}/${KEY}... "
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
  -d "$BODY" "${OZD_URL}/${BUCKET}/${KEY}")
[ "$STATUS" = "200" ] && echo "OK" || { echo "FAIL ($STATUS)"; exit 1; }

# 3. GET → сверка тела
echo -n "GET /${BUCKET}/${KEY}... "
GOT=$(curl -s "${OZD_URL}/${BUCKET}/${KEY}")
[ "$GOT" = "$BODY" ] && echo "OK (body matches)" || { echo "FAIL (got: $GOT)"; exit 1; }

# 4. HEAD → Content-Length
echo -n "HEAD /${BUCKET}/${KEY}... "
CL=$(curl -s -I "${OZD_URL}/${BUCKET}/${KEY}" | grep -i content-length | awk '{print $2}' | tr -d '\r')
EXPECTED=${#BODY}
[ "$CL" = "$EXPECTED" ] && echo "OK (len=$CL)" || { echo "FAIL (got $CL, want $EXPECTED)"; exit 1; }

# 5. ListV2 → ключ виден
echo -n "ListV2 prefix=blocks/... "
LIST=$(curl -s "${OZD_URL}/${BUCKET}?prefix=blocks/&max-keys=100")
echo "$LIST" | grep -q "CIQTEST" && echo "OK (key in list)" || { echo "FAIL (not in list)"; exit 1; }

# 6. DELETE
echo -n "DELETE /${BUCKET}/${KEY}... "
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "${OZD_URL}/${BUCKET}/${KEY}")
[ "$STATUS" = "204" ] && echo "OK" || { echo "FAIL ($STATUS)"; exit 1; }

# 7. GET после DELETE → 404
echo -n "GET after DELETE... "
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${OZD_URL}/${BUCKET}/${KEY}")
[ "$STATUS" = "404" ] && echo "OK (404)" || { echo "FAIL ($STATUS)"; exit 1; }

# 8. /metrics доступен
echo -n "/metrics... "
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${OZD_URL}/metrics")
[ "$STATUS" = "200" ] && echo "OK" || { echo "FAIL ($STATUS)"; exit 1; }

# --- W14.2: расширенные проверки ---

# 9. Large body (1 МиБ)
echo -n "PUT 1MiB body... "
LARGE=$(dd if=/dev/urandom bs=1024 count=1024 2>/dev/null | base64 | head -c 1048576)
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
  -d "$LARGE" "${OZD_URL}/${BUCKET}/blocks/LARGE1MIB")
[ "$STATUS" = "200" ] && echo "OK" || { echo "FAIL ($STATUS)"; exit 1; }

echo -n "GET 1MiB body (size check)... "
SIZE=$(curl -s -I "${OZD_URL}/${BUCKET}/blocks/LARGE1MIB" | grep -i content-length | awk '{print $2}' | tr -d '\r')
[ "$SIZE" = "1048576" ] && echo "OK (1MiB)" || { echo "FAIL (size=$SIZE)"; exit 1; }

# 10. Multiple keys (batch PUT + LIST count)
echo -n "Batch PUT 10 keys... "
for i in $(seq 1 10); do
  curl -s -o /dev/null -X PUT -d "data-$i" "${OZD_URL}/${BUCKET}/blocks/BATCH${i}"
done
echo "OK"

echo -n "ListV2 count >= 10... "
COUNT=$(curl -s "${OZD_URL}/${BUCKET}?prefix=blocks/BATCH&max-keys=100" | grep -c "<Key>")
[ "$COUNT" -ge 10 ] && echo "OK ($COUNT keys)" || { echo "FAIL ($COUNT)"; exit 1; }

# 11. Range GET (bytes=0-9)
echo -n "Range GET bytes=0-9... "
RANGE_BODY=$(curl -s -H "Range: bytes=0-9" "${OZD_URL}/${BUCKET}/blocks/BATCH1")
[ "${#RANGE_BODY}" = "10" ] && echo "OK (10 bytes)" || { echo "FAIL (got ${#RANGE_BODY} bytes)"; exit 1; }

# 12. Cleanup batch
for i in $(seq 1 10); do
  curl -s -o /dev/null -X DELETE "${OZD_URL}/${BUCKET}/blocks/BATCH${i}"
done
curl -s -o /dev/null -X DELETE "${OZD_URL}/${BUCKET}/blocks/LARGE1MIB"

echo ""
echo "=== ALL 12 CHECKS PASSED ==="
