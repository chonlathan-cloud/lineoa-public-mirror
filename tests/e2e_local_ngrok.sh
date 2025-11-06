#!/usr/bin/env bash
set -euo pipefail
BASE="$https://586db59148e8.ngrok-free.app"
ID_TOKEN="${ID_TOKEN:?get_from_LIFF}"
SHOP_ID="${SHOP_ID:-shop_00003}"
pass=0; fail=0
ok(){ echo "✅ $1"; pass=$((pass+1)); }
bad(){ echo "❌ $1"; fail=$((fail+1)); }

echo "=== 1) Owner shops API ==="
resp=$(curl -fsS -H "Authorization: Bearer $ID_TOKEN" "$BASE/api/owners/shops?status=active&limit=50")
echo "$resp" | jq -e '.items | length >= 1' >/dev/null && ok "list shops ok" || bad "list shops failed"
echo "$resp" | jq -e '.items[].display_name' >/dev/null && ok "display_name ok" || bad "display_name missing"

echo "=== 2) Report on-demand URL uses report bucket ==="
r=$(curl -fsS -H "Authorization: Bearer $ID_TOKEN" "$BASE/owner/reports/request?shop_id=$SHOP_ID")
pdf_url=$(echo "$r" | jq -r '.pdf_url // empty')
[[ "$pdf_url" == https://storage.googleapis.com/lineoa-report-for-owner/* ]] && ok "report uses report bucket" || bad "report bucket wrong: $pdf_url"

echo "=== 3) GCS object exists ==="
gs="gs://$(echo "$pdf_url" | sed -E 's#https://storage.googleapis.com/([^/]+)/#\1/#')"
gsutil stat "$gs" >/dev/null 2>&1 && ok "GCS object exists" || bad "GCS missing: $gs"

echo "=== 4) Media page uses media bucket ==="
curl -fsS "$BASE/owner/addon?shop_id=$SHOP_ID" | grep -q "https://storage.googleapis.com/lineoa-media-dev" \
  && ok "media uses media bucket" || bad "media bucket wrong on page"

echo "--- SUMMARY ---"
echo "PASS: $pass  FAIL: $fail"
exit $fail
