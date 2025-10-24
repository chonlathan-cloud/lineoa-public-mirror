#!/usr/bin/env python3
"""
scripts/onboard_shop.py

Usage:
  python3 scripts/onboard_shop.py \
    --shop_id shop_001 \
    --channel_id 2007728636 \
    --channel_secret xxx... \
    --channel_access_token yyy...

ENV required:
  GOOGLE_CLOUD_PROJECT=...
  (and ADC or FIREBASE_SERVICE_ACCOUNT_JSON/FIREBASE_CONFIG_JSON)
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import urllib.request

from firestore_client import get_db

def fetch_bot_info(access_token: str) -> dict:
    req = urllib.request.Request("https://api.line.me/v2/bot/info")
    req.add_header("Authorization", f"Bearer {access_token}")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--shop_id", required=True)
    p.add_argument("--channel_id", required=True)
    p.add_argument("--channel_secret", required=True)
    p.add_argument("--channel_access_token", required=True)
    args = p.parse_args()

    # 1) ดึง bot_user_id จาก LINE
    info = fetch_bot_info(args.channel_access_token)
    bot_user_id = info.get("userId")  # e.g. Uxxxxxxxx...
    display_name = info.get("displayName")

    if not bot_user_id:
        print("ERROR: Cannot fetch bot_user_id. Check access token.", file=sys.stderr)
        print("Response:", json.dumps(info, ensure_ascii=False, indent=2))
        sys.exit(2)

    db = get_db()

    # 2) เขียน shops/{shop_id}
    db.collection("shops").document(args.shop_id).set({
        "line_oa_id": str(args.channel_id),  # เลข Channel ID
        "bot_user_id": bot_user_id,          # U...
    }, merge=True)

    # 3) เขียน shops/{shop_id}/settings/default
    db.collection("shops").document(args.shop_id).collection("settings").document("default").set({
        "line_channel_secret": args.channel_secret,
        "line_channel_access_token": args.channel_access_token,
    }, merge=True)

    # 4) (ออปชัน) เขียน owner_profile.line_display_name จาก LINE info
    if display_name:
        db.collection("shops").document(args.shop_id).collection("owner_profile").document("default").set({
            "line_display_name": display_name
        }, merge=True)

    print("✅ Onboarded:", args.shop_id)
    print("  - line_oa_id      :", args.channel_id)
    print("  - bot_user_id     :", bot_user_id)
    print("  - line_display_name:", display_name or "(n/a)")

if __name__ == "__main__":
    # กันพลาด project
    if not (os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT_ID") or os.getenv("GCLOUD_PROJECT")):
        print("WARNING: GOOGLE_CLOUD_PROJECT is not set.", file=sys.stderr)
    main()


# phase sycs firestore for Resister shop
'''
cd "/Users/chonlathansongsri/Documents/company/line OA/data/coding"
source .venv/bin/activate
export GOOGLE_CLOUD_PROJECT="lineoa-g49"

python3 -m scripts.onboarding.onboard_shop \
  --shop_id shop_00001 \
  --channel_id 2008101064 \
  --channel_secret "95886be80b176157a05e21e509b989b1" \
  --channel_access_token "W6O7gKj1x23ml8M/aKaF2Ng5kjiabjoDtFI3yIkF7A1jfH9eCOQgC5GJdmX9W+7AlG67hJXvoMDGEQOL39SidCNepSXJ6rhd6EE9Zf35L36aTmWgzT4QSfHM3Jl/ZrWtye9T71N+FWritUg8O/w8oAdB04t89/1O/w1cDnyilFU="
'''


''' gcloud auth application-default login
gcloud auth application-default set-quota-project lineoa-g49
gcloud config set project lineoa-g49'''


# phase sing up ADK for test local
'''cd "/Users/chonlathansongsri/Documents/company/line OA/data/coding"
source .venv/bin/activate
python3 - <<'PY'
from firestore_client import get_db
db = get_db()
print("OK project:", db.project)
print("Shops head:", [d.id for d in db.collection("shops").limit(2).stream()])
PY

export GOOGLE_CLOUD_PROJECT="lineoa-g49"
export API_BEARER_TOKEN="dev-token-123"
export MEDIA_BUCKET="lineoa-media-dev"
PORT=8081 python3 lineoa_frontend.py
'''


# phase push and build and deploy to cloud run

# crate setting for firestore 
'''
python3 - <<'PY'
from firestore_client import get_db
db = get_db()
ref = db.collection("shops").document("shop_001").collection("settings").document("default")
ref.set({
  "line_channel_access_token": "JUo5VtENWGC3v01fUYIqS6scVJPjdUCuqpr+OnGJvz5SiipVDpC+1TGDOK9sufXqNcOJX/RVK2/J/iPHwMYCIpbWFsgX7EBYpIJlBcDS6Ev3a5AP9VvHiuoJ8Jf0KE366nAfcl2AjLe+cQ5sbaj8owdB04t89/1O/w1cDnyilFU=",
  "line_channel_secret": "cc761589625b0baa19f79183f1e9b83f",
}, merge=True)
print("✅ updated settings for shop_001")
PY
'''