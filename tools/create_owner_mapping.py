# tools/create_owner_mapping.py
import argparse
from firestore_client import get_db
from firebase_admin import firestore as fb

def upsert_owner_mapping(global_sub: str, shop_id: str, local_owner_user_id: str, display_name: str):
    db = get_db()
    ts = fb.SERVER_TIMESTAMP

    owner_shop_ref = (
        db.collection("owner_shops")
          .document(global_sub)
          .collection("shops")
          .document(shop_id)
    )
    owner_shop_ref.set(
        {
            "local_owner_user_id": local_owner_user_id,
            "display_name": display_name,
            "active": True,
            "created_at": ts,
            "updated_at": ts,
        },
        merge=True,
    )

    shop_owner_ref = (
        db.collection("shops")
          .document(shop_id)
          .collection("owners")
          .document(local_owner_user_id)
    )
    shop_owner_ref.set(
        {
            "active": True,
            "linked_liff_user_id": global_sub,
            "display_name": display_name,
            "created_at": ts,
            "updated_at": ts,
        },
        merge=True,
    )

    print(f"✅ Mapping created/updated: {global_sub} → {shop_id}/{local_owner_user_id}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--global-sub", required=True)
    parser.add_argument("--shop-id", required=True)
    parser.add_argument("--local-owner-id", required=True)
    parser.add_argument("--display-name", default="")
    args = parser.parse_args()

    upsert_owner_mapping(
        global_sub=args.global_sub,
        shop_id=args.shop_id,
        local_owner_user_id=args.local_owner_id,
        display_name=args.display_name,
    )

"""
python tools/create_owner_mapping.py \
  --global-sub "Ue480994542f3bc9b5206a2a290a1bbae" \
  --shop-id "shop_00001" \
  --local-owner-id "<LOCAL_OWNER_USER_ID>" \
  --display-name "MIA"
"""