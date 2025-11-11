# SERVICE MAP

## Services
- **mia-admin-oa** (Admin / A→B)  
  - Routes: `/line/webhook`, `/owner/*`, `/api/*`  
  - Uses Firestore, Storage (report), Secret Manager.
- **consumer-oa-bot** (B→C)  
  - Routes: `/line/webhook`  
  - Owner-binding trigger on `"เริ่มต้นใช้งาน"` → pushes magic link (LIFF boot).

## Endpoints (prod)
- Admin base: https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app
- Consumer base: https://lineoa-consumer-250878482242-250878482242.asia-southeast1.run.app
- Admin webhook: https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app/line/webhook
- Consumer webhook: https://lineoa-consumer-250878482242-250878482242.asia-southeast1.run.app/line/webhook

## Data flows
1) **Owner binding (B→C → A)**  
   Consumer receives `"เริ่มต้นใช้งาน"` → saves local owner (shops/{shop}/owners/{user} active=true) → pushes magic link to **Admin** LIFF boot with `sid` + signed `token`.  
   Admin LIFF boot + callback verify `id_token` (channel 2008442168) → map global `sub` ↔ shop:
   - `owner_shops/{global}/shops/{shop}` (local_owner_user_id, display_name, active=true)  
   - `shops/{shop}/owners/{local}` (active=true, linked_liff_user_id=global)

2) **Reports**  
   Owner requests mini/full report → PDF written to `lineoa-report-for-owner` and public URL under `https://storage.googleapis.com/lineoa-report-for-owner`.

3) **Promotion add‑on**  
   Owner creates promotions; media under `lineoa-media-dev`.
