# LINE OA Onboarding – คู่มือทีม Sales/Support

## สิ่งที่ทีมร้านต้องส่งให้เรา
- Channel ID (ตัวเลข)
- Channel Secret
- Channel Access Token (Long-lived)
- ชื่อร้าน (ถ้ามี) – หรือเราจะดึงจาก LINE ให้อัตโนมัติ
- (ออปชัน) userId ของ "เจ้าของ OA" ถ้าต้องการให้ตั้งค่าผ่าน LINE

## ขั้นตอนสำหรับทีมเรา

### 1) บันทึกคีย์ของร้านเข้า Firestore
ใช้สคริปต์:

```bash
cd coding
source .venv/bin/activate
export GOOGLE_CLOUD_PROJECT="lineoa-<project>"
python3 scripts/onboard_shop.py \
  --shop_id <shop_id> \
  --channel_id <LINE_Channel_ID> \
  --channel_secret "<LINE_Channel_Secret>" \
  --channel_access_token "<LINE_Channel_Access_Token>"

curl -s -X POST -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"user_id":"<U...>"}' \
  "https://<domain>/front/shops/<shop_id>/owners"