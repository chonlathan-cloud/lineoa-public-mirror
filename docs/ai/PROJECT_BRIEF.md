# PROJECT BRIEF — lineoa-g49 / LINE OA Platform

**Updated:** 2025-11-10 08:36

**Project name:** `lineoa`  
**Owner(s):** A (Company) — LINE OA team  
**Mission:** Build an AI-powered LINE OA platform for SMEs. A runs the platform (Admin OA / MIA), sets up consumer OAs for each client (B), and equips owners with promotion tools and on‑demand reports. Data and compute live on GCP (Firestore, Cloud Storage, Cloud Run). Global LIFF provides a unified owner identity across shops for secure access to add‑on and reporting features.

**Key repositories:**  
- **coding** (private, source of truth)  
- **lineoa-public-mirror** (public mirror): https://github.com/chonlathan-cloud/lineoa-public-mirror

**Tech stack:** Python (Flask), Google Cloud (Firestore, Cloud Run, Cloud Storage, Secret Manager), LINE Messaging API, LIFF (LINE Login OpenID Connect).

**Data model (high-level):**
- **B (Business owner)**: `shops/{shopId}`
- **C (Customer)**: `shops/{shopId}/customers/{lineUserId}`
- **Owner mapping (global↔local):**
  - `owner_shops/{global_user_id}/shops/{shop_id}` → {`local_owner_user_id`,`display_name`,`active`}
  - `shops/{shop_id}/owners/{local_user_id}` → {`active`,`linked_liff_user_id`}

**Environments:** dev / staging / prod (current prod URLs below)

**Production base URLs:**  
- **Admin (MIA):** https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app  
- **Consumer (B→C):** https://lineoa-consumer-250878482242-250878482242.asia-southeast1.run.app

**Security:** All secrets in **Secret Manager** (never commit). Deploy via Cloud Build → Cloud Run.

**Release cadence:** on‑demand

**Contacts:** LINE OA team — (add Slack / email)
