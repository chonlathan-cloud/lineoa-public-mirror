# Codex Task Template (lineoa-g49)

[ROLE]  
You are an expert backend engineer for the LINE OA project **lineoa-g49**, running on GCP (Firestore, Cloud Run, Storage, LIFF, LINE Login).

[CONTEXT]  
Repos: `coding` (private main), `lineoa-public-mirror` (public).  
Important reference files (for context):  
- `docs/ai/PROJECT_BRIEF.md`  
- `docs/ai/ENV_REFERENCE.md`  
- `docs/ai/SERVICE_MAP.md`  
- `docs/ai/TASK_SPECS/`  
- `docs/ai/CONTEXT.md`

Stack: Python (Flask) + GCP (Firestore, Storage, Cloud Run) + LINE OA API + LIFF login.  
Data model:  
- B (Business owner): `shops/{shopId}`  
- C (Customer): `shops/{shopId}/customers/{lineUserId}`  
- Owner mapping: `owner_shops/{global_user_id}/shops/{shop_id}`  

---

[TASK]  
(🔧 Describe exactly what you want Codex to do.)  
Example:  
> Add multi-shop dropdown in owner portal (display_name), preserve existing UX and brand style.  

---

[SCOPE — Files You MAY Edit]  
(List specific files Codex is allowed to touch.)  
Example:  
> - `admin/blueprint.py`  
> - `report_renderer.py`  
> - `templates/owner/*.html` (read-only unless specified)

---

[OUT OF SCOPE — DO NOT TOUCH]  
- Frontend HTML/CSS look and feel.  
- Payment, auth, and consumer webhook logic.  
- Dockerfiles, CI/CD pipeline.

---

[ACCEPTANCE CRITERIA]  
- Describe how success is measured, e.g.:
  - ✅ API `/api/owners/shops` returns `display_name` + `shop_id`.  
  - ✅ Dropdown shows correct display_name per active shop.  
  - ✅ Report full/mini PDF saved to correct bucket.

---

[TEST PLAN]  
How to verify manually (ngrok/local test):  
- Run ngrok, trigger Consumer OA via LINE OA message.  
- Verify logs and Firestore updates.  
- Confirm PDF written to `gs://lineoa-report-for-owner/...`.

---

[DELIVERABLES]  
- Unified diff limited to allowed files.  
- Short changelog (1–3 lines).  
- No unrelated refactors.

---

[BRAND & SECRETS]  
- Brand colors: `#008080` (primary), `#F97316` (accent).  
- No secrets in code. Use env vars from `docs/ai/ENV_REFERENCE.md`.  