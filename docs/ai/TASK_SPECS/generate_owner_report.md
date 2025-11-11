# TASK: Generate owner report (mini/full)

**Goal:** Generate owner-facing PDF report into `lineoa-report-for-owner`; serve via `https://storage.googleapis.com/lineoa-report-for-owner`.

**Inputs:** `shop_id`, `start_date`, `end_date`, `kind` in {mini, full}  
**Outputs:** PDF at `reports/{shop_id}/{req_id}.pdf` and public URL.

## Steps
1. Query Firestore metrics for the range:
   - Payments: `shops/{shop}/payments` (status approved) → revenue sum.
   - New customers: customers with first_interaction in range.
   - Active chat users: distinct users with messages in range.
   - (full only) total messages; breakdown by inbound/outbound.
2. Render HTML (brand #008080/#F97316); keep existing layout.
3. Convert to PDF, upload to `lineoa-report-for-owner`.
4. Update Firestore:
   - `shops/{shop}/report_requests/{req_id}` → status=ready, `pdf_url`.
   - mirror under `shops/{shop}/reports/requests/items/{req_id}`.
5. Push URL back to requester (LINE push).

## Acceptance
- Mini shows: Revenue=300, New=1, Active=1 (test data)
- Full shows: Revenue=300, Total Messages>0, New=1
