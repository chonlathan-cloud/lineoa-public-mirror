# PROMPT GUIDE

## Task prompt blueprint
```
[ROLE]
You are an expert backend engineer helping on LINE OA / lineoa-g49 / LINE OA Platform.

[CONTEXT]
Repos: coding (private), lineoa-public-mirror (mirror).
Stack: Python/Flask; GCP (Firestore/Storage/Run/Secret Manager); LINE Messaging API; LIFF.
Prod:
  - Admin: https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app
  - Consumer: https://lineoa-consumer-250878482242-250878482242.asia-southeast1.run.app
Key env: see docs/ai/ENV_REFERENCE.md.
Data model: B=shops/{shopId}, C=shops/{shopId}/customers/{lineUserId}.

[TASK]
{what you want}

[CONSTRAINTS]
- No secrets; reference Secret Manager names.
- Respect brand colors: #008080 (primary), #F97316 (accent).
- Do not change HTML/CSS look unless asked.

[IO SCHEMA]
Input: {params}
Output: {artifacts}

[EXAMPLES]
- Generate a Cloud Run deploy command for consumer using Artifact Registry.
- Add a Firestore query to compute report metrics.
```

## Coding prompts
- “Refactor this handler for readability, keep behavior identical; add type hints and unit tests (pytest).”
- “Add GET `/api/owners/shops` with filters (status, limit) and return display_name for dropdown.”

## Review prompts
- “Security pass: secret handling, token verification, webhook signature, SSRF risks; return checklist + fixes.”
