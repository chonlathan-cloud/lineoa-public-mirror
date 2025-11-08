# LINE OA Automation Platform

Multi-tenant backend, admin, and utility services that power LINE Official Account (OA) operations for multiple shops/brands.  
The project runs entirely on Flask and integrates with Google Cloud (Firestore, Storage, Secret Manager, Pub/Sub) plus LINE Messaging APIs to automate customer interactions, payments, and owner tooling.

## Core Components

| Path | Description |
| --- | --- |
| `app.py` | Lightweight API + webhook service for collecting payments, managing customers, locations, and promotions. Includes LINE webhook per shop (dynamic secrets), proof upload to Cloud Storage, and address/geocode helpers. |
| `lineoa_frontend.py` | Full-featured multi-tenant service that routes LINE events, renders admin/owner views, generates PDF reports, handles onboarding flows, magic links, payment confirmations, and Pub/Sub push handlers. |
| `admin/` | Flask blueprint and Jinja templates for internal admin console (provision shops, manage LIFF apps, view envs). |
| `dao.py` | Firestore data access layer used by both services. |
| `report_renderer.py` | Utilities (matplotlib/WeasyPrint fallback) for owner insight PDFs that `lineoa_frontend.py` can push via LINE. |
| `tests/` | Pytest suites plus `tests/e2e_local_ngrok.sh` helper for exercising LINE webhook flows. |

## Feature Highlights

- **LINE Messaging webhook** (per shop) with signature validation, session state tracking, payment slip handling, and automatic customer upserts.
- **Internal APIs** under `/api/v1/...` for customers, messages, products, promotions, locations (with geohash search) and payments.
- **Owner/Admin portal** for onboarding, magic-link access, and LIFF integrations (global, report, promotion LIFF IDs).
- **Payment automation** including manual payment capture, intent confirmation codes, proof uploads to GCS, and customer tier updates.
- **Location intelligence** with Google Maps geocoding + geohash search endpoints to find nearby stockists.
- **Report generation** that stores owner reports in Firestore, renders PDFs (WeasyPrint or ReportLab fallback), uploads to GCS, and optionally pushes URLs to LINE owners.
- **Google Cloud ready** deployment (see `deploy.sh`) targeting Cloud Run with Artifact Registry images.

## Prerequisites

- Python 3.11+ (tested with CPython).  
- `pip` + `virtualenv` (recommended).  
- Google Cloud project with:
  - Firestore (Native mode)
  - Secret Manager
  - Cloud Storage buckets for proofs/media/reports
  - (Optional) Pub/Sub topics if you use push endpoints
- Service Account credentials with access to the above (export via `GOOGLE_APPLICATION_CREDENTIALS=/path/key.json`).
- LINE Developers accounts for OA + LIFF apps.
- (Optional) Google Maps API key for geocoding addresses.

## Environment Configuration

Create a local env file for LIFF-facing values:

```bash
cp .env.example .env
```

`lineoa_frontend.py` and `admin` templates read these variables to render LIFF launchers. Do **not** commit `.env`.

Common runtime variables (set via shell, `.envrc`, or Cloud Run):

### Core API (`app.py`)

| Variable | Purpose |
| --- | --- |
| `API_BEARER_TOKEN` | Shared secret for `/api/v1/*` endpoints. Required in production. |
| `FRONTEND_ORIGIN` | Allowed origin for CORS (default `http://localhost:5173`). |
| `PROOF_BUCKET` | GCS bucket used to store payment slips. |
| `MAPS_API_KEY` | Google Maps Geocoding key (required when upserting locations via address). |
| `LINE_CHANNEL_ACCESS_TOKEN` / `LINE_CHANNEL_SECRET` | Optional local fallbacks; in prod each shop loads credentials from Firestore/Secret Manager. |
| `SECRET_TTL_SEC` | Cache TTL for Secret Manager lookups (default 300 seconds). |

### Admin / Multi-tenant frontend (`lineoa_frontend.py`)

| Variable | Purpose |
| --- | --- |
| `API_BEARER_TOKEN` | Protects REST helpers exposed by this service. |
| `PUSH_ALL_OWNERS` | When `true`, pushes owner report summaries to every registered owner. |
| `DEFAULT_SHOP_ID` | Fallback shop when destination → shop mapping is incomplete (dev only). |
| `MEDIA_BUCKET` / `MEDIA_PUBLIC_BASE` | Optional storage for rich media uploaded by owners. |
| `REPORT_BUCKET`, `REPORT_LOGO_PATH`, `BRAND_PRIMARY_HEX`, `BRAND_ACCENT_HEX`, `REPORT_TITLE_TH/EN` | Customize owner report output + branding. |
| `PUBSUB_TOKEN` | Token needed by Pub/Sub push endpoints (`/pubsub/...`). |
| `GLOBAL_LIFF_ID`, `LIFF_ID`, `LIFF_ID_REPORT`, `LIFF_ID_PROMOTION` | LIFF apps surfaced in admin templates and owner launcher pages. |

### Google Cloud

- `GOOGLE_APPLICATION_CREDENTIALS` (or `gcloud auth application-default login`) must allow Firestore, Secret Manager, Storage, Pub/Sub usage.

## Setup

```bash
git clone https://github.com/chonlathan-cloud/coding.git
cd coding
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env  # fill in LIFF IDs / LINE channel info
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export API_BEARER_TOKEN=dev-secret-token
export PROOF_BUCKET=your-proof-bucket
export MAPS_API_KEY=your-google-maps-key
# add any other env vars you need (see sections above)
```

## Running Locally

Two Flask entry points exist; you can run either (or both) depending on what you are working on.

### Core API + LINE webhook

```bash
source .venv/bin/activate
python app.py  # listens on 0.0.0.0:8080 by default
# or specify a port
PORT=9000 python app.py
```

Key routes:
- `GET /healthz` – health probe
- `POST /webhook/<shop_id>` – LINE Messaging webhook (use ngrok when testing)
- `GET /api/v1/shops/<shop_id>/customers`
- `GET /api/v1/shops/<shop_id>/messages?customerId=...`
- `POST /api/v1/shops/<shop_id>/products`
- `POST /api/v1/shops/<shop_id>/customers/<customer_id>/payments`
- `POST /api/v1/shops/<shop_id>/locations` – supports address-to-geocode auto fill
- `GET /api/v1/locations/nearby?shop_id=...&lat=...&lng=...`

All `/api/v1/*` endpoints expect `Authorization: Bearer <API_BEARER_TOKEN>` (or `X-Api-Token`).

### Multi-tenant frontend / admin service

```bash
source .venv/bin/activate
python lineoa_frontend.py  # also defaults to port 8080
```

This service registers the admin blueprint (`/admin/...`), exposes owner LIFF bootstrappers (`/owner/...` templates), and provides additional REST + report/PubSub endpoints. When working locally you can set `FLASK_ENV=development` for better logs.

### Debug tips

- Use `ngrok http 8080` to expose the webhook and configure LINE developer console to call `https://<ngrok-id>.ngrok.io/webhook/<shop_id>`.
- Firestore emulator is not wired by default; to use actual Firestore you must export `GOOGLE_APPLICATION_CREDENTIALS`.
- When `ModuleNotFoundError: geohash2` appears, ensure `pip install -r requirements.txt` has been executed (the dependency lives under `# Utils`).

## Tests

This repo uses `pytest`.

```bash
source .venv/bin/activate
pytest
```

Focused suites:
- `tests/test_admin_routes.py` – admin blueprint smoke tests
- `tests/test_consumer_webhook.py` – consumer LINE webhook logic
- `tests/test_handlers.py` – shared handler helpers
- `tests/test_firestore_connect.py` – Firestore client wiring

For end-to-end webhook verification via ngrok, use `tests/e2e_local_ngrok.sh`.

## Deployment

Cloud Run is the reference deployment target (`deploy.sh` shows an example).

```bash
export MAPS_API_KEY=real-key
./deploy.sh
```

The script builds and pushes an image to Artifact Registry (`asia-southeast1-docker.pkg.dev/lineoa-g49/lineoa-repo/lineoa-admin:<timestamp>`) and deploys `lineoa-admin` Cloud Run service with the required env vars. Adapt the script for your project: change project ID, bucket names, tokens, and add secrets via `--set-env-vars` or Secret Manager references.

## Troubleshooting Checklist

- **Divergent branches / rebase conflicts** – ensure working tree is clean (`git status`), resolve `.env` conflicts by keeping it local only (`git rm --cached .env` once, leave `.env` in `.gitignore`), then `git pull --rebase origin main`.
- **Missing `.env` after reset** – re-create from `.env.example` (`cp .env.example .env`) and re-enter secrets locally.
- **Firebase permissions** – verify `GOOGLE_APPLICATION_CREDENTIALS` points to a key that can read/write Firestore, Secret Manager, and Storage buckets referenced by env vars.
- **Webhook signature errors** – confirm Firestore `shops/{shop_id}/settings` contains valid `line_channel_secret`/`line_channel_access_token` or secret manager references.

## Next Steps

- Configure LINE developer console to use your Cloud Run HTTPS URL for `/webhook/<shop_id>`.
- Populate Firestore with `shops/{shop_id}` documents and owner metadata using the admin portal or Firestore console.
- Update `.env.example` whenever you add/remove LIFF apps so teammates know which variables must be provided locally.

Happy building! 🎯
