# ENV REFERENCE

Keep secrets in Secret Manager. This file explains each variable and provides a safe `.env.example` to commit.

## 2.1 `.env.example`
```
# General
NODE_ENV=development
PORT=8080

# === Service base URLs ===
ADMIN_BASE_URL=https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app
CONSUMER_BASE_URL=https://lineoa-consumer-250878482242-250878482242.asia-southeast1.run.app
OWNER_PORTAL_BASE_URL=https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app

# === LINE OA (Consumer OA per shop) ===
# For local/dev only; in prod these come from Firestore per shop.
LINE_CHANNEL_ID=
LINE_CHANNEL_SECRET=
LINE_CHANNEL_ACCESS_TOKEN=
LINE_BASIC_ID=

# === Global LIFF / Owner auth (Admin side) ===
GLOBAL_LINE_LOGIN_CHANNEL_ID=2008442168
GLOBAL_LIFF_ID=2008442168-QM9nPZDr
LIFF_ID_REPORT=2008442168-a3OmXbdJ
LIFF_ID_PROMOTION=2008442168-QM9nPZDr

# === GCP ===
PROJECT_ID=lineoa-g49
MEDIA_BUCKET=lineoa-media-dev
REPORT_BUCKET=lineoa-report-for-owner
MEDIA_PUBLIC_BASE=https://storage.googleapis.com/lineoa-media-dev
REPORT_PUBLIC_BASE=https://storage.googleapis.com/lineoa-report-for-owner

# === Branding ===
BRAND_PRIMARY_HEX=#008080
BRAND_ACCENT_HEX=#F97316

# === App ===
APP_MODULE=lineoa_frontend:app
```

## 2.2 Variable glossary

| Variable | Scope | Type | Example | Description |
|---|---|---|---|---|
| `ADMIN_BASE_URL` | both | URL | https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app | Base URL of Admin (MIA) service. |
| `CONSUMER_BASE_URL` | both | URL | https://lineoa-consumer-250878482242-250878482242.asia-southeast1.run.app | Base URL of Consumer (B→C) service. |
| `OWNER_PORTAL_BASE_URL` | both | URL | https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app | Same as Admin base; owner portal lives on Admin. |
| `MEDIA_BUCKET` | backend | string | lineoa-media-dev | Cloud Storage bucket for general media. |
| `REPORT_BUCKET` | backend | string | lineoa-report-for-owner | Cloud Storage bucket for owner reports. |
| `MEDIA_PUBLIC_BASE` | backend | URL | https://storage.googleapis.com/lineoa-media-dev | Public base for media objects. |
| `REPORT_PUBLIC_BASE` | backend | URL | https://storage.googleapis.com/lineoa-report-for-owner | Public base for report PDFs. |
| `GLOBAL_LINE_LOGIN_CHANNEL_ID` | admin | string | 2008442168 | LINE Login (OpenID) channel id for global LIFF. |
| `GLOBAL_LIFF_ID` | admin | string | 2008442168-QM9nPZDr | Global LIFF used for owner boot/sign‑in. |
| `LIFF_ID_REPORT` | admin | string | 2008442168-a3OmXbdJ | LIFF id for report page. |
| `LIFF_ID_PROMOTION` | admin | string | 2008442168-QM9nPZDr | LIFF id for promotion (same app). |
| `APP_MODULE` | both | string | `lineoa_frontend:app` | Flask module:app entry for Gunicorn. |

> Secrets to bind via Secret Manager (names only): `MAGIC_LINK_SECRET`, `ADMIN_LINE_CHANNEL_ACCESS_TOKEN` (MIA), per‑shop consumer tokens are stored in Firestore `shops/{shop}/settings/default/oa_consumer`.
