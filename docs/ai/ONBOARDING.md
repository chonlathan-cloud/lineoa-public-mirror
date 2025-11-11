# ONBOARDING

## Prereqs
- Python 3.11+, Node LTS (if applicable)
- gcloud CLI (`gcloud auth login`), `gcloud config set project lineoa-g49`
- Access to Firestore/Storage/Secret Manager

## Run (local)
```bash
export FLASK_ENV=development
export ADMIN_BASE_URL=http://localhost:8080
export CONSUMER_BASE_URL=http://localhost:8081
python -m app  # or: gunicorn -w 2 -b :8080 lineoa_frontend:app
```

## Test
- Unit tests: `pytest`
- E2E: Use ngrok for webhooks; see EXAMPLES/webhook payload.

## Deploy (Cloud Run)
- Build to Artifact Registry, deploy Admin and Consumer with envs in `ENV_REFERENCE.md` and secrets via Secret Manager.
- Set LINE webhooks:
  - Admin: https://lineoa-admin-250878482242-250878482242.asia-southeast1.run.app/line/webhook
  - Consumer: https://lineoa-consumer-250878482242-250878482242.asia-southeast1.run.app/line/webhook
- Publish LIFF apps and verify OpenID channel 2008442168.
