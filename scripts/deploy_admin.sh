#!/usr/bin/env bash
set -euo pipefail

REGION=${REGION:-asia-southeast1}
SVC=lineoa-admin

gcloud run deploy "$SVC" \
  --source . \
  --region "$REGION" \
  --allow-unauthenticated \
  --command "gunicorn" \
  --args "app_admin:app, -b :8080, -w 2" \
  --set-env-vars GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT:?set},REPORT_BUCKET=${REPORT_BUCKET:?set},MEDIA_BUCKET=${MEDIA_BUCKET:?set} \
  --set-env-vars LINE_CHANNEL_ACCESS_TOKEN_A=${LINE_CHANNEL_ACCESS_TOKEN_A:?set},LINE_CHANNEL_SECRET_A=${LINE_CHANNEL_SECRET_A:?set} \
  ${EXTRA_FLAGS:-}