#!/bin/bash
set -e

# Config
REGION=asia-southeast1
REPO=line-oa
IMG=line-oa-api
TAG=$(date +%s)

# Require MAPS_API_KEY in your local env
if [ -z "$MAPS_API_KEY" ]; then
  echo "❌ ERROR: MAPS_API_KEY not set in your environment."
  echo "👉 Run: export MAPS_API_KEY=YOUR_REAL_KEY"
  exit 1
fi

echo "🚀 Building image with tag $TAG ..."
gcloud builds submit --tag ${REGION}-docker.pkg.dev/lineoa-g49/${REPO}/${IMG}:${TAG}

echo "🚀 Deploying to Cloud Run ..."
gcloud run deploy line-oa-api \
  --image=${REGION}-docker.pkg.dev/lineoa-g49/${REPO}/${IMG}:${TAG} \
  --region=$REGION \
  --set-env-vars=API_BEARER_TOKEN=dev-secret-token,PROOF_BUCKET=lineoa-g49-proof-uploads,FRONTEND_ORIGIN=http://localhost:5173,MAPS_API_KEY=$MAPS_API_KEY

echo "✅ Deploy complete."