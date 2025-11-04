

# core/media.py
from __future__ import annotations
from typing import Tuple, Dict, Any, Optional
import requests, os, uuid
from google.cloud import storage

LINE_CONTENT_URL = "https://api-data.line.me/v2/bot/message/{message_id}/content"

def download_line_content(access_token: str, message_id: str) -> Tuple[bytes, str]:
  """
  Download binary content from LINE Messaging API by message id.
  Returns (content_bytes, content_type)
  """
  url = LINE_CONTENT_URL.format(message_id=message_id)
  headers = {"Authorization": f"Bearer {access_token}"}
  r = requests.get(url, headers=headers, timeout=20)
  r.raise_for_status()
  ctype = r.headers.get("Content-Type", "application/octet-stream")
  return r.content, ctype

def store_media(shop_id: str, mtype: str, message_id: str, content: bytes, content_type: str) -> Dict[str, Any]:
  """
  Store media to GCS under MEDIA_BUCKET. Returns dict with blob path and public URL.
  """
  bucket_name = os.getenv("MEDIA_BUCKET") or os.getenv("REPORT_BUCKET")
  if not bucket_name:
    raise RuntimeError("MEDIA_BUCKET (or REPORT_BUCKET) not configured")
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  ext = guess_ext_from_ctype(content_type)
  blob_name = f"shops/{shop_id}/media/{mtype}/{message_id or uuid.uuid4().hex}{ext}"
  blob = bucket.blob(blob_name)
  blob.upload_from_string(content, content_type=content_type or "application/octet-stream")
  try:
    blob.cache_control = "public, max-age=86400"
    blob.patch()
  except Exception:
    pass
  public_base = os.getenv("MEDIA_PUBLIC_BASE", f"https://storage.googleapis.com/{bucket_name}")
  return {"bucket": bucket_name, "name": blob_name, "url": f"{public_base}/{blob_name}", "content_type": content_type}

def guess_ext_from_ctype(ctype: str) -> str:
  ctype = (ctype or "").lower()
  if "jpeg" in ctype or "jpg" in ctype: return ".jpg"
  if "png" in ctype: return ".png"
  if "gif" in ctype: return ".gif"
  if "webp" in ctype: return ".webp"
  if "mp4" in ctype: return ".mp4"
  if "mpeg" in ctype or "mp3" in ctype: return ".mp3"
  if "aac" in ctype: return ".aac"
  if "wav" in ctype: return ".wav"
  return ""