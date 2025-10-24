# test_firestore_connect.py
import time
import json
import google.auth
import logging
import firebase_admin
from firebase_admin import firestore, credentials
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("firestore-test")

def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)

def init_firestore_with_adc():
    """
    ใช้ ADC เป็นหลัก:
    - ถ้ามี gcloud ADC (gcloud auth application-default login) → ใช้ได้เลย
    - ถ้ามี GOOGLE_APPLICATION_CREDENTIALS → ก็ใช้ได้เช่นกัน
    """
    try:
        # ถ้ายังไม่ initialize
        if not firebase_admin._apps:
            logger.info("Initializing Firebase Admin with ADC...")
            # พยายามหา project_id จาก ADC หรือ ENV ถ้าไม่มีให้ใช้ค่าใน ENV ชื่อ GOOGLE_CLOUD_PROJECT
            try:
                import google.auth
                creds, detected_project = google.auth.default()
            except Exception:
                detected_project = None
            project_id = detected_project or os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not project_id:
                # สุดท้าย: ใส่ค่า project id ของคุณลงไปตรงนี้ถ้าต้องการ hardcode ชั่วคราว
                project_id = "lineoa-g49"
                logger.warning("No project ID from ADC/ENV; falling back to hardcoded 'lineoa-g49'")

            firebase_admin.initialize_app(options={"projectId": project_id})
        db = firestore.client()
        logger.info("Firestore client ready.")
        return db
    except Exception as e:
        logger.exception("Initialize Firestore failed")
        raise

def main():
    # แสดง project จาก ADC (เพื่อความชัวร์)
    try:
        creds, project_id = google.auth.default()
    except Exception:
        project_id = None
    logger.info(f"ADC project detected: {project_id}")
    logger.info(f"GOOGLE_CLOUD_PROJECT env: {os.environ.get('GOOGLE_CLOUD_PROJECT')}")

    db = init_firestore_with_adc()

    # สร้าง/เขียนเอกสารทดสอบใน diagnostics/connectivity_tests/runs/{doc_id}
    doc_id = str(int(time.time()))
    col_ref = db.collection("diagnostics").document("connectivity_tests").collection("runs")
    doc_ref = col_ref.document(doc_id)

    payload = {
        "ok": True,
        "message": "Hello Firestore from local test",
        "created_at": firestore.SERVER_TIMESTAMP,
        "project_id": project_id,
    }

    logger.info(f"Writing doc: diagnostics/connectivity_tests/runs/{doc_id}")
    doc_ref.set(payload, merge=False)

    # อ่านกลับมาตรวจสอบ
    snap = doc_ref.get()
    if not snap.exists:
        raise RuntimeError("Test doc not found after write")

    data = snap.to_dict()
    logger.info(f"Read back: {json.dumps(data, ensure_ascii=False, default=_json_default)}")
    print("\n✅ Firestore connectivity test PASSED")

if __name__ == "__main__":
    main()