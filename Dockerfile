FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Cloud Run will pass PORT; default to 8080
ENV PORT=8080
EXPOSE 8080

# ใช้ gunicorn รัน Flask app (โมดูล: lineoa_frontend, ตัวแปร app)
# -w 2: สอง worker (เพิ่ม/ลดได้ตามทราฟฟิก)
# --threads 4: เพิ่ม concurrency
# --timeout 60: กันงานโหลดสื่อ/Firestore นาน ๆ
CMD ["sh","-c","gunicorn -w 2 --threads 4 --timeout 60 -b 0.0.0.0:$PORT lineoa_frontend:app"]