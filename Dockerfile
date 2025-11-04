FROM python:3.12-slim-bookworm

# --- Base setup ---
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ✅ ติดตั้ง dependencies ที่ WeasyPrint ต้องใช้
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcairo2 \
    libpango-1.0-0 libpangocairo-1.0-0 libpangoft2-1.0-0 \
    libharfbuzz0b \
    libgdk-pixbuf-2.0-0 \
    libffi8 \
    libjpeg62-turbo zlib1g \
    shared-mime-info \
    fonts-noto fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# --- Working directory ---
WORKDIR /app

# --- Install dependencies ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Copy application code ---
COPY . /app

# ✅ ส่วนสำคัญ: build timestamp เพื่อ bust cache
ARG BUILD_TS
LABEL build_ts=$BUILD_TS

# --- Runtime config ---
ENV PORT=8080
EXPOSE 8080

# --- Start command ---
CMD ["sh","-c","gunicorn -w 2 --threads 4 --timeout 60 -b 0.0.0.0:$PORT ${APP_MODULE:-lineoa_frontend:app}"]