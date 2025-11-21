FROM python:3.11-slim

# Install system deps for pandas/pyarrow/psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ingest.py /app/ingest.py
# Keep data folder mount-point; in docker compose we'll mount ./data -> /data
ENV DATA_DIR=/data
CMD ["python", "ingest.py"]
