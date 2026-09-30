# ─── Stage 1: Build dependencies ─────────────────────────────────────────────
FROM python:3.11-slim AS builder

# Java 17 cần thiết cho PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ─── Stage 2: Runtime image ───────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL maintainer="DataOps Team"
LABEL description="OKX Batch Ingestion Pipeline — Python 3.11 + Java 17 + PySpark 3.5"

# Java 17 runtime (cần cho PySpark JVM)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Copy installed packages từ builder
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy toàn bộ project
COPY . .

# Tạo log storage directory
RUN mkdir -p log/log_storage

# Default command: chạy daily pipeline
CMD ["python", "cli/flows/serving/daily_pipeline.py"]
