# ─── Stage 1: Build dependencies ─────────────────────────────────────────────
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ─── Stage 2: Runtime image (Production & Non-Root) ───────────────────────────
FROM python:3.11-slim AS runtime

LABEL maintainer="DataOps Team"
LABEL description="DataOps Batch Lakehouse Pipeline — Python 3.11 + PySpark 3.5"

# Java runtime (cần cho PySpark JVM) & curl cho healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r appgroup && useradd -r -g appgroup -d /app appuser

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# Cho phép absolute imports từ project root (cli.flows.serving.*, platforms.*, v.v.)
ENV PYTHONPATH=/app

# Copy installed packages từ builder
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy toàn bộ project và cấp quyền cho appuser
COPY . .
RUN mkdir -p log/log_storage && chown -R appuser:appgroup /app

USER appuser

# Default command: chạy gRPC server phục vụ historical queries cho backend-api
CMD ["python", "grpc_server.py"]
