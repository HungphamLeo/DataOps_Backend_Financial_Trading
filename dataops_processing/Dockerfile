# =============================================================================
# dataops_processing/Dockerfile
#
# Multi-stage build:
#   Stage 1 (deps): install Python dependencies (layer-cached)
#   Stage 2 (runtime): copy app code only — không copy test, cache, __pycache__
#
# PySpark requires Java at runtime (driver runs in-process).
# Uses python:3.11-slim to minimize image size + default-jre-headless for JVM.
# =============================================================================

# ── Stage 1: dependency install ───────────────────────────────────────────────
FROM python:3.11-slim AS deps

# Install Java + build essentials trong 1 RUN layer để giảm layer count
RUN apt-get update && apt-get install -y --no-install-recommends \
      default-jre-headless \
      curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install

# Copy requirements first (leverage Docker layer cache — chỉ rebuild khi requirements.txt thay đổi)
COPY requirements.txt .

# Install prod deps, bỏ test deps
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
         $(grep -v '^\s*#\|^\s*$\|pytest\|pytest-mock' requirements.txt | tr '\n' ' ')

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Java runtime — required by PySpark driver
RUN apt-get update && apt-get install -y --no-install-recommends \
      default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark (Spark requires it explicitly in some configs)
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy installed Python packages from deps stage
COPY --from=deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=deps /usr/local/bin /usr/local/bin

# Copy application code (exclude tests, cache, pycache via .dockerignore)
COPY . /app/dataops_processing
COPY __init__.py /app/__init__.py

# Ensure dataops_processing is on Python path
ENV PYTHONPATH=/app

# Default: run VWAP streaming job via centralized runner
# Override: docker run ... volume-bars --symbol BTCUSDT
CMD ["python", "-m", "dataops_processing", "vwap"]
