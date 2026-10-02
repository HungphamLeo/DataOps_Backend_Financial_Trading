# ─── Stage 1: Build binary ─────────────────────────────────────────
FROM golang:1.27-alpine AS builder

WORKDIR /build

# Cài đặt git và build-base nếu cần compile CGO/dependencies
RUN apk add --no-cache git ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o dataops-stream-producer .

# ─── Stage 2: Runtime image (Production Ready & Least Privilege) ──
FROM alpine:3.19 AS runtime

LABEL maintainer="DataOps Team"
LABEL description="DataOps Stream Producer — Binance & VPS/TCBS Market Data Streamer"

RUN apk --no-cache add ca-certificates tzdata curl && \
    addgroup -S appgroup && \
    adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder /build/dataops-stream-producer /app/dataops-stream-producer
COPY --from=builder /build/pkg/schemas /app/pkg/schemas

RUN chown -R appuser:appgroup /app

USER appuser

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
  CMD ps aux | grep -v grep | grep dataops-stream-producer || exit 1

CMD ["/app/dataops-stream-producer"]
