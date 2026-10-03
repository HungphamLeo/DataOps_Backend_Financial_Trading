# ─── Stage 1: Build binary ─────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o dataops_backend_api .

# ─── Stage 2: Runtime image (Production Ready & Least Privilege) ──
FROM alpine:3.19 AS runtime

LABEL maintainer="DataOps Team"
LABEL description="DataOps Analytics Backend API"

RUN apk --no-cache add ca-certificates tzdata curl && \
    addgroup -S appgroup && \
    adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder /build/dataops_backend_api /app/dataops_backend_api

RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["/app/dataops_backend_api"]
