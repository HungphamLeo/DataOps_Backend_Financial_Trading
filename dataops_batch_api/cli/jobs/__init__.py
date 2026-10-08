"""
cli/jobs — Scheduled ETL Jobs cho dataops_batch_api.

Mỗi job là một module độc lập, chứa đúng 1 trách nhiệm.
Tất cả jobs đều tuân theo pattern:
    1. Load config từ exchange_config.yaml
    2. Khởi tạo client (Binance / TCBS)
    3. Fetch data từ source API
    4. Validate qua Pydantic model
    5. Ghi vào Delta Lake (MinIO/S3A)
    6. Rolling retention nếu cần (xóa record cũ hơn ngưỡng)

Jobs:
    binance_4h_job   — [Daily 00:05 UTC] Binance 4h candles, 1 năm rolling
    binance_1d_job   — [Daily 00:05 UTC] Binance 1d candles, 10 năm append-only
    tcbs_eod_job     — [Daily 15:15 ICT] TCBS market EOD snapshot, 10 năm append-only
    tcbs_bsa_job     — [Daily 15:30 ICT] TCBS BSA daily summary, 2 năm rolling
"""
