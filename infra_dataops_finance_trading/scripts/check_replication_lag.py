#!/usr/bin/env python3
import os
import sys
import logging
import mysql.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger("dataops.infrastructure.check_replication_lag")

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "binance_analytics")

LAG_WARNING_SECONDS = int(os.getenv("LAG_WARNING_SECONDS", "60"))
BINLOG_WARNING_BYTES = int(os.getenv("BINLOG_WARNING_BYTES", str(10 * 1024 * 1024 * 1024)))  # 10GB


def get_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        autocommit=True,
        connection_timeout=5,
    )


def get_replica_lag(cursor):
    cursor.execute("SHOW SLAVE STATUS")
    row = cursor.fetchone()
    if not row:
        logger.info("MySQL is not configured as replica; skipping replica lag check")
        return 0

    columns = [c[0] for c in cursor.description]
    info = dict(zip(columns, row))
    lag = info.get("Seconds_Behind_Master")
    if lag is None:
        return 0
    try:
        return int(lag)
    except (TypeError, ValueError):
        return 0


def get_binlog_size_bytes(cursor):
    cursor.execute("SHOW BINARY LOGS")
    rows = cursor.fetchall()
    if not rows:
        return 0

    total = 0
    for row in rows:
        try:
            total += int(row[1])
        except (TypeError, ValueError, IndexError):
            pass
    return total


def main():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        lag_seconds = get_replica_lag(cursor)
        binlog_bytes = get_binlog_size_bytes(cursor)

        logger.info("replica_lag_seconds=%s binlog_total_bytes=%s", lag_seconds, binlog_bytes)

        if lag_seconds > LAG_WARNING_SECONDS:
            logger.warning("ALERT: MySQL replica lag is %ss > %ss", lag_seconds, LAG_WARNING_SECONDS)
            return 2

        if binlog_bytes > BINLOG_WARNING_BYTES:
            logger.warning("ALERT: MySQL binlog total is %s bytes > %s bytes", binlog_bytes, BINLOG_WARNING_BYTES)
            return 2

        logger.info("OK: MySQL replication healthy")
        return 0

    except Exception as exc:
        logger.exception("Failed to check MySQL replication health: %s", exc)
        return 1

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())