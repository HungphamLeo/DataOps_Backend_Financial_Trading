#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone
from kafka import KafkaConsumer

def decode_value(value):
    if value is None:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except Exception:
        return value.decode("utf-8", errors="replace")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--from-beginning", action="store_true")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,
        consumer_timeout_ms=10000
    )

    seen = 0
    for msg in consumer:
        headers = {k: v.decode("utf-8", errors="replace") for k, v in (msg.headers or [])}
        payload = decode_value(msg.value)

        print(f"\n=== message {seen+1} ===")
        print(f"key={msg.key.decode('utf-8', errors='replace') if msg.key else None}")
        print(f"topic={msg.topic}")
        print(f"headers={headers}")
        print(f"payload={json.dumps(payload, ensure_ascii=False, default=str)[:2000]}")

        seen += 1
        if seen >= args.limit:
            break

# if __name__ == "__main__":
#     main()