"""Consume the ingestion topic and report throughput and end-to-end latency."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import deque
from datetime import datetime

from kafka import KafkaConsumer


def event_age_ms(event: dict[str, object]) -> float | None:
    raw = event.get("event_time")
    if not isinstance(raw, str):
        return None
    try:
        event_time = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        now = datetime.now(event_time.tzinfo)
        return max(0.0, (now - event_time).total_seconds() * 1000)
    except ValueError:
        return None


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * p))]


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="traffic-events")
    parser.add_argument("--group", default="traffic-ingestion-demo")
    parser.add_argument("--sample", type=int, default=3, help="Print the first N received events")
    parser.add_argument("--count", type=int, default=0, help="Stop after N events; 0 runs continuously")
    args = parser.parse_args()
    if args.count < 0:
        parser.error("--count must be non-negative")
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap.split(","),
        group_id=args.group,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
        client_id="traffic-ingestion-monitor",
    )
    total = 0
    window_count = 0
    window_started = time.perf_counter()
    ages: list[float] = []
    latency_samples: deque[float] = deque(maxlen=10000)
    event_type_counts: dict[str, int] = {}
    event_ids: set[str] = set()
    sequences: set[int] = set()
    duplicate_event_ids = 0
    duplicate_sequences = 0
    invalid_sequences = 0
    samples = 0
    try:
        # Poll once so the group joins and partitions are assigned before the
        # producer starts; this avoids losing early events with latest offset.
        consumer.poll(timeout_ms=1000, max_records=1)
        print(
            f"READY topic={args.topic} | group={args.group} "
            f"| partitions={len(consumer.assignment())}",
            flush=True,
        )
        started = time.perf_counter()
        while True:
            batch = consumer.poll(timeout_ms=1000, max_records=1000)
            now = time.perf_counter()
            for records in batch.values():
                for record in records:
                    total += 1
                    window_count += 1
                    event_type = str(record.value.get("event_type", "unknown"))
                    event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
                    event_id = record.value.get("event_id")
                    if not isinstance(event_id, str) or not event_id:
                        duplicate_event_ids += 1
                    elif event_id in event_ids:
                        duplicate_event_ids += 1
                    else:
                        event_ids.add(event_id)
                    sequence = record.value.get("sequence")
                    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1 or (args.count and sequence > args.count):
                        invalid_sequences += 1
                    elif sequence in sequences:
                        duplicate_sequences += 1
                    else:
                        sequences.add(sequence)
                    age = event_age_ms(record.value)
                    if age is not None:
                        ages.append(age)
                        latency_samples.append(age)
                    if samples < args.sample:
                        print("EVENT " + json.dumps(record.value, ensure_ascii=False), flush=True)
                        samples += 1
                    if args.count and total >= args.count:
                        break
                if args.count and total >= args.count:
                    break
            if now - window_started >= 5:
                duration = now - window_started
                rate = window_count / duration
                ordered = sorted(ages)
                p95 = ordered[min(len(ordered) - 1, round((len(ordered) - 1) * 0.95))] if ordered else 0.0
                print(
                    f"METRICS total={total} | window_rate={rate:.1f} events/s "
                    f"| event_to_consume_p95={p95:.1f} ms | partitions={len(consumer.assignment())}",
                    flush=True,
                )
                window_count = 0
                window_started = now
                ages.clear()
            if args.count and total >= args.count:
                elapsed = max(time.perf_counter() - started, 0.001)
                ordered = sorted(latency_samples)
                p50 = ordered[min(len(ordered) - 1, round((len(ordered) - 1) * 0.50))] if ordered else 0.0
                p95 = ordered[min(len(ordered) - 1, round((len(ordered) - 1) * 0.95))] if ordered else 0.0
                expected_sequences = args.count if args.count else len(sequences)
                missing_sequences = sum(1 for number in range(1, expected_sequences + 1) if number not in sequences)
                print(
                    f"SUMMARY received={total} | elapsed={elapsed:.2f}s "
                    f"| avg_rate={total / elapsed:.1f} events/s "
                    f"| event_to_consume_p50={p50:.1f} ms | event_to_consume_p95={p95:.1f} ms "
                    f"| latency_sample_count={len(ordered)} "
                    f"| unique_ids={len(event_ids)} | duplicate_ids={duplicate_event_ids} "
                    f"| unique_sequences={len(sequences)} | duplicate_sequences={duplicate_sequences} "
                    f"| invalid_sequences={invalid_sequences} | missing_sequences={missing_sequences} "
                    f"| event_types={json.dumps(event_type_counts, sort_keys=True)} "
                    f"| partitions={len(consumer.assignment())}",
                    flush=True,
                )
                break
    except KeyboardInterrupt:
        print("\nConsumer stopped by user", flush=True)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
