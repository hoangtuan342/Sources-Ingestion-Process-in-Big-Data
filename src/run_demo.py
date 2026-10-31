"""Run a reproducible Kafka ingestion demo and save its console evidence."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_LOCK = threading.Lock()


def write_and_print(log, line: str) -> None:
    with OUTPUT_LOCK:
        log.write(line + "\n")
        log.flush()
        print(line, flush=True)


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap.split(","), client_id="ingestion-demo-admin")
    try:
        try:
            admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
        description = admin.describe_topics([topic])[0]
        actual = len(description["partitions"])
        if actual != partitions:
            raise RuntimeError(f"Topic {topic} has {actual} partitions; expected {partitions}.")
    finally:
        admin.close()


def run_level(log, bootstrap: str, topic: str, rate: float, count: int, run_id: str, sample: int) -> None:
    group = f"ingestion-demo-{run_id}-{str(rate).replace('.', '_')}"
    consumer_cmd = [
        sys.executable, "-u", str(ROOT / "src" / "consumer.py"),
        "--bootstrap", bootstrap, "--topic", topic, "--group", group,
        "--count", str(count), "--sample", str(sample),
    ]
    producer_cmd = [
        sys.executable, "-u", str(ROOT / "src" / "producer.py"),
        "--bootstrap", bootstrap, "--topic", topic, "--count", str(count),
        "--rate", str(rate), "--seed", str(int(rate * 1000) + count),
    ]
    write_and_print(log, f"\n=== LOAD TEST target_rate={rate:g} events/s count={count} ===")
    consumer = subprocess.Popen(
        consumer_cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    )
    ready = threading.Event()
    reader_errors: list[str] = []
    consumer_lines: list[str] = []

    def read_consumer() -> None:
        assert consumer.stdout is not None
        for raw_line in consumer.stdout:
            line = raw_line.rstrip("\r\n")
            consumer_lines.append(line)
            try:
                write_and_print(log, "CONSUMER " + line)
            except Exception as exc:
                reader_errors.append(f"could not write consumer output: {exc}")
                ready.set()
                return
            if line.startswith("READY "):
                ready.set()
        code = consumer.poll()
        if code not in (None, 0):
            reader_errors.append(f"consumer exited with code {code}")
        ready.set()

    reader = threading.Thread(target=read_consumer, daemon=True)
    reader.start()
    if not ready.wait(timeout=45):
        consumer.terminate()
        raise TimeoutError("Consumer did not become ready within 45 seconds")
    if consumer.poll() is not None:
        raise RuntimeError("Consumer exited before becoming ready: " + "; ".join(reader_errors))

    producer = subprocess.run(
        producer_cmd, cwd=ROOT, text=True, encoding="utf-8", errors="replace",
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=max(60, count / max(rate, 1) + 45),
    )
    for line in producer.stdout.splitlines():
        write_and_print(log, "PRODUCER " + line)
    if producer.returncode != 0:
        consumer.terminate()
        raise RuntimeError(f"Producer failed with exit code {producer.returncode}")

    try:
        consumer.wait(timeout=45)
    except subprocess.TimeoutExpired:
        consumer.terminate()
        raise TimeoutError("Consumer did not receive the expected event count")
    reader.join(timeout=5)
    received_marker = f"SUMMARY received={count} |"
    summary = next((line for line in consumer_lines if line.startswith(received_marker)), None)
    if summary is None:
        reader_errors.append(f"consumer did not confirm receiving exactly {count} events")
    else:
        required_quality = (
            f"| unique_ids={count} | duplicate_ids=0 | unique_sequences={count} "
            f"| duplicate_sequences=0 | invalid_sequences=0 | missing_sequences=0 |"
        )
        if required_quality not in summary:
            reader_errors.append("consumer integrity check found duplicate, invalid, or missing events")
        try:
            type_payload = summary.split("event_types=", 1)[1].split(" | partitions=", 1)[0]
            type_counts = json.loads(type_payload)
            if sum(type_counts.values()) != count:
                reader_errors.append("event_type counts do not add up to the received count")
            if count >= 100 and not {"gps_position", "road_sensor", "traffic_incident"}.issubset(type_counts):
                reader_errors.append("the run did not contain all three event types")
        except (IndexError, ValueError, TypeError) as exc:
            reader_errors.append(f"could not validate consumer event type counts: {exc}")
    if consumer.returncode != 0 or reader_errors:
        raise RuntimeError("; ".join(reader_errors) or f"Consumer failed with {consumer.returncode}")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="traffic-events")
    parser.add_argument("--rates", default="20,50,100", help="Comma-separated target events/second")
    parser.add_argument("--count", type=int, default=500, help="Events per rate")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--sample", type=int, default=2, help="Sample events printed per run")
    args = parser.parse_args()
    try:
        rates = [float(value.strip()) for value in args.rates.split(",")]
    except ValueError:
        parser.error("--rates must be comma-separated positive numbers")
    if not rates or any(rate <= 0 for rate in rates) or args.count < 1 or args.partitions < 1:
        parser.error("rates, count, and partitions must be positive")

    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = log_dir / f"ingestion-{run_id}.log"
    try:
        with log_path.open("w", encoding="utf-8", newline="\n") as log:
            write_and_print(log, "SOURCES -> INGESTION | Kafka producer/consumer evidence")
            write_and_print(log, f"bootstrap={args.bootstrap} | topic={args.topic} | partitions={args.partitions}")
            ensure_topic(args.bootstrap, args.topic, args.partitions)
            write_and_print(log, f"TOPIC_READY name={args.topic} | partitions={args.partitions} | replicas=1")
            for rate in rates:
                run_level(log, args.bootstrap, args.topic, rate, args.count, run_id, args.sample)
            write_and_print(log, "\nRESULT PASS | ACKs, counts, IDs, sequences, and event types verified")
            write_and_print(log, f"LOG {log_path.relative_to(ROOT)}")
        return 0
    except Exception as exc:
        with log_path.open("a", encoding="utf-8") as log:
            write_and_print(log, f"RESULT FAIL | {type(exc).__name__}: {exc}")
            write_and_print(log, f"LOG {log_path.relative_to(ROOT)}")
        print(f"Demo failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
