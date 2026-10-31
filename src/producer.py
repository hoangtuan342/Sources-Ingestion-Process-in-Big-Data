"""Generate realistic traffic telemetry and publish it to Kafka."""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

ROADS = [
    ("D1-NGUYEN-HUE", "Quận 1", 10.7731, 106.7030),
    ("D3-VO-VAN-TAN", "Quận 3", 10.7798, 106.6926),
    ("BTH-DIEN-BIEN-PHU", "Bình Thạnh", 10.8012, 106.7108),
    ("TDU-QUOC-LO-13", "Thủ Đức", 10.8505, 106.7536),
    ("Q7-NGUYEN-VAN-LINH", "Quận 7", 10.7290, 106.7218),
    ("TAN-BINH-CONG-HOA", "Tân Bình", 10.8015, 106.6408),
]
running = True


def stop(_signum: int, _frame: object) -> None:
    global running
    running = False


def make_event(sequence: int, rng: random.Random) -> dict[str, object]:
    road_id, district, latitude, longitude = rng.choice(ROADS)
    event_type = rng.choices(["gps_position", "road_sensor", "traffic_incident"], weights=[70, 20, 10])[0]
    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "event_type": event_type,
        "source": {"gps_position": "simulated_vehicle_gps", "road_sensor": "simulated_loop_sensor", "traffic_incident": "simulated_control_center"}[event_type],
        "road_id": road_id,
        "district": district,
        "latitude": round(latitude + rng.uniform(-0.002, 0.002), 6),
        "longitude": round(longitude + rng.uniform(-0.002, 0.002), 6),
        "sequence": sequence,
    }
    if event_type == "gps_position":
        # Periodically inject slow vehicle traffic for a realistic event mix.
        slow_period = (sequence // 500) % 4 == 2
        speed = rng.uniform(4, 14) if slow_period else rng.uniform(18, 55)
        event["payload"] = {
            "vehicle_id": f"BUS-{rng.randint(1, 120):03d}",
            "speed_kmh": round(speed, 1),
            "occupancy_pct": rng.randint(5, 100),
            "traffic_condition": "slow" if speed < 15 else "normal",
        }
    elif event_type == "road_sensor":
        event["payload"] = {
            "sensor_id": f"LOOP-{rng.randint(1, 80):03d}",
            "average_speed_kmh": round(rng.uniform(5, 55), 1),
            "vehicle_count_1m": rng.randint(0, 90),
            "rainfall_mm_h": round(rng.choice([0, 0, 0, rng.uniform(0.2, 18)]), 1),
        }
    else:
        event["payload"] = {
            "incident_id": str(uuid.uuid4()),
            "incident_type": rng.choice(["roadwork", "minor_collision", "blocked_lane", "signal_failure"]),
            "severity": rng.choice(["low", "medium", "high"]),
            "reported_by": "traffic_control_center",
        }
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="traffic-events")
    parser.add_argument("--rate", type=float, default=20, help="Target events per second; 0 means as fast as possible")
    parser.add_argument("--count", type=int, default=0, help="Stop after N events; 0 runs until Ctrl+C")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    if args.rate < 0 or args.count < 0:
        parser.error("--rate and --count must be non-negative")

    rng = random.Random(args.seed)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=5,
        compression_type="gzip",
        client_id="traffic-event-producer",
    )
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent = 0
    acknowledged = 0
    failed = 0
    pending = []
    started = time.perf_counter()
    next_tick = started
    print(f"Producer connected | topic={args.topic} | target_rate={args.rate or 'max'} events/s", flush=True)
    try:
        while running and (args.count == 0 or sent < args.count):
            event = make_event(sent + 1, rng)
            future = producer.send(args.topic, key=event["road_id"], value=event)
            pending.append(future)
            sent += 1
            if len(pending) >= 100 or (args.count and sent == args.count):
                producer.flush()
                for result in pending:
                    try:
                        result.get(timeout=30)
                        acknowledged += 1
                    except KafkaError as exc:
                        failed += 1
                        print(f"Broker acknowledgement failed: {exc}", file=sys.stderr, flush=True)
                pending.clear()
                elapsed = max(time.perf_counter() - started, 0.001)
                print(
                    f"sent={sent} | acknowledged={acknowledged} | failed={failed} "
                    f"| ack_rate={acknowledged / elapsed:.1f} events/s",
                    flush=True,
                )
            if args.rate:
                next_tick += 1 / args.rate
                time.sleep(max(0, next_tick - time.perf_counter()))
        producer.flush(timeout=30)
    except KafkaError as exc:
        print(f"Kafka publish failed: {exc}", file=sys.stderr)
        return 1
    finally:
        producer.close(timeout=10)

    elapsed = max(time.perf_counter() - started, 0.001)
    print(
        f"Producer stopped | sent={sent} | acknowledged={acknowledged} | failed={failed} "
        f"| elapsed={elapsed:.2f}s | ack_rate={acknowledged / elapsed:.1f} events/s",
        flush=True,
    )
    return 0 if acknowledged == sent and failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
