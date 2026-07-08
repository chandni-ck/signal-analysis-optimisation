"""
generate_signal_data.py

Synthetic mobile-signal data producer for Amazon Kinesis Data Streams.

Generates near-real-time records representing mobile network performance
metrics (latency, throughput, packet loss, speed, range, network drops)
across a set of simulated cell/geo locations, and pushes them onto a
Kinesis stream so the rest of the pipeline (Kinesis Data Analytics ->
Lambda -> Firehose -> S3/Redshift/Tableau) can be exercised end to end
without needing a live carrier feed.

Usage:
    python generate_signal_data.py --stream-name mobile-signal-stream \
        --region eu-west-1 --rate 5 --duration 0

    --duration 0 runs indefinitely (Ctrl+C to stop).
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

# A handful of simulated cell tower locations (lat/lon) to give the
# geospatial visualizations something realistic to plot.
LOCATIONS = [
    {"site_id": "IE-DUB-01", "lat": 53.3498, "lon": -6.2603, "city": "Dublin"},
    {"site_id": "IE-COR-01", "lat": 51.8985, "lon": -8.4756, "city": "Cork"},
    {"site_id": "IE-GAL-01", "lat": 53.2707, "lon": -9.0568, "city": "Galway"},
    {"site_id": "IE-LIM-01", "lat": 52.6638, "lon": -8.6267, "city": "Limerick"},
    {"site_id": "IE-DRO-01", "lat": 53.7189, "lon": -6.3478, "city": "Drogheda"},
]

NETWORK_TYPES = ["4G", "5G", "5G-mmWave"]


def make_record(degrade_probability: float = 0.08) -> dict:
    """Build one synthetic signal-quality record.

    With small probability, simulate a "bad" reading (degraded signal)
    so downstream anomaly detection / alerting has something to catch.
    """
    site = random.choice(LOCATIONS)
    degraded = random.random() < degrade_probability

    if degraded:
        latency_ms = round(random.uniform(150, 400), 2)
        throughput_mbps = round(random.uniform(0.5, 5), 2)
        packet_loss_pct = round(random.uniform(3, 15), 2)
        speed_mbps = round(random.uniform(0.5, 8), 2)
        range_km = round(random.uniform(0.1, 1.0), 2)
        network_drop = random.random() < 0.4
    else:
        latency_ms = round(random.uniform(5, 60), 2)
        throughput_mbps = round(random.uniform(20, 500), 2)
        packet_loss_pct = round(random.uniform(0, 1.5), 2)
        speed_mbps = round(random.uniform(15, 900), 2)
        range_km = round(random.uniform(1.0, 6.0), 2)
        network_drop = False

    return {
        "record_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "site_id": site["site_id"],
        "city": site["city"],
        "lat": site["lat"],
        "lon": site["lon"],
        "network_type": random.choice(NETWORK_TYPES),
        "latency_ms": latency_ms,
        "throughput_mbps": throughput_mbps,
        "packet_loss_pct": packet_loss_pct,
        "speed_mbps": speed_mbps,
        "range_km": range_km,
        "network_drop": network_drop,
    }


def run(stream_name: str, region: str, rate_per_sec: float, duration_sec: int, dry_run: bool):
    client = None
    if not dry_run:
        import boto3  # imported lazily so --dry-run works without boto3 installed
        client = boto3.client("kinesis", region_name=region)
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 1.0
    start = time.time()
    sent = 0

    print(f"Starting producer -> stream='{stream_name}' region='{region}' "
          f"rate={rate_per_sec}/s dry_run={dry_run}")

    try:
        while True:
            record = make_record()
            payload = json.dumps(record).encode("utf-8")

            if dry_run:
                print(payload.decode("utf-8"))
            else:
                client.put_record(
                    StreamName=stream_name,
                    Data=payload,
                    PartitionKey=record["site_id"],
                )

            sent += 1
            if duration_sec and (time.time() - start) >= duration_sec:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"Stopped. Sent {sent} records.")


def parse_args():
    parser = argparse.ArgumentParser(description="Simulated mobile signal data producer")
    parser.add_argument("--stream-name", default="mobile-signal-stream",
                         help="Kinesis Data Stream name")
    parser.add_argument("--region", default="eu-west-1", help="AWS region")
    parser.add_argument("--rate", type=float, default=5.0,
                         help="Records per second to emit")
    parser.add_argument("--duration", type=int, default=0,
                         help="Seconds to run (0 = run until Ctrl+C)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Print records instead of sending to Kinesis "
                              "(useful for local testing without AWS credentials)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.stream_name, args.region, args.rate, args.duration, args.dry_run)
