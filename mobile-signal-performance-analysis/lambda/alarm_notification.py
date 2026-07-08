"""
alarm_notification.py

AWS Lambda function that consumes records from the Kinesis stream (or from
the Kinesis Data Analytics output stream), evaluates them against
configurable thresholds, and publishes an alert via Amazon SNS (and/or SES
email) whenever a network-quality breach is detected.

Trigger: configure this function with a Kinesis event source mapping on
either the raw ingest stream or a downstream "alerts" stream produced by
the Kinesis Data Analytics SQL.

Environment variables:
    SNS_TOPIC_ARN         ARN of the SNS topic to publish alerts to
    LATENCY_THRESHOLD_MS       default 150
    PACKET_LOSS_THRESHOLD_PCT  default 3
    THROUGHPUT_MIN_MBPS        default 5
"""

import base64
import json
import os

import boto3

sns_client = boto3.client("sns")

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
LATENCY_THRESHOLD_MS = float(os.environ.get("LATENCY_THRESHOLD_MS", 150))
PACKET_LOSS_THRESHOLD_PCT = float(os.environ.get("PACKET_LOSS_THRESHOLD_PCT", 3))
THROUGHPUT_MIN_MBPS = float(os.environ.get("THROUGHPUT_MIN_MBPS", 5))


def evaluate_record(record: dict) -> list:
    """Return a list of human-readable breach descriptions, if any."""
    breaches = []

    latency = record.get("latency_ms")
    packet_loss = record.get("packet_loss_pct")
    throughput = record.get("throughput_mbps")
    network_drop = record.get("network_drop")

    if latency is not None and latency > LATENCY_THRESHOLD_MS:
        breaches.append(f"High latency: {latency} ms (threshold {LATENCY_THRESHOLD_MS} ms)")

    if packet_loss is not None and packet_loss > PACKET_LOSS_THRESHOLD_PCT:
        breaches.append(
            f"High packet loss: {packet_loss}% (threshold {PACKET_LOSS_THRESHOLD_PCT}%)"
        )

    if throughput is not None and throughput < THROUGHPUT_MIN_MBPS:
        breaches.append(
            f"Low throughput: {throughput} Mbps (min {THROUGHPUT_MIN_MBPS} Mbps)"
        )

    if network_drop:
        breaches.append("Network drop detected")

    return breaches


def publish_alert(record: dict, breaches: list):
    if not SNS_TOPIC_ARN:
        print("SNS_TOPIC_ARN not configured; skipping publish. Breaches:", breaches)
        return

    site_id = record.get("site_id", "unknown-site")
    subject = f"[Signal Alert] {site_id} - {len(breaches)} issue(s) detected"
    message_lines = [
        f"Site: {site_id} ({record.get('city', 'unknown city')})",
        f"Timestamp: {record.get('timestamp')}",
        f"Network type: {record.get('network_type')}",
        "",
        "Issues:",
    ] + [f"  - {b}" for b in breaches]

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],  # SNS subject limit
        Message="\n".join(message_lines),
    )


def lambda_handler(event, context):
    processed = 0
    alerted = 0

    for kinesis_record in event.get("Records", []):
        processed += 1
        raw = kinesis_record["kinesis"]["data"]
        payload = json.loads(base64.b64decode(raw))

        breaches = evaluate_record(payload)
        if breaches:
            publish_alert(payload, breaches)
            alerted += 1

    result = {"processed": processed, "alerted": alerted}
    print(json.dumps(result))
    return result
