# Architecture Notes

## Data flow

1. **Ingestion** — `data-simulator/generate_signal_data.py` (or a real
   mobile-network feed) writes JSON records to an **Amazon Kinesis Data
   Stream** (`mobile-signal-stream`). Each record carries a site/geo
   identifier, network type, and the six core metrics: latency,
   throughput, packet loss, speed, range, and network-drop flag.

2. **Stream processing** — An **Amazon Kinesis Data Analytics** SQL
   application (`kinesis-analytics/real_time_query.sql`) reads the raw
   stream and produces three derived streams:
   - `SITE_HEALTH_STREAM` — rolling 1-minute per-site averages
   - `ANOMALY_STREAM` — records that breach configured thresholds
   - `GEO_DENSITY_STREAM` — 5-minute reading counts per location, used
     for the geospatial heat map

3. **Alerting** — `lambda/alarm_notification.py` is subscribed (directly,
   or via the `ANOMALY_STREAM`) to incoming records. It re-evaluates
   thresholds and publishes a formatted alert to an **Amazon SNS** topic,
   which fans out to email (and can be extended to Slack/SMS).

4. **Delivery & storage** — **Amazon Kinesis Data Firehose** delivers the
   stream to:
   - **Amazon S3** for long-term, cost-effective archival
   - **Amazon Redshift** for ad-hoc/analytical SQL over historical data

5. **Visualization** — **Tableau** connects to the Firehose destination
   (S3 or Redshift) and renders real-time geospatial dashboards with
   auto-refresh enabled, so network providers can see problem areas as
   they emerge.

## Why Tableau over AWS QuickSight

QuickSight was the original choice in the architecture but was replaced
after evaluation showed it couldn't produce the detailed, interactive
heat maps required for geospatial signal-quality analysis at the level
of granularity this project needed. Tableau's mapping and large-dataset
handling made it a better fit.

## Threshold configuration

Thresholds used by both the Kinesis Analytics anomaly stream and the
Lambda alarm function are:

| Metric | Threshold | Direction |
|---|---|---|
| Latency | 150 ms | breach if above |
| Packet loss | 3% | breach if above |
| Throughput | 5 Mbps | breach if below |
| Network drop | — | breach if true |

These are intentionally conservative defaults for demonstration —
production values should be tuned per network type (4G vs 5G vs
5G-mmWave) and per SLA.
