-- real_time_query.sql
--
-- Kinesis Data Analytics (SQL application) queries for the mobile signal
-- performance pipeline. Assumes an input in-application stream named
-- "SOURCE_SQL_STREAM_001" mapped from the raw ingest Kinesis stream, with
-- columns matching the JSON fields produced by data-simulator/generate_signal_data.py:
--
--   record_id, "timestamp", site_id, city, lat, lon, network_type,
--   latency_ms, throughput_mbps, packet_loss_pct, speed_mbps,
--   range_km, network_drop

-- ---------------------------------------------------------------------
-- 1) Rolling 1-minute aggregation per site: average latency/throughput,
--    max packet loss, and drop count. Feeds the "site health" view and
--    the Tableau dashboard.
-- ---------------------------------------------------------------------
CREATE OR REPLACE STREAM "SITE_HEALTH_STREAM" (
    site_id           VARCHAR(32),
    city              VARCHAR(64),
    avg_latency_ms    DOUBLE,
    avg_throughput    DOUBLE,
    max_packet_loss   DOUBLE,
    drop_count        INTEGER,
    window_end        TIMESTAMP
);

CREATE OR REPLACE PUMP "SITE_HEALTH_PUMP" AS
INSERT INTO "SITE_HEALTH_STREAM"
SELECT STREAM
    site_id,
    city,
    AVG(latency_ms)       AS avg_latency_ms,
    AVG(throughput_mbps)  AS avg_throughput,
    MAX(packet_loss_pct)  AS max_packet_loss,
    SUM(CASE WHEN network_drop THEN 1 ELSE 0 END) AS drop_count,
    STEP("SOURCE_SQL_STREAM_001"."ROWTIME" BY INTERVAL '60' SECOND) AS window_end
FROM "SOURCE_SQL_STREAM_001"
GROUP BY
    site_id,
    city,
    STEP("SOURCE_SQL_STREAM_001"."ROWTIME" BY INTERVAL '60' SECOND);

-- ---------------------------------------------------------------------
-- 2) Anomaly stream: rows that breach quality thresholds, forwarded to
--    a downstream Kinesis stream that triggers the alarm Lambda.
-- ---------------------------------------------------------------------
CREATE OR REPLACE STREAM "ANOMALY_STREAM" (
    record_id        VARCHAR(64),
    site_id          VARCHAR(32),
    city             VARCHAR(64),
    lat              DOUBLE,
    lon              DOUBLE,
    latency_ms       DOUBLE,
    packet_loss_pct  DOUBLE,
    throughput_mbps  DOUBLE,
    network_drop     BOOLEAN,
    event_time       TIMESTAMP
);

CREATE OR REPLACE PUMP "ANOMALY_PUMP" AS
INSERT INTO "ANOMALY_STREAM"
SELECT STREAM
    record_id,
    site_id,
    city,
    lat,
    lon,
    latency_ms,
    packet_loss_pct,
    throughput_mbps,
    network_drop,
    "timestamp"
FROM "SOURCE_SQL_STREAM_001"
WHERE
    latency_ms > 150
    OR packet_loss_pct > 3
    OR throughput_mbps < 5
    OR network_drop = TRUE;

-- ---------------------------------------------------------------------
-- 3) Geospatial density view: count of readings per site over a 5-minute
--    tumbling window, used to highlight high-user-density / poor-signal
--    regions on the Tableau heat map.
-- ---------------------------------------------------------------------
CREATE OR REPLACE STREAM "GEO_DENSITY_STREAM" (
    site_id       VARCHAR(32),
    lat           DOUBLE,
    lon           DOUBLE,
    reading_count INTEGER,
    window_end    TIMESTAMP
);

CREATE OR REPLACE PUMP "GEO_DENSITY_PUMP" AS
INSERT INTO "GEO_DENSITY_STREAM"
SELECT STREAM
    site_id,
    lat,
    lon,
    COUNT(*) AS reading_count,
    STEP("SOURCE_SQL_STREAM_001"."ROWTIME" BY INTERVAL '5' MINUTE) AS window_end
FROM "SOURCE_SQL_STREAM_001"
GROUP BY
    site_id,
    lat,
    lon,
    STEP("SOURCE_SQL_STREAM_001"."ROWTIME" BY INTERVAL '5' MINUTE);
