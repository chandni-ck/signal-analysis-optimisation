# Mobile Signal Performance Analysis and Optimization

A near real-time communication signal analysis pipeline built on AWS Serverless
services. The system ingests, processes, stores, and visualizes key mobile
network performance metrics — **latency, throughput, packet loss, speed,
range, and network drops** — to help network providers identify and resolve
signal quality issues geographically and proactively.

> Originating project report: *Work Placement and Professional Practice —
> Final Project Report* (Chandni Chandrasekharan Kalathilparambil,
> Swathi Thekke Kambarath).

## Architecture

```
                     ┌───────────────────────┐
   Simulated / Real  │  Amazon Kinesis        │
   Mobile Signal Data├─▶  Data Streams        │
   (Python producer) │  (ingestion)           │
                     └──────────┬─────────────┘
                                │
                     ┌──────────▼─────────────┐
                     │ Amazon Kinesis Data     │
                     │ Analytics (SQL)         │
                     │ filter / aggregate /    │
                     │ transform in real time  │
                     └──────────┬─────────────┘
                                │
                ┌───────────────┼───────────────┐
                │               │               │
     ┌──────────▼───────┐ ┌─────▼──────┐ ┌──────▼───────────┐
     │ AWS Lambda        │ │ Kinesis    │ │ Amazon S3 /       │
     │ Anomaly detection  │ │ Data       │ │ Amazon Redshift   │
     │ + SNS/SES alerting │ │ Firehose   │ │ (archive & OLAP)  │
     └────────────────────┘ └─────┬──────┘ └───────────────────┘
                                   │
                          ┌────────▼─────────┐
                          │ Tableau           │
                          │ real-time         │
                          │ geospatial dash   │
                          └───────────────────┘
```

## Repository layout

```
mobile-signal-performance-analysis/
├── data-simulator/
│   └── generate_signal_data.py     # synthetic mobile signal data producer → Kinesis
├── kinesis-analytics/
│   └── real_time_query.sql         # in-stream SQL for filtering/aggregation
├── lambda/
│   ├── alarm_notification.py       # anomaly detection + SNS/SES alerting
│   └── requirements.txt
├── infrastructure/
│   └── template.yaml               # CloudFormation stack (Kinesis, Firehose, Lambda, S3, Redshift, SNS)
├── docs/
│   └── architecture.md             # detailed architecture notes / diagram source
├── requirements.txt                 # top-level Python deps (simulator + tooling)
├── .gitignore
└── README.md
```

## Getting started

### 1. Prerequisites
- AWS account with permissions for Kinesis, Lambda, Firehose, S3, Redshift, SNS/SES
- AWS CLI configured (`aws configure`)
- Python 3.10+
- (Optional) Tableau Desktop/Server with a connector to your Firehose/S3/Redshift destination

### 2. Deploy the infrastructure
```bash
aws cloudformation deploy \
  --template-file infrastructure/template.yaml \
  --stack-name mobile-signal-analysis \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides AlertEmail=you@example.com
```

### 3. Install Python dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Run the synthetic data producer
```bash
python data-simulator/generate_signal_data.py \
  --stream-name mobile-signal-stream \
  --region eu-west-1 \
  --rate 5
```

### 5. Deploy the Kinesis Data Analytics SQL
Load `kinesis-analytics/real_time_query.sql` into your Kinesis Data Analytics
application (via console or `aws kinesisanalyticsv2` CLI) with the ingest
stream as input.

### 6. Connect Tableau
Point Tableau at the Firehose destination (S3/Redshift) and enable
auto-refresh (Data Source → Refresh Now schedule, or the extract API) so
dashboards reflect near real-time data.

## Core metrics tracked
| Metric | Description |
|---|---|
| Latency | Time for data to travel from source to destination |
| Throughput | Volume of data transmitted successfully per unit time |
| Packet Loss | Data packets lost in transmission |
| Speed | Data transfer rate |
| Range | Geographic coverage of the signal |
| Network Drops | Count of unexpected disconnections |

## Challenges addressed
- **No real-time data source available** → built a configurable Python
  synthetic data generator (`data-simulator/generate_signal_data.py`).
- **AWS serverless service selection uncertainty** → resolved via
  prototyping, documented in `docs/architecture.md`.
- **AWS QuickSight limitations** → migrated visualization layer to Tableau
  for richer geospatial heat maps.

## License
MIT — see `LICENSE`.
