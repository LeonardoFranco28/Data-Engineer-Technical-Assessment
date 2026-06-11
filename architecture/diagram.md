# Architecture Diagram — Telecom Proximity Analytics

## Mermaid Overview

```mermaid
flowchart TD
    subgraph SOURCES["Data Sources (Daily)"]
        GEO["Geo Table\n(Parquet)"]
        LABELS["Labels Table\n(Parquet)"]
        FUTURE["Future Sources"]
    end

    subgraph INGEST["Ingestion Layer"]
        CRAWLER["AWS Glue Crawler\nauto-discover schemas"]
    end

    subgraph RAW["Raw Layer — S3"]
        RAW_S3["s3://telecom-raw/\ngeo/ · labels/\ndate=YYYY-MM-DD"]
    end

    subgraph ORCH["Orchestration — AWS Step Functions"]
        SF_START(["Start"])
        SF_PROF["Profiling\nLambda"]
        SF_ETL["ETL\nAWS Glue"]
        SF_DQ["DQ Check\nLambda"]
        SF_OUT["Write Output"]
        SF_SNS["SNS Alert"]
    end

    subgraph TRANSFORM["Transformation — AWS Glue PySpark"]
        T1["1. Data Profiling"]
        T2["2. Deduplication"]
        T3["3. Join geo + labels"]
        T4["4. Spatial Optimization\ngeohashing / grid-cell"]
        T5["5. Distance Calc\n50m radius pairs"]
        T6["6. Output Schema\nDS wide table · BI summary"]
    end

    subgraph STAGING["Staging Layer — S3"]
        STG["s3://telecom-staging/\ndate=YYYY-MM-DD\nParquet cleaned"]
    end

    subgraph ANALYTICS["Analytics Layer — S3"]
        ANA["s3://telecom-analytics/\ndate= · comuna=\nDelta Lake features"]
    end

    subgraph CURATED["Curated Layer — S3"]
        CUR1["proximity_summary/\ncomunal=  event_type=\nDelta Lake BI-ready"]
        CUR2["commune_metrics/\nDelta Lake aggregated"]
    end

    subgraph DS_ACCESS["DS Access"]
        SM["SageMaker Studio\nJupyterLab · PySpark\nDirect S3 read"]
    end

    subgraph BI_ACCESS["BI Access"]
        ATHENA["Amazon Athena\nServerless SQL"]
        QS["Amazon QuickSight\nBI Dashboards"]
    end

    subgraph MONITOR["Monitoring & Alerting"]
        CW["CloudWatch\nAlarms"]
        SNS["AWS SNS\nTopics"]
        NOTIF["Slack / Email"]
    end

    GEO & LABELS & FUTURE --> CRAWLER
    CRAWLER --> RAW_S3
    RAW_S3 --> SF_START
    SF_START --> SF_PROF --> SF_ETL --> SF_DQ
    SF_DQ -->|success| SF_OUT
    SF_DQ -->|failure| SF_SNS
    SF_ETL --> T1 --> T2 --> T3 --> T4 --> T5 --> T6
    T6 --> STG
    T6 --> ANA
    STG --> CUR1 & CUR2
    ANA --> SM
    CUR1 & CUR2 --> ATHENA --> QS
    CW --> SNS --> NOTIF
    SF_SNS --> NOTIF
```

---

## ASCII Detail

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCES (Daily)                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                       │
│  │  Geo Table    │    │ Labels Table │    │  Future ...  │                       │
│  │  (Parquet)    │    │  (Parquet)   │    │              │                       │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                       │
└─────────┼──────────────────┼──────────────────┼─────────────────────────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    AWS Glue Crawler (auto-discover schemas)             │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────┬────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              RAW LAYER (S3)                                     │
│                                                                                 │
│  s3://telecom-raw/                                                              │
│  ├── geo/                                                                       │
│  │   └── date=YYYY-MM-DD/     (Parquet, append-only)                           │
│  ├── labels/                                                                    │
│  │   └── date=YYYY-MM-DD/     (Parquet, append-only)                           │
│  └── future_source/                                                             │
│      └── date=YYYY-MM-DD/     (Parquet, append-only)                           │
└────────────────────────────────────┬────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION (AWS Step Functions)                        │
│                                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐                   │
│  │  Start    │───▶│ Profiling│───▶│   ETL    │───▶│  DQ Check│                   │
│  │          │    │ (Lambda) │    │  (Glue)  │    │ (Lambda) │                   │
│  └──────────┘    └──────────┘    └──────────┘    └─────┬────┘                   │
│                                                        │                         │
│                              ┌──────────────────────────┼──────────────┐         │
│                              │ success                  │ failure      │         │
│                              ▼                          ▼              │         │
│                    ┌──────────────┐            ┌──────────────┐       │         │
│                    │ Write Output │            │  SNS Alert   │       │         │
│                    └──────────────┘            └──────────────┘       │         │
└────────────────────────────────────┬────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     TRANSFORMATION (AWS Glue - PySpark)                          │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  1. Data Profiling (row counts, NULLs, duplicates, coord ranges)       │    │
│  │  2. Deduplication (geo: first comuna/lat/lon; labels: aggregate events) │    │
│  │  3. Join geo + labels on customer_id                                    │    │
│  │  4. Spatial optimization (geohashing / grid-cell bucketing)             │    │
│  │  5. Distance calculation (50m radius pairs)                             │    │
│  │  6. Output schema definition (DS wide table / BI summary)              │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────┬────────────────────────────────────────────┘
                                     │
          ┌──────────────────────────┴──────────────────────────┐
          │                                                     │
          ▼                                                     ▼
┌─────────────────────────────┐                   ┌─────────────────────────────┐
│    STAGING LAYER (S3)       │                   │    ANALYTICS LAYER (S3)      │
│                             │                   │                             │
│  s3://telecom-staging/      │                   │  s3://telecom-analytics/     │
│  └── date=YYYY-MM-DD/       │                   │  └── date=YYYY-MM-DD/        │
│      (Parquet, cleaned)     │                   │      comuna=                 │
│                             │                   │      (Delta Lake, features)  │
└─────────────────────────────┘                   └──────────────┬──────────────┘
                                                                 │
                                                                 │
                              ┌──────────────────────────────────┤
                              │                                  │
                              ▼                                  ▼
┌─────────────────────────────────────────┐   ┌─────────────────────────────────┐
│    CURATED LAYER (S3)                   │   │    DS ACCESS (SageMaker)        │
│                                         │   │                                 │
│  s3://telecom-curated/                  │   │  ┌─────────────────────────┐    │
│  ├── proximity_summary/                 │   │  │  SageMaker Studio       │    │
│  │   ├── comuna=                        │   │  │  - JupyterLab notebooks │    │
│  │   │   └── event_type=                │   │  │  - PySpark kernel       │    │
│  │   │       (Delta Lake, BI-ready)     │   │  │  - Direct S3 read       │    │
│  │   │                                  │   │  └─────────────────────────┘    │
│  └── commune_metrics/                   │   │                                 │
│      (Delta Lake, aggregated)           │   │  Reads: s3://telecom-analytics/ │
└──────────────────┬──────────────────────┘   └─────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         BI ACCESS (Amazon Athena)                                │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Amazon Athena (Serverless SQL)                                         │    │
│  │  - Glue Data Catalog auto-discovers Delta tables                       │    │
│  │  - Pay-per-query; no cluster management                                │    │
│  │  - Integrates with QuickSight for dashboards                           │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Amazon QuickSight                                                      │    │
│  │  - BI dashboards connected to Athena                                    │    │
│  │  - Automated reporting for business stakeholders                        │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         MONITORING & ALERTING                                    │
│                                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                       │
│  │ CloudWatch   │───▶│  AWS SNS     │───▶│ Slack/Email  │                       │
│  │  Alarms      │    │  Topics      │    │  Channels    │                       │
│  └──────────────┘    └──────────────┘    └──────────────┘                       │
│                                                                                 │
│  Alerts: Pipeline failure, DQ breach, Late ingestion, Cost anomaly              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Summary

```
Daily Ingestion Flow:
  Source Parquet → Glue Crawler → Raw S3 (partitioned by date)
  
ETL Flow:
  Raw S3 → Glue ETL (PySpark) → Staging S3 → Analytics S3 (Delta)
                                            → Curated S3 (Delta)

Access Flow:
  Data Scientists → SageMaker Studio → reads Analytics layer (Delta)
  BI Analysts     → Athena/QuickSight → reads Curated layer (Delta)

Monitoring Flow:
  Step Functions → Lambda validators → SNS → Slack/Email
  CloudWatch → Alarms → SNS → Alerts
```
