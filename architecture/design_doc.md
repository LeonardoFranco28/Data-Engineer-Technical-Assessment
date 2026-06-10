# Architecture Design Document — Telecom Proximity Analytics

## 1. Planning Document

### 1.1 Main Risks and Constraints

| Risk / Constraint | Impact | Mitigation |
|---|---|---|
| **Combinatorial explosion** in distance calculations (millions × millions) | O(n²) cross-join is computationally infeasible at scale | Spatial partitioning (geohashing or grid-cell bucketing) to reduce candidate pairs |
| **Daily ingestion of millions of records** | Storage and compute costs grow linearly; query latency degrades | Partition by date; use columnar formats; auto-scaling compute |
| **Dual consumer access patterns** (Data Scientists vs BI Analysts) | One schema rarely fits both; notebook latency vs SQL BI dashboard latency differ | Separate Analytics layer (feature-rich for DS) and Curated layer (denormalized for BI) |
| **Data quality issues** (NULLs, duplicates, out-of-range coordinates) | Incorrect proximity calculations; misleading analytics | Profiling step before transformation; automated DQ checks in pipeline |
| **Weekly history requirement** | Need time-travel / versioning for output tables | `week=YYYY-WXX` partition in Analytics/Curated + Delta Lake time travel for point-in-time recovery |
| **Cost-effectiveness requirement** | Must justify every tool choice against budget | Prefer serverless/pay-per-use services where possible |
| **Projected coordinate system** | Source data uses UTM meters, not decimal degrees — Haversine formula does not apply | Use Euclidean distance directly; validate coordinate ranges against expected UTM bounds |

**Assumptions (out of scope for this assessment):**
- *Ingestion mechanism*: source systems deliver Parquet files directly to `s3://telecom-raw/` via S3 PUT. The mechanism (SFTP, API, direct write) depends on upstream systems and is not defined here.
- *PII governance*: customer IDs and commune names are pre-hashed at source (SHA-256). PII anonymization is handled upstream; no additional masking is required within this pipeline.

### 1.2 Design Principles

1. **Scalability** — every component must handle 10x current volume without re-architecture
2. **Cost transparency** — serverless first; clear cost model per layer
3. **Simplicity** — prefer managed services to reduce operational burden
4. **Extensibility** — schema and pipeline must support adding similar data sources (e.g., new event types, new geographies)
5. **Data quality first** — profiling and validation before any transformation
6. **Dual-access** — output optimized for both SQL analytics and notebook-based DS workflows

### 1.3 Cloud Provider Choice: AWS

**Justification:**

| Criterion | AWS Advantage |
|---|---|
| **Lakehouse maturity** | S3 + Glue + Lake Formation + Athena is a proven, cost-effective stack |
| **Serverless ETL** | AWS Glue handles Spark jobs without cluster management; pay-per-use |
| **SQL analytics** | Athena — serverless, pay-per-query, no cluster provisioning |
| **Data science access** | SageMaker Studio notebooks with direct S3 access |
| **Orchestration** | AWS Step Functions integrates natively with Glue, Athena, SNS |
| **Cost** | S3 Intelligent-Tiering auto-optimizes storage costs; Glue crawlers are serverless |
| **Lake governance** | Lake Formation provides fine-grained access control (column/row level) |

**Rejected alternatives:**
- **GCP** — BigQuery is excellent but lock-in risk is higher; Dataproc less mature than EMR/Glue for this workload.
- **Databricks/Snowflake** — strong platforms but higher per-seat costs; harder to justify for cost-effectiveness requirement unless heavy ML use case.
- **Hybrid** — adds networking complexity without clear benefit for a single-country, single-domain workload.

---

## 2. Architecture Components

### 2.1 Storage Tools and Layers

| Layer | Location | Format | Partitioning | Purpose |
|---|---|---|---|---|
| **Raw** | `s3://telecom-raw/geo/` and `s3://telecom-raw/labels/` | Parquet | `date=YYYY-MM-DD/` | Immutable source of truth; append-only daily ingestion |
| **Staging** | `s3://telecom-staging/` | Parquet | `date=YYYY-MM-DD/` | Cleaned and deduplicated only (no join, no distance calc); intermediate checkpoint |
| **Analytics** | `s3://telecom-analytics/proximity_output/` | Delta Lake | `week=YYYY-WXX/` | Full proximity feature table (join + distance calc output) for Data Scientists |
| **Curated** | `s3://telecom-curated/` | Delta Lake | `comuna=`, `event_type=` | Denormalized, BI-friendly summary tables for SQL analysts |

**Data flow per layer:**
```
Raw (daily Parquet) 
  → Glue ETL Step 1 → Staging (clean + dedup only, date= partition)
  → Glue ETL Step 2 → Analytics (join + spatial distance calc, week= partition)
  → Glue ETL Step 3 → Curated (aggregated summary, comuna= + event_type= partition)
```

**Partitioning strategy:**
- **Raw/Staging**: `date=YYYY-MM-DD/` — supports daily ingestion and reprocessing
- **Analytics**: `week=YYYY-WXX/` — each weekly run overwrites the partition; Delta time travel preserves prior versions for recovery
- **Curated**: `comuna=` + `event_type=` — optimized for BI dashboards filtering by commune and event type

**Weekly history implementation:**
- Daily ETL writes to `date=` in Staging (retained 30 days)
- Each run also writes/overwrites `week=YYYY-WXX/` in Analytics using Delta `MERGE INTO` (idempotent)
- DS queries: `spark.read.format("delta").load("s3://telecom-analytics/proximity_output/").filter("week = '2025-W02'")`
- BI queries: `WHERE week = '2025-W02'` via Athena on Curated layer

**Why Delta Lake for Analytics/Curated:**
- ACID transactions prevent partial writes
- `week=` partition + Delta time travel covers both business-week queries and point-in-time recovery
- Schema evolution handles new fields without rewriting data
- Z-ordering on `customer_id` accelerates point lookups

### 2.2 Transformation Tool: AWS Glue (PySpark)

**Justification:**
- Serverless — no cluster provisioning; auto-scales with data volume
- Native integration with S3 (Raw layer) and Delta Lake (output)
- PySpark support means the ETL script runs locally in dev and on Glue in prod with minimal changes
- Glue Data Catalog auto-discovers schemas, enabling Athena queries on any layer
- Cost: pay per DPU-hour; Glue 4.0 with Elastic Views reduces idle cost

**Rejected alternatives:**
- **EMR** — more control but requires cluster management; higher operational overhead
- **Databricks** — excellent but cost premium hard to justify for this pipeline
- **AWS DataBrew** — good for visual prep but not suitable for complex spatial joins

### 2.3 Orchestration: AWS Step Functions

**Justification:**
- Serverless state machine — no infrastructure to manage
- Native integration with Glue jobs, Lambda validators, SNS alerts
- Visual workflow editor for pipeline monitoring
- Error handling with retry/backoff built-in
- Cost: pay per state transition; no idle cost

**Pipeline DAG:**
```flowchart TD
    A[Start] --> B[Data Profiling<br/>Lambda]
    B --> C[Glue ETL Job]
    C --> D[Data Quality Check<br/>Lambda]

    D -->|Failure| E[SNS Alert]

    D -->|Success| F[Write to Analytics Layer]
    F --> G[Write to Curated Layer]
    G --> H[SNS Success Notification]
```

**Backfill strategy:**
```python
# scripts/backfill.py — trigger reprocessing for a date range
for date in pd.date_range("2025-01-03", "2025-01-05"):
    sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=f"backfill-{date.strftime('%Y-%m-%d')}",
        input=json.dumps({"date": date.strftime("%Y-%m-%d"), "mode": "backfill"})
    )
```
Each execution reads from `s3://telecom-raw/geo/date={date}/` and overwrites the corresponding `week=` partition using Delta `MERGE INTO`.

**Rejected alternatives:**
- **Airflow (MWAA)** — powerful DAG tooling but adds ~$300/month minimum cluster cost and operational overhead for a linear 5-step pipeline with no cross-DAG dependencies. Step Functions covers the same flow serverlessly. Airflow would be reconsidered if the pipeline grows beyond 15+ tasks or requires dynamic task mapping.
- **Prefect/Dagster** — good but less native AWS integration; adds external dependency
- **Lambda alone** — no built-in retry, chaining, or visual monitoring

### 2.4 Notification and Alerting: AWS SNS + CloudWatch

| Trigger | Condition | Action |
|---|---|---|
| **Pipeline failure** | Glue job status = FAILED | SNS → Slack/Email alert |
| **Data quality breach** | NULL rate > 5% on key fields; duplicate rate > 1% | SNS → Email to data quality team |
| **Late ingestion** | Raw data not landed by 06:00 UTC | CloudWatch Alarm → SNS |
| **Cost anomaly** | Glue DPU-hours exceed daily threshold | CloudWatch Alarm → SNS |
| **Pipeline success** | Daily ETL completes successfully | SNS → Slack notification (optional) |

**Implementation:**
- CloudWatch Alarms on Glue job metrics (DPU hours, rows processed)
- Step Functions error states trigger Lambda → SNS publish
- SNS topics: `data-pipeline-alerts`, `data-quality-alerts`, `pipeline-success`

### 2.5 Data Scientists Access: Amazon SageMaker Studio

**Justification:**
- Managed JupyterLab environment with direct S3 access
- Pre-built Spark kernels for large-scale data exploration
- Lineage to training pipelines (SageMaker Training, Processing)
- Git integration for version control

**How they query data:**
```python
# Read Analytics layer (Delta Lake)
df = spark.read.format("delta").load("s3://telecom-analytics/")

# Or use Athena for SQL exploration
# %spark.athena --query "SELECT * FROM telecom_analytics.features LIMIT 100"
```

**Access pattern:**
- Read-only access to `s3://telecom-analytics/` via IAM role
- Can create personal feature stores in `s3://telecom-ds-features/{username}/`
- Notebook templates pre-configured with common queries

### 2.6 Business Analysts Access: Amazon Athena

**Justification:**
- Serverless SQL — no cluster to manage; pay per query
- Direct query on Delta Lake tables in S3
- Glue Data Catalog auto-discovers table schemas
- Integrates with QuickSight for dashboards

**How they query data:**
```sql
-- Curated layer exposed as Athena table
SELECT commune, event_type, customer_count, proximity_events
FROM telecom_curated.proximity_summary
WHERE date >= '2025-01-01'
ORDER BY proximity_events DESC;
```

**Access pattern:**
- IAM-based access to Curated layer tables
- Glue Crawler runs after each ETL to update Data Catalog
- QuickSight dashboards connected to Athena for automated reporting
- Column-level security via Lake Formation for sensitive fields

### 2.7 Storage Format by Layer

| Layer | Format | Justification |
|---|---|---|
| **Raw** | Parquet | Columnar, compressed, widely compatible; immutable source data doesn't need ACID |
| **Staging** | Parquet | Intermediate format; fast write/read for Glue job chaining |
| **Analytics** | Delta Lake | ACID writes (prevents partial job failures); time travel for weekly history; Z-ordering for spatial queries; schema evolution for new features |
| **Curated** | Delta Lake | Same benefits; BI tools (Athena/QuickSight) read Delta natively |

**Why not Iceberg?**
Delta Lake has stronger AWS Glue integration (Glue 4.0 supports Delta natively). Iceberg is a strong alternative but adds catalog configuration overhead without clear benefit for this workload.

**Why not plain Parquet for Analytics/Curated?**
No ACID means partial writes corrupt data. No time travel means weekly history requires manual versioning. Delta Lake solves both at minimal cost overhead.

**Note on coordinate system:**
Source data uses projected UTM coordinates (meters), not decimal degrees. Valid range for the observed dataset: easting ~100K–900K, northing ~6M–8M. Values outside this range are anomalous and filtered in the Staging step. Because coordinates are already in meters, distance between two points is computed with Euclidean distance — Haversine formula does not apply and would produce incorrect results.

---

## 3. Extensibility: Adding Future Data Sources

The architecture supports new data sources through:

1. **Raw layer pattern**: new data source → new prefix in `s3://telecom-raw/{source}/date=YYYY-MM-DD/`
2. **Glue Crawler**: auto-discovers new tables; adds to Data Catalog
3. **ETL job**: modular Spark jobs per data source; join logic in separate modules
4. **Output layers**: new Analytics/Curated tables per use case; shared partitioning convention

Adding a new event type or geography requires only:
- New Raw prefix and partition
- New Glue job (or extension of existing job)
- New Analytics/Curated table with appropriate schema

---

## 4. Cost Estimate (Monthly, at current scale)

| Component | Estimated Cost | Notes |
|---|---|---|
| S3 Storage (500K records/day, 30 days) | ~$15 | Parquet compressed; Intelligent-Tiering |
| Glue ETL (1 job/day, 30 min avg) | ~$30 | 10 DPU × 0.5 hr × $0.44/DPU-hr |
| Athena (100 queries/day) | ~$10 | $5/TB scanned; Parquet columnar prunes data |
| Step Functions (30 runs/month) | ~$1 | Minimal state transitions |
| SNS (100 notifications/month) | ~$0.10 | First 1M notifications free |
| SageMaker (2 users, 4 hr/day) | ~$120 | ml.t3.medium instances |
| **Total** | **~$176/month** | Scales with volume; SageMaker is the main cost driver |
