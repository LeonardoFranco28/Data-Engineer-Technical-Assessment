# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

Data engineering take-home assessment for a telecom company. Three deliverables:

1. **Architecture doc** — storage layers, transformation tools, orchestration, alerting, access patterns for Data Scientists and BI Analysts
2. **PySpark ETL script** — standalone script processing ~500K records (geo + labels parquet files), scalable to millions
3. **Analytics notebook** — Colab notebook connecting to SQLite, data quality checks, window functions, CTEs

Full case definition: `docs/Case_DataEngineer.docx.md`

## Expected Output Structure

```
etl/
  spark_job.py          # standalone PySpark script
notebooks/
  analysis.ipynb        # Colab notebook (Point 3)
architecture/
  diagram.*             # architecture diagram
  design_doc.md         # planning document
data/
  raw/geo/              # input parquet files
  raw/labels/
  output/               # processed parquet output
```

## Key Technical Requirements

**ETL script must (in order):**
1. Data profiling — row counts, schema, NULLs in ID/comuna/event/lat/lon, out-of-range coords, duplicate IDs
2. Deduplicate geo (keep first comuna/lat/lon), deduplicate labels (aggregate event field)
3. Join geo + labels on ID — justify join type based on profiling findings
4. Calculate distances between all customers within 50-meter radius
5. Implement and document an optimization strategy for the combinatorial explosion problem (geohashing, spatial partitioning, or grid-cell bucketing)
6. Define output schema (may differ for Data Scientists vs BI Analysts)
7. Save output as weekly-history-aware parquet

**Distance calculation:** Euclidean, Manhattan, or Haversine — document choice. Coordinates are lat/lon, so Haversine is most accurate for real geo distances; Euclidean acceptable if documented.

**Scalability constraint:** naive cross-join of millions of records is explicitly forbidden. Must use a spatial optimization.

**SQLite notebook must include:**
- Load labels table + Point 2 result table into SQLite
- NULL checks on key fields
- Duplicate detection post-deduplication
- Coordinate outlier detection (commune bounding box via subquery)
- RANK/DENSE_RANK: top 20 communes by type 2 events + share of national total
- CTE for type 1 events per commune: avg/max/min lat/lon, count, lat range — filter communes where avg lat deviates >10% from national avg
- Conditional aggregation: type 1 count, type 2 count, type_2/type_1 ratio per commune (no JOINs)

## Running the ETL

```bash
# Local PySpark
spark-submit etl/spark_job.py --input-geo data/raw/geo --input-labels data/raw/labels --output data/output

# Or inside Colab/Jupyter
pip install pyspark
python etl/spark_job.py
```

## Data Schema

**geo table:** customer ID, latitude, longitude, comuna (commune)  
**labels table:** customer ID, event type (type 1 or type 2)  
Both partitioned daily; sample is ~500K rows combined.

## Architecture Decision Context

The case requires justifying cloud choice (AWS vs GCP vs hybrid) and storage format per layer:
- Raw → Parquet, partitioned by `date`
- Staging/Analytics → Delta Lake or Iceberg (ACID, time travel for weekly history)
- Curated → Delta/Iceberg exposed via Athena or BigQuery for BI Analysts; same data accessible from SageMaker/JupyterHub for Data Scientists
