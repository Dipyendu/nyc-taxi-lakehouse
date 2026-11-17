# Architecture

```mermaid
flowchart LR
  subgraph Source["NYC TLC (public CloudFront)"]
    P["yellow_tripdata_YYYY-MM.parquet<br/>monthly, ~2-month delay"]
    Z["taxi_zone_lookup.csv"]
  end

  subgraph Laptop["Laptop / Airflow container"]
    I["taxi.ingest<br/>stream download, size check,<br/>Parquet footer: rows + schema"]
  end

  subgraph Databricks["Databricks Free Edition (serverless)"]
    V[("landing volume<br/>LANDING_ROOT/yellow/year=/month=<br/>+ _manifest/yellow/*.json")]
    B[("taxi_bronze.yellow_trips<br/>Delta, partitioned by file_month,<br/>merged schema")]
    M[("taxi_bronze.ingest_manifest")]
    S[("taxi_silver.yellow_trips")]
    Q[("taxi_silver.yellow_trips_quarantine<br/>rule name per row")]
    G[("taxi_gold (dbt)<br/>stg_trips, dim_zone, dim_date,<br/>dim_vendor, dim_ratecode, dim_payment_type,<br/>fct_trips, mart_daily_zone_demand, mart_hourly_demand")]
  end

  P --> I --> V
  Z -->|dbt seed| G
  V -->|"job taxi_bronze (PySpark)"| B
  V --> M
  B -->|"job taxi_silver (PySpark)"| S
  B --> Q
  S -->|"dbt build (SQL warehouse)"| G
  M -. reconciliation test .-> G

  A["Airflow DAG taxi_monthly<br/>ingest → bronze → silver → dbt_build → reconcile"]
  A -.orchestrates.-> I
  A -.Jobs API.-> B
  A -.Jobs API.-> S
  A -.dbt.-> G
```

Per month, every row is accounted for: Parquet footer rows (manifest) = bronze rows = silver rows + quarantined rows, and fact rows = silver rows. The dbt test `reconcile_months` checks this for every loaded month.

The render check of this diagram and the others, at their current labels, is in `docs/evidence/mermaid-render-2026-11-17.txt`.
