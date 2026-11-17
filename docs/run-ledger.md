# Databricks run ledger

| start (IST) | what | data size | duration | result |
|---|---|---|---|---|
| 2026-11-16 20:08:04 | auth test auth: databricks current-user me | none | 2 s | OK |
| 2026-11-16 20:08:13 | warehouse test warehouse: SELECT 1, current_catalog() | none | 4 s | FAIL rc=1 |
| 2026-11-16 20:08:29 | warehouse test warehouse: SELECT 1, current_catalog() (retry, host without ?o=) | none | 11 s | OK |
| 2026-11-16 20:09:45 | volume test volume: create schema taxi_landing + volume raw, upload + list small file | 47 bytes | 4 s | FAIL rc=1 |
| 2026-11-16 20:10:39 | volume test volume (retry, catalog analytics): schema taxi_landing + volume raw, upload + list small file | 47 bytes | 7 s | OK |
| 2026-11-16 20:10:53 | ingest ingest upload to volume: 2024-12, 2025-01 (Files API, not routed through the ledger; logged after the fact) | 120,682,323 bytes | not measured | OK |
| 2026-11-16 20:13:02 | job deployment test: job taxi_bronze month=2025-01 (bundle-deployed, serverless) | 1 file, 59,158,238 bytes, 3,475,226 rows | 38 s | OK |
| 2026-11-16 20:13:51 | job deployment test check: counts on analytics.taxi_bronze | metadata queries | 14 s | OK |
| 2026-11-16 20:14:13 | dbt connection test dbt debug | none | 10 s | OK |
| 2026-11-16 20:14:23 | dbt connection test dbt run dim_date | 1,096 rows | 17 s | OK |
| 2026-11-16 20:15:54 | bronze and silver: job taxi_bronze month=2024-12 | 1 file, 61,524,085 bytes, 3,668,371 rows | 30 s | OK |
| 2026-11-16 20:16:30 | bronze and silver: job taxi_silver month=2024-12 | 1 month from bronze | 30 s | OK |
| 2026-11-16 20:17:00 | bronze and silver: job taxi_silver month=2025-01 | 1 month from bronze | 30 s | OK |
| 2026-11-16 20:17:41 | bronze and silver check: counts, rules, schema on taxi_bronze/taxi_silver | 2 months, metadata + group-by queries | 11 s | FAIL rc=1 |
| 2026-11-16 20:18:00 | bronze and silver check: cbd_congestion_fee by month | 2 months | 4 s | OK |
| 2026-11-16 20:18:25 | dbt dbt build (all models, seeds, tests) on 2 sample months | 7,143,597 bronze rows | 27 s | FAIL rc=1 |
| 2026-11-16 20:19:20 | dbt dbt build retry (all models, seeds, tests) on 2 sample months | 7,143,597 bronze rows | 49 s | OK |
| 2026-11-16 20:20:23 | reload check snapshot before reload 2025-01 | 1 month | 9 s | OK |
| 2026-11-16 20:20:38 | make ingest 2025-01 (download + volume upload) | see manifest | 5 s | OK |
| 2026-11-16 20:20:43 | make bronze 2025-01 | 1 month | 30 s | OK |
| 2026-11-16 20:21:13 | make silver 2025-01 | 1 month | 31 s | OK |
| 2026-11-16 20:21:44 | make dbt build 2025-01 | 1 month | 47 s | OK |
| 2026-11-16 20:22:31 | make dbt reconcile | all months | 13 s | OK |
| 2026-11-16 20:22:53 | reload check snapshot after reload 2025-01 | 1 month | 9 s | OK |
| 2026-11-16 20:23:09 | reload check control checksum | 1 month | 3 s | OK |
| 2026-11-16 20:24:35 | airflow ingest 2026-11 (time converted from container UTC) | 1 month | 6 s | SKIP rc=99 (month not published) |
| 2026-11-16 20:24:52 | airflow ingest 2025-02 (time converted from container UTC) | 1 month | 291 s | OK |
| 2026-11-16 20:29:43 | airflow bronze 2025-02 (time converted from container UTC) | 1 month | 30 s | OK |
| 2026-11-16 20:30:14 | airflow silver 2025-02 (time converted from container UTC) | 1 month | 29 s | OK |
| 2026-11-16 20:30:44 | airflow dbt build 2025-02 (time converted from container UTC) | 1 month | 48 s | OK |
| 2026-11-16 20:31:32 | airflow dbt reconcile 2025-02 (time converted from container UTC) | 1 month | 13 s | OK |
| 2026-11-16 20:35:29 | duplicate-row check 2025-01 | 1 month | 7 s | OK |
| 2026-11-16 20:37:30 | upload speed test from Airflow container (test path _speedtest/, deleted after) | 69,964,745 bytes | 30 s | OK |
| 2026-11-16 21:04:06 | live check of timeout-configured client: ingest skip 2025-01 | manifest read only | 5 s | OK |
| 2026-11-16 21:04:11 | live check: SELECT 1 via workspace_client | none | 7 s | OK |
| 2026-11-16 21:11:00 | airflow ingest 2024-01 | 1 month | 18 s | OK |
| 2026-11-16 21:11:18 | airflow bronze 2024-01 | 1 month | 30 s | OK |
| 2026-11-16 21:11:48 | airflow silver 2024-01 | 1 month | 31 s | OK |
| 2026-11-16 21:12:19 | airflow dbt build 2024-01 | 1 month | 58 s | OK |
| 2026-11-16 21:13:18 | airflow dbt reconcile 2024-01 | 1 month | 12 s | OK |
| 2026-11-16 21:13:32 | airflow ingest 2024-02 | 1 month | 17 s | OK |
| 2026-11-16 21:13:50 | airflow bronze 2024-02 | 1 month | 28 s | OK |
| 2026-11-16 21:14:19 | airflow silver 2024-02 | 1 month | 29 s | OK |
| 2026-11-16 21:14:48 | airflow dbt build 2024-02 | 1 month | 48 s | OK |
| 2026-11-16 21:15:37 | airflow dbt reconcile 2024-02 | 1 month | 12 s | OK |
| 2026-11-16 21:15:50 | airflow ingest 2024-03 | 1 month | 23 s | OK |
| 2026-11-16 21:16:14 | airflow bronze 2024-03 | 1 month | 29 s | OK |
| 2026-11-16 21:16:44 | airflow silver 2024-03 | 1 month | 29 s | OK |
| 2026-11-16 21:17:14 | airflow dbt build 2024-03 | 1 month | 51 s | OK |
| 2026-11-16 21:18:05 | airflow dbt reconcile 2024-03 | 1 month | 13 s | OK |
| 2026-11-16 21:18:20 | airflow ingest 2024-04 | 1 month | 24 s | OK |
| 2026-11-16 21:18:44 | airflow bronze 2024-04 | 1 month | 31 s | OK |
| 2026-11-16 21:19:16 | airflow silver 2024-04 | 1 month | 29 s | OK |
| 2026-11-16 21:19:46 | airflow dbt build 2024-04 | 1 month | 55 s | OK |
| 2026-11-16 21:20:41 | airflow dbt reconcile 2024-04 | 1 month | 14 s | OK |
| 2026-11-16 21:20:57 | airflow ingest 2024-05 | 1 month | 311 s | OK |
| 2026-11-16 21:26:08 | airflow bronze 2024-05 | 1 month | 26 s | OK |
| 2026-11-16 21:26:35 | airflow silver 2024-05 | 1 month | 30 s | OK |
| 2026-11-16 21:27:06 | airflow dbt build 2024-05 | 1 month | 58 s | OK |
| 2026-11-16 21:28:05 | airflow dbt reconcile 2024-05 | 1 month | 12 s | OK |
| 2026-11-16 21:28:19 | airflow ingest 2024-06 | 1 month | 38 s | OK |
| 2026-11-16 21:28:58 | airflow bronze 2024-06 | 1 month | 30 s | OK |
| 2026-11-16 21:29:29 | airflow silver 2024-06 | 1 month | 30 s | OK |
| 2026-11-16 21:29:59 | airflow dbt build 2024-06 | 1 month | 59 s | OK |
| 2026-11-16 21:30:58 | airflow dbt reconcile 2024-06 | 1 month | 15 s | OK |
| 2026-11-16 21:31:15 | airflow ingest 2024-07 | 1 month | 24 s | OK |
| 2026-11-16 21:31:39 | airflow bronze 2024-07 | 1 month | 32 s | OK |
| 2026-11-16 21:32:11 | airflow silver 2024-07 | 1 month | 30 s | OK |
| 2026-11-16 21:32:42 | airflow dbt build 2024-07 | 1 month | 56 s | OK |
| 2026-11-16 21:33:39 | airflow dbt reconcile 2024-07 | 1 month | 12 s | OK |
| 2026-11-16 21:33:53 | airflow ingest 2024-08 | 1 month | 20 s | OK |
| 2026-11-16 21:34:13 | airflow bronze 2024-08 | 1 month | 31 s | OK |
| 2026-11-16 21:34:44 | airflow silver 2024-08 | 1 month | 31 s | OK |
| 2026-11-16 21:35:15 | airflow dbt build 2024-08 | 1 month | 52 s | OK |
| 2026-11-16 21:36:09 | airflow dbt reconcile 2024-08 | 1 month | 12 s | OK |
| 2026-11-16 21:36:24 | airflow ingest 2024-09 | 1 month | 33 s | OK |
| 2026-11-16 21:36:58 | airflow bronze 2024-09 | 1 month | 29 s | OK |
| 2026-11-16 21:37:28 | airflow silver 2024-09 | 1 month | 29 s | OK |
| 2026-11-16 21:37:58 | airflow dbt build 2024-09 | 1 month | 50 s | OK |
| 2026-11-16 21:38:48 | airflow dbt reconcile 2024-09 | 1 month | 12 s | OK |
| 2026-11-16 21:39:02 | airflow ingest 2024-10 | 1 month | 32 s | OK |
| 2026-11-16 21:39:35 | airflow bronze 2024-10 | 1 month | 29 s | OK |
| 2026-11-16 21:40:04 | airflow silver 2024-10 | 1 month | 30 s | OK |
| 2026-11-16 21:40:34 | airflow dbt build 2024-10 | 1 month | 49 s | OK |
| 2026-11-16 21:41:24 | airflow dbt reconcile 2024-10 | 1 month | 12 s | OK |
| 2026-11-16 21:41:37 | airflow ingest 2024-11 | 1 month | 37 s | OK |
| 2026-11-16 21:42:14 | airflow bronze 2024-11 | 1 month | 29 s | OK |
| 2026-11-16 21:42:43 | airflow silver 2024-11 | 1 month | 330 s | OK |
| 2026-11-16 21:48:13 | airflow dbt build 2024-11 | 1 month | 54 s | OK |
| 2026-11-16 21:49:12 | airflow dbt reconcile 2024-11 | 1 month | 14 s | OK |
| 2026-11-16 21:50:10 | backfill check chunk 2024: per-month reconciliation | all loaded months, counts | 6 s | OK |
| 2026-11-16 21:50:01 | airflow ingest 2025-03 | 1 month | 54 s | OK |
| 2026-11-16 21:50:56 | airflow bronze 2025-03 | 1 month | 30 s | OK |
| 2026-11-16 21:51:31 | airflow silver 2025-03 | 1 month | 30 s | OK |
| 2026-11-16 21:52:02 | airflow dbt build 2025-03 | 1 month | 51 s | OK |
| 2026-11-16 21:52:54 | airflow dbt reconcile 2025-03 | 1 month | 14 s | OK |
| 2026-11-16 21:53:10 | airflow ingest 2025-04 | 1 month | 37 s | OK |
| 2026-11-16 21:53:48 | airflow bronze 2025-04 | 1 month | 30 s | OK |
| 2026-11-16 21:54:19 | airflow silver 2025-04 | 1 month | 28 s | OK |
| 2026-11-16 21:54:52 | airflow dbt build 2025-04 | 1 month | 56 s | OK |
| 2026-11-16 21:55:48 | airflow dbt reconcile 2025-04 | 1 month | 14 s | OK |
| 2026-11-16 21:56:03 | airflow ingest 2025-05 | 1 month | 37 s | OK |
| 2026-11-16 21:56:41 | airflow bronze 2025-05 | 1 month | 28 s | OK |
| 2026-11-16 21:57:10 | airflow silver 2025-05 | 1 month | 36 s | OK |
| 2026-11-16 21:57:47 | airflow dbt build 2025-05 | 1 month | 51 s | OK |
| 2026-11-16 21:58:38 | airflow dbt reconcile 2025-05 | 1 month | 12 s | OK |
| 2026-11-16 21:58:52 | airflow ingest 2025-06 | 1 month | 384 s | OK |
| 2026-11-16 22:05:17 | airflow bronze 2025-06 | 1 month | 28 s | OK |
| 2026-11-16 22:05:46 | airflow silver 2025-06 | 1 month | 30 s | OK |
| 2026-11-16 22:06:16 | airflow dbt build 2025-06 | 1 month | 51 s | OK |
| 2026-11-16 22:07:07 | airflow dbt reconcile 2025-06 | 1 month | 12 s | OK |
| 2026-11-16 22:07:21 | airflow ingest 2025-07 | 1 month | 36 s | OK |
| 2026-11-16 22:07:58 | airflow bronze 2025-07 | 1 month | 29 s | OK |
| 2026-11-16 22:08:28 | airflow silver 2025-07 | 1 month | 29 s | OK |
| 2026-11-16 22:08:58 | airflow dbt build 2025-07 | 1 month | 51 s | OK |
| 2026-11-16 22:09:50 | airflow dbt reconcile 2025-07 | 1 month | 12 s | OK |
| 2026-11-16 22:10:04 | airflow ingest 2025-08 | 1 month | 25 s | OK |
| 2026-11-16 22:10:30 | airflow bronze 2025-08 | 1 month | 30 s | OK |
| 2026-11-16 22:11:00 | airflow silver 2025-08 | 1 month | 31 s | OK |
| 2026-11-16 22:11:32 | airflow dbt build 2025-08 | 1 month | 49 s | OK |
| 2026-11-16 22:12:22 | airflow dbt reconcile 2025-08 | 1 month | 12 s | OK |
| 2026-11-16 22:12:36 | airflow ingest 2025-09 | 1 month | 43 s | OK |
| 2026-11-16 22:13:19 | airflow bronze 2025-09 | 1 month | 30 s | OK |
| 2026-11-16 22:13:49 | airflow silver 2025-09 | 1 month | 30 s | OK |
| 2026-11-16 22:14:19 | airflow dbt build 2025-09 | 1 month | 53 s | OK |
| 2026-11-16 22:15:13 | airflow dbt reconcile 2025-09 | 1 month | 13 s | OK |
| 2026-11-16 22:15:27 | airflow ingest 2025-10 | 1 month | 36 s | OK |
| 2026-11-16 22:16:04 | airflow bronze 2025-10 | 1 month | 29 s | OK |
| 2026-11-16 22:16:34 | airflow silver 2025-10 | 1 month | 29 s | OK |
| 2026-11-16 22:17:04 | airflow dbt build 2025-10 | 1 month | 52 s | OK |
| 2026-11-16 22:17:57 | airflow dbt reconcile 2025-10 | 1 month | 13 s | OK |
| 2026-11-16 22:18:12 | airflow ingest 2025-11 | 1 month | 37 s | OK |
| 2026-11-16 22:18:49 | airflow bronze 2025-11 | 1 month | 30 s | OK |
| 2026-11-16 22:19:20 | airflow silver 2025-11 | 1 month | 29 s | OK |
| 2026-11-16 22:19:50 | airflow dbt build 2025-11 | 1 month | 51 s | OK |
| 2026-11-16 22:20:42 | airflow dbt reconcile 2025-11 | 1 month | 13 s | OK |
| 2026-11-16 22:20:55 | airflow ingest 2025-12 | 1 month | 34 s | OK |
| 2026-11-16 22:21:29 | airflow bronze 2025-12 | 1 month | 30 s | OK |
| 2026-11-16 22:22:00 | airflow silver 2025-12 | 1 month | 28 s | OK |
| 2026-11-16 22:22:29 | airflow dbt build 2025-12 | 1 month | 52 s | OK |
| 2026-11-16 22:23:22 | airflow dbt reconcile 2025-12 | 1 month | 13 s | OK |
| 2026-11-16 22:24:22 | backfill check chunk 2025: per-month reconciliation | all loaded months, counts | 6 s | OK |
| 2026-11-16 22:24:11 | airflow ingest 2026-01 | 1 month | 34 s | OK |
| 2026-11-16 22:24:46 | airflow bronze 2026-01 | 1 month | 30 s | OK |
| 2026-11-16 22:25:17 | airflow silver 2026-01 | 1 month | 36 s | OK |
| 2026-11-16 22:25:53 | airflow dbt build 2026-01 | 1 month | 55 s | OK |
| 2026-11-16 22:26:49 | airflow dbt reconcile 2026-01 | 1 month | 13 s | OK |
| 2026-11-16 22:27:04 | airflow ingest 2026-02 | 1 month | 31 s | OK |
| 2026-11-16 22:27:36 | airflow bronze 2026-02 | 1 month | 29 s | OK |
| 2026-11-16 22:28:06 | airflow silver 2026-02 | 1 month | 30 s | OK |
| 2026-11-16 22:28:37 | airflow dbt build 2026-02 | 1 month | 51 s | OK |
| 2026-11-16 22:29:30 | airflow dbt reconcile 2026-02 | 1 month | 12 s | OK |
| 2026-11-16 22:29:43 | airflow ingest 2026-03 | 1 month | 33 s | OK |
| 2026-11-16 22:30:18 | airflow bronze 2026-03 | 1 month | 29 s | OK |
| 2026-11-16 22:30:48 | airflow silver 2026-03 | 1 month | 28 s | OK |
| 2026-11-16 22:31:17 | airflow dbt build 2026-03 | 1 month | 54 s | OK |
| 2026-11-16 22:32:12 | airflow dbt reconcile 2026-03 | 1 month | 12 s | OK |
| 2026-11-16 22:32:26 | airflow ingest 2026-04 | 1 month | 26 s | OK |
| 2026-11-16 22:32:53 | airflow bronze 2026-04 | 1 month | 28 s | OK |
| 2026-11-16 22:33:22 | airflow silver 2026-04 | 1 month | 32 s | OK |
| 2026-11-16 22:33:54 | airflow dbt build 2026-04 | 1 month | 53 s | OK |
| 2026-11-16 22:34:47 | airflow dbt reconcile 2026-04 | 1 month | 13 s | OK |
| 2026-11-16 22:35:01 | airflow ingest 2026-05 | 1 month | 28 s | OK |
| 2026-11-16 22:35:30 | airflow bronze 2026-05 | 1 month | 30 s | OK |
| 2026-11-16 22:36:00 | airflow silver 2026-05 | 1 month | 30 s | OK |
| 2026-11-16 22:36:31 | airflow dbt build 2026-05 | 1 month | 55 s | OK |
| 2026-11-16 22:37:27 | airflow dbt reconcile 2026-05 | 1 month | 13 s | OK |
| 2026-11-16 22:37:41 | airflow ingest 2026-06 | 1 month | 38 s | OK |
| 2026-11-16 22:38:21 | airflow bronze 2026-06 | 1 month | 27 s | OK |
| 2026-11-16 22:38:49 | airflow silver 2026-06 | 1 month | 30 s | OK |
| 2026-11-16 22:39:20 | airflow dbt build 2026-06 | 1 month | 56 s | OK |
| 2026-11-16 22:40:17 | airflow dbt reconcile 2026-06 | 1 month | 12 s | OK |
| 2026-11-16 22:40:30 | airflow ingest 2026-07 | 1 month | 36 s | OK |
| 2026-11-16 22:41:07 | airflow bronze 2026-07 | 1 month | 29 s | OK |
| 2026-11-16 22:41:37 | airflow silver 2026-07 | 1 month | 28 s | OK |
| 2026-11-16 22:42:06 | airflow dbt build 2026-07 | 1 month | 54 s | OK |
| 2026-11-16 22:43:01 | airflow dbt reconcile 2026-07 | 1 month | 13 s | OK |
| 2026-11-16 22:43:15 | airflow ingest 2026-08 | 1 month | 27 s | OK |
| 2026-11-16 22:43:42 | airflow bronze 2026-08 | 1 month | 29 s | OK |
| 2026-11-16 22:44:11 | airflow silver 2026-08 | 1 month | 30 s | OK |
| 2026-11-16 22:44:41 | airflow dbt build 2026-08 | 1 month | 53 s | OK |
| 2026-11-16 22:45:35 | airflow dbt reconcile 2026-08 | 1 month | 12 s | OK |
| 2026-11-16 22:45:49 | airflow ingest 2026-09 | 1 month | 85 s | OK |
| 2026-11-16 22:47:15 | airflow bronze 2026-09 | 1 month | 30 s | OK |
| 2026-11-16 22:47:46 | airflow silver 2026-09 | 1 month | 29 s | OK |
| 2026-11-16 22:48:16 | airflow dbt build 2026-09 | 1 month | 55 s | OK |
| 2026-11-16 22:49:12 | airflow dbt reconcile 2026-09 | 1 month | 12 s | OK |
| 2026-11-16 22:49:50 | backfill final checks: reconciliation, totals, rules, schema | 33 months | 13 s | OK |
| 2026-11-16 22:51:41 | final verification snapshot before reload 2026-09 | 1 month | 10 s | OK |
| 2026-11-16 22:51:51 | make ingest 2026-09 (download + volume upload) | see manifest | 5 s | OK |
| 2026-11-16 22:51:56 | make bronze 2026-09 | 1 month | 28 s | OK |
| 2026-11-16 22:52:24 | make silver 2026-09 | 1 month | 29 s | OK |
| 2026-11-16 22:52:53 | make dbt build 2026-09 | 1 month | 56 s | OK |
| 2026-11-16 22:53:49 | make dbt reconcile | all months | 14 s | OK |
| 2026-11-16 22:54:03 | final verification snapshot after reload 2026-09 | 1 month | 9 s | OK |
| 2026-11-16 22:54:30 | final verification checks 3 and 4 | 33 months, counts | 7 s | OK |
| 2026-11-16 22:54:37 | final verification check 7: list job runs | metadata | 3 s | OK |
| 2026-11-16 22:59:16 | final verification fix: dbt build month=2026-09 after moving the month filter | 1 month + all tests | 62 s | OK |
| 2026-11-16 23:00:18 | final verification fix check: stg view rows vs silver; 2026-09 snapshot | counts | 13 s | OK |
| 2026-11-17 08:24:36 | rule tests warehouse test of silver rules on 14 handmade rows | 14 rows | 3 s | OK |
| 2026-11-17 08:24:49 | rule tests mutation control for the warehouse rule test | 14 rows | 4 s | OK |
| 2026-11-17 08:25:19 | snapshot fix snapshot dry check on 2026-07 | 1 month | 19 s | OK |
| 2026-11-17 08:25:50 | review fixes bundle deploy (silver CASE rules, bronze month check) | 2 job files | 10 s | OK |
| 2026-11-17 08:26:38 | review fixes snapshot before scheduled run (2026-07) | 1 month | 13 s | OK |
| 2026-11-17 08:26:55 | airflow ingest 2026-07 | 1 month | 5 s | OK |
| 2026-11-17 08:27:01 | airflow bronze 2026-07 | 1 month | 29 s | OK |
| 2026-11-17 08:27:31 | airflow silver 2026-07 | 1 month | 28 s | OK |
| 2026-11-17 08:27:59 | gross amounts query: negative-fare rows by payment type | 3.76 M rows, group-by | 5 s | OK |
| 2026-11-17 08:28:00 | airflow dbt build 2026-07 | 1 month | 65 s | OK |
| 2026-11-17 08:29:05 | airflow dbt reconcile 2026-07 | 1 month | 13 s | OK |
| 2026-11-17 08:29:33 | review fixes snapshot after scheduled run (2026-07) | 1 month | 15 s | OK |
| 2026-11-17 08:31:14 | review fixes dbt build month=2026-07 after dbt fixes | 1 month + all tests | 46 s | OK |
| 2026-11-17 08:32:00 | review fixes dbt reconcile (all tables' months) | 33 months | 15 s | OK |
| 2026-11-17 08:32:15 | review fixes duplicate test over all months | 123 M rows, group-by | 36 s | OK |
| 2026-11-17 08:32:51 | review fixes check: fct_trips refuses to run without month | none | 12 s | FAIL rc=1 |
| 2026-11-17 08:33:15 | duplicate check duplicate groups by month | 123 M rows, group-by | 20 s | OK |
| 2026-11-17 08:39:30 | review fixes fresh checks 3, 4, calendar fix, gross amounts | 33 months, counts | 8 s | OK |
| 2026-11-17 08:39:38 | review fixes check 7: list job runs | metadata | 5 s | OK |
| 2026-11-17 08:43:55 | make ingest 2026-07 (download + volume upload) | see manifest | 6 s | OK |
| 2026-11-17 08:44:35 | review fixes check 7 re-run: taxi jobs only | metadata | 5 s | OK |
| 2026-11-17 08:45:04 | reconciliation fix control: reconcile with empty manifest | 33 months, counts | 4 s | OK |
| 2026-11-17 09:01:03 | publish prep: warehouse path from local.env | none | 13 s | OK |
