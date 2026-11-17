# Design notes

This file explains each part of the pipeline: what it does, why it was built this way, what was rejected, and what would change with ten times the data. Numbers come from files in `docs/evidence/` or from `docs/run-ledger.md`, named next to each figure.

## Source data

The NYC Taxi and Limousine Commission publishes one Parquet file of yellow-taxi trips per month, typically two months late according to the TLC. Reading only the footer of every file (by HTTP range request) showed 33 published months from 2024-01 to 2026-09, 123,471,721 rows, and three schemas (`docs/evidence/a1-all-months-footer-schemas.txt`). Every schema change adds a column and none renames or retypes one: `cbd_congestion_fee` from 2025-01 and `request_source` from 2026-06. Code meanings for vendor, rate code and payment type come from the TLC data dictionary dated 18 March 2025, stored as dbt seeds in `dbt/seeds/`.

## Ingest (`src/taxi/ingest.py`, `src/taxi/storage.py`)

**What it does.** For one month it streams the file to disk in 1 MiB chunks, checks the byte count against `Content-Length`, reads the row count and schema from the Parquet footer without loading data pages, uploads the file to `LANDING_ROOT/yellow/year=YYYY/month=MM/`, and writes a JSON manifest record to `LANDING_ROOT/_manifest/yellow/YYYY-MM.json`. A second call for the same month finds the manifest and a remote file of the recorded size, and does nothing (`docs/evidence/a1-ingest-volume-idempotency.txt`). HTTP 403 or 404 for a month at most six months old means "not published"; if no month in the call was published, ingest exits with code 99, which Airflow's `BashOperator` treats as a skip. A month more than six months old that returns 403 or 404 fails instead, and HTTP 5xx and broken downloads are retried.

**Why.** Databricks Free Edition restricts outbound internet access to a limited set of trusted domains ([Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)), so the laptop downloads and uploads. A download goes to a `.part` file and is renamed only after the size check, so a cut-off download never looks complete. The manifest lives in the landing volume, not on the laptop, because ingest in the Airflow container and the bronze job read it too; the bronze job loads it into `taxi_bronze.ingest_manifest`, which the dbt reconciliation test reads.

**Rejected.** Downloading inside Databricks (blocked by the outbound restriction). A local manifest file (only the host could read it). Pandas for the row count (it loads the data; the footer already holds the count).

**At 10x.** Ingest several months in parallel, and land in an S3 bucket through an external location so the upload path does not depend on the laptop. `LANDING_ROOT` in `src/taxi/config.py` is the only setting that names the location.

## Bronze (`jobs/bronze.py`)

**What it does.** A serverless PySpark job reads one month's file and overwrites exactly that month in the Delta table `analytics.taxi_bronze.yellow_trips`, which is partitioned by `file_month`. It adds the source file name, the row's position in the file (`_metadata.row_index`), the load time and the month. It also loads the manifest record into `taxi_bronze.ingest_manifest`, and fails the run if the table's row count for the month differs from the manifest.

**Why.** `replaceWhere` on the month makes a reload idempotent: loading 2025-01 twice left the row count and a content checksum unchanged (`docs/evidence/acc2-idempotency-2025-01.txt`). `mergeSchema` adds new source columns to the table instead of dropping them; 2024-12 rows read `NULL` for `cbd_congestion_fee` and all 3,475,226 rows of 2025-01 carry it (`docs/evidence/a2-bronze-silver-sample.txt`). The row position gives every trip a key, `file_month-_source_row` (from `_metadata.row_index`), which stays the same on every reload of an unchanged file; a republished file would move it (README, Known limits).

**Rejected.** `MERGE` on a business key (the source has no trip id). A hash of all columns as the key: 2025-01 has no exact duplicate rows (`docs/evidence/a2-duplicate-rows-2025-01.txt`), but nothing in the source promises that for other months, and a collision would merge two trips. Append-only loads (a reload would duplicate a month).

**At 10x.** Same design. Consider liquid clustering on pickup date instead of month partitions if queries filter by day.

## Silver and quarantine (`jobs/silver.py`)

**What it does.** It renames columns to snake case, casts types, and checks seven rules in a fixed order. A row that breaks a rule goes to `taxi_silver.yellow_trips_quarantine` with the first rule it broke; every other row goes to `taxi_silver.yellow_trips`. So for each month, bronze rows equal silver rows plus quarantined rows, and the job fails if they do not.

**Why these rules.** They come from a profile of the two sample months, 2024-12 and 2025-01 (`docs/evidence/a1-sample-profile.txt`): `pickup_outside_file_month` (34 and 22 rows), `dropoff_before_pickup` (99 and 124), `duration_over_24h` (18 and 20), and `trip_distance_over_500mi` (83 and 118). The largest group is `negative_fare_or_total`: 79,017 and 144,416 quarantined rows (`docs/evidence/a2-bronze-silver-sample.txt`). Across all months these rows are 57.8 % payment type 0 (Flex Fare), mostly with a positive total, 26.4 % disputes, 10.1 % cash and 5.6 % no charge (`docs/evidence/a8/m7-negative-fare-rows.txt`), so the rule catches more than refunds; the README lists this under Known limits. `missing_timestamp` and `negative_trip_distance` matched no rows in the sample or across all 33 months (`docs/evidence/a5-backfill-final.txt`); the checks cost nothing and guard later months. Zero-distance trips and rate code 99 stay in silver, because the dictionary defines 99 as "Null/unknown" and short or cancelled trips are real records.

**Rejected.** Dropping bad rows (the counts would no longer reconcile, and nobody could see what was removed). Tagging a row with every rule it breaks (one rule per row keeps the reconciliation a plain sum).

**At 10x.** Same logic. Move the rules into a table so they can change without a code release, and alert on a rule whose share jumps between months.

## Gold layer with dbt (`dbt/`)

**What it does.** `stg_trips` is a view over silver. Dimensions for zone (seeded from the TLC lookup file), date, vendor, rate code and payment type are tables. `fct_trips` is an incremental table: every incremental run needs `--vars "{month: 'YYYY-MM'}"` and uses the `replace_where` strategy to replace exactly that month; without the variable the model stops with a compile error (`docs/evidence/a8/dbt-fixes-fresh-build.txt`). Two marts aggregate the fact: daily demand by pickup zone, and demand by month and hour, with gross amounts. There are 36 tests: unique and not-null keys on every model, relationships from the fact to every dimension, accepted values on the coded fields, `dbt/tests/reconcile_months.sql`, and the warn-level `dbt/tests/duplicate_source_rows.sql` (`docs/evidence/a8/dbt-fixes-fresh-build.txt`).

**Why.** The reconciliation test checks the whole chain for every month found in any of the five tables, in one query: footer rows in the manifest = bronze rows = silver rows + quarantined rows, and fact rows = silver rows; an empty manifest fails it. Because each bronze row goes to exactly one of silver or quarantine, the test catches lost or partial writes, not a wrong rule; the rule tests do that (`tests/test_rules.py`, `tests/test_rules_warehouse.py`). Dimensions and marts are tables so dashboard queries do not rescan the fact table. Sources take the catalog from the dbt target, so dbt reads it from one place, `dbt/profiles.yml`; the Spark jobs get it from `databricks.yml` and the Python code from `src/taxi/config.py`.

**Rejected.** Running the gold models as PySpark (dbt gives tests, lineage and documentation for free, and the models are plain SQL). Views for the marts (each read would aggregate the full fact table).

**At 10x.** Make the marts incremental by month like the fact, and run the tests on the changed month only, keeping a weekly full run.

## Orchestration with Airflow (`airflow/`)

**What it does.** The DAG `taxi_monthly` runs ingest, bronze, silver, dbt build and the reconciliation test in that order. `taxi.schedule.target_month` picks the month: the `month` parameter when given; for a backfill run, the month of its logical date; for a scheduled run or a manual run without `month`, the month four months before its date. TLC files appeared 18 to 83 days after their month ended (`docs/evidence/a8/b1-publication-lag.txt`). A run on the 1st comes 89 to 92 days after the end of the month four back; all 33 files were out by then, the closest with 7 days to spare, while two or three months back would have missed some (README, Known limits). The scheduled run with logical date 1 Nov 2026, cleared and re-run on 17 Nov 2026 with no month given, loaded 2026-07 this way (`docs/evidence/a8/b1-scheduled-run-2026-07.txt`). `max_active_runs=1` allows one run at a time, and a failed task is retried twice after 2 minutes. A run from a clean start loaded 2025-02 in 6 minutes 53 seconds from the first task's start to the last task's end, 4 minutes 51 of them in ingest, and, before the four-month rule existed, the scheduled run for the unpublished month 2026-11 skipped all its tasks instead of failing (`docs/evidence/a4-airflow-clean-start-2025-02.txt`).

**Why this setup.** The machine has 8 GB of RAM, shared with a second project. When Airflow was built, its container limit was 1,280 MiB and Postgres had 256 MiB (`docs/evidence/a4-airflow-idle-memory.txt`); the Airflow limit is now 1,536 MiB (`airflow/docker-compose.yml`). The [official Docker Compose file](https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml) for Airflow 3.3.2 uses the Celery executor with Redis and runs the API server, scheduler, DAG processor, Celery worker and triggerer in separate containers, which does not fit. `airflow standalone` in one container idled at 1.02 GiB. Running only the scheduler, the API server and the DAG processor, with parallelism 1, idled at 624 MiB (`docs/evidence/a4-airflow-idle-memory.txt`) and peaked at 1,207 MiB during a run, with Postgres at 103 MiB (`docs/evidence/a4-airflow-clean-start-2025-02.txt`). The image is the official `apache/airflow:3.3.2-python3.12` plus this project's dependencies in a separate virtual environment, installed from `airflow/requirements.txt` with hash checking, so dbt and the Databricks SDK cannot conflict with Airflow's pinned packages. Airflow starts Databricks jobs through the Jobs API and waits for them.

**Rejected.** The full official stack (memory). Installing dbt into Airflow's own environment (version conflicts). Databricks Workflows as the only orchestrator (this project set out to use Airflow; Workflows would be the simpler choice on Databricks alone).

**At 10x.** Run Airflow on a managed service or Kubernetes with more workers, raise `max_active_runs` for backfills, and let each task request its own resources.

## Databricks jobs and deployment (`databricks.yml`)

**What it does.** A Databricks Asset Bundle deploys two serverless jobs, `taxi_bronze` and `taxi_silver`, each with one `spark_python_task` and a `month` job parameter. The layout follows the serverless template that ships with Databricks CLI v1.26.0.

**Why.** The job definitions live in the repository and deploy with one command (`make deploy`). Free Edition has serverless compute only, which suits jobs that take about 30 seconds per month (median 29.5 s for bronze and 30 s for silver, `docs/run-ledger.md`).

**Rejected.** Creating jobs by hand in the UI (not reproducible). A wheel package for the job code (each job file is self-contained, so the bundle needs no build step).

**At 10x.** The jobs stay the same, but a backfill would take ten times as long one month at a time. Raise `max_concurrent_runs` to load several months at once, after testing that concurrent `replaceWhere` writes to different months of one Delta table do not conflict (not tested here). Move the jobs to a paid workspace if the daily serverless quota becomes the limit.

## Operations: ledger, quota guard and secrets (`scripts/`)

**What it does.** Every Databricks job run, dbt build or test, ingest and deploy started from `make` or Airflow goes through `scripts/ledger.sh`. When the optional quota guard is on (`TAXI_QUOTA_LOG` names a quota log shared with other projects on the same Databricks account), it refuses to start while that log's last `QUOTA-STOP` or `QUOTA-RESUME` line is a `QUOTA-STOP`. It writes the start time, size, duration and result to `docs/run-ledger.md`. `scripts/secret_scan.sh` is run before every commit, chained with `&&` (no git hook enforces it); it proved itself by finding planted fake tokens, AWS keys and a workspace address (`docs/evidence/a0-secret-scan-proof.txt`). Credentials come only from a `.secrets.env` file next to the repository folder (or the file named by `TAXI_SECRETS_FILE`), never from a file inside the repository. `make ci` and `.github/workflows/ci.yml` run lint, unit tests and an offline `dbt parse`.

**Why.** Free Edition shuts compute down for the rest of the day when an account exceeds its quota, whose size is not published ([Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)). So every run is logged, and the optional quota guard lets every project on one account honour a shared stop.

**Time limits.** A network call without a timeout can wait forever, and a background run that never ends wakes nobody. Downloads use a 10 s connect and 120 s read timeout with three attempts; uploads and manifest writes get three attempts; Databricks SDK calls have a 120 s request timeout and stop retrying after 300 s (`src/taxi/config.py`). A job wait is cancelled after 20 minutes (`src/taxi/run_job.py`), every ledgered command is killed after its limit (`scripts/ledger.sh`), and in Airflow each task has 25 minutes and each run 60 minutes. Progress lines are unbuffered. `tests/test_timeouts.py` covers the download and upload retries, the SDK client limits and the cancelled job wait; `tests/test_ledger.py` covers the ledger's limit. The Airflow task and run limits are checked by loading the DAG, not by a test.

**Rejected.** A git pre-commit hook for the scan (a hook must be installed in every clone; for a single author the `&&` chain gives the same protection). Logging runs in a database table (the ledger must keep working when Databricks is the thing that failed).

**At 10x.** Move the ledger into the orchestrator's own run history and alerting, and make the CI secret scan a required check before merge.

## Problems found on the way

- The workspace's default catalog, `workspace`, does not exist in the account used here, although `current_catalog()` still returns it. Creating the landing schema in `workspace` failed with "Catalog 'workspace' does not exist", and the retry in `analytics` succeeded (`docs/run-ledger.md`, 16 Nov 2026 20:09 and 20:10). Every name in this project is fully qualified with `analytics`.
- The workspace address was pasted with a `?o=<workspace id>` suffix. The Databricks CLI accepts it; the Python SDK fails with a JSON error. `taxi.config.databricks_host()` and the dbt profile strip it.

## Likely interview questions

1. **How do you make a monthly load safe to re-run?** Each layer replaces exactly one month: `replaceWhere` in `jobs/bronze.py` and `jobs/silver.py`, and the `replace_where` strategy in `dbt/models/facts/fct_trips.sql`. A reload of 2026-07 wrote a new Delta version of every layer and left counts and checksums unchanged (`docs/evidence/a8/b1-scheduled-run-2026-07.txt`).
2. **How do you know no rows were lost?** `dbt/tests/reconcile_months.sql` compares footer rows, bronze, silver plus quarantine, and the fact for every month, and fails on any difference.
3. **What happens when the source adds a column?** `mergeSchema` adds it to bronze; silver lists its columns explicitly and reads a missing column as `NULL` (`COLUMNS` in `jobs/silver.py`). The footer scan found two such additions (`docs/evidence/a1-all-months-footer-schemas.txt`).
4. **Why quarantine instead of filtering?** Filtered rows disappear and the counts no longer add up. The quarantine table keeps every rejected row with its rule name, so counts per rule are one query.
5. **How did you choose the data quality rules?** From a profile of two months (`docs/evidence/a1-sample-profile.txt`), with the TLC dictionary for the meaning of the codes.
6. **Where does the trip key come from?** The month and the row's position in its file (`_metadata.row_index` in `jobs/bronze.py`). The source has no trip id. A content hash would also be unique for 2025-01 (`docs/evidence/a2-duplicate-rows-2025-01.txt`), but only the row position is unique by construction.
7. **Why is the fact incremental?** A monthly load should touch one month of the fact, not rebuild about 120 million rows. The `month` variable sets the `replace_where` predicate and the month filter (`dbt/models/facts/fct_trips.sql`).
8. **What does the schedule load, and what if a month is not published yet?** A scheduled run loads the month four months before its date, because TLC files appeared up to 83 days late (`src/taxi/schedule.py`, `docs/evidence/a8/b1-publication-lag.txt`). If a month is still missing, ingest exits 99, `BashOperator` marks the task skipped, and the downstream tasks are skipped (`docs/evidence/a4-airflow-clean-start-2025-02.txt`); a month more than six months old that is missing fails the run instead.
9. **How did you fit Airflow into the memory limit?** Three Airflow processes in one container with parallelism 1, measured at 624 MiB idle and 1,207 MiB peak (`airflow/docker-compose.yml`, `docs/evidence/a4-airflow-idle-memory.txt`, `docs/evidence/a4-airflow-clean-start-2025-02.txt`).
10. **How do you keep secrets out of git?** Credentials stay in a file outside the repository; `.gitignore` excludes env files; `scripts/secret_scan.sh` runs before each commit, chained with `&&` so a hit stops the commit (there is no git hook), and it looks for tokens, keys and the workspace address. Its proof is in `docs/evidence/a0-secret-scan-proof.txt`.
11. **How would you run this on ten times the data?** Parallel ingest to S3, incremental marts, Airflow with more workers, and month-scoped tests (the "At 10x" notes above).
12. **What would you do differently?** Land files in S3 from the start, and use Auto Loader or `COPY INTO` if months arrived as many small files instead of one large one. With one file a month, explicit per-month loads are easier to reason about and to re-run.
