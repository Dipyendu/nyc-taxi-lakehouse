# Acceptance checks

The project is done when all ten checks pass. Evidence files with "acceptance" or a check number in their name refer to this list.

| # | Check | Latest evidence |
|---|---|---|
| 1 | `make ci` (lint, unit tests, offline `dbt parse`) exits 0. | `docs/evidence/make-ci-2026-11-17.txt` |
| 2 | Loading one month twice leaves the bronze and fact row counts and a content checksum unchanged. | `docs/evidence/a8/b1-scheduled-run-2026-07.txt` |
| 3 | For every loaded month, Parquet footer rows = bronze rows = silver rows + quarantined rows. | `docs/evidence/a8/acceptance-fresh.txt`, `docs/evidence/a8/dbt-fixes-fresh-build.txt` |
| 4 | 2024-12 and 2025-01 sit in one bronze table, and the recorded schema difference matches the files. | `docs/evidence/a8/acceptance-fresh.txt`, `docs/evidence/a1-schema-diff-2024-12-vs-2025-01.txt` |
| 5 | `dbt build` ends with zero failures, and every model has its key tests. | `docs/evidence/a8/dbt-fixes-fresh-build.txt` |
| 6 | An Airflow run for a month not loaded before succeeds from a clean start. | `docs/evidence/a4-airflow-clean-start-2025-02.txt` |
| 7 | The run ledger lists every Databricks job run with its duration. | `docs/evidence/a8/acceptance-fresh.txt`, `docs/run-ledger.md` |
| 8 | Every number in the README names its evidence file, and `docs/EXPLAIN.md` is complete. | `docs/evidence/a8/acceptance-fresh.txt` |
| 9 | The secret scan is clean, and it is proven to find a planted fake secret. | `docs/evidence/a8/acceptance-fresh.txt`, `docs/evidence/a0-secret-scan-proof.txt` |
| 10 | All work is committed, the working tree is clean, and no remote is configured. | `git status`, `git remote` |
