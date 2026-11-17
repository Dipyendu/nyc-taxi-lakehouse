SHELL := /bin/bash
export PYTHONUNBUFFERED := 1
SECRETS ?= $(or $(TAXI_SECRETS_FILE),../.secrets.env)
WITH_SECRETS := set -a; . $(SECRETS); [ -f local.env ] && . ./local.env; set +a; export DATABRICKS_HOST="$${DATABRICKS_HOST%%\?*}";
LANDING_ROOT := $(shell uv run --quiet python -c 'from taxi.config import LANDING_ROOT; print(LANDING_ROOT)')
BUNDLE_VARS := --var landing_root=$(LANDING_ROOT)

.PHONY: lint test ci scan deploy ingest run

lint:
	uv run ruff check .

test:
	uv run pytest -q

ci: lint test
	cd dbt && uv run dbt parse --profiles-dir . --target ci

scan:
	scripts/secret_scan.sh

deploy:
	$(WITH_SECRETS) scripts/ledger.sh "make deploy" "bundle files" databricks bundle deploy $(BUNDLE_VARS)

ingest:
	$(WITH_SECRETS) scripts/ledger.sh "make ingest $(MONTH) (download + volume upload)" "see manifest" uv run python -m taxi.ingest $(MONTH)

# One month end to end without Airflow, same task order as the DAG:
# ingest, bronze, silver, dbt build, reconciliation. Every Databricks step goes through the ledger.
run: ingest
	$(WITH_SECRETS) scripts/ledger.sh "make bronze $(MONTH)" "1 month" uv run python -m taxi.run_job taxi_bronze $(MONTH)
	$(WITH_SECRETS) scripts/ledger.sh "make silver $(MONTH)" "1 month" uv run python -m taxi.run_job taxi_silver $(MONTH)
	$(WITH_SECRETS) cd dbt && ../scripts/ledger.sh "make dbt build $(MONTH)" "1 month" uv run dbt build --profiles-dir . --vars "{month: '$(MONTH)'}" --exclude reconcile_months
	$(WITH_SECRETS) cd dbt && ../scripts/ledger.sh "make dbt reconcile" "all months" uv run dbt test --profiles-dir . --select reconcile_months
