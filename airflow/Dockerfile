# Official Airflow image + this project's dependencies in a separate virtualenv, so dbt and the
# Databricks SDK never conflict with Airflow's own pinned packages.
FROM apache/airflow:3.3.2-python3.12

USER root
COPY airflow/requirements.txt /opt/taxi-requirements.txt
RUN python -m venv /opt/taxi-venv \
 && /opt/taxi-venv/bin/pip install --no-cache-dir --require-hashes -r /opt/taxi-requirements.txt \
 && chown -R airflow:root /opt/taxi-venv
USER airflow

# Project code is mounted at /opt/taxi at run time (see docker-compose.yml).
ENV PYTHONPATH=/opt/taxi/src
