FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY --chown=airflow:root requirements-airflow.txt requirements.txt requirements-dbt.txt /tmp/
RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt \
    -r /tmp/requirements-airflow.txt
RUN grep -v '^ruff' /tmp/requirements.txt > /tmp/requirements-runtime.txt && \
    pip install --upgrade --no-cache-dir \
    apache-airflow==2.10.5 opentelemetry-exporter-otlp==1.45.2 \
    -r /tmp/requirements-runtime.txt \
    -r /tmp/requirements-dbt.txt
RUN pip install --no-cache-dir 'ruff>=0.11,<1'

COPY --chown=airflow:root . /opt/portpulse
COPY --chown=airflow:root dbt/profiles.example.yml /home/airflow/.dbt/profiles.yml

ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/portpulse/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    DBT_PROFILES_DIR=/home/airflow/.dbt

WORKDIR /opt/portpulse
