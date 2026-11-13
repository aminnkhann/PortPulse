FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir apache-airflow==2.10.5 \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt \
    && pip install --upgrade --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:root . /opt/portpulse
COPY --chown=airflow:root dbt/profiles.example.yml /home/airflow/.dbt/profiles.yml

ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/portpulse/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    DBT_PROFILES_DIR=/home/airflow/.dbt

WORKDIR /opt/portpulse
