#!/usr/bin/env bash
set -e

airflow db migrate

if airflow users list --output json | python -c 'import json, sys; sys.exit(0 if any(user["username"] == "admin" for user in json.load(sys.stdin)) else 1)'; then
    airflow users reset-password --username admin --password admin
else
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
fi

pids=()
airflow api-server --port 8080 & pids+=("$!")
airflow scheduler & pids+=("$!")
airflow dag-processor & pids+=("$!")
airflow triggerer & pids+=("$!")

stop_airflow() {
    trap - EXIT INT TERM
    kill "${pids[@]}" 2>/dev/null || true
    wait
}

trap stop_airflow EXIT INT TERM
wait -n "${pids[@]}"
