#!/usr/bin/env bash
set -e

admin_user=${AIRFLOW_ADMIN_USERNAME:-admin}
admin_password=${AIRFLOW_ADMIN_PASSWORD:-admin}
admin_email=${AIRFLOW_ADMIN_EMAIL:-admin@example.com}

airflow db migrate

if airflow users list --output json | python -c 'import json, sys; sys.exit(0 if any(user["username"] == sys.argv[1] for user in json.load(sys.stdin)) else 1)' "$admin_user"; then
    airflow users reset-password --username "$admin_user" --password "$admin_password"
else
    airflow users create \
        --username "$admin_user" \
        --password "$admin_password" \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email "$admin_email"
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
