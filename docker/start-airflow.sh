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

exec airflow standalone
