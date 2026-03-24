#!/bin/bash
# deploy.sh

echo "[1/2] Running airflow init..."
docker compose up airflow-init
if [ $? -ne 0 ]; then
    echo "ERROR: airflow-init failed, aborting deploy"
    exit 1
fi

echo "[2/2] Starting all services..."
docker compose up -d

echo "Deploy complete!"