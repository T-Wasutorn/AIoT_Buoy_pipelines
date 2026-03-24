#!/bin/bash
echo "Triggering ML training DAG..."
docker exec aiot_ocean-airflow-webserver-1 \
    airflow dags trigger ocean_ml_training_daily
echo "Check progress at http://${SERVER_IP}:8081"