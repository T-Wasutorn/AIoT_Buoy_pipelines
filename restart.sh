#!/bin/bash
SERVICE=$1
if [ -z "$SERVICE" ]; then
    echo "Usage: ./restart.sh <service_name>"
    echo "Example: ./restart.sh inference-consumer"
    exit 1
fi
echo "Rebuilding and restarting $SERVICE..."
docker compose build $SERVICE
docker compose up -d $SERVICE
echo "Done."