# AIoT Ocean Monitoring & Predictive Analytics Pipeline

An end-to-end Data Engineering and Machine Learning platform designed to ingest real-time IoT sensor data from ocean buoys, transform it using modern data stack tools, and provide real-time predictive insights on water quality.

## System Architecture & Workflow

### 1. Real-Time Data Ingestion
* **Source:** High-frequency IoT sensors from ocean buoys (measuring Turbidity, CO2, Temperature, PM2.5, etc.).
* **Message Broker:** **RabbitMQ** acts as the ingestion layer, handling asynchronous data streams.
* **Ingestion Worker:** A Python-based `rabbit_consumer.py` listens to the queue and persists every event into the **PostgreSQL** `raw_iot_event` table.

### 2. Automated Data Transformation (dbt)
The transformation layer is managed by **dbt (data build tool)**, ensuring data quality and modularity:
* **Initial Setup:** Uses a **Full Refresh** strategy to initialize schemas and tables if they don't exist.
* **Efficiency:** Implements **Incremental Models** for high-volume sensor data, processing only new records since the last run to optimize performance.

### 3. Orchestration (Apache Airflow)
Two specialized DAGs manage the automation of the entire lifecycle:
* **`marts_pipeline.py` (Data Marts):** * Runs **every 15 minutes**.
    * Triggers dbt to transform raw data into analytics-ready tables.
* **`ml_pipeline.py` (Model Training):** * Runs **weekly**.
    * Retrains the Machine Learning model using updated historical data to prevent model drift and ensure prediction accuracy.

## Data Storage & Schema
The pipeline organizes data into the following key tables:

| Table Name | Layer | Description |
| :--- | :--- | :--- |
| **`raw_iot_event`** | Raw | Original JSON payloads from sensors. |
| **`stg_iot_event`** | Staging | Cleaned, casted, and filtered sensor data. |
| **`iot_metrics_hourly`** | Marts | Aggregated hourly statistics for environmental monitoring. |
| **`feature_water_quality`** | Feature | Finalized feature set prepared for ML model input. |
| **`prediction_table`** | Output | Real-time inference results produced by the ML model. |

## AI & Real-Time Inference
* **Model Management:** Models are trained using `scikit-learn`, serialized as `.joblib` files, and automatically versioned by the weekly pipeline.
* **Inference Pipeline:** * The `inference-consumer` service runs a dedicated Python worker (`consumer_ml.py`).
    * It listens to the live RabbitMQ stream.
    * For every incoming event, it performs real-time scaling and **Predictive Inference**, then saves the results immediately into the `prediction_table`.

## Technology Stack
* **Orchestration:** Apache Airflow (Celery Executor)
* **Transformation:** dbt (Postgres Adapter)
* **Messaging:** RabbitMQ
* **Database:** PostgreSQL
* **Machine Learning:** Scikit-learn, Pandas, Joblib
* **DevOps:** Docker & Docker Compose, Shell Scripting (`deploy.sh`, `restart.sh`)

## Deployment
1. Configure your `.env` file with `SERVER_IP`, `AIRFLOW_WEBSERVER_PORT`, and DB credentials.
2. Initialize and start the system:
   ```bash
   chmod +x *.sh
   ./deploy.sh
3. Access the Airflow Dashboard at http://<SERVER_IP>:<PORT>

## Project Impact (DE + AI)
By integrating Data Engineering with AI, this project provides:
1. End-to-End Automation: No manual intervention required from data ingestion to model deployment.
2. Scalable Infrastructure: Incremental loading and containerized services allow for handling increased buoy deployments.
3. Proactive Intelligence: Transforms raw "historical" data into "future" insights through real-time predictive analytics.