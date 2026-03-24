from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'ml_engineer_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

db_env = {
    "DB_POSTGRES_HOST": os.environ.get("DB_POSTGRES_HOST", ""),
    "DB_POSTGRES_PORT": os.environ.get("DB_POSTGRES_PORT", ""),
    "DB_POSTGRES_NAME": os.environ.get("DB_POSTGRES_NAME", ""),
    "DB_POSTGRES_USER": os.environ.get("DB_POSTGRES_USER", ""),
    "DB_POSTGRES_PASS": os.environ.get("DB_POSTGRES_PASS", ""),
}

with DAG(
    'nemo_ml_training_daily',
    default_args=default_args,
    description='Retrain Forecast + Anomaly Models using data from dbt marts',
    schedule_interval='@weekly',
    catchup=False,
    tags=['ml', 'xgboost', 'isolation_forest', 'training'],
) as dag:

    dbt_runtime_setup = "export DBT_RUNTIME_DIR=$(mktemp -d /tmp/dbt_runtime_XXXXXX) && mkdir -p $DBT_RUNTIME_DIR/target $DBT_RUNTIME_DIR/logs $DBT_RUNTIME_DIR/packages && export DBT_TARGET_PATH=$DBT_RUNTIME_DIR/target && export DBT_LOG_PATH=$DBT_RUNTIME_DIR/logs && export DBT_PACKAGES_INSTALL_PATH=$DBT_RUNTIME_DIR/packages"

    # 1. check feature_water_quality data readiness
    check_data_readiness = BashOperator(
        task_id='check_data_readiness',
        bash_command="""
        set -euo pipefail
        cd /opt/airflow/dbt && \
        {{ params.dbt_runtime_setup }} && \
        dbt deps \
        --project-dir /opt/airflow/dbt \
        --profiles-dir /home/airflow/.dbt \
        --target prod \
        2>&1 && \
        dbt test \
        --select feature_water_quality \
        --project-dir /opt/airflow/dbt \
        --profiles-dir /home/airflow/.dbt \
        --target prod \
        2>&1
        """,
        params={"dbt_runtime_setup": dbt_runtime_setup},
    )

    # 2. train forecast model (XGBoost)
    train_forecast_model = BashOperator(
        task_id='train_forecast_model',
        bash_command="python3 /opt/airflow/ml_process/forecast_model.py",
        env={
            **db_env,
            "MODEL_DIR": "/opt/airflow/ml_process/models",
        }
    )

    # 3. train anomaly model (Isolation Forest)
    train_anomaly_model = BashOperator(
        task_id='train_anomaly_model',
        bash_command="python3 /opt/airflow/ml_process/anomaly_model.py",
        env={
            **db_env,
            "MODEL_DIR": "/opt/airflow/ml_process/models",
        }
    )

    # 4. Validate both bundle is set
    validate_model_bundles = BashOperator(
        task_id='validate_model_bundles',
        bash_command="""
        FORECAST=/opt/airflow/ml_process/models/forecast_bundle.pkl
        ANOMALY=/opt/airflow/ml_process/models/anomaly_bundle.pkl

        if [ ! -f "$FORECAST" ]; then
            echo "[ERROR] forecast_bundle.pkl NOT found!" && exit 1
        fi
        if [ ! -f "$ANOMALY" ]; then
            echo "[ERROR] anomaly_bundle.pkl NOT found!" && exit 1
        fi

        echo "[OK] forecast_bundle.pkl found"
        echo "[OK] anomaly_bundle.pkl found"
        """
    )

    # 5. Notify
    def notify_training_success():
        print("[DONE] forecast_bundle.pkl and anomaly_bundle.pkl saved to ml_process/")

    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=notify_training_success
    )

    # Flow: check -> train both model -> validate -> notify
    check_data_readiness >> [train_forecast_model, train_anomaly_model] >> validate_model_bundles >> success_notification