FROM apache/airflow:2.10.5

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

COPY ml_process/ /opt/airflow/ml_process/
RUN mkdir -p /opt/airflow/ml_process/models && \
    chown -R airflow:root /opt/airflow/ml_process && \
    chmod -R 775 /opt/airflow/ml_process

USER airflow

RUN pip install --no-cache-dir \
    "dbt-core==1.9.1" \
    "dbt-postgres==1.9.1" \
    "dbt-adapters==1.10.1" \
    "dbt-common==1.13.0"

RUN pip install --no-cache-dir \
    apache-airflow-providers-docker \
    apache-airflow-providers-http \
    pika \
    pandas \
    xgboost \
    scikit-learn \
    joblib \
    psycopg2-binary \
    pyarrow

RUN pip install --no-cache-dir --upgrade "protobuf>=5.26.0,<6.0.0"