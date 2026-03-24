from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# def check_feature_table(**context):
#     import psycopg2
#     conn = psycopg2.connect(
#         host=os.getenv("DB_POSTGRES_HOST"),
#         port=os.getenv("DB_POSTGRES_PORT"),
#         database=os.getenv("DB_POSTGRES_NAME"),
#         user=os.getenv("DB_POSTGRES_USER"),
#         password=os.getenv("DB_POSTGRES_PASS")
#     )
#     cur = conn.cursor()
#     cur.execute("SELECT COUNT(*) FROM feature_water_quality")
#     count = cur.fetchone()[0]
#     cur.close()
#     conn.close()

#     print(f"[*] feature_water_quality has {count} rows")

#     if count == 0:
#         return 'dbt_run_full_refresh'
#     return 'dbt_run_incremental'

def check_feature_table(**context):
    import psycopg2
    import os
    
    conn = None
    cur = None
    table_name = "feature_water_quality"
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_POSTGRES_HOST"),
            port=os.getenv("DB_POSTGRES_PORT"),
            database=os.getenv("DB_POSTGRES_NAME"),
            user=os.getenv("DB_POSTGRES_USER"),
            password=os.getenv("DB_POSTGRES_PASS")
        )
        cur = conn.cursor()

        # check if table is exists in Database
        check_table_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """
        cur.execute(check_table_query)
        table_exists = cur.fetchone()[0]

        if not table_exists:
            print(f"[*] Table {table_name} does not exist yet. Running full refresh.")
            return 'dbt_run_full_refresh'

        # if table exists, check row
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"[*] {table_name} has {count} rows")

        if count == 0:
            return 'dbt_run_full_refresh'
        
        return 'dbt_run_incremental'

    except Exception as e:
        print(f"[!] Error checking table: {e}")
        # In-case it's Error by other reason, decided Full Refresh for security
        return 'dbt_run_full_refresh'
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

with DAG(
    'nemo_dbt_pipeline',
    default_args=default_args,
    description='Pipeline for Water Quality Transformation and ML Training',
    schedule_interval=timedelta(minutes=30), # run every 30 min (Micro-batch)
    catchup=False,
    tags=['aiot', 'dbt', 'ml'],
) as dag:

    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt debug \
        --project-dir /opt/airflow/dbt \
        --profiles-dir /home/airflow/.dbt
        """,
        dag=dag
    )

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt deps \
        --project-dir /opt/airflow/dbt \
        --profiles-dir /home/airflow/.dbt
        """,
    )

    check_table = BranchPythonOperator(
        task_id='check_feature_table',
        python_callable=check_feature_table,
    )

    dbt_run_full_refresh = BashOperator(
        task_id='dbt_run_full_refresh',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run \
        --full-refresh \
        --project-dir /opt/airflow/dbt \
        --profiles-dir /home/airflow/.dbt
        """,
    )

    dbt_run_incremental = BashOperator(
        task_id='dbt_run_incremental',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run \
        --project-dir /opt/airflow/dbt \
        --profiles-dir /home/airflow/.dbt
        """,
    )

    # check data quality
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt test \
        --project-dir /opt/airflow/dbt \
        --profiles-dir /home/airflow/.dbt
        """,
        trigger_rule='none_failed_min_one_success',
        dag=dag
    )

    def notify_new_data():
        print("Successfully updated dbt models. Dashboard is now fresh.")

    post_update_notify = PythonOperator(
        task_id='post_update_notify',
        python_callable=notify_new_data,
        trigger_rule='none_failed_min_one_success',
    )

    dbt_debug >> dbt_deps >> check_table
    check_table >> dbt_run_full_refresh >> dbt_test
    check_table >> dbt_run_incremental >> dbt_test
    dbt_test >> post_update_notify