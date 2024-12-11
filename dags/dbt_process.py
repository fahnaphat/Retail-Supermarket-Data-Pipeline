from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG('dbt_process', default_args=default_args, schedule_interval='@daily', catchup=False):
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='dbt debug --project-dir /opt/airflow/dbt/mydbt --profiles-dir /opt/airflow/dbt/mydbt'
    )

    dbt_run_stg_models = BashOperator(
        task_id='dbt_run_stg_models',
        bash_command='dbt run --project-dir /opt/airflow/dbt/mydbt --profiles-dir /opt/airflow/dbt/mydbt --select staging.* '
    )

    dbt_run_marts_models = BashOperator(
        task_id='dbt_run_marts_models',
        bash_command='dbt run --project-dir /opt/airflow/dbt/mydbt --profiles-dir /opt/airflow/dbt/mydbt --select marts.* '
    )

    dbt_generate_models = BashOperator(
        task_id = 'dbt_generate_models',
        bash_command = 'dbt docs generate --project-dir /opt/airflow/dbt/mydbt --profiles-dir /opt/airflow/dbt/mydbt'
    )

    dbt_debug >> dbt_run_stg_models >> dbt_run_marts_models >> dbt_generate_models