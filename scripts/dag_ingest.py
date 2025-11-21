from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "shopzada",
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
}

with DAG(
    dag_id="shopzada_ingestion_pipeline",
    default_args=default_args,
    schedule_interval="@daily",    
    catchup=False,
    tags=["shopzada", "ingestion"],
) as dag:

    check_docker = BashOperator(
        task_id="check_docker",
        bash_command="docker info"
    )

    reset_env = BashOperator(
        task_id="reset_env",
        bash_command="cd /opt/airflow/dags/dwh-proj && chmod +x reset.sh && ./reset.sh"
    )

    run_ingest = BashOperator(
        task_id="run_ingest",
        bash_command=(
            "cd /opt/airflow/dags/dwh-proj && "
            "docker compose run --rm ingest"
        )
    )

    check_docker >> reset_env >> run_ingest
