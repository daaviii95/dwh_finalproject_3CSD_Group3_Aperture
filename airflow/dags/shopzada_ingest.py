"""
ShopZada Kimball ingestion DAG.

This DAG orchestrates the existing `scripts/ingest.py` loader so that the
Operations, Marketing, Customer Management, Enterprise, and Business datasets
are landed into the `stg_*` tables that serve as the raw vault / Kimball staging
layer for downstream dimensional modeling.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

SHOPZADA_DATA_DIR = Path(os.environ.get("SHOPZADA_DATA_DIR", "/opt/airflow/data"))
SHOPZADA_REPO_ROOT = Path(os.environ.get("SHOPZADA_REPO_ROOT", "/opt/airflow/repo"))
INGEST_SCRIPT = SHOPZADA_REPO_ROOT / "scripts" / "ingest.py"

DB_HOST = os.environ.get("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "shopzada")
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "postgres")

STAGING_TABLES = [
    "stg_business_department_product_list_xlsx",
    "stg_customer_management_department_user_data_json",
    "stg_enterprise_department_order_with_merchant_data3_csv",
    "stg_marketing_department_transactional_campaign_data_csv",
    "stg_operations_department_order_data_20211001_20220101_csv",
]


def run_shopzada_ingestion(**_context) -> None:
    """Invoke scripts/ingest.py with the Airflow environment variables."""
    if not INGEST_SCRIPT.exists():
        raise FileNotFoundError(f"Ingestion script not found: {INGEST_SCRIPT}")

    env = os.environ.copy()
    env["DATA_DIR"] = str(SHOPZADA_DATA_DIR)
    env["POSTGRES_HOST"] = DB_HOST
    env["POSTGRES_PORT"] = DB_PORT
    env["POSTGRES_DB"] = DB_NAME
    env["POSTGRES_USER"] = DB_USER
    env["POSTGRES_PASSWORD"] = DB_PASS

    logging.info("Starting ShopZada ingestion via %s", INGEST_SCRIPT)
    subprocess.run(["python", str(INGEST_SCRIPT)], check=True, env=env)
    logging.info("ShopZada ingestion completed.")


def snapshot_staging_tables(**_context) -> None:
    """Emit row-count telemetry for key Kimball staging tables."""
    from sqlalchemy import create_engine, text

    engine_url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    engine = create_engine(engine_url)

    metrics = {}
    with engine.begin() as conn:
        for table in STAGING_TABLES:
            query = text(f'SELECT COUNT(*) AS cnt FROM "{table}"')
            metrics[table] = conn.execute(query).scalar_one()

    logging.info("Kimball staging snapshot: %s", metrics)


default_args = {
    "owner": "shopzada-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="shopzada_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["shopzada", "kimball", "staging"],
    description="Daily ingestion of ShopZada source data into Kimball staging tables.",
) as dag:
    validate_data_mount = BashOperator(
        task_id="validate_data_mount",
        bash_command=f"test -d {SHOPZADA_DATA_DIR} && ls -1 {SHOPZADA_DATA_DIR}",
    )

    run_ingestion = PythonOperator(
        task_id="ingest_all_departments",
        python_callable=run_shopzada_ingestion,
    )

    snapshot_tables = PythonOperator(
        task_id="snapshot_staging_counts",
        python_callable=snapshot_staging_tables,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    validate_data_mount >> run_ingestion >> snapshot_tables

