FROM apache/airflow:2.10.2-python3.11

USER root

# Copy requirements file as root (file operations allowed)
COPY requirements.txt /opt/airflow/requirements-shopzada.txt

# Switch to airflow user before installing packages (Airflow requirement)
USER airflow

# Reuse project dependencies inside Airflow environment so DAG tasks can call scripts/ingest.py
RUN pip install --no-cache-dir -r /opt/airflow/requirements-shopzada.txt

