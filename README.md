ShopZada is a rapidly growing e-commerce platform that has expanded globally, now 
handling over half a million orders and two million line items from diverse product 
categories. Despite this growth, their data remains fragmented across multiple departments:
Business, Customer Management, Enterprise, Marketing, and Operations.

**Groupname**, was tasked to design, implement, and operationalize a complete Data Warehouse 
solution that integrates these datasets, delivers analytical insights, and supports data-driven decision-making.

Chosen Data Warehouse Methodology: Data vault and Kimball (Dimensional Modeling)


## Dockerized Staging Layer

- Build and run the ShopZada images:
  ```powershell
  docker compose -f docker/docker-compose.yml up --build shopzada-ingest
  ```
- Custom images:
  - `shopzada-db:latest` – Postgres 15 with the `shopzada` warehouse database.
  - `shopzada-ingest:latest` – runs `scripts/ingest.py` against the mounted `data/` folder.
- Data sources must live in `./data`; the compose file mounts it read-only into each container.

## Airflow Orchestration

- DAGs live in `airflow/dags`. The primary DAG `shopzada_ingest` orchestrates the Kimball staging ingest.
- Start the full stack (Postgres + Airflow scheduler/webserver) and open the UI on http://localhost:8080:
  ```powershell
  docker compose -f docker/docker-compose.yml up --build shopzada-airflow-webserver shopzada-airflow-scheduler
  ```
  The first run will also start `shopzada-airflow-init` to apply migrations and provision the default admin user (`admin` / `admin`).
- The DAG calls the existing ingestion script so there is a single source of truth for cleaning, staging, and logging.



