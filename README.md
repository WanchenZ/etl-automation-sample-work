# Weekly Spent Hours ETL Sample Work

Python ETL pipeline for weekly student expense (in hours) tracking using a Bronze/Silver/Gold medallion pattern on PostgreSQL. Mimics the actual ETL pipeline at a tutoring facility with confidential info removed.

## What it does

- Ingests tutoring and test data from Google Sheets with `gspread`
- Stores raw snapshots in PostgreSQL bronze tables: `raw_tutoring`, `raw_tests`
- Cleans and transforms data into silver tables: `silver_tutoring`, `silver_tests`, `silver_student_weekly_hours`
- Publishes a gold presentation table: `gold_student_engagement_trends`
- Publishes `v_student_engagement_trends` for Tableau and Power BI
- Orchestrates the flow weekly in Airflow every Monday at 8:00 AM America/New_York

## Project layout

- `src/student_hours_etl`: ETL package
- `dags/student_hours_etl_dag.py`: Airflow DAG
- `sql/views.sql`: BI-facing SQL view
- `tests/test_transforms.py`: unit tests
- `.github/workflows/main.yml`: CI pipeline

## Environment

Copy `.env.example` and provide valid PostgreSQL and Google service account settings.

## Run locally

```bash
pip install -r requirements.txt
export PYTHONPATH=./src
python -m student_hours_etl.cli run-all
```

## Prototype visualization

To generate sample Bronze, Silver, and Gold outputs plus a static dashboard preview:

```bash
export PYTHONPATH=./src
python3 scripts/prototype_visualization_demo.py
```

This writes demo artifacts under `artifacts/prototype_demo/`, including:

- `bronze_raw_tutoring.csv`
- `bronze_raw_tests.csv`
- `silver_tutoring.csv`
- `silver_tests.csv`
- `gold_student_engagement_trends.csv`
- `prototype_dashboard.html`

## Airflow task order

`Ingest >> Transform >> Load_Gold >> Load_to_BI_View`

## Assumptions

- Each run replaces the bronze tables with the latest full Google Sheet snapshot.
- The weekly summary reflects the data present in the source sheets at run time.
- `silver_student_weekly_hours` stores a new snapshot on each scheduled run so the gold layer and BI view can show historical trends over time.
