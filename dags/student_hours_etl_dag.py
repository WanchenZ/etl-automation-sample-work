from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from student_hours_etl.pipeline import ingest_bronze, load_bi_view, load_gold, transform_silver


with DAG(
    dag_id="student_hours_etl",
    description="Weekly ETL for student hour tracking",
    schedule="0 8 * * 1",
    start_date=pendulum.datetime(2026, 3, 23, 8, 0, tz="America/New_York"),
    catchup=False,
    tags=["etl", "student-hours", "medallion"],
) as dag:
    ingest = PythonOperator(
        task_id="Ingest",
        python_callable=ingest_bronze,
    )

    transform = PythonOperator(
        task_id="Transform",
        python_callable=transform_silver,
    )

    load_gold_task = PythonOperator(
        task_id="Load_Gold",
        python_callable=load_gold,
    )

    load_to_bi_view = PythonOperator(
        task_id="Load_to_BI_View",
        python_callable=load_bi_view,
    )

    ingest >> transform >> load_gold_task >> load_to_bi_view
