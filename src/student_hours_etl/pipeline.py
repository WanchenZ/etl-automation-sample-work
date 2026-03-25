from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import inspect

from student_hours_etl.config import Settings
from student_hours_etl.database import append_table, get_engine, replace_table, run_sql_file
from student_hours_etl.google_sheets import fetch_sheet_as_dataframe
from student_hours_etl.transforms import (
    build_tests_silver,
    build_total_weekly_hours,
    build_tutoring_silver,
)


BASE_DIR = Path(__file__).resolve().parents[2]
SQL_DIR = BASE_DIR / "sql"


def ingest_bronze() -> None:
    settings = Settings()
    engine = get_engine(settings.sqlalchemy_url)

    tutoring = fetch_sheet_as_dataframe(
        service_account_file=settings.google_service_account_file,
        spreadsheet_id=settings.tutoring_sheet_id,
        worksheet_name=settings.tutoring_worksheet,
    )
    tests = fetch_sheet_as_dataframe(
        service_account_file=settings.google_service_account_file,
        spreadsheet_id=settings.tests_sheet_id,
        worksheet_name=settings.tests_worksheet,
    )

    replace_table(engine, tutoring, "raw_tutoring")
    replace_table(engine, tests, "raw_tests")


def transform_silver() -> None:
    settings = Settings()
    engine = get_engine(settings.sqlalchemy_url)

    raw_tutoring = pd.read_sql_table("raw_tutoring", con=engine)
    raw_tests = pd.read_sql_table("raw_tests", con=engine)

    tutoring_silver = build_tutoring_silver(raw_tutoring)
    tests_silver = build_tests_silver(raw_tests)
    weekly_summary = build_total_weekly_hours(raw_tutoring, raw_tests)

    replace_table(engine, tutoring_silver, "silver_tutoring")
    replace_table(engine, tests_silver, "silver_tests")

    inspector = inspect(engine)
    if inspector.has_table("silver_student_weekly_hours"):
        append_table(engine, weekly_summary, "silver_student_weekly_hours")
    else:
        replace_table(engine, weekly_summary, "silver_student_weekly_hours")


def load_gold() -> None:
    settings = Settings()
    engine = get_engine(settings.sqlalchemy_url)

    silver_summary = pd.read_sql_table("silver_student_weekly_hours", con=engine)
    replace_table(engine, silver_summary, "gold_student_engagement_trends")


def load_bi_view() -> None:
    settings = Settings()
    engine = get_engine(settings.sqlalchemy_url)
    run_sql_file(engine, str(SQL_DIR / "views.sql"))


def run_all() -> None:
    ingest_bronze()
    transform_silver()
    load_gold()
    load_bi_view()
