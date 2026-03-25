from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def normalize_boolean(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def tutoring_hours(attendance: object, is_one_on_one: object) -> int:
    if str(attendance).strip().lower() == "excused":
        return 0
    if normalize_boolean(is_one_on_one):
        return 4
    return 2


def build_tutoring_silver(raw_tutoring: pd.DataFrame) -> pd.DataFrame:
    tutoring = raw_tutoring.copy()
    tutoring = tutoring.dropna(subset=["student_id"])
    tutoring["session_date"] = pd.to_datetime(tutoring["session_date"], errors="coerce")
    tutoring["hours"] = tutoring.apply(
        lambda row: tutoring_hours(row.get("attendance"), row.get("is_one_on_one")),
        axis=1,
    )
    return tutoring


def build_tests_silver(raw_tests: pd.DataFrame) -> pd.DataFrame:
    tests = raw_tests.copy()
    tests = tests.dropna(subset=["student_id"])
    tests["hours"] = 2
    return tests


def build_total_weekly_hours(raw_tutoring: pd.DataFrame, raw_tests: pd.DataFrame) -> pd.DataFrame:
    tutoring = build_tutoring_silver(raw_tutoring)
    tests = build_tests_silver(raw_tests)

    tutoring_summary = (
        tutoring.groupby(["student_id", "student_name"], dropna=False, as_index=False)["hours"]
        .sum()
        .rename(columns={"hours": "tutoring_hours"})
    )
    tests_summary = (
        tests.groupby(["student_id", "student_name"], dropna=False, as_index=False)["hours"]
        .sum()
        .rename(columns={"hours": "test_hours"})
    )

    merged = tutoring_summary.merge(
        tests_summary,
        on=["student_id", "student_name"],
        how="outer",
    )
    merged["tutoring_hours"] = merged["tutoring_hours"].fillna(0)
    merged["test_hours"] = merged["test_hours"].fillna(0)
    merged["total_weekly_hours"] = merged["tutoring_hours"] + merged["test_hours"]
    merged["report_date"] = datetime.now(timezone.utc)

    return merged[
        ["student_id", "student_name", "tutoring_hours", "test_hours", "total_weekly_hours", "report_date"]
    ]

