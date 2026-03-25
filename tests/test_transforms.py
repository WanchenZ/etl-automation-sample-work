from __future__ import annotations

import pandas as pd

from student_hours_etl.transforms import (
    build_total_weekly_hours,
    build_tutoring_silver,
    tutoring_hours,
)


def test_tutoring_hours_excused_is_zero() -> None:
    assert tutoring_hours("excused", True) == 0


def test_tutoring_hours_one_on_one_is_four() -> None:
    assert tutoring_hours("present", True) == 4


def test_tutoring_hours_group_session_is_two() -> None:
    assert tutoring_hours("present", False) == 2


def test_build_tutoring_silver_drops_missing_student_ids() -> None:
    raw = pd.DataFrame(
        [
            {
                "student_name": "Ana",
                "student_id": "100",
                "tutor_name": "T1",
                "session_date": "2026-03-20",
                "subject": "Math",
                "is_one_on_one": True,
                "attendance": "present",
            },
            {
                "student_name": "Ben",
                "student_id": None,
                "tutor_name": "T2",
                "session_date": "2026-03-20",
                "subject": "Reading",
                "is_one_on_one": False,
                "attendance": "present",
            },
        ]
    )

    silver = build_tutoring_silver(raw)

    assert silver["student_id"].tolist() == ["100"]
    assert silver["hours"].tolist() == [4]


def test_build_total_weekly_hours_combines_tutoring_and_tests() -> None:
    raw_tutoring = pd.DataFrame(
        [
            {
                "student_name": "Ana",
                "student_id": "100",
                "tutor_name": "T1",
                "session_date": "2026-03-20",
                "subject": "Math",
                "is_one_on_one": True,
                "attendance": "present",
            },
            {
                "student_name": "Ana",
                "student_id": "100",
                "tutor_name": "T1",
                "session_date": "2026-03-21",
                "subject": "Science",
                "is_one_on_one": False,
                "attendance": "excused",
            },
        ]
    )
    raw_tests = pd.DataFrame(
        [
            {
                "student_name": "Ana",
                "student_id": "100",
                "subject": "Math",
                "test_number": 1,
            }
        ]
    )

    summary = build_total_weekly_hours(raw_tutoring, raw_tests)

    record = summary.iloc[0].to_dict()
    assert record["tutoring_hours"] == 4
    assert record["test_hours"] == 2
    assert record["total_weekly_hours"] == 6

