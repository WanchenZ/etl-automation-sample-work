from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from student_hours_etl.transforms import build_tests_silver, build_tutoring_silver


OUTPUT_DIR = Path(__file__).resolve().parents[1] / "artifacts" / "prototype_demo"


def sample_tutoring_data() -> pd.DataFrame:
    rows = [
        ["Alicia Brown", "1001", "Mr. Lee", "2026-03-02", "Math", True, "present"],
        ["Alicia Brown", "1001", "Ms. Park", "2026-03-03", "Science", False, "present"],
        ["Brandon Diaz", "1002", "Mr. Lee", "2026-03-02", "Math", False, "present"],
        ["Brandon Diaz", "1002", "Mr. Lee", "2026-03-05", "Reading", False, "excused"],
        ["Chloe Evans", "1003", "Ms. Park", "2026-03-04", "Science", True, "present"],
        ["Darius Ford", "1004", "Mr. Khan", "2026-03-05", "Math", False, "present"],
        ["Alicia Brown", "1001", "Mr. Lee", "2026-03-09", "Math", True, "present"],
        ["Brandon Diaz", "1002", "Mr. Lee", "2026-03-10", "Math", False, "present"],
        ["Chloe Evans", "1003", "Ms. Park", "2026-03-10", "Science", False, "present"],
        ["Darius Ford", "1004", "Mr. Khan", "2026-03-12", "Math", True, "present"],
        ["Alicia Brown", "1001", "Ms. Park", "2026-03-16", "Science", False, "excused"],
        ["Brandon Diaz", "1002", "Mr. Lee", "2026-03-16", "Reading", False, "present"],
        ["Chloe Evans", "1003", "Ms. Park", "2026-03-18", "Science", True, "present"],
        ["Darius Ford", "1004", "Mr. Khan", "2026-03-19", "Math", False, "present"],
        ["Alicia Brown", "1001", "Mr. Lee", "2026-03-23", "Math", True, "present"],
        ["Brandon Diaz", "1002", "Mr. Lee", "2026-03-23", "Math", False, "present"],
        ["Chloe Evans", "1003", "Ms. Park", "2026-03-24", "Science", False, "present"],
        ["Darius Ford", "1004", "Mr. Khan", "2026-03-24", "Math", True, "excused"],
    ]
    columns = [
        "student_name",
        "student_id",
        "tutor_name",
        "session_date",
        "subject",
        "is_one_on_one",
        "attendance",
    ]
    return pd.DataFrame(rows, columns=columns)


def sample_tests_data() -> pd.DataFrame:
    rows = [
        ["Alicia Brown", "1001", "Math", 1, "2026-03-06"],
        ["Brandon Diaz", "1002", "Reading", 1, "2026-03-06"],
        ["Chloe Evans", "1003", "Science", 1, "2026-03-06"],
        ["Alicia Brown", "1001", "Science", 2, "2026-03-13"],
        ["Darius Ford", "1004", "Math", 1, "2026-03-13"],
        ["Brandon Diaz", "1002", "Math", 2, "2026-03-20"],
        ["Chloe Evans", "1003", "Science", 2, "2026-03-20"],
        ["Alicia Brown", "1001", "Math", 3, "2026-03-27"],
        ["Darius Ford", "1004", "Math", 2, "2026-03-27"],
    ]
    columns = ["student_name", "student_id", "subject", "test_number", "test_date"]
    return pd.DataFrame(rows, columns=columns)


def build_weekly_gold(raw_tutoring: pd.DataFrame, raw_tests: pd.DataFrame) -> pd.DataFrame:
    tutoring = build_tutoring_silver(raw_tutoring)
    tests = build_tests_silver(raw_tests)

    tutoring["report_week"] = tutoring["session_date"].dt.to_period("W-SUN").dt.start_time
    tests["test_date"] = pd.to_datetime(raw_tests["test_date"], errors="coerce")
    tests["report_week"] = tests["test_date"].dt.to_period("W-SUN").dt.start_time

    tutoring_summary = (
        tutoring.groupby(["report_week", "student_id", "student_name"], as_index=False)["hours"]
        .sum()
        .rename(columns={"hours": "tutoring_hours"})
    )
    test_summary = (
        tests.groupby(["report_week", "student_id", "student_name"], as_index=False)["hours"]
        .sum()
        .rename(columns={"hours": "test_hours"})
    )

    gold = tutoring_summary.merge(
        test_summary,
        on=["report_week", "student_id", "student_name"],
        how="outer",
    )
    gold["tutoring_hours"] = gold["tutoring_hours"].fillna(0)
    gold["test_hours"] = gold["test_hours"].fillna(0)
    gold["total_weekly_hours"] = gold["tutoring_hours"] + gold["test_hours"]
    gold["report_date"] = gold["report_week"] + pd.Timedelta(days=6, hours=8)
    return gold.sort_values(["report_week", "student_name"]).reset_index(drop=True)


def svg_bar_chart(latest_week: pd.DataFrame) -> str:
    chart_width = 760
    chart_height = 280
    margin_left = 140
    margin_bottom = 40
    plot_width = chart_width - margin_left - 20
    plot_height = chart_height - 30 - margin_bottom
    max_value = max(float(latest_week["total_weekly_hours"].max()), 1.0)
    scale = plot_width / max_value
    bar_height = 26
    gap = 18
    palette = {"tutoring": "#2a9d8f", "tests": "#e9c46a"}

    rows = []
    for idx, row in enumerate(latest_week.itertuples(index=False)):
        y = 20 + idx * (bar_height + gap)
        tutoring_width = row.tutoring_hours * scale
        test_width = row.test_hours * scale
        rows.append(
            f"<text x='8' y='{y + 18}' font-size='13' fill='#243b53'>{row.student_name}</text>"
            f"<rect x='{margin_left}' y='{y}' width='{tutoring_width}' height='{bar_height}' fill='{palette['tutoring']}' rx='4' />"
            f"<rect x='{margin_left + tutoring_width}' y='{y}' width='{test_width}' height='{bar_height}' fill='{palette['tests']}' rx='4' />"
            f"<text x='{margin_left + tutoring_width + test_width + 8}' y='{y + 18}' font-size='12' fill='#102a43'>{int(row.total_weekly_hours)}h</text>"
        )

    axis = []
    for tick in range(int(max_value) + 1):
        x = margin_left + tick * scale
        axis.append(f"<line x1='{x}' y1='10' x2='{x}' y2='{chart_height - margin_bottom}' stroke='#d9e2ec' stroke-width='1' />")
        axis.append(f"<text x='{x}' y='{chart_height - 14}' text-anchor='middle' font-size='11' fill='#486581'>{tick}</text>")

    return (
        f"<svg viewBox='0 0 {chart_width} {chart_height}' width='100%' height='100%' role='img' aria-label='Hours by student'>"
        + "".join(axis)
        + "".join(rows)
        + "</svg>"
    )


def svg_line_chart(gold: pd.DataFrame) -> str:
    chart_width = 760
    chart_height = 320
    margin_left = 60
    margin_right = 20
    margin_top = 20
    margin_bottom = 50
    plot_width = chart_width - margin_left - margin_right
    plot_height = chart_height - margin_top - margin_bottom

    weeks = sorted(gold["report_week"].drop_duplicates().tolist())
    students = gold.groupby("student_name")["total_weekly_hours"].sum().nlargest(3).index.tolist()
    max_value = max(float(gold["total_weekly_hours"].max()), 1.0)
    palette = ["#2a9d8f", "#e76f51", "#264653"]

    def x_pos(index: int) -> float:
        if len(weeks) == 1:
            return margin_left + plot_width / 2
        return margin_left + (plot_width * index / (len(weeks) - 1))

    def y_pos(value: float) -> float:
        return margin_top + plot_height - ((value / max_value) * plot_height)

    axis = [
        f"<line x1='{margin_left}' y1='{margin_top}' x2='{margin_left}' y2='{chart_height - margin_bottom}' stroke='#9fb3c8' />",
        f"<line x1='{margin_left}' y1='{chart_height - margin_bottom}' x2='{chart_width - margin_right}' y2='{chart_height - margin_bottom}' stroke='#9fb3c8' />",
    ]

    for idx, week in enumerate(weeks):
        x = x_pos(idx)
        axis.append(f"<line x1='{x}' y1='{margin_top}' x2='{x}' y2='{chart_height - margin_bottom}' stroke='#eef2f6' />")
        axis.append(
            f"<text x='{x}' y='{chart_height - 20}' text-anchor='middle' font-size='11' fill='#486581'>"
            f"{pd.Timestamp(week).strftime('%b %d')}</text>"
        )

    for tick in range(int(max_value) + 1):
        y = y_pos(tick)
        axis.append(f"<line x1='{margin_left}' y1='{y}' x2='{chart_width - margin_right}' y2='{y}' stroke='#eef2f6' />")
        axis.append(f"<text x='{margin_left - 10}' y='{y + 4}' text-anchor='end' font-size='11' fill='#486581'>{tick}</text>")

    series = []
    legend = []
    for color, student in zip(palette, students):
        subset = (
            gold[gold["student_name"] == student][["report_week", "total_weekly_hours"]]
            .set_index("report_week")
            .reindex(weeks, fill_value=0)
            .reset_index()
        )
        points = " ".join(
            f"{x_pos(idx)},{y_pos(value)}"
            for idx, value in enumerate(subset["total_weekly_hours"].tolist())
        )
        series.append(f"<polyline fill='none' stroke='{color}' stroke-width='3' points='{points}' />")
        for idx, value in enumerate(subset["total_weekly_hours"].tolist()):
            series.append(f"<circle cx='{x_pos(idx)}' cy='{y_pos(value)}' r='4' fill='{color}' />")
        legend_y = 18 + len(legend) * 18
        legend.append(
            f"<rect x='{chart_width - 180}' y='{legend_y - 10}' width='12' height='12' fill='{color}' rx='2' />"
            f"<text x='{chart_width - 162}' y='{legend_y}' font-size='12' fill='#243b53'>{student}</text>"
        )

    return (
        f"<svg viewBox='0 0 {chart_width} {chart_height}' width='100%' height='100%' role='img' aria-label='Weekly trend by student'>"
        + "".join(axis)
        + "".join(series)
        + "".join(legend)
        + "</svg>"
    )


def render_html_dashboard(gold: pd.DataFrame) -> str:
    latest_week_value = gold["report_week"].max()
    latest_week = gold[gold["report_week"] == latest_week_value].sort_values("total_weekly_hours", ascending=False)
    total_hours = int(latest_week["total_weekly_hours"].sum())
    active_students = int(latest_week["student_id"].nunique())
    one_on_one_share = int(round((latest_week["tutoring_hours"].sum() / max(total_hours, 1)) * 100))

    table_html = latest_week[
        ["student_name", "tutoring_hours", "test_hours", "total_weekly_hours"]
    ].rename(
        columns={
            "student_name": "Student",
            "tutoring_hours": "Tutoring Hours",
            "test_hours": "Test Hours",
            "total_weekly_hours": "Total Hours",
        }
    ).to_html(index=False, classes="data-table", border=0)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Student Engagement Trends Prototype</title>
  <style>
    :root {{
      --bg: #f4f7fb;
      --card: #ffffff;
      --ink: #102a43;
      --muted: #486581;
      --line: #d9e2ec;
      --teal: #2a9d8f;
      --gold: #e9c46a;
      --coral: #e76f51;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at top left, #dff6f0, transparent 30%), var(--bg);
      color: var(--ink);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .hero {{
      display: flex;
      justify-content: space-between;
      gap: 24px;
      align-items: end;
      margin-bottom: 24px;
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 2.2rem;
    }}
    .hero p {{
      margin: 0;
      color: var(--muted);
      max-width: 720px;
      line-height: 1.5;
    }}
    .badge {{
      background: #e6fffa;
      color: #0f766e;
      padding: 10px 14px;
      border-radius: 999px;
      font-size: 0.95rem;
      white-space: nowrap;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid rgba(217, 226, 236, 0.9);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 14px 28px rgba(15, 23, 42, 0.06);
    }}
    .kpi-label {{
      color: var(--muted);
      font-size: 0.9rem;
      margin-bottom: 8px;
    }}
    .kpi-value {{
      font-size: 2rem;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    .chart-title {{
      margin: 0 0 6px;
      font-size: 1.1rem;
    }}
    .chart-subtitle {{
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
    }}
    th {{
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
    }}
    @media (max-width: 900px) {{
      .grid {{ grid-template-columns: 1fr; }}
      .hero {{ flex-direction: column; align-items: start; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div>
        <h1>Student Engagement Trends</h1>
        <p>
          Prototype Gold-layer dashboard for Tableau or Power BI. It highlights weekly student hours
          derived from tutoring attendance and test participation, with the latest refresh covering
          the week of {pd.Timestamp(latest_week_value).strftime('%B %d, %Y')}.
        </p>
      </div>
      <div class="badge">Gold View: v_student_engagement_trends</div>
    </section>

    <section class="kpis">
      <div class="card"><div class="kpi-label">Total Weekly Hours</div><div class="kpi-value">{total_hours}</div></div>
      <div class="card"><div class="kpi-label">Active Students</div><div class="kpi-value">{active_students}</div></div>
      <div class="card"><div class="kpi-label">Tutoring Share</div><div class="kpi-value">{one_on_one_share}%</div></div>
    </section>

    <section class="grid">
      <div class="card">
        <h2 class="chart-title">Latest Week by Student</h2>
        <p class="chart-subtitle">Stacked tutoring and test hours for the most recent weekly snapshot.</p>
        {svg_bar_chart(latest_week)}
      </div>
      <div class="card">
        <h2 class="chart-title">Top Student Trends</h2>
        <p class="chart-subtitle">Weekly total-hour trend lines for the highest-engagement students.</p>
        {svg_line_chart(gold)}
      </div>
    </section>

    <section class="card">
      <h2 class="chart-title">Latest Week Detail</h2>
      <p class="chart-subtitle">This table mirrors what a BI tool would read directly from the Gold layer.</p>
      <div class="table-wrap">{table_html}</div>
    </section>
  </div>
</body>
</html>
"""


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw_tutoring = sample_tutoring_data()
    raw_tests = sample_tests_data()
    silver_tutoring = build_tutoring_silver(raw_tutoring)
    silver_tests = build_tests_silver(raw_tests)
    gold = build_weekly_gold(raw_tutoring, raw_tests)

    raw_tutoring.to_csv(OUTPUT_DIR / "bronze_raw_tutoring.csv", index=False)
    raw_tests.to_csv(OUTPUT_DIR / "bronze_raw_tests.csv", index=False)
    silver_tutoring.to_csv(OUTPUT_DIR / "silver_tutoring.csv", index=False)
    silver_tests.to_csv(OUTPUT_DIR / "silver_tests.csv", index=False)
    gold.to_csv(OUTPUT_DIR / "gold_student_engagement_trends.csv", index=False)
    (OUTPUT_DIR / "prototype_dashboard.html").write_text(render_html_dashboard(gold), encoding="utf-8")

    print(f"Prototype artifacts written to: {OUTPUT_DIR}")
    print(f"HTML dashboard: {OUTPUT_DIR / 'prototype_dashboard.html'}")


if __name__ == "__main__":
    main()
