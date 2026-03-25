CREATE OR REPLACE VIEW v_student_engagement_trends AS
SELECT
    student_id,
    student_name,
    tutoring_hours,
    test_hours,
    total_weekly_hours,
    report_date,
    DATE_TRUNC('week', report_date) AS report_week
FROM gold_student_engagement_trends;
