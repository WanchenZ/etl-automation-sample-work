from __future__ import annotations

import argparse

from student_hours_etl.pipeline import ingest_bronze, load_bi_view, load_gold, run_all, transform_silver


def main() -> None:
    parser = argparse.ArgumentParser(description="Student hour tracking ETL")
    parser.add_argument(
        "command",
        choices=["ingest", "transform", "load-gold", "load-bi-view", "run-all"],
        help="Pipeline action to run",
    )
    args = parser.parse_args()

    if args.command == "ingest":
        ingest_bronze()
    elif args.command == "transform":
        transform_silver()
    elif args.command == "load-gold":
        load_gold()
    elif args.command == "load-bi-view":
        load_bi_view()
    else:
        run_all()


if __name__ == "__main__":
    main()
