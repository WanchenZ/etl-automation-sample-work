from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_engine(sqlalchemy_url: str) -> Engine:
    return create_engine(sqlalchemy_url, future=True)


def run_sql_file(engine: Engine, sql_file: str) -> None:
    sql = Path(sql_file).read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def replace_table(engine: Engine, frame: pd.DataFrame, table_name: str, schema: str = "public") -> None:
    frame.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
    )


def append_table(engine: Engine, frame: pd.DataFrame, table_name: str, schema: str = "public") -> None:
    frame.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
    )
