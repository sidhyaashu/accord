import csv
import io
import uuid

import pandas as pd
from sqlalchemy import text

from app.normalizer import safe_value


def dataframe_to_csv_buffer(df: pd.DataFrame, columns: list[str]) -> io.StringIO:
    selected_df = df.loc[:, columns]

    if len(selected_df.columns) != len(columns):
        raise ValueError(
            f"COPY column mismatch. selected={list(selected_df.columns)}, expected={columns}"
        )

    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    for row in selected_df.itertuples(index=False, name=None):
        writer.writerow([safe_value(v) for v in row])

    buffer.seek(0)
    return buffer


def create_temp_staging_table(conn, target_table: str) -> str:
    staging_table = f"stg_{target_table.lower()}_{uuid.uuid4().hex[:10]}"

    conn.execute(
        text(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{target_table}" INCLUDING DEFAULTS)')
    )

    return staging_table


def copy_dataframe_to_staging(
    conn,
    df: pd.DataFrame,
    staging_table: str,
    copy_columns: list[str],
) -> None:
    raw = conn.connection
    cursor = raw.cursor()

    try:
        buffer = dataframe_to_csv_buffer(df, copy_columns)
        cols_sql = ", ".join(f'"{c}"' for c in copy_columns)

        copy_sql = f"""
            COPY "{staging_table}" ({cols_sql})
            FROM STDIN WITH (FORMAT CSV, NULL '')
        """

        cursor.copy_expert(copy_sql, buffer)
    finally:
        cursor.close()