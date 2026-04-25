import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.config import ETL_BATCH_SIZE, PRIMARY_KEYS
from app.normalizer import preprocess_dataframe_for_table
from app.staging_loader import create_temp_staging_table, copy_dataframe_to_staging


def chunk_dataframe(df: pd.DataFrame, batch_size: int):
    for start in range(0, len(df), batch_size):
        yield df.iloc[start:start + batch_size].copy()


def build_delete_sql(target_table: str, staging_table: str, pk_cols: list[str]) -> str:
    join_condition = " AND ".join([f't."{pk}" = s."{pk}"' for pk in pk_cols])
    pk_not_null = " AND ".join([f's."{pk}" IS NOT NULL' for pk in pk_cols])

    return f"""
        DELETE FROM "{target_table}" t
        USING "{staging_table}" s
        WHERE s.flag = 'D'
          AND {pk_not_null}
          AND {join_condition}
    """


def build_upsert_sql(
    target_table: str,
    staging_table: str,
    insert_cols: list[str],
    pk_cols: list[str],
) -> str:
    col_list = ", ".join(f'"{c}"' for c in insert_cols)
    conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
    select_cols = ", ".join(f's."{c}"' for c in insert_cols)

    non_pk_cols = [c for c in insert_cols if c not in pk_cols]

    if non_pk_cols:
        update_set = ",\n            ".join(
            [f'"{c}" = COALESCE(EXCLUDED."{c}", t."{c}")' for c in non_pk_cols]
        )

        change_condition = " OR ".join(
            [f'EXCLUDED."{c}" IS DISTINCT FROM t."{c}"' for c in non_pk_cols]
        )

        conflict_action = f"""
        DO UPDATE SET
            {update_set}
        WHERE {change_condition}
        """
    else:
        conflict_action = "DO NOTHING"

    pk_order = ", ".join(f's."{c}"' for c in pk_cols)

    return f"""
        INSERT INTO "{target_table}" AS t ({col_list})
        SELECT {select_cols}
        FROM (
            SELECT DISTINCT ON ({pk_order}) *
            FROM "{staging_table}" s
            WHERE s.flag IN ('A', 'O')
            ORDER BY {pk_order}
        ) s
        ON CONFLICT ({conflict_cols})
        {conflict_action}
    """


def process_dataframe(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
) -> tuple[int, int]:
    table_name = table_name.lower()

    if table_name not in PRIMARY_KEYS:
        raise ValueError(f"No primary key configured for table={table_name}")

    inspector = inspect(engine)
    db_columns_info = inspector.get_columns(table_name)

    if not db_columns_info:
        raise ValueError(f"Table not found or no columns found: {table_name}")

    df, db_cols = preprocess_dataframe_for_table(df, table_name, db_columns_info)

    pk_cols = PRIMARY_KEYS[table_name]

    missing_pk = [pk for pk in pk_cols if pk not in df.columns]
    if missing_pk:
        raise ValueError(f"Missing PK columns for {table_name}: {missing_pk}")

    insert_cols = [c for c in db_cols if c in df.columns]
    copy_columns = insert_cols

    total_upserted = 0
    total_deleted = 0

    for chunk in chunk_dataframe(df, ETL_BATCH_SIZE):
        with engine.begin() as conn:
            staging_table = create_temp_staging_table(conn, table_name)

            copy_dataframe_to_staging(
                conn=conn,
                df=chunk,
                staging_table=staging_table,
                copy_columns=copy_columns,
            )

            upsert_count = int(
                conn.execute(
                    text(f'SELECT COUNT(*) FROM "{staging_table}" WHERE flag IN (\'A\', \'O\')')
                ).scalar()
                or 0
            )

            delete_count = int(
                conn.execute(
                    text(f'SELECT COUNT(*) FROM "{staging_table}" WHERE flag = \'D\'')
                ).scalar()
                or 0
            )

            if delete_count:
                conn.execute(text(build_delete_sql(table_name, staging_table, pk_cols)))

            if upsert_count:
                conn.execute(text(build_upsert_sql(table_name, staging_table, insert_cols, pk_cols)))

            conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))

            total_upserted += upsert_count
            total_deleted += delete_count

    return total_upserted, total_deleted