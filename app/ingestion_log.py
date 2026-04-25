import json
from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.config import SQL_DIR


def create_ingestion_tables(engine: Engine) -> None:
    path = f"{SQL_DIR}/ingestion_tables.sql"

    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))


def save_raw_payload(
    engine: Engine,
    feed_name: str,
    requested_date: date,
    payload: dict,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw_api_payloads
                (feed_name, requested_date, payload)
                VALUES
                (:feed_name, :requested_date, CAST(:payload AS JSONB))
            """),
            {
                "feed_name": feed_name,
                "requested_date": requested_date,
                "payload": json.dumps(payload),
            },
        )


def log_run(
    engine: Engine,
    feed_name: str,
    requested_date: date,
    status: str,
    http_status: int | None = None,
    rows_received: int = 0,
    rows_upserted: int = 0,
    rows_deleted: int = 0,
    error_message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO ingestion_runs
                (feed_name, requested_date, status, http_status,
                 rows_received, rows_upserted, rows_deleted,
                 error_message, finished_at)
                VALUES
                (:feed_name, :requested_date, :status, :http_status,
                 :rows_received, :rows_upserted, :rows_deleted,
                 :error_message, now())
            """),
            {
                "feed_name": feed_name,
                "requested_date": requested_date,
                "status": status,
                "http_status": http_status,
                "rows_received": rows_received,
                "rows_upserted": rows_upserted,
                "rows_deleted": rows_deleted,
                "error_message": error_message[:1000] if error_message else None,
            },
        )