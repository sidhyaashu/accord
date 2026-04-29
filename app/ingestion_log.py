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
    rows_rejected: int = 0,
    duration_seconds: int = 0,
    error_message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO ingestion_runs
                (feed_name, requested_date, status, http_status,
                 rows_received, rows_upserted, rows_deleted, rows_rejected,
                 duration_seconds, error_message, finished_at)
                VALUES
                (:feed_name, :requested_date, :status, :http_status,
                 :rows_received, :rows_upserted, :rows_deleted, :rows_rejected,
                 :duration_seconds, :error_message, now())
            """),
            {
                "feed_name": feed_name,
                "requested_date": requested_date,
                "status": status,
                "http_status": http_status,
                "rows_received": rows_received,
                "rows_upserted": rows_upserted,
                "rows_deleted": rows_deleted,
                "rows_rejected": rows_rejected,
                "duration_seconds": duration_seconds,
                "error_message": error_message[:1000] if error_message else None,
            },
        )

def has_successful_run(engine: Engine, feed_name: str, requested_date: date) -> bool:
    with engine.begin() as conn:
        count = conn.execute(
            text("""
                SELECT 1 FROM ingestion_runs 
                WHERE feed_name = :feed_name 
                  AND requested_date = :requested_date 
                  AND status IN ('SUCCESS', 'NO_CONTENT', 'EMPTY')
                LIMIT 1
            """),
            {"feed_name": feed_name, "requested_date": requested_date}
        ).scalar()
        return bool(count)

def update_daily_summary(engine: Engine, requested_date: date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO daily_ingestion_summary (
                    summary_date, feeds_success, feeds_failed,
                    rows_received, rows_upserted, rows_deleted, rows_rejected,
                    total_duration_seconds, updated_at
                )
                SELECT 
                    :requested_date as summary_date,
                    COUNT(*) FILTER (WHERE status IN ('SUCCESS', 'NO_CONTENT', 'EMPTY')) as feeds_success,
                    COUNT(*) FILTER (WHERE status NOT IN ('SUCCESS', 'NO_CONTENT', 'EMPTY')) as feeds_failed,
                    COALESCE(SUM(rows_received), 0) as rows_received,
                    COALESCE(SUM(rows_upserted), 0) as rows_upserted,
                    COALESCE(SUM(rows_deleted), 0) as rows_deleted,
                    COALESCE(SUM(rows_rejected), 0) as rows_rejected,
                    COALESCE(SUM(duration_seconds), 0) as total_duration_seconds,
                    now() as updated_at
                FROM ingestion_runs
                WHERE requested_date = :requested_date
                ON CONFLICT (summary_date) DO UPDATE SET
                    feeds_success = EXCLUDED.feeds_success,
                    feeds_failed = EXCLUDED.feeds_failed,
                    rows_received = EXCLUDED.rows_received,
                    rows_upserted = EXCLUDED.rows_upserted,
                    rows_deleted = EXCLUDED.rows_deleted,
                    rows_rejected = EXCLUDED.rows_rejected,
                    total_duration_seconds = EXCLUDED.total_duration_seconds,
                    updated_at = EXCLUDED.updated_at
            """),
            {"requested_date": requested_date}
        )