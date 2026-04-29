import json
from datetime import date

from sqlalchemy import text

from app.db import build_engine, wait_for_db
from app.config import TIMEZONE


def check_db_health(engine) -> dict:
    """Checks if the database is reachable and responding."""
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_rejected_queue_health(engine) -> dict:
    """Returns the count of unresolved rejected rows."""
    try:
        with engine.begin() as conn:
            unresolved = conn.execute(
                text("SELECT COUNT(*) FROM rejected_ingestion_rows WHERE resolved = FALSE")
            ).scalar()
            exhausted = conn.execute(
                text("""
                    SELECT COUNT(*) FROM rejected_ingestion_rows 
                    WHERE resolved = FALSE AND retry_count >= 5
                """)
            ).scalar()
        return {
            "status": "healthy" if unresolved == 0 else "warning",
            "unresolved_rows": unresolved,
            "exhausted_rows": exhausted,
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_ingestion_health(engine) -> dict:
    """Returns today's ingestion summary."""
    try:
        today = date.today()
        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT * FROM daily_ingestion_summary WHERE summary_date = :today"),
                {"today": today}
            ).fetchone()
        if row:
            return {
                "status": "healthy",
                "summary_date": str(row[0]),
                "feeds_success": row[1],
                "feeds_failed": row[2],
                "rows_received": row[3],
                "rows_upserted": row[4],
                "rows_deleted": row[5],
                "rows_rejected": row[6],
                "total_duration_seconds": row[7],
            }
        return {"status": "no_data", "summary_date": str(today)}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_raw_payload_storage(engine) -> dict:
    """Returns total raw payload row count to detect storage bloat."""
    try:
        with engine.begin() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM raw_api_payloads")).scalar()
        return {"status": "healthy", "total_rows": count}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def full_health_report() -> dict:
    """Runs all health checks and returns a comprehensive JSON snapshot."""
    engine = build_engine()
    wait_for_db(engine)

    db = check_db_health(engine)
    rejected_queue = check_rejected_queue_health(engine)
    ingestion = check_ingestion_health(engine)
    raw_storage = check_raw_payload_storage(engine)

    all_healthy = all(
        c.get("status") in ("healthy", "no_data", "warning")
        for c in [db, rejected_queue, ingestion, raw_storage]
    )

    report = {
        "overall_status": "healthy" if all_healthy else "unhealthy",
        "timezone": TIMEZONE,
        "db": db,
        "rejected_queue": rejected_queue,
        "today_ingestion": ingestion,
        "raw_payload_storage": raw_storage,
    }

    print(json.dumps(report, indent=2, default=str))
    return report


if __name__ == "__main__":
    full_health_report()
