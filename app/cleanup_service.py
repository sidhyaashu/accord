from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.alert_service import send_alert
from app.config import RAW_PAYLOAD_RETENTION_DAYS, ENABLE_RAW_PAYLOAD_CLEANUP


def cleanup_old_raw_payloads(engine: Engine) -> int:
    """
    Deletes raw_api_payloads rows older than RAW_PAYLOAD_RETENTION_DAYS.
    Returns the count of deleted rows.
    Safe to run if the table is empty.
    """
    if not ENABLE_RAW_PAYLOAD_CLEANUP:
        print("⏭️ Raw payload cleanup is disabled (ENABLE_RAW_PAYLOAD_CLEANUP=false)")
        return 0

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    DELETE FROM raw_api_payloads
                    WHERE received_at < now() - interval ':days days'
                    RETURNING id
                """.replace(":days days", f"{RAW_PAYLOAD_RETENTION_DAYS} days"))
            )
            deleted = result.rowcount

        if deleted > 0:
            print(f"🧹 Cleanup: deleted {deleted} raw payload rows older than {RAW_PAYLOAD_RETENTION_DAYS} days.")
        else:
            print(f"🧹 Cleanup: no raw payload rows to delete (retention window: {RAW_PAYLOAD_RETENTION_DAYS} days).")

        return deleted

    except Exception as e:
        msg = f"Raw payload cleanup failed: {str(e)}"
        print(f"❌ {msg}")
        send_alert("Cleanup Failure", msg)
        return 0
