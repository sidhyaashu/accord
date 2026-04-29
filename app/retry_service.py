import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.alert_service import send_alert
from app.utils import resolve_table_name
from app.merge_service import process_dataframe
from app.config import ENABLE_REJECTED_RETRY, MAX_REJECTED_ROW_RETRY

def retry_rejected_rows(engine: Engine) -> dict:
    summary = {
        "retried": 0,
        "resolved": 0,
        "still_pending": 0,
        "failed": 0
    }

    if not ENABLE_REJECTED_RETRY:
        return summary
    
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT id, feed_name, requested_date, row_payload, retry_count
                FROM rejected_ingestion_rows
                WHERE resolved = FALSE AND retry_count < :max_retry_count
            """),
            {"max_retry_count": MAX_REJECTED_ROW_RETRY}
        ).fetchall()
        
    for row in result:
        row_id, feed_name, requested_date, row_payload, retry_count = row
        summary["retried"] += 1
        
        try:
            # We don't necessarily know the case of FINCODE in payload
            # Usually it's in uppercase in raw payload before normalization
            fincode = row_payload.get("FINCODE") or row_payload.get("fincode")
            if not fincode:
                _increment_retry(engine, row_id, retry_count)
                summary["failed"] += 1
                continue
                
            with engine.begin() as conn:
                exists = conn.execute(
                    text("SELECT 1 FROM company_master WHERE fincode = :fincode"),
                    {"fincode": fincode}
                ).scalar()
                
            if exists:
                df = pd.DataFrame([row_payload])
                table_name = resolve_table_name(engine, feed_name)
                
                upserted, deleted, rejected = process_dataframe(
                    engine=engine,
                    table_name=table_name,
                    df=df,
                    feed_name=feed_name,
                    requested_date=requested_date,
                )
                
                if rejected == 0 and (upserted > 0 or deleted > 0 or (upserted==0 and deleted==0)):
                    # Even if 0 upserted, if it was successfully processed without being rejected again
                    with engine.begin() as conn:
                        conn.execute(
                            text("""
                                UPDATE rejected_ingestion_rows
                                SET resolved = TRUE, resolved_at = now(), last_retry_at = now()
                                WHERE id = :id
                            """),
                            {"id": row_id}
                        )
                    summary["resolved"] += 1
                else:
                    _increment_retry(engine, row_id, retry_count)
                    if retry_count + 1 >= MAX_REJECTED_ROW_RETRY:
                        send_alert("Max Retries Exceeded", f"Rejected row {row_id} for {feed_name} has exhausted all {MAX_REJECTED_ROW_RETRY} retries and remains unresolved.")
                    summary["still_pending"] += 1
            else:
                _increment_retry(engine, row_id, retry_count)
                if retry_count + 1 >= MAX_REJECTED_ROW_RETRY:
                    send_alert("Max Retries Exceeded", f"Rejected row {row_id} for {feed_name} has exhausted all {MAX_REJECTED_ROW_RETRY} retries and remains unresolved.")
                summary["still_pending"] += 1
                
        except Exception:
            _increment_retry(engine, row_id, retry_count)
            summary["failed"] += 1
            
    return summary

def _increment_retry(engine: Engine, row_id: int, current_count: int):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE rejected_ingestion_rows
                SET retry_count = :new_count, last_retry_at = now()
                WHERE id = :id
            """),
            {"new_count": current_count + 1, "id": row_id}
        )
