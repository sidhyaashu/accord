from datetime import datetime
import time

from sqlalchemy import inspect

from app.accord_client import fetch_accord_feed
from app.config import (
    ACCORD_API_TOKEN, API_DATE, LOAD_ORDER, PRIMARY_KEYS, 
    ENABLE_IDEMPOTENCY, MAX_CONSECUTIVE_FAILURES, LOG_RAW_PAYLOAD
)
from app.db import build_engine, wait_for_db
from app.ingestion_log import create_ingestion_tables, log_run, save_raw_payload, has_successful_run, update_daily_summary
from app.merge_service import process_dataframe
from app.normalizer import apply_renames, payload_to_dataframe
from app.validation_service import validate_payload_df
from app.alert_service import send_alert


def resolve_table_name(engine, feed_name: str) -> str | None:
    inspector = inspect(engine)
    db_tables = inspector.get_table_names()

    return next((t for t in db_tables if t.lower() == feed_name.lower()), None)


def run_incremental_for_feeds(feeds: list[str], override_date: str = None, force: bool = False) -> None:
    engine = build_engine()
    wait_for_db(engine)
    create_ingestion_tables(engine)

    date_ddmmyyyy = override_date or API_DATE.strip() or datetime.now().strftime("%d%m%Y")
    requested_date = datetime.strptime(date_ddmmyyyy, "%d%m%Y").date()

    print(f"\n🚀 Starting ingestion for date={date_ddmmyyyy}")
    print(f"Feeds: {feeds}")

    consecutive_failures = 0

    for feed_name in feeds:
        if ENABLE_IDEMPOTENCY and not force and has_successful_run(engine, feed_name, requested_date):
            print(f"\n⏭️ Skipping {feed_name} for {date_ddmmyyyy} (already successful)")
            continue

        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            print(f"\n🛑 Circuit breaker activated: {MAX_CONSECUTIVE_FAILURES} consecutive failures reached.")
            send_alert("Circuit Breaker Activated", f"Skipping remaining feeds due to consecutive failures.")
            break

        success = process_single_feed(
            engine=engine,
            feed_name=feed_name,
            date_ddmmyyyy=date_ddmmyyyy,
            requested_date=requested_date,
        )
        if success:
            consecutive_failures = 0
        else:
            consecutive_failures += 1

def process_single_feed(engine, feed_name: str, date_ddmmyyyy: str, requested_date) -> bool:
    print(f"\n🌐 Feed: {feed_name}")

    table_name = resolve_table_name(engine, feed_name)

    if not table_name:
        msg = f"No DB table found for feed={feed_name}"
        print(f"❌ {msg}")
        log_run(engine, feed_name, requested_date, "TABLE_NOT_FOUND", error_message=msg)
        update_daily_summary(engine, requested_date)
        return False

    start_time = time.time()
    try:
        http_status, payload = fetch_feed(feed_name, date_ddmmyyyy)

        if http_status == 204:
            print("⏭️ No incremental data")
            duration = int(time.time() - start_time)
            log_run(engine, feed_name, requested_date, "NO_CONTENT", http_status=http_status, duration_seconds=duration)
            update_daily_summary(engine, requested_date)
            return True

        if http_status in (403, 404):
            msg = f"API returned HTTP {http_status}"
            print(f"❌ {msg}")
            send_alert(f"API Error {http_status}", f"{feed_name} returned {http_status}")
            duration = int(time.time() - start_time)
            log_run(engine, feed_name, requested_date, "API_ERROR", http_status=http_status, error_message=msg, duration_seconds=duration)
            update_daily_summary(engine, requested_date)
            return False

        if LOG_RAW_PAYLOAD:
            save_raw_payload(engine, feed_name, requested_date, payload)

        df = payload_to_dataframe(payload)

        if df.empty:
            duration = int(time.time() - start_time)
            log_run(engine, feed_name, requested_date, "EMPTY", http_status=http_status, duration_seconds=duration)
            update_daily_summary(engine, requested_date)
            return True

        df = apply_renames(df, feed_name)

        pk_cols = PRIMARY_KEYS.get(table_name, [])
        val_result = validate_payload_df(df, table_name, pk_cols)

        for w in val_result["warnings"]:
            print(f"⚠️ Validation Warning: {w}")

        if not val_result["valid"]:
            error_str = "; ".join(val_result["errors"])
            print(f"❌ Validation Errors: {error_str}")
            send_alert("Validation Failed", f"{feed_name}: {error_str}")
            duration = int(time.time() - start_time)
            log_run(engine, feed_name, requested_date, "FAILED", error_message=error_str, duration_seconds=duration)
            update_daily_summary(engine, requested_date)
            return False

        upserted, deleted, rejected = process_dataframe(
            engine=engine,
            table_name=table_name,
            df=df,
            feed_name=feed_name,
            requested_date=requested_date,
        )

        duration = int(time.time() - start_time)
        log_run(
            engine=engine,
            feed_name=feed_name,
            requested_date=requested_date,
            status="SUCCESS",
            http_status=http_status,
            rows_received=len(df),
            rows_upserted=upserted,
            rows_deleted=deleted,
            rows_rejected=rejected,
            duration_seconds=duration,
        )

        print(f"✅ Success: received={len(df)}, upserted={upserted}, deleted={deleted}, rejected={rejected}")
        if rejected > 0:
            send_alert("Rows Rejected", f"{feed_name} had {rejected} rows rejected")
            
        update_daily_summary(engine, requested_date)
        return True

    except Exception as e:
        duration = int(time.time() - start_time)
        print(f"❌ Failed feed={feed_name}: {e}")
        send_alert("Feed Failure", f"{feed_name} failed: {str(e)}")
        log_run(engine, feed_name, requested_date, "FAILED", error_message=str(e), duration_seconds=duration)
        update_daily_summary(engine, requested_date)
        return False

def fetch_feed(feed_name: str, date_ddmmyyyy: str):
    if not ACCORD_API_TOKEN:
        raise RuntimeError("ACCORD_API_TOKEN is required")

    return fetch_accord_feed(feed_name, date_ddmmyyyy, ACCORD_API_TOKEN)


def run_incremental() -> None:
    run_incremental_for_feeds(LOAD_ORDER)

def run_backfill_last_7_days() -> None:
    import datetime
    today = datetime.datetime.now().date()
    
    # fetch missing dates in chronological order (oldest first)
    for i in range(7, -1, -1):
        backfill_date = today - datetime.timedelta(days=i)
        date_ddmmyyyy = backfill_date.strftime("%d%m%Y")
        print(f"\n⏳ Running backfill for {date_ddmmyyyy}")
        try:
            run_incremental_for_feeds(LOAD_ORDER, override_date=date_ddmmyyyy, force=False)
        except Exception as e:
            send_alert("Backfill Failure", f"Failed for {date_ddmmyyyy}: {str(e)}")



if __name__ == "__main__":
    run_incremental()