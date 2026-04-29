from datetime import datetime
import time

from sqlalchemy import inspect

from app.accord_client import fetch_accord_feed
from app.config import ACCORD_API_TOKEN, API_DATE, LOAD_ORDER
from app.db import build_engine, wait_for_db
from app.ingestion_log import create_ingestion_tables, log_run, save_raw_payload
from app.merge_service import process_dataframe
from app.normalizer import apply_renames, payload_to_dataframe


def resolve_table_name(engine, feed_name: str) -> str | None:
    inspector = inspect(engine)
    db_tables = inspector.get_table_names()

    return next((t for t in db_tables if t.lower() == feed_name.lower()), None)


def run_incremental_for_feeds(feeds: list[str], override_date: str = None) -> None:
    engine = build_engine()
    wait_for_db(engine)
    create_ingestion_tables(engine)

    date_ddmmyyyy = override_date or API_DATE.strip() or datetime.now().strftime("%d%m%Y")
    requested_date = datetime.strptime(date_ddmmyyyy, "%d%m%Y").date()

    print(f"\n🚀 Starting ingestion for date={date_ddmmyyyy}")
    print(f"Feeds: {feeds}")

    for feed_name in feeds:
        process_single_feed(
            engine=engine,
            feed_name=feed_name,
            date_ddmmyyyy=date_ddmmyyyy,
            requested_date=requested_date,
        )

def process_single_feed(engine, feed_name: str, date_ddmmyyyy: str, requested_date):
    print(f"\n🌐 Feed: {feed_name}")

    table_name = resolve_table_name(engine, feed_name)

    if not table_name:
        msg = f"No DB table found for feed={feed_name}"
        print(f"❌ {msg}")
        log_run(engine, feed_name, requested_date, "TABLE_NOT_FOUND", error_message=msg)
        return

    try:
        start_time = time.time()
        http_status, payload = fetch_feed(feed_name, date_ddmmyyyy)

        if http_status == 204:
            print("⏭️ No incremental data")
            duration = int(time.time() - start_time)
            log_run(engine, feed_name, requested_date, "NO_CONTENT", http_status=http_status, duration_seconds=duration)
            return

        if http_status in (403, 404):
            msg = f"API returned HTTP {http_status}"
            print(f"❌ {msg}")
            duration = int(time.time() - start_time)
            log_run(engine, feed_name, requested_date, "API_ERROR", http_status=http_status, error_message=msg, duration_seconds=duration)
            return

        save_raw_payload(engine, feed_name, requested_date, payload)

        df = payload_to_dataframe(payload)

        if df.empty:
            duration = int(time.time() - start_time)
            log_run(engine, feed_name, requested_date, "EMPTY", http_status=http_status, duration_seconds=duration)
            return

        df = apply_renames(df, feed_name)

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

    except Exception as e:
        print(f"❌ Failed feed={feed_name}: {e}")
        log_run(engine, feed_name, requested_date, "FAILED", error_message=str(e))

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
        run_incremental_for_feeds(LOAD_ORDER, override_date=date_ddmmyyyy)



if __name__ == "__main__":
    run_incremental()