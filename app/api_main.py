from datetime import datetime

from sqlalchemy import inspect

from app.accord_client import fetch_accord_feed, mock_fetch_feed
from app.config import ACCORD_API_TOKEN, API_DATE, API_MODE, LOAD_ORDER
from app.db import build_engine, wait_for_db
from app.ingestion_log import create_ingestion_tables, log_run, save_raw_payload
from app.merge_service import process_dataframe
from app.normalizer import apply_renames, payload_to_dataframe


def resolve_table_name(engine, feed_name: str) -> str | None:
    inspector = inspect(engine)
    db_tables = inspector.get_table_names()

    return next((t for t in db_tables if t.lower() == feed_name.lower()), None)


def run_incremental_for_feeds(feeds: list[str]) -> None:
    engine = build_engine()
    wait_for_db(engine)
    create_ingestion_tables(engine)

    date_ddmmyyyy = API_DATE.strip() or datetime.now().strftime("%d%m%Y")
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
        http_status, payload = fetch_feed(feed_name, date_ddmmyyyy)

        if http_status == 204:
            print("⏭️ No incremental data")
            log_run(engine, feed_name, requested_date, "NO_CONTENT", http_status=http_status)
            return

        if http_status in (403, 404):
            msg = f"API returned HTTP {http_status}"
            print(f"❌ {msg}")
            log_run(engine, feed_name, requested_date, "API_ERROR", http_status=http_status, error_message=msg)
            return

        save_raw_payload(engine, feed_name, requested_date, payload)

        df = payload_to_dataframe(payload)

        if df.empty:
            log_run(engine, feed_name, requested_date, "EMPTY", http_status=http_status)
            return

        df = apply_renames(df, feed_name)

        upserted, deleted = process_dataframe(
            engine=engine,
            table_name=table_name,
            df=df,
        )

        log_run(
            engine=engine,
            feed_name=feed_name,
            requested_date=requested_date,
            status="SUCCESS",
            http_status=http_status,
            rows_received=len(df),
            rows_upserted=upserted,
            rows_deleted=deleted,
        )

        print(f"✅ Success: received={len(df)}, upserted={upserted}, deleted={deleted}")

    except Exception as e:
        print(f"❌ Failed feed={feed_name}: {e}")
        log_run(engine, feed_name, requested_date, "FAILED", error_message=str(e))

def fetch_feed(feed_name: str, date_ddmmyyyy: str):
    if API_MODE == "mock":
        return mock_fetch_feed(feed_name)

    if not ACCORD_API_TOKEN:
        raise RuntimeError("ACCORD_API_TOKEN is required for real API mode")

    return fetch_accord_feed(feed_name, date_ddmmyyyy, ACCORD_API_TOKEN)


def run_incremental() -> None:
    engine = build_engine()
    wait_for_db(engine)
    create_ingestion_tables(engine)

    date_ddmmyyyy = API_DATE.strip() or datetime.now().strftime("%d%m%Y")
    requested_date = datetime.strptime(date_ddmmyyyy, "%d%m%Y").date()

    print(f"\n🚀 Starting incremental API ingestion for date={date_ddmmyyyy}")
    print(f"Mode: {API_MODE}")

    for feed_name in LOAD_ORDER:
        print(f"\n🌐 Feed: {feed_name}")

        table_name = resolve_table_name(engine, feed_name)

        if not table_name:
            msg = f"No DB table found for feed={feed_name}"
            print(f"❌ {msg}")
            log_run(engine, feed_name, requested_date, "TABLE_NOT_FOUND", error_message=msg)
            continue

        try:
            http_status, payload = fetch_feed(feed_name, date_ddmmyyyy)

            if http_status == 204:
                print("⏭️ No incremental data")
                log_run(engine, feed_name, requested_date, "NO_CONTENT", http_status=http_status)
                continue

            if http_status in (403, 404):
                msg = f"API returned HTTP {http_status}"
                print(f"❌ {msg}")
                log_run(
                    engine,
                    feed_name,
                    requested_date,
                    "API_ERROR",
                    http_status=http_status,
                    error_message=msg,
                )
                continue

            if http_status != 200 or not payload:
                msg = f"Invalid response HTTP {http_status}"
                print(f"❌ {msg}")
                log_run(
                    engine,
                    feed_name,
                    requested_date,
                    "INVALID_RESPONSE",
                    http_status=http_status,
                    error_message=msg,
                )
                continue

            save_raw_payload(engine, feed_name, requested_date, payload)

            df = payload_to_dataframe(payload)

            if df.empty:
                print("⏭️ Empty Table array")
                log_run(engine, feed_name, requested_date, "EMPTY", http_status=http_status)
                continue

            df = apply_renames(df, feed_name)

            upserted, deleted = process_dataframe(
                engine=engine,
                table_name=table_name,
                df=df,
            )

            log_run(
                engine=engine,
                feed_name=feed_name,
                requested_date=requested_date,
                status="SUCCESS",
                http_status=http_status,
                rows_received=len(df),
                rows_upserted=upserted,
                rows_deleted=deleted,
            )

            print(
                f"✅ Success: received={len(df)}, upserted={upserted}, deleted={deleted}"
            )

        except Exception as e:
            print(f"❌ Failed feed={feed_name}: {e}")
            log_run(
                engine,
                feed_name,
                requested_date,
                "FAILED",
                error_message=str(e),
            )

    print("\n✅ Incremental ingestion completed")


if __name__ == "__main__":
    run_incremental()