from apscheduler.schedulers.blocking import BlockingScheduler
from app.api_main import run_incremental_for_feeds
from app.db import build_engine
from app.retry_service import retry_rejected_rows


COMPANY_MASTER_FEEDS = [
    "Company_master",
]

RESULTS_HOURLY_FEEDS = [
    "Resultsf_IND_Ex1",
    "Resultsf_IND_Cons_Ex1",
]

EOD_FEEDS = [
    "Industrymaster_Ex1",
    "Housemaster",
    "Stockexchangemaster",
    "Complistings",
    "Companyaddress",

    "Registrarmaster",
    "Registrardata",

    "Board",

    "Finance_bs",
    "Finance_cons_bs",
    "Finance_pl",
    "Finance_cons_pl",
    "Finance_cf",
    "Finance_cons_cf",
    "Finance_fr",
    "Finance_cons_fr",

    "company_equity",
    "company_equity_cons",

    "Shpsummary",
    "Shp_details",
    "Shp_catmaster_2",

    "Monthlyprice",
    "Nse_Monthprice",
]


from app.config import (
    TIMEZONE, COMPANY_MASTER_HOURS, COMPANY_MASTER_MINUTE,
    RESULTS_START_HOUR, RESULTS_END_HOUR, RESULTS_MINUTE,
    RESULTS_FINAL_HOUR, RESULTS_FINAL_MINUTE,
    EOD_HOUR, EOD_MINUTE, EOD_RETRY_HOUR, EOD_RETRY_MINUTE
)

def run_feeds_with_retry(feeds: list[str]):
    run_incremental_for_feeds(feeds)
    engine = build_engine()
    print("\n🔄 Running retry service for rejected rows...")
    summary = retry_rejected_rows(engine)
    print(f"🔄 Retry summary: {summary}\n")

def main():
    scheduler = BlockingScheduler(timezone=TIMEZONE)

    # Company Master: Intraday 4 times
    scheduler.add_job(
        lambda: run_feeds_with_retry(COMPANY_MASTER_FEEDS),
        "cron",
        hour=COMPANY_MASTER_HOURS,
        minute=COMPANY_MASTER_MINUTE,
        id="company_master_intraday",
        replace_existing=True,
    )

    # Results: Every 1 hour from 9 AM to 11:30 PM
    scheduler.add_job(
        lambda: run_feeds_with_retry(RESULTS_HOURLY_FEEDS),
        "cron",
        hour=f"{RESULTS_START_HOUR}-{RESULTS_END_HOUR}",
        minute=RESULTS_MINUTE,
        id="results_hourly",
        replace_existing=True,
    )

    # Extra final result check near 11:30 PM
    scheduler.add_job(
        lambda: run_feeds_with_retry(RESULTS_HOURLY_FEEDS),
        "cron",
        hour=RESULTS_FINAL_HOUR,
        minute=RESULTS_FINAL_MINUTE,
        id="results_final_2330",
        replace_existing=True,
    )

    # EOD feeds: vendor timing 10:30 PM, run after delay
    scheduler.add_job(
        lambda: run_feeds_with_retry(EOD_FEEDS),
        "cron",
        hour=EOD_HOUR,
        minute=EOD_MINUTE,
        id="eod_feeds_2245",
        replace_existing=True,
    )

    # Retry EOD feeds in case vendor data is delayed
    scheduler.add_job(
        lambda: run_feeds_with_retry(EOD_FEEDS),
        "cron",
        hour=EOD_RETRY_HOUR,
        minute=EOD_RETRY_MINUTE,
        id="eod_retry_2330",
        replace_existing=True,
    )

    print("✅ Accord API scheduler started")
    scheduler.start()


if __name__ == "__main__":
    main()