from apscheduler.schedulers.blocking import BlockingScheduler
from app.api_main import run_incremental_for_feeds


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


def main():
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")

    # Company Master: Intraday 4 times
    scheduler.add_job(
        lambda: run_incremental_for_feeds(COMPANY_MASTER_FEEDS),
        "cron",
        hour="10,13,16,22",
        minute=35,
        id="company_master_intraday",
        replace_existing=True,
    )

    # Results: Every 1 hour from 9 AM to 11:30 PM
    scheduler.add_job(
        lambda: run_incremental_for_feeds(RESULTS_HOURLY_FEEDS),
        "cron",
        hour="9-23",
        minute=5,
        id="results_hourly",
        replace_existing=True,
    )

    # Extra final result check near 11:30 PM
    scheduler.add_job(
        lambda: run_incremental_for_feeds(RESULTS_HOURLY_FEEDS),
        "cron",
        hour=23,
        minute=30,
        id="results_final_2330",
        replace_existing=True,
    )

    # EOD feeds: vendor timing 10:30 PM, run after delay
    scheduler.add_job(
        lambda: run_incremental_for_feeds(EOD_FEEDS),
        "cron",
        hour=22,
        minute=45,
        id="eod_feeds_2245",
        replace_existing=True,
    )

    # Retry EOD feeds in case vendor data is delayed
    scheduler.add_job(
        lambda: run_incremental_for_feeds(EOD_FEEDS),
        "cron",
        hour=23,
        minute=30,
        id="eod_retry_2330",
        replace_existing=True,
    )

    print("✅ Accord API scheduler started")
    scheduler.start()


if __name__ == "__main__":
    main()