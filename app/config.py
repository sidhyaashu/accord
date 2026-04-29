import os

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

ACCORD_API_TOKEN = os.getenv("ACCORD_API_TOKEN", "")
API_DATE = os.getenv("API_DATE", "")

ETL_BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "50000"))
SQL_DIR = os.getenv("SQL_DIR", "/app/sql")

# API INGESTION HARDENING
ENABLE_IDEMPOTENCY = os.getenv("ENABLE_IDEMPOTENCY", "true").lower() == "true"
ENABLE_REJECTED_RETRY = os.getenv("ENABLE_REJECTED_RETRY", "true").lower() == "true"
MAX_REJECTED_ROW_RETRY = int(os.getenv("MAX_REJECTED_ROW_RETRY", "5"))
ALLOW_MASS_DELETE = os.getenv("ALLOW_MASS_DELETE", "false").lower() == "true"

MAX_FEED_CONSECUTIVE_FAILURES = int(
    os.getenv("MAX_FEED_CONSECUTIVE_FAILURES", "3")
)

# RAW PAYLOAD CLEANUP
RAW_PAYLOAD_RETENTION_DAYS = int(os.getenv("RAW_PAYLOAD_RETENTION_DAYS", "14"))
ENABLE_RAW_PAYLOAD_CLEANUP = (
    os.getenv("ENABLE_RAW_PAYLOAD_CLEANUP", "true").lower() == "true"
)

# API CLIENT RETRY CONFIG
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))
API_RETRY_BACKOFF_1 = int(os.getenv("API_RETRY_BACKOFF_1", "2"))
API_RETRY_BACKOFF_2 = int(os.getenv("API_RETRY_BACKOFF_2", "5"))
API_RETRY_BACKOFF_3 = int(os.getenv("API_RETRY_BACKOFF_3", "10"))
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "60"))

# SCHEDULER CONFIG
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")

COMPANY_MASTER_HOURS = os.getenv("COMPANY_MASTER_HOURS", "10,13,16,22")
COMPANY_MASTER_MINUTE = int(os.getenv("COMPANY_MASTER_MINUTE", "35"))

RESULTS_START_HOUR = int(os.getenv("RESULTS_START_HOUR", "9"))
RESULTS_END_HOUR = int(os.getenv("RESULTS_END_HOUR", "23"))
RESULTS_MINUTE = int(os.getenv("RESULTS_MINUTE", "5"))
RESULTS_FINAL_HOUR = int(os.getenv("RESULTS_FINAL_HOUR", "23"))
RESULTS_FINAL_MINUTE = int(os.getenv("RESULTS_FINAL_MINUTE", "30"))

EOD_HOUR = int(os.getenv("EOD_HOUR", "22"))
EOD_MINUTE = int(os.getenv("EOD_MINUTE", "45"))
EOD_RETRY_HOUR = int(os.getenv("EOD_RETRY_HOUR", "23"))
EOD_RETRY_MINUTE = int(os.getenv("EOD_RETRY_MINUTE", "30"))

# ALERTING
ENABLE_ALERTS = os.getenv("ENABLE_ALERTS", "true").lower() == "true"

# LOGGING
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_RAW_PAYLOAD = os.getenv("LOG_RAW_PAYLOAD", "true").lower() == "true"

# INGESTION LOAD ORDER
# Parent/root feeds first, dependent feeds later.
LOAD_ORDER = [
    # Reference/master tables
    "Industrymaster_Ex1",
    "Housemaster",
    "Stockexchangemaster",
    "Registrarmaster",
    "Shp_catmaster_2",

    # Root company dependency
    "Company_master",
    "Companyaddress",
    "Board",
    "Registrardata",

    # Financial statements
    "Finance_bs",
    "Finance_cons_bs",
    "Finance_pl",
    "Finance_cons_pl",
    "Finance_cf",
    "Finance_cons_cf",
    "Finance_fr",
    "Finance_cons_fr",

    # Results
    "Resultsf_IND_Ex1",
    "Resultsf_IND_Cons_Ex1",

    # Equity identity feeds before listing/price feeds
    "company_equity",
    "company_equity_cons",

    # Listing after company/equity identity
    "Complistings",

    # Shareholding
    "Shpsummary",
    "Shp_details",

    # Price feeds last
    "Monthlyprice",
    "Nse_Monthprice",
]

PRIMARY_KEYS = {
    "company_master": ["fincode"],
    "industrymaster_ex1": ["ind_code"],
    "housemaster": ["house_code"],
    "stockexchangemaster": ["stk_id"],
    "registrarmaster": ["registrarno"],
    "shp_catmaster_2": ["shp_catid"],

    "companyaddress": ["fincode"],
    "board": ["fincode", "yrc", "serialno", "dirtype_id"],
    "registrardata": ["fincode", "registrarno"],
    "complistings": ["fincode", "stk_id"],

    "finance_bs": ["fincode", "year_end", "type"],
    "finance_cons_bs": ["fincode", "year_end", "type"],
    "finance_pl": ["fincode", "year_end", "type"],
    "finance_cons_pl": ["fincode", "year_end", "type"],
    "finance_cf": ["fincode", "year_end", "type"],
    "finance_cons_cf": ["fincode", "year_end", "type"],
    "finance_fr": ["fincode", "year_end", "type"],
    "finance_cons_fr": ["fincode", "year_end", "type"],

    "resultsf_ind_ex1": ["fincode", "result_type", "date_end"],
    "resultsf_ind_cons_ex1": ["fincode", "result_type", "date_end"],

    "company_equity": ["fincode"],
    "company_equity_cons": ["fincode"],

    "shpsummary": ["fincode", "date_end"],
    "shp_details": ["fincode", "date_end", "srno"],

    "monthlyprice": ["fincode", "month", "year"],
    "nse_monthprice": ["fincode", "month", "year"],
}

COLUMN_RENAMES = {
    "finance_bs": {
        "outstanding_forward_exchange_contract": "outstanding_forward_exchange_contra",
    },
    "finance_cons_bs": {
        "outstanding_forward_exchange_contract": "outstanding_forward_exchange_contra",
    },
    "resultsf_ind_ex1": {
        "interest coverage ratio": "interest_coverage_ratio",
        "inventory turnover ratio": "inventory_turnover_ratio",
        "dividend per share": "dividend_per_share",
        "deebtor turnover ratio": "debtor_turnover_ratio",
        "debtor turnover ratio": "debtor_turnover_ratio",
        "debt/equity ratio": "debt_equity_ratio",
        "dividend payout ratio": "dividend_payout_ratio",
        "return on capital employed": "return_on_capital_employed",
    },
    "resultsf_ind_cons_ex1": {
        "interest coverage ratio": "interest_coverage_ratio",
        "inventory turnover ratio": "inventory_turnover_ratio",
        "dividend per share": "dividend_per_share",
        "deebtor turnover ratio": "debtor_turnover_ratio",
        "debtor turnover ratio": "debtor_turnover_ratio",
        "debt/equity ratio": "debt_equity_ratio",
        "dividend payout ratio": "dividend_payout_ratio",
        "return on capital employed": "return_on_capital_employed",
    },
}