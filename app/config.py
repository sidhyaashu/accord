import os

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

ACCORD_API_TOKEN = os.getenv("ACCORD_API_TOKEN", "")
API_DATE = os.getenv("API_DATE", "")
API_MODE = os.getenv("API_MODE", "mock")  # mock or real

ETL_BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "50000"))

SQL_DIR = os.getenv("SQL_DIR", "/app/sql")

LOAD_ORDER = [
    "Industrymaster_Ex1",
    "Housemaster",
    "Stockexchangemaster",
    "Registrarmaster",
    "Shp_catmaster_2",
    "Company_master",
    "Companyaddress",
    "Board",
    "Registrardata",
    "Complistings",
    "Finance_bs",
    "Finance_cons_bs",
    "Finance_pl",
    "Finance_cons_pl",
    "Finance_cf",
    "Finance_cons_cf",
    "Finance_fr",
    "Finance_cons_fr",
    "Resultsf_IND_Ex1",
    "Resultsf_IND_Cons_Ex1",
    "company_equity",
    "company_equity_cons",
    "Shpsummary",
    "Shp_details",
    "Monthlyprice",
    "Nse_Monthprice",
]

PRIMARY_KEYS = {
    "company_master": ["fincode"],
    "industrymaster_ex1": ["ind_code"],
    "housemaster": ["house_code"],
    "stockexchangemaster": ["stk_id"],
    "complistings": ["fincode", "stk_id"],
    "companyaddress": ["fincode"],
    "registrarmaster": ["registrarno"],
    "registrardata": ["fincode", "registrarno"],
    "board": ["fincode", "yrc", "serialno", "dirtype_id"],
    "finance_bs": ["fincode", "year_end", "type"],
    "finance_cf": ["fincode", "year_end", "type"],
    "finance_pl": ["fincode", "year_end", "type"],
    "finance_fr": ["fincode", "year_end", "type"],
    "finance_cons_bs": ["fincode", "year_end", "type"],
    "finance_cons_cf": ["fincode", "year_end", "type"],
    "finance_cons_pl": ["fincode", "year_end", "type"],
    "finance_cons_fr": ["fincode", "year_end", "type"],
    "resultsf_ind_ex1": ["fincode", "result_type", "date_end"],
    "resultsf_ind_cons_ex1": ["fincode", "result_type", "date_end"],
    "company_equity": ["fincode"],
    "company_equity_cons": ["fincode"],
    "shp_details": ["fincode", "date_end", "srno"],
    "shp_catmaster_2": ["shp_catid"],
    "monthlyprice": ["fincode", "month", "year"],
    "nse_monthprice": ["fincode", "month", "year"],
    "shpsummary": ["fincode", "date_end"],
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