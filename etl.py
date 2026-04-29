"""
=============================================================================
 PRODUCTION ETL LOADER (FAST STAGING + COPY VERSION)
 Loads JSON .txt data files into PostgreSQL using provided .sql schema files.
=============================================================================

FLOW:
  1. CREATE TABLES
  2. LOAD JSON FILE
  3. NORMALIZE / RENAME / FILTER COLUMNS
  4. STREAM INTO TEMP STAGING TABLE USING COPY
  5. APPLY:
       - DELETE for flag='D'
       - UPSERT for flag in ('A', 'O')
  6. FINAL REPORT
=============================================================================
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.sqltypes import BigInteger, Integer, SmallInteger

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ETL")

# ─────────────────────────────────────────────────────────────────────────────
# Environment
# ─────────────────────────────────────────────────────────────────────────────
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL must be set")

SCHEMA_DIR = os.getenv("SCHEMA_DIR", "./schemas")
DATA_DIR = os.getenv("DATA_DIR", "./data")

BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "50000"))  # larger batch for COPY
SLEEP_BETWEEN_BATCHES = float(os.getenv("ETL_BATCH_SLEEP", "0"))
RECREATE_SCHEMA = os.getenv("ETL_RECREATE_SCHEMA", "false").lower() == "true"
FORCE_RESET = os.getenv("ETL_FORCE_RESET", "false").lower() == "true"
TABLE_RETRIES = int(os.getenv("ETL_TABLE_RETRIES", "3"))

# If true, commit each chunk independently for very large files
CHUNK_COMMIT_MODE = os.getenv("ETL_CHUNK_COMMIT_MODE", "true").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# SQL Server → PostgreSQL type mapping
# ─────────────────────────────────────────────────────────────────────────────
TYPE_REPLACEMENTS = {
    r"\bDATETIME\b": "TIMESTAMP",
    r"\bNTEXT\b": "TEXT",
}

# ─────────────────────────────────────────────────────────────────────────────
# Column rename mappings (JSON key → SQL column)
# ─────────────────────────────────────────────────────────────────────────────
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
        "return_on_capital_employed": "return_on_capital_employed",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Dependency/load order
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# Conflict key definitions
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class FileReport:
    file: str
    table: str
    structure: str
    load: str
    rows_loaded: int
    notes: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def normalize_column_name(name: str) -> str:
    return str(name).strip().lower()


def convert_sql_to_postgres(sql_content: str) -> str:
    result = sql_content

    for pattern, replacement in TYPE_REPLACEMENTS.items():
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    result = re.sub(
        r"^\s+([\w]+(?:\s+[\w/]+){1,})\s+"
        r"(FLOAT|INT|INTEGER|VARCHAR|TEXT|NUMERIC|TIMESTAMP|BIGINT|DOUBLE PRECISION)",
        lambda m: f'    "{m.group(1)}" {m.group(2)}',
        result,
        flags=re.MULTILINE | re.IGNORECASE,
    )

    return result


# def get_table_name_from_sql(sql_content: str) -> Optional[str]:
#     match = re.search(r"CREATE\s+TABLE\s+(\w+)", sql_content, re.IGNORECASE)
#     return match.group(1) if match else None

def get_table_name_from_sql(sql_content: str) -> Optional[str]:
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
        sql_content,
        re.IGNORECASE,
    )
    return match.group(1) if match else None


def ordered_existing_files(directory: str, ext: str) -> list[str]:
    all_files = {f for f in os.listdir(directory) if f.endswith(ext)}
    ordered = [f"{name}{ext}" for name in LOAD_ORDER if f"{name}{ext}" in all_files]
    extras = sorted(all_files - set(ordered))
    return ordered + extras


def load_json_to_dataframe(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if "Table" not in raw or not isinstance(raw["Table"], list):
        raise ValueError("JSON missing valid 'Table' array")

    return pd.DataFrame(raw["Table"])


def apply_renames(df: pd.DataFrame, base_name: str) -> tuple[pd.DataFrame, dict[str, str]]:
    rename_key = base_name.lower()
    rename_map = COLUMN_RENAMES.get(rename_key, {})

    if not rename_map:
        return df, {}

    incoming_map = {normalize_column_name(col): col for col in df.columns}
    actual_renames: dict[str, str] = {}

    for source_col_normalized, target_col in rename_map.items():
        actual_source = incoming_map.get(source_col_normalized)
        if actual_source:
            actual_renames[actual_source] = target_col

    if actual_renames:
        df = df.rename(columns=actual_renames)

    return df, actual_renames


def recreate_public_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    logger.info("Recreated public schema.")


def create_tables(engine: Engine, schema_dir: str) -> tuple[dict[str, str], int, int]:
    ordered_sql_files = ordered_existing_files(schema_dir, ".sql")
    tables_created: dict[str, str] = {}
    step_pass = 0
    step_fail = 0

    print("\n" + "=" * 70)
    print("  STEP 1: CREATING TABLES FROM SQL SCHEMA FILES")
    print("=" * 70)

    for sql_file in ordered_sql_files:
        path = os.path.join(schema_dir, sql_file)
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw_sql = f.read()

            pg_sql = convert_sql_to_postgres(raw_sql)
            pg_sql = re.sub(
                r"CREATE\s+TABLE\s+",
                "CREATE TABLE IF NOT EXISTS ",
                pg_sql,
                flags=re.IGNORECASE,
            )

            table_name = get_table_name_from_sql(pg_sql)
            if not table_name:
                raise ValueError("Could not extract table name from SQL")

            with engine.begin() as conn:
                conn.execute(text(pg_sql))

            tables_created[sql_file] = table_name
            print(f"  ✅  {sql_file:<35} → Table: {table_name}")
            step_pass += 1

        except Exception as e:
            error_msg = str(e).split("\n")[0][:180]
            print(f"  ❌  {sql_file:<35} → FAILED: {error_msg}")
            step_fail += 1

    print(f"\n  Summary: {step_pass} created, {step_fail} failed\n")
    return tables_created, step_pass, step_fail


def chunk_dataframe(df: pd.DataFrame, batch_size: int):
    for start in range(0, len(df), batch_size):
        yield start, df.iloc[start:start + batch_size].copy()


def preprocess_dataframe_for_table(
    df: pd.DataFrame,
    table_name: str,
    db_columns_info: list[dict[str, Any]],
) -> tuple[pd.DataFrame, list[str]]:
    df = df.copy()
    df.columns = [normalize_column_name(c) for c in df.columns]

    # remove duplicate columns early
    df = df.loc[:, ~pd.Index(df.columns).duplicated()]

    db_columns = {c["name"].lower(): c["type"] for c in db_columns_info}
    db_cols = list(db_columns.keys())

    keep_cols = [c for c in df.columns if c in db_columns or c == "flag"]
    if not keep_cols:
        raise ValueError(f"No valid columns after schema filtering for table: {table_name}")

    df = df.loc[:, keep_cols].copy()

    if "flag" not in df.columns:
        raise ValueError(f"Required 'flag' column not present for table: {table_name}")

    int_cols = [
        col_name
        for col_name, col_type in db_columns.items()
        if isinstance(col_type, (Integer, BigInteger, SmallInteger))
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = df.where(pd.notnull(df), None)
    df["flag"] = df["flag"].astype(str).str.upper().str.strip()

    final_cols = [c for c in db_cols if c in df.columns]
    if "flag" not in final_cols and "flag" in df.columns:
        final_cols.append("flag")

    # remove duplicates again while preserving order
    seen = set()
    final_cols = [c for c in final_cols if not (c in seen or seen.add(c))]

    df = df.loc[:, final_cols]

    return df, db_cols

def safe_value(v):
    if v is None:
        return ""

    v = str(v).strip()

    if v.lower() in ("<na>", "nan", "none"):
        return ""

    return v  # ❌ no truncation

def wait_for_db(engine, retries=10, delay=3):
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Database connection successful\n")
            return
        except Exception as e:
            print(f"⏳ Waiting for DB... ({attempt}/{retries})")
            time.sleep(delay)

    raise RuntimeError("❌ Could not connect to DB after retries")

def dataframe_to_csv_buffer(df: pd.DataFrame, columns: list[str]) -> io.StringIO:
    selected_df = df.loc[:, columns]

    if len(selected_df.columns) != len(columns):
        raise ValueError(
            f"COPY column mismatch. selected={list(selected_df.columns)}, expected={columns}"
        )

    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    for row in selected_df.itertuples(index=False, name=None):
        # writer.writerow(["" if v is None else v for v in row])
        writer.writerow([safe_value(v) for v in row])

    buf.seek(0)
    return buf

def create_temp_staging_table(conn, target_table: str, staging_table: str) -> None:
    conn.execute(
        text(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{target_table}" INCLUDING DEFAULTS)')
    )


def copy_chunk_to_staging(conn, df_chunk: pd.DataFrame, staging_table: str, copy_columns: list[str]) -> None:
    raw = conn.connection
    cursor = raw.cursor()

    try:
        buffer = dataframe_to_csv_buffer(df_chunk, copy_columns)
        cols_sql = ", ".join(f'"{c}"' for c in copy_columns)

        copy_sql = f"""
            COPY "{staging_table}" ({cols_sql})
            FROM STDIN WITH (FORMAT CSV, NULL '')
        """
        cursor.copy_expert(copy_sql, buffer)

        print(f"  │  DEBUG staging_table={staging_table}")
        print(f"  │  DEBUG copy_columns={copy_columns}")
        print(f"  │  DEBUG df_chunk_columns={list(df_chunk.columns)}")
        print(f"  │  DEBUG copy_col_count={len(copy_columns)} df_col_count={len(df_chunk.columns)}")

        missing = [c for c in copy_columns if c not in df_chunk.columns]
        extra = [c for c in df_chunk.columns if c not in copy_columns]

        print(f"  │  DEBUG missing_in_df={missing}")
        print(f"  │  DEBUG extra_in_df={extra}")
    finally:
        cursor.close()


def build_delete_sql_from_staging(target_table: str, staging_table: str, pk_cols: list[str]) -> str:
    join_cond = " AND ".join([f't."{pk}" = s."{pk}"' for pk in pk_cols])

    pk_not_null = " AND ".join([f's."{pk}" IS NOT NULL' for pk in pk_cols])

    return f"""
        DELETE FROM "{target_table}" t
        USING "{staging_table}" s
        WHERE s.flag = 'D'
          AND {pk_not_null}
          AND {join_cond}
    """

def build_upsert_sql_from_staging(
    target_table: str,
    staging_table: str,
    insert_cols: list[str],
    pk_cols: list[str],
) -> str:
    col_list = ", ".join(f'"{c}"' for c in insert_cols)
    conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)

    non_pk_cols = [c for c in insert_cols if c not in pk_cols]

    # ✅ Build NULL-safe updates
    if non_pk_cols:
        update_set_parts = [
            f'''"{c}" = COALESCE(EXCLUDED."{c}", t."{c}")'''
            for c in non_pk_cols
        ]

        update_set = ",\n            ".join(update_set_parts)

        # ✅ Update only if something actually changed
        change_condition = " OR ".join(
            [f'EXCLUDED."{c}" IS DISTINCT FROM t."{c}"' for c in non_pk_cols]
        )

        conflict_action = f"""
        DO UPDATE SET
            {update_set}
        WHERE {change_condition}
        """
    else:
        conflict_action = "DO NOTHING"

    # ✅ Deduplicate staging rows (important)
    pk_order = ", ".join(f's."{c}"' for c in pk_cols)

    select_cols = ", ".join(f's."{c}"' for c in insert_cols)

    return f"""
        INSERT INTO "{target_table}" AS t ({col_list})
        SELECT {select_cols}
        FROM (
            SELECT DISTINCT ON ({pk_order}) *
            FROM "{staging_table}" s
            WHERE s.flag IN ('A', 'O')
            ORDER BY {pk_order}
        ) s
        ON CONFLICT ({conflict_cols})
        {conflict_action}
    """

def process_chunk_via_staging(
    conn,
    table_name: str,
    df_chunk: pd.DataFrame,
    db_insert_cols: list[str],
    pk_cols: list[str],
) -> tuple[int, int]:
    """
    One chunk:
      1. create temp staging
      2. COPY chunk
      3. DELETE flagged rows
      4. UPSERT flagged rows
    """
    staging_table = f"stg_{table_name.lower()}_{uuid.uuid4().hex[:10]}"
    copy_columns = db_insert_cols

    create_temp_staging_table(conn, table_name, staging_table)
    copy_chunk_to_staging(conn, df_chunk, staging_table, copy_columns)

    delete_sql = build_delete_sql_from_staging(table_name, staging_table, pk_cols)
    upsert_sql = build_upsert_sql_from_staging(table_name, staging_table, db_insert_cols, pk_cols)

    deleted_count = int(
        conn.execute(text(f'SELECT COUNT(*) FROM "{staging_table}" WHERE flag = \'D\'')).scalar() or 0
    )
    upsert_count = int(
        conn.execute(text(f'SELECT COUNT(*) FROM "{staging_table}" WHERE flag IN (\'A\', \'O\')')).scalar() or 0
    )

    if deleted_count:
        conn.execute(text(delete_sql))

    if upsert_count:
        conn.execute(text(upsert_sql))

    conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))

    return upsert_count, deleted_count


def process_single_table_file(
    engine: Engine,
    inspector,
    table_name: str,
    df: pd.DataFrame,
) -> tuple[int, int]:
    table_key = table_name.lower()

    if table_key not in PRIMARY_KEYS:
        raise ValueError(f"No PRIMARY_KEYS entry configured for table: {table_name}")

    pk_cols = PRIMARY_KEYS[table_key]
    db_columns_info = inspector.get_columns(table_name)

    df, db_cols = preprocess_dataframe_for_table(df, table_name, db_columns_info)

    missing_pk = [c for c in pk_cols if c not in df.columns]
    if missing_pk:
        raise ValueError(f"Missing PK columns for {table_name}: {missing_pk}")

    db_insert_cols = [c for c in db_cols if c in df.columns]

    total_upserted = 0
    total_deleted = 0
    total_rows = len(df)
    total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE if total_rows else 0

    if CHUNK_COMMIT_MODE:
        for batch_no, (start, chunk) in enumerate(chunk_dataframe(df, BATCH_SIZE), start=1):
            with engine.begin() as conn:
                upserted_count, deleted_count = process_chunk_via_staging(
                    conn=conn,
                    table_name=table_name,
                    df_chunk=chunk,
                    db_insert_cols=db_insert_cols,
                    pk_cols=pk_cols,
                )

            total_upserted += upserted_count
            total_deleted += deleted_count

            print(
                f"  │  ⏳ {table_name}: chunk {batch_no}/{total_batches} "
                f"(upserted={total_upserted}, deleted={total_deleted}, "
                f"processed={min(start + len(chunk), total_rows)}/{total_rows})",
                flush=True,
            )

            if SLEEP_BETWEEN_BATCHES > 0:
                time.sleep(SLEEP_BETWEEN_BATCHES)
    else:
        with engine.begin() as conn:
            for batch_no, (start, chunk) in enumerate(chunk_dataframe(df, BATCH_SIZE), start=1):
                upserted_count, deleted_count = process_chunk_via_staging(
                    conn=conn,
                    table_name=table_name,
                    df_chunk=chunk,
                    db_insert_cols=db_insert_cols,
                    pk_cols=pk_cols,
                )

                total_upserted += upserted_count
                total_deleted += deleted_count

                print(
                    f"  │  ⏳ {table_name}: chunk {batch_no}/{total_batches} "
                    f"(upserted={total_upserted}, deleted={total_deleted}, "
                    f"processed={min(start + len(chunk), total_rows)}/{total_rows})",
                    flush=True,
                )

                if SLEEP_BETWEEN_BATCHES > 0:
                    time.sleep(SLEEP_BETWEEN_BATCHES)

    return total_upserted, total_deleted


def process_single_table_file_with_retry(
    engine: Engine,
    inspector,
    table_name: str,
    df: pd.DataFrame,
    retries: int = TABLE_RETRIES,
) -> tuple[int, int]:
    for attempt in range(1, retries + 1):
        try:
            return process_single_table_file(
                engine=engine,
                inspector=inspector,
                table_name=table_name,
                df=df,
            )
        except OperationalError as e:
            logger.warning(
                "Transient DB error while loading table %s (attempt %s/%s): %s",
                table_name,
                attempt,
                retries,
                str(e).split("\n")[0][:180],
            )
            engine.dispose()

            if attempt == retries:
                raise

            time.sleep(min(2 * attempt, 10))


def load_all_data(engine: Engine, data_dir: str) -> tuple[list[FileReport], int, int, int]:
    print("=" * 70)
    print("  STEP 2: STRUCTURAL AUDIT (JSON keys vs SQL columns)")
    print("  STEP 3: FULL DATA LOAD (COPY + STAGING + MERGE)")
    print("=" * 70)

    inspector = inspect(engine)
    db_table_names = inspector.get_table_names()
    ordered_data_files = ordered_existing_files(data_dir, ".txt")

    step_warnings = 0
    step_pass = 0
    step_fail = 0
    results: list[FileReport] = []

    for data_file in ordered_data_files:
        base_name = data_file.replace(".txt", "")
        print(f"\n  ┌─── {data_file} ───")

        table_name = next((t for t in db_table_names if t.lower() == base_name.lower()), None)

        if not table_name:
            print("  │  ❓ SKIP: No matching table found in database.")
            print("  └───")
            results.append(
                FileReport(
                    file=data_file,
                    table="NOT FOUND",
                    structure="SKIP",
                    load="SKIP",
                    rows_loaded=0,
                    notes="No matching table",
                )
            )
            continue

        path = os.path.join(data_dir, data_file)

        try:
            df = load_json_to_dataframe(path)
        except Exception as e:
            error_msg = str(e).split("\n")[0][:180]
            print(f"  │  ❌ JSON PARSE ERROR: {error_msg}")
            print("  └───")
            results.append(
                FileReport(
                    file=data_file,
                    table=table_name,
                    structure="PARSE ERROR",
                    load="SKIP",
                    rows_loaded=0,
                    notes=error_msg,
                )
            )
            step_fail += 1
            continue

        df, actual_renames = apply_renames(df, base_name)
        if actual_renames:
            print(f"  │  🔄 Applied column renames: {actual_renames}")

        df_audit = df.copy()
        df_audit.columns = [normalize_column_name(c) for c in df_audit.columns]

        db_columns_info = inspector.get_columns(table_name)
        db_cols = {c["name"].lower() for c in db_columns_info}
        json_cols = set(df_audit.columns)

        extra_in_json = json_cols - db_cols - {"flag"}
        missing_in_json = db_cols - json_cols

        struct_status = "✅ MATCH"
        notes: list[str] = []

        if extra_in_json:
            print(f"  │  ⚠️  Extra in JSON (not in SQL): {sorted(extra_in_json)}")
            struct_status = "⚠️ MISMATCH"
            step_warnings += 1
            notes.append(f"extra_json={sorted(extra_in_json)}")

        if missing_in_json:
            print(f"  │  ℹ️  Missing in JSON (will be NULL): {sorted(missing_in_json)}")
            if struct_status == "✅ MATCH":
                struct_status = "⚠️ MISMATCH"
            notes.append(f"missing_json={sorted(missing_in_json)}")

        if not extra_in_json and not missing_in_json:
            print(f"  │  ✅ Structure: Perfect column match ({len(json_cols)} cols incl flag)")

        try:
            upserted_count, deleted_count = process_single_table_file_with_retry(
                engine=engine,
                inspector=inspector,
                table_name=table_name,
                df=df,
            )

            processed_count = upserted_count + deleted_count
            print(
                f"  │  ✅ FULL LOAD: Upserted {upserted_count} rows, "
                f"deleted {deleted_count} rows successfully"
            )

            step_pass += 1
            load_status = "✅ PASS"

        except Exception as e:
            error_msg = str(e).split("\n")[0][:200]
            print(f"  │  ❌ LOAD FAILED: {error_msg}")
            step_fail += 1
            load_status = "❌ FAIL"
            processed_count = 0
            notes.append(error_msg)

        results.append(
            FileReport(
                file=data_file,
                table=table_name,
                structure=struct_status,
                load=load_status,
                rows_loaded=processed_count,
                notes=" | ".join(notes),
            )
        )
        print("  └───")

    return results, step_warnings, step_pass, step_fail


def print_final_report(
    results: list[FileReport],
    create_pass: int,
    create_fail: int,
    struct_warnings: int,
    load_pass: int,
    load_fail: int,
) -> None:
    print("\n" + "=" * 70)
    print("  FINAL REPORT")
    print("=" * 70)
    print(f"\n  {'File':<35} {'Table':<30} {'Structure':<15} {'Load':<10} {'Rows':<12}")
    print(f"  {'─'*35} {'─'*30} {'─'*15} {'─'*10} {'─'*12}")

    for r in results:
        print(f"  {r.file:<35} {r.table:<30} {r.structure:<15} {r.load:<10} {r.rows_loaded:<12}")

    print(f"\n  ══════════════════════════════════════")
    print(f"  Tables Created:      {create_pass}/{create_pass + create_fail}")
    print(f"  Structure Warnings:  {struct_warnings}")
    print(f"  Load Pass:           {load_pass}/{load_pass + load_fail}")
    print(f"  Load Fail:           {load_fail}/{load_pass + load_fail}")
    print(f"  ══════════════════════════════════════\n")


def build_engine() -> Engine:
    return create_engine(
        DB_URL,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=2,
        max_overflow=0,
        connect_args={"connect_timeout": 10},
    )


def validate_environment() -> None:
    if not os.path.isdir(SCHEMA_DIR):
        raise FileNotFoundError(f"Schema directory not found: {SCHEMA_DIR}")

    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR}")

    if BATCH_SIZE <= 0:
        raise ValueError("ETL_BATCH_SIZE must be > 0")

    if SLEEP_BETWEEN_BATCHES < 0:
        raise ValueError("ETL_BATCH_SLEEP cannot be negative")


def main():
    print("\n" + "=" * 35)
    print("  FINANCIAL DATABASE — FAST PRODUCTION ETL")
    print("=" * 35 + "\n")

    validate_environment()

    engine = build_engine()

    wait_for_db(engine)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful\n")
    except Exception as e:
        raise RuntimeError(f"❌ DB CONNECTION FAILED: {e}") from e

    if RECREATE_SCHEMA:
        if not FORCE_RESET:
            raise RuntimeError("Set ETL_FORCE_RESET=true to allow schema drop")
        recreate_public_schema(engine)

    _, create_pass, create_fail = create_tables(engine, SCHEMA_DIR)
    if create_fail > 0:
        raise RuntimeError(f"Table creation failed for {create_fail} tables. Aborting ETL.")

    results, struct_warnings, load_pass, load_fail = load_all_data(engine, DATA_DIR)

    print_final_report(
        results=results,
        create_pass=create_pass,
        create_fail=create_fail,
        struct_warnings=struct_warnings,
        load_pass=load_pass,
        load_fail=load_fail,
    )


if __name__ == "__main__":
    main()