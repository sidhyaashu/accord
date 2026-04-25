import pandas as pd
from sqlalchemy.sql.sqltypes import BigInteger, Integer, SmallInteger

from app.config import COLUMN_RENAMES


def normalize_column_name(name: str) -> str:
    return str(name).strip().lower()


def payload_to_dataframe(payload: dict) -> pd.DataFrame:
    if not payload:
        return pd.DataFrame()

    if "Table" not in payload or not isinstance(payload["Table"], list):
        raise ValueError("Invalid payload: missing Table array")

    return pd.DataFrame(payload["Table"])


def apply_renames(df: pd.DataFrame, feed_name: str) -> pd.DataFrame:
    rename_key = feed_name.lower()
    rename_map = COLUMN_RENAMES.get(rename_key, {})

    if not rename_map:
        return df

    incoming_map = {normalize_column_name(col): col for col in df.columns}
    actual_renames = {}

    for source_col_normalized, target_col in rename_map.items():
        actual_source = incoming_map.get(source_col_normalized)
        if actual_source:
            actual_renames[actual_source] = target_col

    if actual_renames:
        print(f"🔄 Applied renames for {feed_name}: {actual_renames}")
        df = df.rename(columns=actual_renames)

    return df


def preprocess_dataframe_for_table(
    df: pd.DataFrame,
    table_name: str,
    db_columns_info: list[dict],
) -> tuple[pd.DataFrame, list[str]]:
    df = df.copy()
    df.columns = [normalize_column_name(c) for c in df.columns]

    df = df.loc[:, ~pd.Index(df.columns).duplicated()]

    db_columns = {c["name"].lower(): c["type"] for c in db_columns_info}
    db_cols = list(db_columns.keys())

    keep_cols = [c for c in df.columns if c in db_columns]
    if not keep_cols:
        raise ValueError(f"No matching DB columns found for table={table_name}")

    df = df.loc[:, keep_cols].copy()

    if "flag" not in df.columns:
        raise ValueError(f"Required flag column missing for table={table_name}")

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

    seen = set()
    final_cols = [c for c in final_cols if not (c in seen or seen.add(c))]

    df = df.loc[:, final_cols]

    return df, db_cols


def safe_value(v):
    if v is None:
        return ""

    value = str(v).strip()

    if value.lower() in ("<na>", "nan", "none"):
        return ""

    return value