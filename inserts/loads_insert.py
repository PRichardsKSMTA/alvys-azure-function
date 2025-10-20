"""Insert Alvys load data into SQL Server efficiently
----------------------------------------------------
Vectorised pandas -> SQL bulk load with audit column.
Uses ``db.get_engine()`` for connections.
This revision fixes the `NoneType is not subscriptable` error by:
* Introducing a **null-safe truncation helper** `_s(val, max_len)` and using it everywhere (no slicing outside `_s`).
* Keeps single UTC `INSERTED_DTTM` for every row.
"""
import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Any

import pandas as pd
from sqlalchemy import types
from dotenv import load_dotenv  # type: ignore

import db
from utils.datetime_utc import to_utc_naive

load_dotenv()

# --------------------------------------------
# CONFIG
# --------------------------------------------

CHUNK_SIZE = 1_000
RUN_TS = datetime.now(tz=timezone.utc).replace(tzinfo=None)  # naive UTC timestamp

# --------------------------------------------
# COLUMNS & SQL TYPES
# --------------------------------------------
LOAD_COLS: List[str] = [
    "ID", "LOAD_NUMBER", "ORDER_NUMBER", "LOAD_STATUS", "CUSTOMER_ID",
    "FLEET_ID", "FLEET_NAME", "INVOICE_AS", "LINEHAUL_AMOUNT",
    "FUEL_SURCHARGE", "ACCESSORIALS_AMOUNT", "CUSTOMER_RATE",
    "CUSTOMER_MILEAGE", "MILEAGE_SOURCE", "TOTAL_WEIGHT",
    "SCHEDULED_PICKUP", "SCHEDULED_DELIVERY", "PICKED_UP_AT",
    "DELIVERED_AT", "CREATED_DTTM", "CUSTOMER_SERVICE_REP_ID",
    "CUSTOMER_SALES_AGENT_ID", "UPDATED_DTTM", "IS_DELETED", "FILE_ID", "INSERTED_DTTM",
    "LOAD_TYPE", "BILLTO_ID",
]

DTYPE_LOADS = {
    "ID": types.VARCHAR(100),
    "LOAD_NUMBER": types.VARCHAR(100),
    "ORDER_NUMBER": types.VARCHAR(100),
    "LOAD_STATUS": types.VARCHAR(50),
    "CUSTOMER_ID": types.VARCHAR(100),
    "FLEET_ID": types.VARCHAR(100),
    "FLEET_NAME": types.VARCHAR(100),
    "INVOICE_AS": types.VARCHAR(50),
    "LINEHAUL_AMOUNT": types.Numeric(18, 2),
    "FUEL_SURCHARGE": types.Numeric(18, 2),
    "ACCESSORIALS_AMOUNT": types.Numeric(18, 2),
    "CUSTOMER_RATE": types.Numeric(18, 2),
    "CUSTOMER_MILEAGE": types.Numeric(18, 2),
    "MILEAGE_SOURCE": types.VARCHAR(50),
    "TOTAL_WEIGHT": types.Numeric(18, 2),
    "SCHEDULED_PICKUP": types.DateTime(),
    "SCHEDULED_DELIVERY": types.DateTime(),
    "PICKED_UP_AT": types.DateTime(),
    "DELIVERED_AT": types.DateTime(),
    "CREATED_DTTM": types.DateTime(),
    "CUSTOMER_SERVICE_REP_ID": types.VARCHAR(100),
    "CUSTOMER_SALES_AGENT_ID": types.VARCHAR(100),
    "UPDATED_DTTM": types.DateTime(),
    "IS_DELETED": types.Integer,
    "FILE_ID": types.VARCHAR(50),
    "INSERTED_DTTM": types.DateTime(),
    "LOAD_TYPE": types.VARCHAR(50),
    "BILLTO_ID": types.VARCHAR(100),
}

# --------------------------------------------
# ENGINE
# --------------------------------------------
# Database engine provided by db.get_engine()

# --------------------------------------------
# HELPERS
# --------------------------------------------

def _s(val: Optional[Any], max_len: int | None = None) -> Optional[str]:
    """Safe string: trim, truncate, and return None if blank/None."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


def _f(val) -> Optional[float]:
    try:
        return float(val) if val not in (None, "") else None
    except (TypeError, ValueError):
        return None


def g(d: Optional[dict], *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

# --------------------------------------------
# FLATTEN ONE LOAD
# --------------------------------------------

def flatten_load(load: dict, file_id: str):
    return [
        _s(load.get("Id"), 100),
        _s(load.get("LoadNumber"), 100),
        _s(load.get("OrderNumber"), 100),
        _s(load.get("Status"), 50),
        _s(load.get("CustomerId"), 100),
        _s(g(load, "Fleet", "Id"), 100),
        _s(g(load, "Fleet", "Name"), 100),
        _s(load.get("InvoiceAs"), 50),
        _f(g(load, "Linehaul", "Amount")),
        _f(g(load, "FuelSurcharge", "Amount")),
        _f(g(load, "CustomerAccessorials", "Amount")),
        _f(g(load, "CustomerRate", "Amount")),
        _f(g(load, "CustomerMileage", "Distance", "Value")),
        _s(g(load, "CustomerMileage", "Source"), 50),
        _f(g(load, "Weight", "Value")),
        load.get("ScheduledPickupAt"),
        load.get("ScheduledDeliveryAt"),
        load.get("PickedUpAt"),
        load.get("DeliveredAt"),
        load.get("CreatedAt"),
        _s(load.get("CustomerServiceRepId"), 100),
        _s(load.get("CustomerSalesAgentId"), 100),
        load.get("UpdatedAt"),
        int(load.get("IsDeleted") or False),
        file_id,  # already truncated to 50 below
        RUN_TS,
        _s(load.get("LoadType"), 50),
        _s(load.get("CustomerNumber"), 100),
    ]

# --------------------------------------------
# BUILD DATAFRAME
# --------------------------------------------

def build_dataframe(data_dir: Path) -> pd.DataFrame:
    rows: list[list] = []
    for fname in sorted(os.listdir(data_dir)):
        if not fname.startswith("LOADS_API_") or not fname.endswith(".json"):
            continue
        with open(data_dir / fname, encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            continue
        file_id = _s(data[0].get("FILE_ID"), 50)
        print(f"Processing {fname} ... {len(data):,} objects (FILE_ID={file_id})")
        rows.extend(flatten_load(rec, file_id) for rec in data)

    df = pd.DataFrame(rows, columns=LOAD_COLS)

    # Convert datetime columns
    for col in [
        "SCHEDULED_PICKUP", "SCHEDULED_DELIVERY", "PICKED_UP_AT", "DELIVERED_AT",
        "CREATED_DTTM", "UPDATED_DTTM",
    ]:
        df[col] = to_utc_naive(df[col])
    return df

# --------------------------------------------
# BULK INSERT
# --------------------------------------------

def bulk_insert(engine, schema: str, df: pd.DataFrame):
    if df.empty:
        print("WARN  No load records to insert.")
        return
    df = df.where(pd.notnull(df), None)
    start = time.perf_counter()
    table = "LOADS_RAW"
    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=CHUNK_SIZE,
        dtype=DTYPE_LOADS,
    )
    print(f"OK {len(df):,} rows inserted into {schema}.{table} in {time.perf_counter() - start:.1f}s")

# --------------------------------------------
# MAIN
# --------------------------------------------

def main(argv: List[str] | None = None, data_dir: Path | None = None):
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--scac", required=True, dest="schema", help="Target DB schema")
    args = parser.parse_args(argv)
    schema = args.schema.upper()
    base_dir = Path(data_dir or Path("alvys_weekly_data") / schema)

    print("Loading loads JSON ...")
    df = build_dataframe(base_dir)
    print(f"Found {len(df):,} loads. Inserting ...")
    bulk_insert(db.get_engine(), schema, df)


if __name__ == "__main__":
    main()
