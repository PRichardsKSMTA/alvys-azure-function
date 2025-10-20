"""Insert Alvys trip & stop data into SQL Server efficiently
---------------------------------------------------------
* Vectorised pandas bulk-insert (`fast_executemany=True`).
* Adds `INSERTED_DTTM` audit column (single UTC timestamp per run).
* Null-safe helpers prevent slicing errors.
* Uses ``db.get_engine()`` for connections.
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
from utils.datetime import to_utc_naive

load_dotenv()

# --------------------------------------------
# CONFIG
# --------------------------------------------

CHUNK_SIZE = 1_000
RUN_TS = datetime.now(tz=timezone.utc).replace(tzinfo=None)

# --------------------------------------------
# COLUMNS & DTYPES
# --------------------------------------------
TRIP_COLS: List[str] = [
    "ID", "TRIP_NUMBER", "TRIP_STATUS", "LOAD_NUMBER", "TENDER_AS",
    "TOTAL_MILEAGE", "MILEAGE_SOURCE", "MILEAGE_PROFILE_NAME", "EMPTY_MILEAGE",
    "LOADED_MILEAGE", "PICKUP_DTTM", "DELIVERY_DTTM", "PICKED_UP_DTTM",
    "DELIVERED_DTTM", "CARRIER_ASSIGNED_DTTM", "RELEASED_DTTM", "TRIP_VALUE",
    "TRUCK_ID", "TRUCK_FLEET_ID", "TRUCK_FLEET_NAME", "TRAILER_ID",
    "TRAILER_TYPE", "DRIVER1_ID", "DRIVER1_TYPE", "DRIVER1_FLEET_ID",
    "DRIVER2_ID", "DRIVER2_TYPE", "DRIVER2_FLEET_ID", "OWNER_OPERATOR_ID",
    "RELEASED_BY", "DISPATCHED_BY", "DISPATCHER_ID", "IS_CARRIER_PAY_ON_HOLD",
    "CARRIER_ID", "CARRIER_INVOICE", "CARRIER_RATE", "CARRIER_LINEHAUL",
    "CARRIER_FUEL", "CARRIER_ACCESSORIALS", "CARRIER_TOTAL_PAYABLE",
    "UPDATED_DTTM", "IS_DELETED", "FILE_ID", "INSERTED_DTTM",
]

STOP_COLS: List[str] = [
    "ID", "TRIP_ID", "TRIP_NUMBER", "STOP_SEQUENCE", "IS_APPOINTMENT_REQUESTED",
    "IS_APPOINTMENT_CONFIRMED", "EARLIEST_APPOINTMENT_DTTM", "LATEST_APPOINTMENT_DTTM",
    "STREET_ADDRESS", "CITY", "STATE_PROVINCE", "POSTAL_CD", "LATITUDE", "LONGITUDE",
    "STOP_STATUS", "STOP_TYPE", "STOP_SCHEDULE_TYPE", "LOADING_TYPE",
    "ARRIVED_DTTM", "DEPARTED_DTTM", "FILE_ID", "INSERTED_DTTM", "LOC_ID", "LOC_NAME",
]

# NOTE: Many VARCHAR lengths chosen generically; adjust if DB schema differs.
NUM18_2 = types.Numeric(18, 2)
NUM18_6 = types.Numeric(18, 6)
DTYPE_TRIPS = {
    "ID": types.VARCHAR(100),
    "TRIP_NUMBER": types.VARCHAR(100),
    "TRIP_STATUS": types.VARCHAR(50),
    "LOAD_NUMBER": types.VARCHAR(100),
    "TENDER_AS": types.VARCHAR(50),
    "TOTAL_MILEAGE": NUM18_2,
    "MILEAGE_SOURCE": types.VARCHAR(50),
    "MILEAGE_PROFILE_NAME": types.VARCHAR(100),
    "EMPTY_MILEAGE": NUM18_2,
    "LOADED_MILEAGE": NUM18_2,
    "PICKUP_DTTM": types.DateTime(),
    "DELIVERY_DTTM": types.DateTime(),
    "PICKED_UP_DTTM": types.DateTime(),
    "DELIVERED_DTTM": types.DateTime(),
    "CARRIER_ASSIGNED_DTTM": types.DateTime(),
    "RELEASED_DTTM": types.DateTime(),
    "TRIP_VALUE": NUM18_2,
    "TRUCK_ID": types.VARCHAR(100),
    "TRUCK_FLEET_ID": types.VARCHAR(100),
    "TRUCK_FLEET_NAME": types.VARCHAR(100),
    "TRAILER_ID": types.VARCHAR(100),
    "TRAILER_TYPE": types.VARCHAR(50),
    "DRIVER1_ID": types.VARCHAR(100),
    "DRIVER1_TYPE": types.VARCHAR(50),
    "DRIVER1_FLEET_ID": types.VARCHAR(100),
    "DRIVER2_ID": types.VARCHAR(100),
    "DRIVER2_TYPE": types.VARCHAR(50),
    "DRIVER2_FLEET_ID": types.VARCHAR(100),
    "OWNER_OPERATOR_ID": types.VARCHAR(100),
    "RELEASED_BY": types.VARCHAR(100),
    "DISPATCHED_BY": types.VARCHAR(100),
    "DISPATCHER_ID": types.VARCHAR(100),
    "IS_CARRIER_PAY_ON_HOLD": types.Integer,
    "CARRIER_ID": types.VARCHAR(100),
    "CARRIER_INVOICE": types.VARCHAR(100),
    "CARRIER_RATE": NUM18_2,
    "CARRIER_LINEHAUL": NUM18_2,
    "CARRIER_FUEL": NUM18_2,
    "CARRIER_ACCESSORIALS": NUM18_2,
    "CARRIER_TOTAL_PAYABLE": NUM18_2,
    "UPDATED_DTTM": types.DateTime(),
    "IS_DELETED": types.Integer,
    "FILE_ID": types.VARCHAR(50),
    "INSERTED_DTTM": types.DateTime(),
}
DTYPE_STOPS = {
    "ID": types.VARCHAR(100),
    "TRIP_ID": types.VARCHAR(100),
    "TRIP_NUMBER": types.VARCHAR(100),
    "STOP_SEQUENCE": types.Integer,
    "IS_APPOINTMENT_REQUESTED": types.Integer,
    "IS_APPOINTMENT_CONFIRMED": types.Integer,
    "EARLIEST_APPOINTMENT_DTTM": types.DateTime(),
    "LATEST_APPOINTMENT_DTTM": types.DateTime(),
    "STREET_ADDRESS": types.VARCHAR(200),
    "CITY": types.VARCHAR(100),
    "STATE_PROVINCE": types.VARCHAR(50),
    "POSTAL_CD": types.VARCHAR(20),
    "LATITUDE": NUM18_6,
    "LONGITUDE": NUM18_6,
    "STOP_STATUS": types.VARCHAR(50),
    "STOP_TYPE": types.VARCHAR(50),
    "STOP_SCHEDULE_TYPE": types.VARCHAR(50),
    "LOADING_TYPE": types.VARCHAR(50),
    "ARRIVED_DTTM": types.DateTime(),
    "DEPARTED_DTTM": types.DateTime(),
    "FILE_ID": types.VARCHAR(50),
    "INSERTED_DTTM": types.DateTime(),
    "LOC_ID": types.VARCHAR(100),
    "LOC_NAME": types.VARCHAR(200),
}

# --------------------------------------------
# ENGINE
# --------------------------------------------

# Database engine provided by db.get_engine()

# --------------------------------------------
# HELPERS
# --------------------------------------------

def _s(val: Optional[Any], max_len: int | None = None) -> Optional[str]:
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
# FLATTENERS
# --------------------------------------------

def flatten_trip(trip: dict, file_id: str):
    return [
        _s(trip.get("Id"), 100),
        _s(trip.get("TripNumber"), 100),
        _s(trip.get("Status"), 50),
        _s(trip.get("LoadNumber"), 100),
        _s(trip.get("TenderAs"), 50),
        _f(g(trip, "TotalMileage", "Distance", "Value")),
        _s(g(trip, "TotalMileage", "Source"), 50),
        _s(g(trip, "TotalMileage", "ProfileName"), 100),
        _f(g(trip, "EmptyMileage", "Distance", "Value")),
        _f(g(trip, "LoadedMileage", "Distance", "Value")),
        trip.get("PickupDate"),
        trip.get("DeliveryDate"),
        trip.get("PickedUpAt"),
        trip.get("DeliveredAt"),
        trip.get("CarrierAssignedAt"),
        trip.get("ReleasedAt"),
        _f(g(trip, "TripValue", "Amount")),
        _s(g(trip, "Truck", "Id"), 100),
        _s(g(trip, "Truck", "Fleet", "Id"), 100),
        _s(g(trip, "Truck", "Fleet", "Name"), 100),
        _s(g(trip, "Trailer", "Id"), 100),
        _s(g(trip, "Trailer", "EquipmentType"), 50),
        _s(g(trip, "Driver1", "Id"), 100),
        _s(g(trip, "Driver1", "ContractorType"), 50),
        _s(g(trip, "Driver1", "Fleet", "Id"), 100),
        _s(g(trip, "Driver2", "Id"), 100),
        _s(g(trip, "Driver2", "ContractorType"), 50),
        _s(g(trip, "Driver2", "Fleet", "Id"), 100),
        _s(g(trip, "OwnerOperator", "Id"), 100),
        _s(trip.get("ReleasedBy"), 100),
        _s(trip.get("DispatchedBy"), 100),
        _s(trip.get("DispatcherId"), 100),
        int(trip.get("CarrierPayOnHold") or False),
        _s(g(trip, "Carrier", "Id"), 100),
        _s(g(trip, "Carrier", "CarrierInvoiceNumber"), 100),
        _f(g(trip, "Carrier", "Rate", "Amount")),
        _f(g(trip, "Carrier", "Linehaul", "Amount")),
        _f(g(trip, "Carrier", "Fuel", "Amount")),
        _f(g(trip, "Carrier", "Accessorials", "Amount")),
        _f(g(trip, "Carrier", "TotalPayable", "Amount")),
        trip.get("UpdatedAt"),
        int(trip.get("IsDeleted") or False),
        file_id,
        RUN_TS,
    ]


def flatten_stops(trip: dict, file_id: str):
    trip_id = _s(trip.get("Id"), 100)
    trip_num = _s(trip.get("TripNumber"), 100)
    stops = []
    for seq, stop in enumerate(trip.get("Stops", []), 1):
        address = stop.get("Address", {})
        coords = stop.get("Coordinates", {})
        # Determine appointment window fields
        earliest = stop.get("AppointmentDate") or g(stop, "StopWindow", "Begin")
        latest = g(stop, "StopWindow", "End") if stop.get("StopWindow") else None
        stops.append([
            _s(stop.get("Id") or f"{trip_id}_{seq}", 100),
            trip_id,
            trip_num,
            seq,
            int(stop.get("AppointmentRequested") or False),
            int(stop.get("AppointmentConfirmed") or False),
            earliest,
            latest,
            _s(address.get("Street"), 200),
            _s(address.get("City"), 100),
            _s(address.get("State"), 50),
            _s(address.get("ZipCode"), 20),
            _f(coords.get("Latitude")),
            _f(coords.get("Longitude")),
            _s(stop.get("Status"), 50),
            _s(stop.get("StopType"), 50),
            _s(stop.get("ScheduleType"), 50),
            _s(stop.get("LoadingType"), 50),
            stop.get("ArrivedAt"),
            stop.get("DepartedAt"),
            file_id,
            RUN_TS,
            _s(stop.get("CompanyNumber"), 100),
            _s(stop.get("CompanyName"), 200),
        ])
    return stops

# --------------------------------------------
# BUILD DATAFRAMES
# --------------------------------------------

def build_dfs(data_dir: Path):
    trip_rows: list[list] = []
    stop_rows: list[list] = []
    for fname in sorted(os.listdir(data_dir)):
        if not fname.startswith("TRIPS_API_") or not fname.endswith(".json"):
            continue
        with open(data_dir / fname, encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            continue
        file_id = _s(data[0].get("FILE_ID"), 50)
        print(f"Processing {fname} ... {len(data):,} objects (FILE_ID={file_id})")
        for trip in data:
            trip_rows.append(flatten_trip(trip, file_id))
            stop_rows.extend(flatten_stops(trip, file_id))

    trips_df = pd.DataFrame(trip_rows, columns=TRIP_COLS)
    stops_df = pd.DataFrame(stop_rows, columns=STOP_COLS)

    # Parse datetime columns
    datetime_trip_cols = [
        "PICKUP_DTTM", "DELIVERY_DTTM", "PICKED_UP_DTTM", "DELIVERED_DTTM",
        "CARRIER_ASSIGNED_DTTM", "RELEASED_DTTM", "UPDATED_DTTM",
    ]
    trips_df[datetime_trip_cols] = trips_df[datetime_trip_cols].apply(to_utc_naive)

    datetime_stop_cols = [
        "EARLIEST_APPOINTMENT_DTTM", "LATEST_APPOINTMENT_DTTM", "ARRIVED_DTTM", "DEPARTED_DTTM",
    ]
    stops_df[datetime_stop_cols] = stops_df[datetime_stop_cols].apply(to_utc_naive)

    return trips_df, stops_df

# --------------------------------------------
# BULK INSERT
# --------------------------------------------

def bulk_insert(engine, schema: str, table: str, df: pd.DataFrame, dtype_map: dict):
    if df.empty:
        print(f"WARN  No records for {table}.")
        return
    df = df.where(pd.notnull(df), None)
    start = time.perf_counter()
    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=CHUNK_SIZE,
        dtype=dtype_map,
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

    trips_df, stops_df = build_dfs(base_dir)
    print(f"Found {len(trips_df):,} trips & {len(stops_df):,} stops. Inserting ...")
    eng = db.get_engine()
    bulk_insert(eng, schema, "TRIPS_RAW", trips_df, DTYPE_TRIPS)
    bulk_insert(eng, schema, "TRIP_STOPS_RAW", stops_df, DTYPE_STOPS)


if __name__ == "__main__":
    main()
