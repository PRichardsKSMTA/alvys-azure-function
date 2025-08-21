import os
import pandas as pd
import sys
import time
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv  # type: ignore
from utils.io import load_json, safe_datetime

import db

load_dotenv()

# === Configuration ===
# Database connection handled via db.get_conn()
DATA_DIR = "alvys_weekly_data"
BATCH_SIZE = 500
SCHEMA = "TBXX"

def sanitize_driver(d: Dict) -> Dict:
    return {
        "ID": d.get("Id"),
        "EMPLOYEE_ID": d.get("EmployeeId"),
        "DRIVER_TYPE": d.get("Type"),
        "SUBSIDIARY_ID": d.get("SubsidiaryId"),
        "ZIP_CODE": d.get("Address", {}).get("ZipCode"),
        "FLEET_ID": d.get("Fleet", {}).get("Id"),
        "FLEET_NAME": d.get("Fleet", {}).get("Name"),
        "CREATED_DTTM": safe_datetime(d.get("CreatedAt")),
        "IS_ACTIVE": int(d.get("IsActive", False)),
        "HIRED_DTTM": safe_datetime(d.get("HiredAt")),
        "FILE_ID": d.get("FILE_ID")
    }

def sanitize_truck(t: Dict) -> Dict:
    return {
        "ID": t.get("Id"),
        "TRUCK_NUM": t.get("TruckNum"),
        "TRUCK_STATUS": t.get("Status"),
        "VIN_NUMBER": t.get("VinNumber"),
        "YEAR": str(t.get("Year")) if t.get("Year") is not None else None,
        "MAKE": t.get("Make"),
        "MODEL": t.get("Model"),
        "LICENSE_STATE": t.get("LicenseState"),
        "TRUCK_TYPE": t.get("TruckType"),
        "SUBSIDIARY_ID": t.get("SubsidiaryId"),
        "FLEET_ID": t.get("Fleet", {}).get("Id"),
        "FLEET_NAME": t.get("Fleet", {}).get("Name"),
        "CREATED_DTTM": safe_datetime(t.get("CreatedAt")),
        "FILE_ID": t.get("FILE_ID")
    }

def sanitize_trailer(t: Dict) -> Dict:
    return {
        "ID": t.get("Id"),
        "TRAILER_NUM": t.get("TrailerNum"),
        "TRAILER_TYPE": t.get("TrailerType"),
        "TRAILER_STATUS": t.get("Status"),
        "CREATED_DTTM": safe_datetime(t.get("CreatedAt")),
        "FILE_ID": t.get("FILE_ID")
    }

def sanitize_customer(c: Dict) -> Dict:
    billing = c.get("BillingAddress", {})
    return {
        "ID": c.get("Id"),
        "CUSTOMER_NAME": c.get("Name"),
        "COMPANY_NUMBER": c.get("CompanyNumber"),
        "CUSTOMER_TYPE": c.get("Type"),
        "CUSTOMER_STATUS": c.get("Status"),
        "BILLING_ADDRESS": billing.get("Street"),
        "CITY": billing.get("City"),
        "STATE_PROVINCE": billing.get("State"),
        "POSTAL_CD": billing.get("ZipCode"),
        "INVOICING_NAME": c.get("InvoicingInformation", {}).get("InvoicingName"),
        "INVOICING_ALIAS": c.get("InvoicingInformation", {}).get("InvoicingNameAlias"),
        "CREATED_DTTM": safe_datetime(c.get("DateCreated")),
        "FILE_ID": c.get("FILE_ID")
    }

def sanitize_carrier(c: Dict) -> Dict:
    # address = c.get("Address", {})
    return {
        "ID": c.get("Id"),
        "CARRIER_NAME": c.get("Name"),
        "EXTERNAL_NAME": c.get("ExternalName"),
        # "CITY": address.get("City"),
        # "STATE": address.get("State"),
        # "ZIP": address.get("ZipCode"),
        "MC_NUM": c.get("McNum"),
        "DOT_NUM": c.get("UsDotNum"),
        "CARRIER_STATUS": c.get("Status"),
        "CARRIER_TYPE": c.get("Type"),
        "CARRIER_SOURCE": c.get("Source"),
        "CREATED_DTTM": safe_datetime(c.get("CreatedAt")),
        "UPDATED_DTTM": safe_datetime(c.get("UpdatedAt")),
        "FILE_ID": c.get("FILE_ID")
    }

def batch_insert(table: str, records: List[Dict], conn):
    if not records:
        print(f"No data to insert for {table}.")
        return

    inserted_dttm = datetime.now()
    for rec in records:
        rec["INSERTED_DTTM"] = inserted_dttm

    df = pd.DataFrame(records)
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_sql = f"INSERT INTO {SCHEMA}.{table} ({columns}) VALUES ({placeholders})"

    cursor = conn.cursor()
    cursor.fast_executemany = True
    start = time.time()
    for i in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE].where(pd.notnull(df), None).values.tolist()
        cursor.executemany(insert_sql, batch)
    conn.commit()
    duration = time.time() - start
    print(f"OK Inserted {len(df)} records into {table} in {duration:.2f} seconds")

def main():
    args = [arg.lower() for arg in sys.argv[1:]]
    run_all = len(args) == 0

    conn = db.get_conn()

    if run_all or "trailers" in args:
        print("Loading trailers JSON...")
        trailers = [
            sanitize_trailer(t)
            for t in load_json(os.path.join(DATA_DIR, "TRAILERS.json"))
        ]
        batch_insert("TRAILERS_RAW", trailers, conn)

    if run_all or "trucks" in args:
        print("Loading trucks JSON...")
        trucks = [
            sanitize_truck(t)
            for t in load_json(os.path.join(DATA_DIR, "TRUCKS.json"))
        ]
        batch_insert("TRUCKS_RAW", trucks, conn)

    if run_all or "drivers" in args:
        print("Loading drivers JSON...")
        drivers = [
            sanitize_driver(d)
            for d in load_json(os.path.join(DATA_DIR, "DRIVERS.json"))
        ]
        batch_insert("DRIVERS_RAW", drivers, conn)

    if run_all or "customers" in args:
        print("Loading customers JSON...")
        customers = [
            sanitize_customer(c)
            for c in load_json(os.path.join(DATA_DIR, "CUSTOMERS.json"))
        ]
        batch_insert("CUSTOMERS_RAW", customers, conn)

    if run_all or "carriers" in args:
        print("Loading carriers JSON...")
        raw = load_json(os.path.join(DATA_DIR, "CARRIERS.json"))
        items = raw.get("Items") if isinstance(raw, dict) else raw
        carriers = [sanitize_carrier(c) for c in items]
        batch_insert("CARRIERS_RAW", carriers, conn)

    print("\nOK All data inserted.")

if __name__ == "__main__":
    main()
