import logging
import os
from typing import Dict

import azure.functions as func
import pyodbc

from main import run_export, run_insert, ENTITIES


def main(mytimer: func.TimerRequest) -> None:
    logging.info("Weekly ingest timer triggered")

    conn_str = os.environ["ALVYS_SQL_CONN_STR"]
    query = (
        "SELECT SCAC, TENANT_ID, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE "
        "FROM dbo.ALVYS_CLIENTS"
    )

    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()

    for scac, tenant_id, client_id, client_secret, grant_type in rows:
        creds: Dict[str, str] = {
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type,
        }

        os.environ["ALVYS_TENANT_ID"] = creds["tenant_id"]
        os.environ["ALVYS_CLIENT_ID"] = creds["client_id"]
        os.environ["ALVYS_CLIENT_SECRET"] = creds["client_secret"]
        os.environ["ALVYS_GRANT_TYPE"] = creds["grant_type"]

        logging.info("Processing %s", scac)
        run_export(scac, ENTITIES, weeks_ago=0, dry_run=False)
        run_insert(scac, ENTITIES, dry_run=False)
