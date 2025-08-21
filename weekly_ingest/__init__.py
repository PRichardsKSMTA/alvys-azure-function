import logging
import os
from typing import Dict

import azure.functions as func
import db

from main import run_export, run_insert, ENTITIES


def main(mytimer: func.TimerRequest) -> None:
    logging.info("Weekly ingest timer triggered")

    query = (
        "SELECT SCAC, TENANT_ID, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE "
        "FROM dbo.ALVYS_CLIENTS"
    )

    # Fetch all client credentials using a single connection/cursor
    with db.get_conn() as conn, conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

        for scac, tenant_id, client_id, client_secret, grant_type in rows:
            try:
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
            except Exception:
                logging.exception("Failed processing %s", scac)
