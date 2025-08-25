"""Activity to export and insert data for a single client."""

import logging
import os
from pathlib import Path
from typing import Dict, Any

from main import ENTITIES, DATA_DIR, run_export, run_insert
import db
from utils.blob import upload_weekly_json


def main(params: Dict[str, Any]) -> str:
    scac = params["scac"]
    creds = params["credentials"]
    data_dir = Path(params.get("data_dir") or (DATA_DIR / scac.upper()))

    os.environ["ALVYS_TENANT_ID"] = creds["tenant_id"]
    os.environ["ALVYS_CLIENT_ID"] = creds["client_id"]
    os.environ["ALVYS_CLIENT_SECRET"] = creds["client_secret"]
    os.environ["ALVYS_GRANT_TYPE"] = creds["grant_type"]

    logging.info("Processing %s", scac)
    run_export(scac, ENTITIES, weeks_ago=0, dry_run=False, output_dir=data_dir)
    run_insert(scac, ENTITIES, dry_run=False, data_dir=data_dir)
    db.exec_client_upload_id(scac)
    upload_weekly_json(scac, data_dir)
    return scac

