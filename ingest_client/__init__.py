"""Activity to export and insert data for a single client."""

import logging
import os
from typing import Dict

from main import ENTITIES, run_export, run_insert


def main(params: Dict[str, Dict[str, str]]) -> str:
    scac = params["scac"]
    creds = params["credentials"]

    os.environ["ALVYS_TENANT_ID"] = creds["tenant_id"]
    os.environ["ALVYS_CLIENT_ID"] = creds["client_id"]
    os.environ["ALVYS_CLIENT_SECRET"] = creds["client_secret"]
    os.environ["ALVYS_GRANT_TYPE"] = creds["grant_type"]

    logging.info("Processing %s", scac)
    run_export(scac, ENTITIES, weeks_ago=0, dry_run=False)
    run_insert(scac, ENTITIES, dry_run=False)
    return scac

