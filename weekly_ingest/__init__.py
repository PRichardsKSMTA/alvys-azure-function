"""Durable orchestrator for weekly Alvys data ingest."""
from typing import Dict, List
import logging
from pathlib import Path
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    # get the list of clients (SCAC + creds) via an activity (I/O outside orchestrator)
    clients: List[Dict[str, str]] = yield context.call_activity("list_clients", None)

    base_dir = Path(__file__).resolve().parent.parent
    failed_entity = df.EntityId("failed_scacs", "log")

    # Fan-out sequentially with per-client error capture (simple + durable-safe)
    for c in clients:
        scac = c["SCAC"]
        creds = {
            "tenant_id": c["TENANT_ID"],
            "client_id": c["CLIENT_ID"],
            "client_secret": c["CLIENT_SECRET"],
            "grant_type": c["GRANT_TYPE"],
        }
        payload = {
            "scac": scac,
            "credentials": creds,
            "data_dir": str(base_dir / "alvys_weekly_data" / scac),
        }
        try:
            yield context.call_activity("ingest_client", payload)
        except Exception as err:  # keep going; record who failed
            logging.error("Ingest failed for %s: %s", scac, err)
            context.signal_entity(failed_entity, "add", scac)

    return "OK"

main = df.Orchestrator.create(orchestrator_function)
