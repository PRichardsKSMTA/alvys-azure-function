"""Durable orchestrator for weekly Alvys data ingest."""
from typing import Dict, List
import logging
import azure.durable_functions as df

from main import DATA_DIR


def orchestrator_function(context: df.DurableOrchestrationContext):
    try:
        # get the list of clients (SCAC + creds) via an activity (I/O outside orchestrator)
        clients: List[Dict[str, str]] = yield context.call_activity("list_clients", None)

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
                "data_dir": str(DATA_DIR / scac.upper()),
            }
            try:
                yield context.call_activity("ingest_client", payload)
            except Exception as err:  # keep going; record who failed
                logging.error("Ingest failed for %s: %s", scac, err)
                context.signal_entity(failed_entity, "add", scac)

        return "OK"
    except Exception as err:
        yield context.call_activity(
            "notify_failure",
            {
                "functionName": "weekly_ingest",
                "message": str(err),
                "stackTrace": getattr(err, "stack_trace", str(err)),
            },
        )
        raise


main = df.Orchestrator.create(orchestrator_function)
