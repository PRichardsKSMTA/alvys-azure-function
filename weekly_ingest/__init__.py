"""Durable orchestrator for weekly Alvys data ingest."""

from typing import Dict, List

import logging
import azure.durable_functions as df
import db


def orchestrator_function(context: df.DurableOrchestrationContext):
    """Query client credentials and fan out ingest activities."""
    query = (
        "SELECT SCAC, TENANT_ID, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE "
        "FROM dbo.ALVYS_CLIENTS"
    )

    with db.get_conn() as conn, conn.cursor() as cur:
        cur.execute(query)
        rows: List[tuple] = cur.fetchall()

    entity_id = df.EntityId("failed_scacs", "log")

    for scac, tenant_id, client_id, client_secret, grant_type in rows:
        creds: Dict[str, str] = {
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type,
        }
        try:
            yield context.call_activity(
                "ingest_client", {"scac": scac, "credentials": creds}
            )
        except Exception as err:  # pylint: disable=broad-except
            logging.error("Ingest failed for %s: %s", scac, err)
            context.signal_entity(entity_id, "add", scac)


main = df.Orchestrator.create(orchestrator_function)

