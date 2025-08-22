"""Durable orchestrator for weekly Alvys data ingest."""

from typing import Dict, List

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

    tasks = []
    for scac, tenant_id, client_id, client_secret, grant_type in rows:
        creds: Dict[str, str] = {
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type,
        }
        tasks.append(
            context.call_activity(
                "ingest_client", {"scac": scac, "credentials": creds}
            )
        )

    yield context.task_all(tasks)


main = df.Orchestrator.create(orchestrator_function)

