"""Activity to list SCACs and credentials from dbo.ALVYS_CLIENTS."""
from typing import List, Dict, Any
import traceback

import db
from utils.alerts import send_error_notification


def main(input: Any) -> List[Dict[str, str]]:
    try:
        query = (
            "SELECT SCAC, TENANT_ID, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE "
            "FROM dbo.ALVYS_CLIENTS"
        )
        with db.get_conn() as conn, conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        # return a list of dicts with consistent keys the orchestrator expects
        return [
            {
                "SCAC": r[0],
                "TENANT_ID": r[1],
                "CLIENT_ID": r[2],
                "CLIENT_SECRET": r[3],
                "GRANT_TYPE": r[4],
            }
            for r in rows
        ]
    except Exception as err:  # pragma: no cover - notify and re-raise
        stack = traceback.format_exc()
        send_error_notification("list_clients", str(err), stack)
        raise
