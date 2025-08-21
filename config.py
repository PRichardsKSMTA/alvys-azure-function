#!/usr/bin/env python
"""Client credential resolver & SQL helper for the Alvys ingestion pipeline.

Why a dedicated module?
-----------------------
* **Single source of truth** for where and how we store multi-tenant
  connection info (SCAC -> credentials).
* Centralised cache so repeated calls in one run hit memory, not SQL.
* Keeps secrets out of source control-expects the **connection string** to
  live in an environment variable.
"""
from __future__ import annotations

import os
import functools
from typing import Dict
from dotenv import load_dotenv  # type: ignore
import pyodbc

from db import get_conn

load_dotenv()
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Table and columns that map SCAC -> auth credentials
_TABLE = "dbo.ALVYS_CLIENTS"
_COLS = ["TENANT_ID", "CLIENT_ID", "CLIENT_SECRET", "GRANT_TYPE"]


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _get_sql_connection() -> pyodbc.Connection:
    """Return a live ``pyodbc`` connection using ``ALVYS_SQL_CONN_STR``."""
    return get_conn()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=128)
def get_credentials(scac: str) -> Dict[str, str]:
    """Look up a client's auth credentials by SCAC.

    If the four ``ALVYS_*`` environment variables are present
    (``ALVYS_TENANT_ID``, ``ALVYS_CLIENT_ID``, ``ALVYS_CLIENT_SECRET`` and
    ``ALVYS_GRANT_TYPE``) their values are returned directly and the
    database is not queried.

    Returns a dict ::
        {
          "tenant_id": str,
          "client_id": str,
          "client_secret": str,
          "grant_type": str,
        }

    Results are **LRU-cached** so subsequent calls in the same run avoid extra
    network trips.
    """
    env_creds = {
        "tenant_id": os.getenv("ALVYS_TENANT_ID"),
        "client_id": os.getenv("ALVYS_CLIENT_ID"),
        "client_secret": os.getenv("ALVYS_CLIENT_SECRET"),
        "grant_type": os.getenv("ALVYS_GRANT_TYPE"),
    }
    if all(env_creds.values()):
        return env_creds  # type: ignore[return-value]

    scac = scac.upper().strip()
    placeholders = ", ".join(_COLS)
    query = (
        f"SELECT {placeholders} FROM {_TABLE} WITH (NOLOCK) WHERE SCAC = ?"
    )

    with _get_sql_connection() as conn, conn.cursor() as cur:
        cur.execute(query, scac)
        row = cur.fetchone()
        if row is None:
            raise KeyError(
                f"SCAC '{scac}' not found in {_TABLE}. Did you add the client?"
            )

    creds = dict(zip([c.lower() for c in _COLS], row))
    return creds  # type: ignore[return-value]


def build_auth_urls(tenant_id: str, api_version: str = "1") -> Dict[str, str]:
    """Return auth & base API URLs for a given tenant."""
    auth_url = (
        f"https://integrations.alvys.com/api/authentication/{tenant_id}/token"
    )
    base_url = f"https://integrations.alvys.com/api/p/v{api_version}"
    return {"auth_url": auth_url, "base_url": base_url}


__all__ = ["get_credentials", "build_auth_urls"]
