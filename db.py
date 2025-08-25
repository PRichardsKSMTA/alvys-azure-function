"""Database connection helpers for Alvys SQL Server.

Provides a uniform interface for creating pyodbc connections and
SQLAlchemy engines using the ``ALVYS_SQL_CONN_STR`` environment variable.
"""

from __future__ import annotations
import os
import time
from datetime import datetime, timedelta
import urllib.parse
import pyodbc
from sqlalchemy import create_engine

def _get_conn_str() -> str:
    conn_str = os.getenv("ALVYS_SQL_CONN_STR")
    if not conn_str:
        raise RuntimeError("Environment variable 'ALVYS_SQL_CONN_STR' is not set")
    return conn_str

def _upgrade_driver_and_tls(conn_str: str) -> str:
    s = conn_str
    # Prefer ODBC 18 on Azure Functions Linux images; 17 may be missing.
    if "ODBC Driver 17 for SQL Server" in s:
        s = s.replace("ODBC Driver 17 for SQL Server", "ODBC Driver 18 for SQL Server")
    # ODBC 18 defaults to Encrypt=yes; make TrustServerCertificate explicit if not present
    # to avoid cert chain issues on app hosts. Add only when missing.
    tokens = {kv.split("=", 1)[0].strip().lower(): kv for kv in s.split(";") if "=" in kv}
    if "encrypt" not in tokens:
        s = s.rstrip(";") + ";Encrypt=yes"
    if "trustservercertificate" not in tokens:
        s = s.rstrip(";") + ";TrustServerCertificate=yes"
    return s

def get_conn(*, retries: int = 4, base_delay: float = 0.5) -> pyodbc.Connection:
    """Return a ``pyodbc.Connection`` with simple retry/backoff and ODBC18 fallback."""
    raw = _get_conn_str()
    attempt = 0
    while True:
        try:
            return pyodbc.connect(raw)
        except pyodbc.Error as exc:
            msg = str(exc)
            # If Driver 17 is missing on the host, try again with Driver 18 + TLS flags.
            if "ODBC Driver 17 for SQL Server" in msg and "file not found" in msg.lower():
                raw = _upgrade_driver_and_tls(raw)
                # single retry immediately with upgraded string
                try:
                    return pyodbc.connect(raw)
                except pyodbc.Error:
                    pass  # fall through to retry loop below
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(base_delay * (2 ** (attempt - 1)))

def get_engine(**kw):
    """Return a SQLAlchemy engine using ``mssql+pyodbc``."""
    conn_str = urllib.parse.quote_plus(_upgrade_driver_and_tls(_get_conn_str()))
    url = f"mssql+pyodbc:///?odbc_connect={conn_str}"
    return create_engine(url, **kw)


def exec_client_upload_id(scac: str) -> None:
    """Execute ``dbo.INSERT_CLIENT_UPLOAD_ID`` for ``scac`` using this week's Monday."""
    today = datetime.utcnow().date()
    monday = today - timedelta(days=today.weekday())
    upload_id = monday.strftime("%Y%m%d")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "EXEC dbo.INSERT_CLIENT_UPLOAD_ID @SCAC=?, @Uploadid=?",
            scac,
            upload_id,
        )
        conn.commit()

