"""Database connection helpers for Alvys SQL Server.

Provides a uniform interface for creating pyodbc connections and
SQLAlchemy engines using the ``ALVYS_SQL_CONN_STR`` environment variable.
"""

from __future__ import annotations

import os
import time
import urllib.parse

import pyodbc
from sqlalchemy import create_engine


def _get_conn_str() -> str:
    """Return the ODBC connection string from ``ALVYS_SQL_CONN_STR``.

    Raises
    ------
    RuntimeError
        If the environment variable is missing or empty.
    """

    conn_str = os.getenv("ALVYS_SQL_CONN_STR")
    if not conn_str:
        raise RuntimeError("Environment variable 'ALVYS_SQL_CONN_STR' is not set")
    return conn_str


def get_conn(*, retries: int = 4, base_delay: float = 0.5) -> pyodbc.Connection:
    """Return a ``pyodbc.Connection`` with simple retry/backoff."""

    conn_str = _get_conn_str()
    attempt = 0
    while True:
        try:
            return pyodbc.connect(conn_str)
        except pyodbc.Error:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(base_delay * (2 ** (attempt - 1)))


def get_engine(**kw):
    """Return a SQLAlchemy engine using ``mssql+pyodbc``."""

    conn_str = urllib.parse.quote_plus(_get_conn_str())
    url = f"mssql+pyodbc:///?odbc_connect={conn_str}"
    return create_engine(url, **kw)

