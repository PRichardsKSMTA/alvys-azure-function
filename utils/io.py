#!/usr/bin/env python
"""I/O helpers for the Alvys ingestion pipeline."""

from __future__ import annotations

import json
from datetime import datetime


def load_json(path: str) -> list[dict]:
    """Read a JSON file and return the parsed object."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def safe_datetime(val: str | None) -> datetime | None:
    """Parse ISO-8601 datetime strings into :class:`datetime` objects.

    Returns ``None`` for falsy values or parsing errors.
    """
    if not val:
        return None
    s = val.replace("Z", "+00:00")
    if "." in s and "+" in s:
        prefix, rest = s.split(".", 1)
        frac, offset = rest.split("+", 1)
        frac6 = frac[:6].ljust(6, "0")
        s = f"{prefix}.{frac6}+{offset}"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None
