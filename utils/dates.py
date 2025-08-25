#!/usr/bin/env python
"""
Date helpers for the Alvys ingestion pipeline.

Provides :func:`get_last_week_range`, which returns the start and end
:class:`datetime.datetime` objects (UTC-aware) for the **most recently
*completed* Sunday-to-Saturday week**.

``weeks_ago`` lets you step back further:

* ``weeks_ago = 0`` - the week that ended last *Saturday*.
* ``weeks_ago = 1`` - the week before that, and so on.

The helpers are deterministic, side-effect-free, and make no external
calls-ideal for unit testing.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Tuple

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

UTC = timezone.utc  # Single timezone constant so we don’t repeat ourselves


def _start_of_week(dt: datetime, tz: timezone = UTC) -> datetime:
    """Return the **Sunday 00:00:00.000** for the week containing ``dt``.

    Parameters
    ----------
    dt : datetime
        A timezone-aware datetime.
    tz : timezone, optional
        Timezone to normalise the result to (default: UTC).
    """
    dt = dt.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    # In Python’s weekday(), Monday = 0 ... Sunday = 6.
    # We want the most recent Sunday **on or before** dt.
    days_since_sunday = (dt.weekday() + 1) % 7
    return dt - timedelta(days=days_since_sunday)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_last_week_range(weeks_ago: int = 0, tz: timezone = UTC) -> Tuple[datetime, datetime]:
    """Return *(start, end)* of the last completed Sunday-Saturday week.

    Both datetimes are timezone-aware in the supplied ``tz`` and millisecond
    precise-``end`` is always `Saturday 23:59:59.999`.
    """
    if weeks_ago < 0:
        raise ValueError("weeks_ago must be >= 0")

    now = datetime.now(tz)
    start_of_current_week = _start_of_week(now, tz)

    # The week we’re interested in is the one *before* the current week.
    start = start_of_current_week - timedelta(days=7 * (weeks_ago + 1))
    end = start + timedelta(days=7,
                            hours=5,
                            minutes=59,
                            seconds=59,
                            milliseconds=999)

    return start, end


def iso_range(
    reference: datetime | None = None,
    tz: timezone = timezone.utc,
) -> Tuple[str, str]:
    """Convenience wrapper that returns the tuple as ISO‑8601 strings exactly
    to the millisecond (*YYYY‑MM‑DDTHH:MM:SS.mmmZ*)."""

    start_dt, end_dt = get_last_week_range(reference, tz)
    return (
        start_dt.isoformat(timespec="milliseconds"),
        end_dt.isoformat(timespec="milliseconds"),
    )
