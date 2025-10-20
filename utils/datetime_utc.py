#!/usr/bin/env python
"""Utility helpers for working with datetimes.

Currently exposes :func:`to_utc_naive` which converts pandas ``Series`` or
scalar values to naive UTC ``datetime`` objects, mirroring the conversion
logic previously duplicated across insert scripts.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def to_utc_naive(series_or_val: Any):
    """Convert a Series or scalar to naive UTC datetimes.

    Parameters
    ----------
    series_or_val:
        A :class:`pandas.Series` or a scalar value representing date/time(s).

    Returns
    -------
    Series | datetime | None
        The converted naive UTC timestamps. Invalid inputs result in ``None``.
    """

    if isinstance(series_or_val, pd.Series):
        return pd.to_datetime(series_or_val, utc=True, errors="coerce").dt.tz_localize(None)
    if series_or_val:
        return pd.to_datetime(series_or_val, utc=True, errors="coerce").tz_localize(None)
    return None

