#!/usr/bin/env python
"""
Alvys weekly exporter - verbose & fault-tolerant
===============================================

Key additions
-------------
* **WEEK_RANGES** fixed to **2025-07-13 -> 2025-07-19**.
* Timestamped `log()` helper for progress visibility.
* `fetch_paginated_data()` now treats an HTTP 404 on a subsequent page as an
  *end-of-data* signal (handles exact-multiple page counts like 400 drivers).
* Per-entity `try/except` so a failure in one endpoint no longer stops the
  whole export.
* Keeps the original standalone-CLI fallback (`python alvys_export.py ...`) for
  parity with the earlier script.

Drop-in replacement; no new dependencies.
"""
from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import requests
from dotenv import load_dotenv  # type: ignore
from config import build_auth_urls

# --------------------------------------------
# ENV & CONSTANTS
# --------------------------------------------
load_dotenv()

TENANT_ID = os.getenv("ALVYS_TENANT_ID")
AUTH_URL = f"https://integrations.alvys.com/api/authentication/{TENANT_ID}/token"
API_VERSION = "1"
BASE_URL = f"https://integrations.alvys.com/api/p/v{API_VERSION}"

CREDENTIALS = {
    "client_id": os.getenv("ALVYS_CLIENT_ID"),
    "client_secret": os.getenv("ALVYS_CLIENT_SECRET"),
    "grant_type": os.getenv("ALVYS_GRANT_TYPE", "client_credentials"),
}

# Pull exactly Sunday 13 Jul -> Saturday 19 Jul 2025 (UTC)
WEEK_RANGES: List[Tuple[str, str]] = [
    # ("2025-01-01T00:00:00.001Z", "2025-01-04T23:59:59.999Z"),
    # ("2025-01-05T00:00:00.001Z", "2025-01-11T23:59:59.999Z"),
    # ("2025-01-12T00:00:00.001Z", "2025-01-18T23:59:59.999Z"),
    # ("2025-01-19T00:00:00.001Z", "2025-01-25T23:59:59.999Z"),
    # ("2025-01-26T00:00:00.001Z", "2025-02-01T23:59:59.999Z"),
    # ("2025-02-02T00:00:00.001Z", "2025-02-08T23:59:59.999Z"),
    # ("2025-02-09T00:00:00.001Z", "2025-02-15T23:59:59.999Z"),
    # ("2025-02-16T00:00:00.001Z", "2025-02-22T23:59:59.999Z"),
    # ("2025-02-23T00:00:00.001Z", "2025-03-01T23:59:59.999Z"),
    # ("2025-03-02T00:00:00.001Z", "2025-03-08T23:59:59.999Z"),
    # ("2025-03-09T00:00:00.001Z", "2025-03-15T23:59:59.999Z"),
    # ("2025-03-16T00:00:00.001Z", "2025-03-22T23:59:59.999Z"),
    # ("2025-03-23T00:00:00.001Z", "2025-03-29T23:59:59.999Z"),
    # ("2025-03-30T00:00:00.001Z", "2025-04-05T23:59:59.999Z"),
    # ("2025-04-06T00:00:00.001Z", "2025-04-12T23:59:59.999Z"),
    # ("2025-04-13T00:00:00.001Z", "2025-04-19T23:59:59.999Z"),
    # ("2025-04-20T00:00:00.001Z", "2025-04-26T23:59:59.999Z"),
    # ("2025-04-27T00:00:00.001Z", "2025-05-03T23:59:59.999Z"),
    # ("2025-05-04T00:00:00.001Z", "2025-05-10T23:59:59.999Z"),
    # ("2025-05-11T00:00:00.001Z", "2025-05-17T23:59:59.999Z"),
    # ("2025-05-18T00:00:00.001Z", "2025-05-24T23:59:59.999Z"),
    # ("2025-05-25T00:00:00.001Z", "2025-05-31T23:59:59.999Z"),
    # ("2025-06-01T00:00:00.001Z", "2025-06-07T23:59:59.999Z"),
    # ("2025-06-08T00:00:00.001Z", "2025-06-14T23:59:59.999Z"),
    # ("2025-06-15T00:00:00.001Z", "2025-06-21T23:59:59.999Z"),
    # ("2025-06-22T00:00:00.001Z", "2025-06-28T23:59:59.999Z"),
    # ("2025-06-29T00:00:00.001Z", "2025-07-05T23:59:59.999Z"),
    # ("2025-07-06T00:00:00.001Z", "2025-07-12T23:59:59.999Z"),
    # ("2025-07-13T00:00:00.001Z", "2025-07-19T23:59:59.999Z"),
    # ("2025-07-20T00:00:00.001Z", "2025-07-26T23:59:59.999Z"),
    # ("2025-07-27T00:00:00.001Z", "2025-08-03T06:00:00.000Z"),
    # ("2025-08-10T00:00:00.001Z", "2025-08-17T06:00:00.000Z")
]

OUTPUT_DIR = "alvys_weekly_data"
PAGE_SIZE = 200

# --------------------------------------------
# LOGGING
# --------------------------------------------
def _now_iso() -> str:  # millisecond-precise UTC timestamp
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(
        timespec="milliseconds"
    )


def log(*msg):
    print(f"[{_now_iso()}]", *msg, flush=True)


# --------------------------------------------
# AUTH
# --------------------------------------------
def get_token() -> str:
    """Fetch OAuth token using the static .env credentials (legacy mode)."""
    resp = requests.post(AUTH_URL, data=CREDENTIALS)
    resp.raise_for_status()
    return resp.json()["access_token"]


# --------------------------------------------
# PAGINATION HELPER - now 404-safe
# --------------------------------------------
def fetch_paginated_data(
    url: str,
    headers: Dict[str, str],
    base_payload: Dict,
    max_items: int | None = None,
    *,
    entity_name: str = "",
) -> List[dict]:
    """
    Return **all** items from a paginated Alvys endpoint.

    Alvys returns HTTP 404 when you request a page beyond the last valid page.
    We treat that 404 as an empty page -> break the loop gracefully.
    """
    page = 0
    items: List[dict] = []

    while True:
        payload = dict(base_payload, page=page, pageSize=PAGE_SIZE)
        log(
            "POST",
            url,
            f"page={page}",
            "... payload keys=" + str(list(payload.keys())),
            f"(entity={entity_name})",
        )

        try:
            resp = requests.post(url, headers=headers, json=payload)
            if resp.status_code == 404:
                log(
                    f"STOP Page {page} returned 404 - assuming no more pages "
                    f"(entity={entity_name})"
                )
                break
            resp.raise_for_status()
        except requests.HTTPError as exc:
            # Allow other HTTP errors to propagate to caller
            if exc.response is not None and exc.response.status_code == 404:
                log(
                    f"STOP Page {page} returned 404 - assuming no more pages "
                    f"(entity={entity_name})"
                )
                break
            raise

        batch = resp.json().get("Items") or resp.json().get("items") or []
        log("-> received", len(batch), "objects")
        if not batch:
            break

        items.extend(batch)
        if len(batch) < PAGE_SIZE or (max_items and len(items) >= max_items):
            break
        page += 1

    return items[:max_items] if max_items else items


# --------------------------------------------
# FILE HELPERS
# --------------------------------------------
def save_json(data: List[dict], filename: str, output_dir: str | Path = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)
    path = Path(output_dir) / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    log("SAVE Wrote", len(data), "records ->", path)


def format_range(start_iso: str, end_iso: str) -> str:
    return f"{start_iso[:10].replace('-', '')}-{end_iso[:10].replace('-', '')}"


def get_file_id() -> str:
    return datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]  # yyyymmddHHMMSSmmm


# --------------------------------------------
# MAIN EXPORTER (used by main.py)
# --------------------------------------------
def export_endpoints(
    entities: Iterable[str],
    credentials: Dict[str, str],
    date_range: Tuple[datetime, datetime],
    output_dir: str | Path,
):
    """Export selected endpoints for *one* week range."""
    urls = build_auth_urls(credentials["tenant_id"], API_VERSION)

    log("Authenticating tenant ->", urls["auth_url"])
    token_resp = requests.post(
        urls["auth_url"],
        data={
            "client_id": credentials["client_id"],
            "client_secret": credentials["client_secret"],
            "grant_type": credentials.get("grant_type", "client_credentials"),
        },
    )
    token_resp.raise_for_status()
    log("OK Token acquired")
    token = token_resp.json()["access_token"]

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/*+json",
    }

    start_iso = (
        date_range[0]
        .astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    end_iso = (
        date_range[1]
        .astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

    # ----- Range-based endpoints -----
    endpoints = {
        "trips": {
            "url": f"{urls['base_url']}/trips/search",
            "extra": {
                "status": [],
                "range_field": "updatedAtRange",
                "IncludeDeleted": True,
            },
        },
        "loads": {
            "url": f"{urls['base_url']}/loads/search",
            "extra": {
                "status": [],
                "range_field": "updatedAtRange",
                "IncludeDeleted": True,
            },
        },
        "invoices": {
            "url": f"{urls['base_url']}/invoices/search",
            "extra": {"status": ["Paid"], "range_field": "invoicedDateRange"},
        },
    }

    for name, cfg in endpoints.items():
        if name not in entities:
            continue
        log("RUN Exporting", name.upper(), "...")
        try:
            rf = cfg["extra"].get("range_field", "updatedAtRange")
            extra = {k: v for k, v in cfg["extra"].items() if k != "range_field"}
            payload = {rf: {"start": start_iso, "end": end_iso}, **extra}
            data = fetch_paginated_data(
                cfg["url"], headers, payload, entity_name=name.upper()
            )
            file_id = get_file_id()
            for rec in data:
                rec["FILE_ID"] = file_id
            fname = f"{name.upper()}_API_{format_range(start_iso, end_iso)}.json"
            save_json(data, fname, output_dir)
            log("OK", name.upper(), "done")
        except Exception as exc:
            log(
                f"!!  Failed to export {name.upper()} -> "
                f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc()}"
            )

    # ----- Non-range endpoints -----
    def do_simple(entity: str, payload: Dict):
        if entity.lower() not in [e.lower() for e in entities]:
            return
        log("RUN Exporting", entity, "...")
        try:
            data = fetch_paginated_data(
                f"{urls['base_url']}/{entity.lower()}/search",
                headers,
                payload,
                entity_name=entity,
            )
            file_id = get_file_id()
            for rec in data:
                rec["FILE_ID"] = file_id
            save_json(data, f"{entity}.json", output_dir)
            log("OK", entity, "done")
        except Exception as exc:
            log(
                f"!!  Failed to export {entity} -> "
                f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc()}"
            )

    do_simple("DRIVERS", {"name": "", "employeeId": "", "fleetName": "", "status": []})
    do_simple(
        "TRUCKS",
        {
            "truckNumber": "",
            "fleetName": "",
            "vinNumber": "",
            "registeredName": "",
            "status": [],
        },
    )
    do_simple(
        "TRAILERS",
        {"status": [], "trailerNumber": "", "fleetName": "", "vinNumber": ""},
    )
    do_simple("CUSTOMERS", {"statuses": ["Active", "Inactive", "Disabled"]})
    do_simple(
        "CARRIERS",
        {
            "status": [
                "Pending",
                "Active",
                "Expired Insurance",
                "Interested",
                "Invited",
                "Packet Sent",
                "Packet Completed",
            ],
        },
    )


# --------------------------------------------
# STANDALONE CLI (optional legacy path)
# --------------------------------------------
def cli_run(argv: List[str]) -> None:
    """
    Allows running *just* this file:

        python alvys_export.py trips loads

    Uses the static .env credentials (no SCAC lookup) and writes into
    `alvys_weekly_data/`.
    """
    args = [a.lower() for a in argv]
    run_all = len(args) == 0

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/*+json",
    }

    endpoints = {
        "trips": {
            "url": f"{BASE_URL}/trips/search",
            "extra_payload": {
                "status": [],
                "range_field": "updatedAtRange",
                "IncludeDeleted": True,
            },
        },
        "loads": {
            "url": f"{BASE_URL}/loads/search",
            "extra_payload": {
                "status": [],
                "range_field": "updatedAtRange",
                "IncludeDeleted": True,
            },
        },
        "invoices": {
            "url": f"{BASE_URL}/invoices/search",
            "extra_payload": {"status": ["Paid"], "range_field": "invoicedDateRange"},
        },
    }

    for (start, end) in WEEK_RANGES:
        log("-> Date range", start, "->", end)
        for name, cfg in endpoints.items():
            if not (run_all or name in args):
                continue

            log("RUN Exporting", name.upper(), "...")
            rf = cfg["extra_payload"].get("range_field", "updatedAtRange")
            extra = {
                k: v for k, v in cfg["extra_payload"].items() if k != "range_field"
            }
            payload = {
                rf: {"start": start, "end": end},
                **extra,
            }

            try:
                data = fetch_paginated_data(
                    cfg["url"],
                    headers,
                    payload,
                    entity_name=name.upper(),
                )
                file_id = get_file_id()
                for rec in data:
                    rec["FILE_ID"] = file_id
                fname = f"{name.upper()}_API_{format_range(start, end)}.json"
                save_json(data, fname)
                log("OK", name.upper(), "done")
            except Exception as exc:
                log(
                    f"!!  Failed to export {name.upper()} -> "
                    f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc()}"
                )

    # Simple (non-range) endpoints
    simple_endpoints = {
        "drivers": {"name": "", "employeeId": "", "fleetName": "", "status": []},
        "trucks": {
            "truckNumber": "",
            "fleetName": "",
            "vinNumber": "",
            "registeredName": "",
            "status": [],
        },
        "trailers": {"status": [], "trailerNumber": "", "fleetName": "", "vinNumber": ""},
        "customers": {"statuses": ["Active", "Inactive", "Disabled"]},
        "carriers": {
            "status": [
                "Pending",
                "Active",
                "Expired Insurance",
                "Interested",
                "Invited",
                "Packet Sent",
                "Packet Completed",
            ]
        },
    }

    for name, payload in simple_endpoints.items():
        if not (run_all or name in args):
            continue
        log("RUN Exporting", name.upper(), "...")
        try:
            data = fetch_paginated_data(
                f"{BASE_URL}/{name}/search",
                headers,
                payload,
                entity_name=name.upper(),
            )
            file_id = get_file_id()
            for rec in data:
                rec["FILE_ID"] = file_id
            save_json(data, f"{name.upper()}.json")
            log("OK", name.upper(), "done")
        except Exception as exc:
            log(
                f"!!  Failed to export {name.upper()} -> "
                f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc()}"
            )


if __name__ == "__main__":
    cli_run(sys.argv[1:])
