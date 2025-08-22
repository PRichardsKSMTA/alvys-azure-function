#!/usr/bin/env python
"""Alvys multi-tenant ingestion CLI
==================================
Exports last-week data from the Alvys API, inserts JSON into SQL Server, or
runs both steps in sequence - all keyed by client SCAC (each SCAC == DB
schema).

Key design points
-----------------
* **Argparse sub-commands** - `export`, `insert`, `export-insert`.
* **Credential lookup** - `config.get_credentials(scac)` pulls OAuth creds
  from the DB. Alternatively, set `ALVYS_TENANT_ID`, `ALVYS_CLIENT_ID`,
  `ALVYS_CLIENT_SECRET`, and `ALVYS_GRANT_TYPE` to bypass the DB lookup.
* **Single loader for active entities** - `active_entities_insert.py`
  handles drivers, trucks, trailers, customers, and carriers.
* **Per-entity loaders** - `inserts.<entity>_insert` for loads, trips,
  invoices.
* **Dry-run mode** - `--dry-run` prints what *would* happen without hitting
  network or DB.
"""
from __future__ import annotations

import argparse
import importlib
import sys
from dotenv import load_dotenv  # type: ignore
from pathlib import Path
from typing import Iterable, List


load_dotenv()
# --------------------------------------------
# PATH & TOP-LEVEL IMPORTS
# --------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:  # ensure project root is importable
    sys.path.insert(0, str(ROOT_DIR))

from utils.dates import get_last_week_range  # noqa: E402  - after sys.path tweak
from config import get_credentials            # noqa: E402
import inserts.active_entities_insert as aei          # noqa: E402

# Default output folder shared by export & insert steps
DATA_DIR = ROOT_DIR / "alvys_weekly_data"

# Entities supported by both API export **and** DB insert
ENTITIES = [
    "loads",
    "trips",
    "invoices",
    "drivers",
    "trucks",
    "trailers",
    "customers",
    "carriers",
]

# Shorthand - the five entity types handled by *active_entities_insert.py*
ACTIVE_ENTS = {"drivers", "trucks", "trailers", "customers", "carriers"}

# --------------------------------------------
# ARGPARSE
# --------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("Alvys ingestion CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    # export ---------------------------------------------------------------
    exp = sub.add_parser("export", help="Export JSON from Alvys API")
    exp.add_argument("entities", nargs="*", default=["all"], choices=ENTITIES + ["all"],
                     help="Which entities to export (default: all)")
    exp.add_argument("--scac", required=True, help="Client SCAC - resolves creds & schema")
    exp.add_argument("--weeks-ago", type=int, default=0,
                     help="How many weeks back to pull (0 = last full week)")
    exp.add_argument("--dry-run", action="store_true", help="Skip network + file writes")

    # insert ---------------------------------------------------------------
    ins = sub.add_parser("insert", help="Insert JSON into SQL Server")
    ins.add_argument("entities", nargs="*", default=["all"], choices=ENTITIES + ["all"],
                     help="Which entities to insert (default: all)")
    ins.add_argument("--scac", required=True, help="Target DB schema (same as SCAC)")
    ins.add_argument("--dry-run", action="store_true", help="Skip DB writes")

    # export-insert --------------------------------------------------------
    ei = sub.add_parser("export-insert", help="Run export *then* insert")
    ei.add_argument("entities", nargs="*", default=["all"], choices=ENTITIES + ["all"],
                    help="Which entities (default: all)")
    ei.add_argument("--scac", required=True)
    ei.add_argument("--weeks-ago", type=int, default=0)
    ei.add_argument("--dry-run", action="store_true")

    return p

# --------------------------------------------
# HELPERS
# --------------------------------------------

def normalise(entity_args: Iterable[str]) -> List[str]:
    """Expand ["all"] or mixed list into canonical entity order."""
    ent_set = set(entity_args)
    if not ent_set or ent_set == {"all"}:
        return ENTITIES
    return [e for e in ENTITIES if e in ent_set]

# --------------------------------------------
# EXPORT LOGIC
# --------------------------------------------

def run_export(
    scac: str,
    entities: List[str],
    weeks_ago: int,
    dry_run: bool,
    output_dir: Path | None = None,
) -> None:
    start, end = get_last_week_range(weeks_ago)
    print(start)
    print(end)
    creds = get_credentials(scac)

    out_dir = output_dir or DATA_DIR / scac.upper()

    if dry_run:
        print(
            "[DRY-RUN] Would export:",
            entities,
            "for",
            scac,
            "range",
            start,
            "->",
            end,
            "into",
            out_dir,
        )
        return

    # Heavy dependency import only when needed
    from alvys_export import export_endpoints  # type: ignore  # noqa: WPS433

    export_endpoints(
        entities=entities,
        credentials=creds,
        date_range=(start, end),
        output_dir=out_dir,
    )

# --------------------------------------------
# INSERT LOGIC
# --------------------------------------------

def run_insert(
    scac: str,
    entities: List[str],
    dry_run: bool,
    data_dir: Path | None = None,
) -> None:
    schema = scac.upper()
    dir_path = Path(data_dir or DATA_DIR / schema)

    if dry_run:
        print("[DRY-RUN] Would insert:", entities, "into schema", schema, "from", dir_path)
        return

    for ent in entities:
        # 1)  Route active entities to active_entities_insert.py
        if ent in ACTIVE_ENTS:
            print(f"-> inserting {ent.upper()} ...")
            aei.main([ent, "--scac", scac], data_dir=dir_path)
            continue

        # 2)  Loads, trips, invoices use their dedicated modules
        mod_name = f"inserts.{ent}_insert"
        try:
            mod = importlib.import_module(mod_name)
        except ModuleNotFoundError as exc:
            sys.exit(f"X Insert module not found: {mod_name} -> {exc}")

        if not hasattr(mod, "main"):
            sys.exit(f"X {mod_name} lacks a main() entry-point")

        print(f"-> inserting {ent.upper()} ...")
        mod.main(["--scac", scac], data_dir=dir_path)

# --------------------------------------------
# MAIN ENTRY-POINT
# --------------------------------------------

def main(argv: List[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    ents = normalise(args.entities)

    if args.cmd == "export":
        out_dir = DATA_DIR / args.scac.upper()
        run_export(args.scac, ents, args.weeks_ago, args.dry_run, out_dir)

    elif args.cmd == "insert":
        run_insert(args.scac, ents, args.dry_run)

    elif args.cmd == "export-insert":
        out_dir = DATA_DIR / args.scac.upper()
        run_export(args.scac, ents, args.weeks_ago, args.dry_run, out_dir)
        # Skip insert if export was dry-run but insert wasn't explicitly dry-run
        run_insert(args.scac, ents, args.dry_run)


if __name__ == "__main__":
    main()
