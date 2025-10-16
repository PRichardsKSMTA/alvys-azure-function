"""Blob storage helpers for weekly JSON files."""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

from azure.core.exceptions import ResourceExistsError # type: ignore
from azure.storage.blob import BlobServiceClient, ContentSettings # type: ignore


# Azure requires: 3–63 chars, lowercase letters/numbers/hyphens, must start/end alphanumeric
_CONTAINER_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,61})[a-z0-9]$")


def _get_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _normalize_and_validate_container(raw: str) -> str:
    s = raw.strip()
    if s.startswith("http://") or s.startswith("https://"):
        raise ValueError(
            "ALVYS_BLOB_CONTAINER must be a container name (e.g., 'alvys-weekly-data'), not a full URL."
        )
    s = s.lower()
    if not _CONTAINER_RE.match(s):
        raise ValueError(
            "Invalid container name. It must be 3–63 chars, lowercase letters, numbers, and hyphens, and start/end with alphanumeric."
        )
    return s


def _iter_json_files(directory: Path) -> Iterable[Path]:
    if not directory.exists():
        raise FileNotFoundError(f"Local directory does not exist: {directory}")
    if not directory.is_dir():
        raise NotADirectoryError(f"Local path is not a directory: {directory}")
    return directory.glob("*.json")


def _format_run_folder(run_date: date | None) -> str:
    actual_date = run_date or datetime.now(timezone.utc).date()
    return actual_date.strftime("%Y%m%d")


def _has_nested_path(relative_path: str) -> bool:
    if not relative_path:
        return False
    return "/" in relative_path


def upload_weekly_json(scac: str, local_dir: Path, *, run_date: date | None = None) -> None:
    """
    Upload the week's JSON files for ``scac`` to Azure Blob Storage.

    Files are written beneath ``<scac>/<YYYYMMDD>/`` where the folder name is
    derived from ``run_date`` (or today in UTC when omitted). Legacy flat blobs
    under ``<scac>/`` are moved into ``<scac>/Archive/`` so older runs remain
    accessible.

    Environment variables:
      ALVYS_BLOB_CONN_STR  - Azure Storage connection string
      ALVYS_BLOB_CONTAINER - Container name (e.g., 'alvys-weekly-data')
    """
    conn_str = _get_env("ALVYS_BLOB_CONN_STR")
    raw_container = _get_env("ALVYS_BLOB_CONTAINER")
    container_name = _normalize_and_validate_container(raw_container)

    service = BlobServiceClient.from_connection_string(conn_str)
    container = service.get_container_client(container_name)

    try:
        container.create_container()
    except ResourceExistsError:
        pass

    scac = scac.lower()
    run_folder = _format_run_folder(run_date)
    prefix = f"{scac}/"
    archive_prefix = f"{scac}/Archive/"
    run_prefix = f"{prefix}{run_folder}/"

    # Archive legacy blobs that live directly under scac/ so we can maintain
    # a clean tree of dated folders without losing history.
    for blob in container.list_blobs(name_starts_with=prefix, include=["metadata"]):
        if blob.name.startswith(archive_prefix):
            continue
        if blob.name.endswith("/"):
            # Hierarchical namespace accounts expose directory placeholders that
            # cannot be deleted while they still contain children. We only care
            # about actual blobs, so skip the virtual directory entries.
            continue
        if getattr(blob, "metadata", {}).get("hdi_isfolder") == "true":
            # Azure Data Lake Gen2 surfaces folders with metadata flag. Skip
            # them so we don't attempt to delete a non-empty directory.
            continue
        relative_name = blob.name[len(prefix):]
        if not relative_name:
            # Ignore empty names (shouldn't happen, but defensive)
            continue
        if _has_nested_path(relative_name):
            # Anything already under a dated (or other nested) directory should
            # stay in place. Only the legacy flat structure needs archiving.
            continue
        src_client = container.get_blob_client(blob)
        data = src_client.download_blob().readall()
        dest_name = archive_prefix + relative_name
        dest_client = container.get_blob_client(dest_name)
        dest_client.upload_blob(data, overwrite=True)
        try:
            src_client.delete_blob()
        except ResourceExistsError as exc:  # pragma: no cover - safety net
            if getattr(exc, "error_code", None) == "DirectoryIsNotEmpty":
                # Another client may have created children after we listed, in
                # which case the directory delete would fail. Leave it in place
                # so the orchestration can continue uploading the new blobs.
                continue
            raise

    # Upload current week's files with JSON content type
    content_settings = ContentSettings(content_type="application/json")
    any_file = False
    for path in _iter_json_files(local_dir):
        any_file = True
        dest_name = run_prefix + path.name
        with path.open("rb") as fh:
            blob_client = container.get_blob_client(dest_name)
            try:
                blob_client.upload_blob(
                    fh,
                    overwrite=False,
                    content_settings=content_settings,
                )
            except ResourceExistsError as exc:
                raise FileExistsError(
                    f"Blob already exists for this run: {dest_name}"
                ) from exc
    if not any_file:
        # No files is considered a hard error to avoid silently doing nothing.
        raise FileNotFoundError(f"No *.json files found in {local_dir}")
