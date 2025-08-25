"""Blob storage helpers for weekly JSON files."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings


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


def upload_weekly_json(scac: str, local_dir: Path) -> None:
    """
    Upload the week's JSON files for ``scac`` to Azure Blob Storage.

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
    prefix = f"{scac}/"
    archive_prefix = f"{scac}/Archive/"

    # Archive existing blobs under scac/ -> scac/Archive/
    for blob in container.list_blobs(name_starts_with=prefix):
        if blob.name.startswith(archive_prefix):
            continue
        src_client = container.get_blob_client(blob)
        data = src_client.download_blob().readall()
        dest_name = archive_prefix + blob.name[len(prefix):]
        dest_client = container.get_blob_client(dest_name)
        dest_client.upload_blob(data, overwrite=True)
        src_client.delete_blob()

    # Upload current week's files with JSON content type
    content_settings = ContentSettings(content_type="application/json")
    any_file = False
    for path in _iter_json_files(local_dir):
        any_file = True
        dest_name = prefix + path.name
        with path.open("rb") as fh:
            container.get_blob_client(dest_name).upload_blob(
                fh,
                overwrite=True,
                content_settings=content_settings,
            )
    if not any_file:
        # No files is considered a hard error to avoid silently doing nothing.
        raise FileNotFoundError(f"No *.json files found in {local_dir}")
