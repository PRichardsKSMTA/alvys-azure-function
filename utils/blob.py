"""Blob storage helpers for weekly JSON files."""

from __future__ import annotations

import os
from pathlib import Path

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient


def upload_weekly_json(scac: str, local_dir: Path) -> None:
    """Upload the week's JSON files for ``scac`` to Azure Blob Storage.

    Parameters
    ----------
    scac:
        Customer SCAC code (case-insensitive).
    local_dir:
        Directory containing the JSON files to upload.

    Environment
    -----------
    ``ALVYS_BLOB_CONN_STR``
        Azure Storage connection string.
    ``ALVYS_BLOB_PATH``
        Name of the target container in the storage account.
    """

    conn_str = os.environ["ALVYS_BLOB_CONN_STR"]
    container_name = os.environ["ALVYS_BLOB_PATH"]

    service = BlobServiceClient.from_connection_string(conn_str)
    container = service.get_container_client(container_name)

    try:
        container.create_container()
    except ResourceExistsError:
        pass

    scac = scac.upper()
    prefix = f"{scac}/"
    archive_prefix = f"{scac}/Archive/"

    # Move any existing blobs into the Archive folder
    for blob in container.list_blobs(name_starts_with=prefix):
        if blob.name.startswith(archive_prefix):
            continue
        src = container.get_blob_client(blob)
        data = src.download_blob().readall()
        dest_name = archive_prefix + blob.name[len(prefix):]
        container.get_blob_client(dest_name).upload_blob(data, overwrite=True)
        src.delete_blob()

    # Upload current week's files
    for path in local_dir.glob("*.json"):
        dest_name = prefix + path.name
        with path.open("rb") as fh:
            container.get_blob_client(dest_name).upload_blob(fh, overwrite=True)
