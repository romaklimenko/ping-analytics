import os
from datetime import date

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

LANDING = "logs-landing"
ARCHIVE = "logs-landing-archive"
DATA_DIR = "data"


def get_blob_service_client() -> BlobServiceClient:
    account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
    return BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )


def archive_logs(client: BlobServiceClient) -> None:
    """Move completed day files from landing to archive.

    Today's file is only copied (not deleted) since new visits may still arrive.
    Older files are moved (copied then deleted).
    """
    landing = client.get_container_client(LANDING)
    archive = client.get_container_client(ARCHIVE)
    today_filename = f"{date.today()}.jsonl"

    for blob in landing.list_blobs():
        src_url = landing.get_blob_client(blob.name).url
        dest_blob = archive.get_blob_client(blob.name)

        if blob.name == today_filename:
            dest_blob.start_copy_from_url(src_url)
            print(f"Copied (kept in landing): {blob.name}")
        else:
            dest_blob.start_copy_from_url(src_url)
            landing.delete_blob(blob.name)
            print(f"Moved to archive: {blob.name}")


def download_logs(client: BlobServiceClient) -> None:
    """Download all archived logs + today's landing file into data/."""
    os.makedirs(DATA_DIR, exist_ok=True)
    archive = client.get_container_client(ARCHIVE)
    landing = client.get_container_client(LANDING)
    today_filename = f"{date.today()}.jsonl"

    for blob in archive.list_blobs():
        dest = os.path.join(DATA_DIR, blob.name)
        with open(dest, "wb") as f:
            f.write(archive.download_blob(blob.name).readall())
        print(f"Downloaded (archive): {blob.name}")

    # Overwrite today's file with the latest from landing (has all visits)
    landing_blob = landing.get_blob_client(today_filename)
    if landing_blob.exists():
        dest = os.path.join(DATA_DIR, today_filename)
        with open(dest, "wb") as f:
            f.write(landing_blob.download_blob().readall())
        print(f"Downloaded (landing, latest): {today_filename}")


if __name__ == "__main__":
    client = get_blob_service_client()
    archive_logs(client)
    download_logs(client)
