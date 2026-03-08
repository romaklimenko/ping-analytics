import os
from datetime import date
from pathlib import Path

import duckdb
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from invoke import task

load_dotenv()

LANDING_CONTAINER = "logs-landing"
ARCHIVE_CONTAINER = "logs-landing-archive"
DATA_DIR = Path("data")
DB_PATH = Path("db/ping.duckdb")


def _blob_client() -> BlobServiceClient:
    return BlobServiceClient(
        account_url=f"https://{os.environ['AZURE_STORAGE_ACCOUNT_NAME']}.blob.core.windows.net",
        credential=os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
    )


@task
def landing(ctx):
    """Archive logs from landing and download all JSONL files."""
    client = _blob_client()
    landing_c = client.get_container_client(LANDING_CONTAINER)
    archive_c = client.get_container_client(ARCHIVE_CONTAINER)
    today_filename = f"{date.today()}.jsonl"

    # Move completed days to archive; copy (keep) today's file
    for blob in landing_c.list_blobs():
        src_url = landing_c.get_blob_client(blob.name).url
        dest_blob = archive_c.get_blob_client(blob.name)

        if blob.name == today_filename:
            dest_blob.start_copy_from_url(src_url)
            print(f"Copied (kept in landing): {blob.name}")
        else:
            dest_blob.start_copy_from_url(src_url)
            landing_c.delete_blob(blob.name)
            print(f"Moved to archive: {blob.name}")

    # Download all archive files
    DATA_DIR.mkdir(exist_ok=True)
    for blob in archive_c.list_blobs():
        dest = DATA_DIR / blob.name
        dest.write_bytes(archive_c.download_blob(blob.name).readall())
        print(f"Downloaded (archive): {blob.name}")

    # Overwrite today's file with latest from landing (has all visits)
    landing_blob = landing_c.get_blob_client(today_filename)
    if landing_blob.exists():
        (DATA_DIR / today_filename).write_bytes(landing_blob.download_blob().readall())
        print(f"Downloaded (landing, latest): {today_filename}")


@task
def bronze(ctx):
    """Load JSONL files from data/ into the bronze layer in DuckDB."""
    DB_PATH.parent.mkdir(exist_ok=True)
    db = duckdb.connect(str(DB_PATH))

    db.execute("create schema if not exists bronze")
    db.execute(r"""
        create or replace table bronze.logs as
        select
            *,
            regexp_extract(filename, '(\d{4}-\d{2}-\d{2})\.jsonl$', 1)
                as _source_file_date,
            filename as _source_file
        from read_json(
            'data/*.jsonl',
            filename = true,
            union_by_name = true
        )
    """)

    count = db.execute("select count(*) from bronze.logs").fetchone()[0]
    print(f"Bronze layer loaded: {count} rows")
    db.close()


@task
def silver(ctx):
    """Build silver layer: filter out ignored pins and non-klimenko.dk domains."""
    pin_to_ignore = os.environ.get("PIN_TO_IGNORE")
    db = duckdb.connect(str(DB_PATH))

    db.execute("create schema if not exists silver")
    db.execute("""
        create or replace table silver.logs as
        select *
        from bronze.logs
        where regexp_extract(url, 'https?://([^/]+)', 1) like '%klimenko.dk'
          and (pin is null or pin != $pin_to_ignore)
    """, {"pin_to_ignore": pin_to_ignore})

    count = db.execute("select count(*) from silver.logs").fetchone()[0]
    print(f"Silver layer loaded: {count} rows")
    db.close()
