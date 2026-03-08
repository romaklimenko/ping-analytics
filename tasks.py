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


@task
def gold(ctx):
    """Build gold layer: Kimball star schema with dimensions and facts."""
    db = duckdb.connect(str(DB_PATH))
    db.execute("create schema if not exists gold")

    # dim_page: one row per unique page path
    db.execute("""
        create or replace table gold.dim_page as
        with pages as (
            select distinct
                regexp_replace(url, 'https?://[^/]+', '') as page_path
            from silver.logs
        )
        select
            row_number() over (order by page_path) as page_key,
            page_path,
            case
                when page_path = '/' then 'home'
                when page_path like '/blog/%' then 'blog'
                else split_part(ltrim(page_path, '/'), '/', 1)
            end as section,
            case
                when page_path like '/blog/%'
                then try_cast(split_part(page_path, '/', 3) as integer)
            end as blog_year,
            split_part(rtrim(page_path, '/'), '/', -1) as slug
        from pages
    """)

    # dim_geo: one row per unique city + country + region
    db.execute("""
        create or replace table gold.dim_geo as
        with geos as (
            select
                coalesce(headers."x-vercel-ip-city", 'Unknown') as city,
                coalesce(headers."x-vercel-ip-country-region", 'Unknown') as country_region,
                coalesce(headers."x-vercel-ip-country", 'Unknown') as country_code,
                coalesce(headers."x-vercel-ip-continent", 'Unknown') as continent_code,
                mode(headers."x-vercel-ip-timezone") as timezone,
                mode(try_cast(headers."x-vercel-ip-latitude" as double)) as latitude,
                mode(try_cast(headers."x-vercel-ip-longitude" as double)) as longitude
            from silver.logs
            group by city, country_region, country_code, continent_code
        )
        select
            row_number() over (order by continent_code, country_code, country_region, city) as geo_key,
            *
        from geos
    """)

    # fact_events: one row per event, with session stitching
    db.execute("""
        create or replace table gold.fact_events as
        with events_with_ip as (
            select
                *,
                regexp_replace(url, 'https?://[^/]+', '') as page_path,
                coalesce(headers."x-vercel-ip-city", 'Unknown') as _city,
                coalesce(headers."x-vercel-ip-country-region", 'Unknown') as _country_region,
                coalesce(headers."x-vercel-ip-country", 'Unknown') as _country_code,
                coalesce(headers."x-vercel-ip-continent", 'Unknown') as _continent_code,
                coalesce(headers."x-real-ip", headers."x-forwarded-for") as ip_address,
                cast(serverTimestamp as timestamp) as event_ts
            from silver.logs
        ),
        with_gaps as (
            select
                *,
                epoch(event_ts) - epoch(
                    lag(event_ts) over (partition by ip_address order by event_ts)
                ) as gap_seconds
            from events_with_ip
        ),
        with_session_flag as (
            select
                *,
                case when gap_seconds is null or gap_seconds > 1800 then 1 else 0 end as new_session
            from with_gaps
        ),
        with_session_num as (
            select
                *,
                sum(new_session) over (partition by ip_address order by event_ts rows unbounded preceding) as session_num
            from with_session_flag
        )
        select
            row_number() over (order by w.event_ts) as event_key,
            p.page_key,
            g.geo_key,
            md5(w.ip_address || '::' || cast(w.session_num as varchar)) as session_id,
            w.type as event_type,
            w.referrer,
            w."from" as from_path,
            w."to" as to_path,
            regexp_matches(w.userAgent, 'bot|crawl|spider|slurp|bingbot|googlebot|yandex|baidu|duckduck|semrush|ahref|mj12bot|dotbot|petalbot|bytespider', 'i') as is_bot,
            case
                when regexp_matches(w.userAgent, 'googlebot', 'i') then 'Googlebot'
                when regexp_matches(w.userAgent, 'bingbot', 'i') then 'Bingbot'
                when regexp_matches(w.userAgent, 'yandex', 'i') then 'Yandex'
                when regexp_matches(w.userAgent, 'baidu|bytespider', 'i') then 'Baidu'
                when regexp_matches(w.userAgent, 'semrush', 'i') then 'SEMrush'
                when regexp_matches(w.userAgent, 'ahref', 'i') then 'Ahrefs'
                when regexp_matches(w.userAgent, 'bot|crawl|spider|slurp', 'i') then 'Other Bot'
            end as bot_name,
            w.event_ts as event_timestamp,
            cast(w.event_ts as date) as event_date,
            try_cast(w.timestamp as timestamp) as client_timestamp,
            w.ip_address
        from with_session_num w
        join gold.dim_page p on p.page_path = w.page_path
        join gold.dim_geo g on g.city = w._city
            and g.country_region = w._country_region
            and g.country_code = w._country_code
            and g.continent_code = w._continent_code
    """)

    # fact_sessions: one row per session
    db.execute("""
        create or replace table gold.fact_sessions as
        select
            e.session_id,
            (array_agg(e.geo_key order by e.event_timestamp))[1] as geo_key,
            (array_agg(e.page_key order by e.event_timestamp))[1] as entry_page_key,
            (array_agg(e.page_key order by e.event_timestamp desc))[1] as exit_page_key,
            min(e.event_timestamp) as session_start,
            max(e.event_timestamp) as session_end,
            epoch(max(e.event_timestamp)) - epoch(min(e.event_timestamp)) as duration_seconds,
            count(*) as page_count,
            count(*) = 1 as is_bounce,
            bool_or(e.is_bot) as is_bot
        from gold.fact_events e
        group by e.session_id
    """)

    # Print row counts
    for table in ["dim_page", "dim_geo", "fact_events", "fact_sessions"]:
        count = db.execute(f"select count(*) from gold.{table}").fetchone()[0]
        print(f"gold.{table}: {count} rows")

    db.close()
