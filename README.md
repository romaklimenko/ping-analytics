# Ping Analytics

Processes website visitor logs from [klimenko.dk](https://klimenko.dk). Logs are downloaded from Azure Blob Storage, transformed in DuckDB using Kimball dimensional modeling, exported to Parquet files, and visualized in Power BI.

## Tech Stack

- **Language:** Python (uv, Pydantic)
- **Storage:** Azure Blob Storage
- **Processing:** DuckDB
- **Output:** Parquet files
- **Visualization:** Power BI

## Tasks

Run tasks with `uv run invoke <task>`:

| Task | Description |
|------|-------------|
| `landing` | Archive logs from Azure and download JSONL files |
| `bronze` | Load JSONL files into DuckDB bronze layer |
| `silver` | Filter to klimenko.dk visits, exclude ignored pins |
| `gold` | Build Kimball star schema (dim_page, dim_geo, fact_events, fact_sessions) |

## Setup

1. Install dependencies: `uv sync`
2. Copy `.env.example` to `.env` and fill in Azure credentials
3. Run the pipeline: `uv run invoke landing bronze silver gold`
