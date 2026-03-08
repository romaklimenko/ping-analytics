# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ping Analytics processes website visitor logs. Logs are downloaded from Azure Blob Storage, transformed in DuckDB using Kimball dimensional modeling, exported to Parquet files, and visualized in Power BI reports.

## Tech Stack

- **Language:** Python (use uv for package management, Pydantic for data validation)
- **Storage:** Azure Blob Storage (credentials via `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`)
- **Processing:** DuckDB
- **Output:** CSV files (exported from DuckDB gold layer)
- **Visualization:** Power BI
