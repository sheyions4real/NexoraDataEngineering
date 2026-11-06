
# Nexora Data Engineering Platform

An enterprise-oriented data engineering platform for ingesting, validating,
and transforming data from NexoraEnterprise into Azure Data Lake Storage Gen2.

## Architecture

- Source: Microsoft SQL Server
- Initial source scope: NexoraEnterprise / Sales schema
- Storage: Azure Data Lake Storage Gen2
- Raw data layer: Bronze
- File format: Apache Parquet
- Language: Python

## Development Setup

### 1. Create virtual environment

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

### 3. Configure environment

Copy `.env.example` to `.env` and supply local configuration.

### 4. Run tests

```powershell
pytest
```

### 5. Check code quality

```powershell
ruff check .
ruff format --check .
```

## Initial Scope

Extract data from the Sales schema in NexoraEnterprise and land
the raw extracts in the Bronze layer of ADLS Gen2.
