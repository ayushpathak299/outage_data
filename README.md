# Outage Data ETL

This repository contains ETL scripts to fetch outage data from Site24x7 API and store it in a PostgreSQL database.

## Files

- `daily_etl.py` - Daily ETL script for previous day's data
- `main_bulk_etl.py` - Bulk ETL script for historical data ranges
- `.github/workflows/daily-etl.yml` - GitHub Actions workflow for daily automation

## Setup GitHub Actions

### 1. Add Repository Secrets

Go to your repository → Settings → Secrets and variables → Actions, and add these secrets:

**Database Secrets:**
- `DB_HOST` - Database host
- `DB_NAME` - Database name  
- `DB_USER` - Database username
- `DB_PASSWORD` - Database password
- `DB_PORT` - Database port (usually 5432)

**Zoho API Secrets:**
- `ZOHO_CLIENT_ID` - Your Zoho client ID
- `ZOHO_CLIENT_SECRET` - Your Zoho client secret  
- `ZOHO_REFRESH_TOKEN` - Your Zoho refresh token

### 2. Schedule

The workflow runs automatically daily at 6:00 AM UTC. It processes the previous day's outage data.

### 3. Manual Trigger

You can manually trigger the workflow from GitHub Actions tab:
- Go to Actions → Daily Outage Data ETL → Run workflow
- Optionally specify a custom date (YYYY-MM-DD format)

## Local Usage

### Daily ETL
```bash
# Run for yesterday (default)
python daily_etl.py

# Run for specific date
python daily_etl.py 2025-10-22
```

### Bulk ETL
```bash
# Run for date range
python main_bulk_etl.py
```

## Environment Variables

Set these environment variables for local development:

```bash
export DB_HOST="your-db-host"
export DB_NAME="your-db-name"
export DB_USER="your-db-user"
export DB_PASSWORD="your-db-password"
export DB_PORT="5432"

export ZOHO_CLIENT_ID="your-client-id"
export ZOHO_CLIENT_SECRET="your-client-secret"
export ZOHO_REFRESH_TOKEN="your-refresh-token"
```
