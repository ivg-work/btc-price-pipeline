# BTC Price Pipeline

An end-to-end hourly Bitcoin price ELT pipeline built to enterprise standards —
ingesting live market data from the CoinGecko API, orchestrating through Apache
Airflow, storing and transforming in BigQuery, and visualising in Looker Studio.

## Architecture
```
CoinGecko API
     │
     ▼
Apache Airflow (hourly DAG)
     │  - Incremental load logic
     │  - Retry handling (3 retries, exponential backoff)
     │  - SLA monitoring (30-minute SLA)
     │
     ▼
BigQuery (cloud data warehouse)
     │  - bitcoin_prices        (raw hourly records)
     │  - btc_hourly_analytics  (moving averages, % change)
     │  - btc_daily_summary     (OHLC, volatility, record count)
     │  - btc_latest            (current price)
     │  - clean_bitcoin_view    (clean interface layer)
     │
     ▼
Looker Studio (analytics dashboard)
     - Current price scorecard
     - 7-day and 30-day moving averages
     - Daily volatility
     - Price range (high / low / avg)
     - Recent records table
```

## Stack

| Layer         | Tool                  | Purpose                              |
|---------------|-----------------------|--------------------------------------|
| Ingestion     | Python + CoinGecko API| Hourly BTC price extraction          |
| Orchestration | Apache Airflow 2.9.3  | Scheduling, retries, SLA monitoring  |
| Warehouse     | Google BigQuery       | Storage and transformation           |
| Visualisation | Looker Studio         | Analytics dashboards                 |
| CI            | GitHub Actions        | DAG validation and unit tests        |
| Portability   | Docker Compose        | One-command local deployment         |

## Repository Structure
```
btc-price-pipeline/
├── dags/
│   └── btc_price_pipeline.py   # Airflow DAG — hourly ingestion
├── sql/
│   └── views/
│       ├── bitcoin_prices.sql          # Raw table definition
│       ├── btc_hourly_analytics.sql    # Moving averages, % change
│       ├── btc_daily_summary.sql       # OHLC + volatility
│       ├── btc_latest.sql              # Current price view
│       └── clean_bitcoin_view.sql      # Clean interface layer
├── tests/
│   └── test_btc_pipeline.py    # Unit tests for DAG functions
├── docker/
│   └── docker-compose.yml      # One-command local deployment
├── docs/
├── requirements.txt
└── .github/
    └── workflows/
        └── ci.yml              # GitHub Actions CI validation
```

## Pipeline Design

### Incremental load logic
Each DAG run checks whether a record already exists for the current
hour before inserting. Duplicate runs are safely skipped — making the
pipeline idempotent.

### Retry and reliability
The DAG is configured with 3 retries, exponential backoff, and a
30-minute SLA. Failed runs trigger SLA miss callbacks visible in the
Airflow UI.

### Historical backfill
To backfill historical data, enable `catchup=True` in the DAG and set
`start_date` to the desired historical date. The incremental check
ensures no duplicates are created.

## Local Setup

### Prerequisites
- Python 3.12+
- WSL2 (Windows) or Linux/macOS
- GCP service account key with BigQuery Admin role

### Installation
```bash
git clone https://github.com/ivg-work/btc-price-pipeline.git
cd btc-price-pipeline
python3 -m venv airflow_env
source airflow_env/bin/activate
pip install -r requirements.txt
```

### Configure credentials

Place your GCP service account key at:
```
credentials/gcp_key.json
```

Export the environment variable:
```bash
export AIRFLOW_HOME=$(pwd)/airflow
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/credentials/gcp_key.json
```

### Run Airflow
```bash
airflow db init
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin \
  --email admin@example.com
airflow scheduler &
airflow webserver --port 8080
```

Open `http://localhost:8080` and enable the `btc_price_pipeline` DAG.

### Docker deployment (alternative)
```bash
cp credentials/gcp_key.json docker/credentials/gcp_key.json
cd docker
docker-compose up
```

## BigQuery Setup

Run the SQL files in `sql/views/` in BigQuery to create all views:
```bash
bq query --use_legacy_sql=false < sql/views/btc_hourly_analytics.sql
bq query --use_legacy_sql=false < sql/views/btc_daily_summary.sql
bq query --use_legacy_sql=false < sql/views/btc_latest.sql
bq query --use_legacy_sql=false < sql/views/clean_bitcoin_view.sql
```

## Running Tests
```bash
pytest tests/ -v
```

## CI

Every push to `main` automatically:
- Validates DAG Python syntax
- Runs unit tests
- Validates docker-compose configuration
- Confirms all SQL files are present

## Data Source

Bitcoin price data is sourced from the
[CoinGecko API](https://www.coingecko.com/en/api) — free tier,
no API key required for basic endpoints.

## License

MIT
