from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import requests
import logging
from google.cloud import bigquery

# ── Constants ────────────────────────────────────────────────
PROJECT_ID  = "crypto-pipeline-486808"
DATASET_ID  = "crypto_pipeline_486808_crypto_warehouse"
TABLE_ID    = "bitcoin_prices"
FULL_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=bitcoin&vs_currencies=usd"
)

# ── Default args (retry + SLA behaviour) ─────────────────────
default_args = {
    "owner": "vghodke",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "sla": timedelta(minutes=30),
}

# ── Helper: fetch BTC price from CoinGecko ───────────────────
def fetch_btc_price() -> float:
    logging.info("Fetching BTC price from CoinGecko...")
    response = requests.get(COINGECKO_URL, timeout=10)
    response.raise_for_status()
    data = response.json()
    price = data["bitcoin"]["usd"]
    logging.info(f"BTC price fetched: ${price}")
    return price

# ── Helper: check if record already exists (incremental) ─────
def record_exists(client: bigquery.Client, ingested_at: datetime) -> bool:
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{FULL_TABLE}`
        WHERE ingested_at = @ingested_at
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "ingested_at", "TIMESTAMP", ingested_at
            )
        ]
    )
    result = client.query(query, job_config=job_config).result()
    for row in result:
        return row.cnt > 0
    return False

# ── Main task function ────────────────────────────────────────
def ingest_btc_price(**context):
    # Round execution time to the hour for clean incremental key
    execution_dt = context["data_interval_start"].replace(
        minute=0, second=0, microsecond=0
    )
    logging.info(f"Starting ingestion for slot: {execution_dt}")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Incremental load: skip if this hour already exists
    if record_exists(client, execution_dt):
        logging.info(f"Record for {execution_dt} already exists. Skipping.")
        return
        
    price = fetch_btc_price()
    
    # Package the row as a list of Python dictionaries
    rows_to_insert = [{
        "ingested_at": execution_dt.isoformat(),
        "coin_id": "bitcoin",
        "price_usd": price,
        "source": "coingecko",
    }]
    
    # Configure the load job to append to the table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    try:
        # Run a batch load job (Allowed in the Free Tier/Sandbox)
        job = client.load_table_from_json(
            rows_to_insert, 
            FULL_TABLE, 
            job_config=job_config
        )
        job.result()  # Wait for the job to complete
        
        logging.info(f"Successfully loaded BTC price ${price} for {execution_dt}")
    except Exception as e:
        raise RuntimeError(f"BigQuery load failed for {execution_dt}: {e}")

# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id="btc_price_pipeline",
    description="Hourly BTC price ingestion from CoinGecko to BigQuery",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["bitcoin", "coingecko", "bigquery", "crypto"],
) as dag:
    
    ingest_task = PythonOperator(
        task_id="ingest_btc_price",
        python_callable=ingest_btc_price,
        sla=timedelta(minutes=30),
    )
