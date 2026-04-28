# dags/ingest_stock_prices.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import os
import logging

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY")
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ------------------------------------------------------------------ #
# Task 1 — get active tickers from the watchlist                      #
# ------------------------------------------------------------------ #
def get_tickers(**context):
    hook = PostgresHook(postgres_conn_id="postgres_etl")
    rows = hook.get_records(
        "SELECT ticker FROM raw.tickers WHERE active = TRUE ORDER BY ticker"
    )
    tickers = [row[0] for row in rows]
    logger.info(f"Found {len(tickers)} active tickers: {tickers}")
    context["ti"].xcom_push(key="tickers", value=tickers)


# ------------------------------------------------------------------ #
# Task 2 — fetch OHLCV from Alpha Vantage                             #
# ------------------------------------------------------------------ #
def fetch_prices(**context):
    tickers = context["ti"].xcom_pull(key="tickers", task_ids="get_tickers")
    conf = context.get("dag_run").conf or {}
    fetch_date = conf.get("fetch_date") or context["ds"]

    logger.info(f"Fetching prices for date: {fetch_date}")

    all_records = []
    failed_tickers = []

    for ticker in tickers:
        try:
            resp = requests.get(
                ALPHA_VANTAGE_URL,
                params={
                    "function":   "TIME_SERIES_DAILY",
                    "symbol":     ticker,
                    "outputsize": "compact",   # last 100 trading days
                    "apikey":     ALPHA_VANTAGE_KEY,
                },
                timeout=15,
            )
            resp.raise_for_status()
            payload = resp.json()

            if "Time Series (Daily)" not in payload:
                logger.warning(f"No time series data for {ticker}: {payload.get('Note') or payload.get('Information') or payload}")
                failed_tickers.append(ticker)
                continue

            series = payload["Time Series (Daily)"]

            if fetch_date not in series:
                logger.warning(f"No data for {ticker} on {fetch_date} (market closed or future date)")
                failed_tickers.append(ticker)
                continue

            day = series[fetch_date]
            all_records.append({
                "ticker":     ticker,
                "trade_date": fetch_date,
                "open":       float(day["1. open"]),
                "high":       float(day["2. high"]),
                "low":        float(day["3. low"]),
                "close":      float(day["4. close"]),
                "volume":     int(day["5. volume"]),
            })
            logger.info(f"Fetched {ticker}: close={day['4. close']}")

        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")
            failed_tickers.append(ticker)

    logger.info(f"Fetched {len(all_records)} records. Failed: {failed_tickers}")
    context["ti"].xcom_push(key="raw_records",    value=all_records)
    context["ti"].xcom_push(key="failed_tickers", value=failed_tickers)


# ------------------------------------------------------------------ #
# Task 3 — validate                                                    #
# ------------------------------------------------------------------ #
def validate_data(**context):
    records = context["ti"].xcom_pull(key="raw_records", task_ids="fetch_prices")
    valid, invalid = [], []

    for rec in records:
        issues = []
        for field in ["open", "high", "low", "close"]:
            if rec.get(field) is None or rec[field] <= 0:
                issues.append(f"{field} invalid ({rec.get(field)})")
        if rec.get("volume", -1) < 0:
            issues.append("negative volume")
        if rec["high"] < rec["low"]:
            issues.append("high < low")
        if rec["high"] < rec["open"] or rec["high"] < rec["close"]:
            issues.append("high < open or close")

        rec["is_valid"] = len(issues) == 0
        if issues:
            logger.warning(f"{rec['ticker']} failed validation: {issues}")

        (valid if rec["is_valid"] else invalid).append(rec)

    logger.info(f"Validation: {len(valid)} valid, {len(invalid)} invalid")
    context["ti"].xcom_push(key="validated_records", value=records)
    context["ti"].xcom_push(key="valid_count",        value=len(valid))
    context["ti"].xcom_push(key="invalid_count",      value=len(invalid))


# ------------------------------------------------------------------ #
# Task 4 — write to raw.stock_prices                                  #
# ------------------------------------------------------------------ #
def write_to_raw(**context):
    records = context["ti"].xcom_pull(key="validated_records", task_ids="validate_data")
    hook = PostgresHook(postgres_conn_id="postgres_etl")

    insert_sql = """
        INSERT INTO raw.stock_prices
            (ticker, trade_date, open, high, low, close, volume, is_valid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
            open        = EXCLUDED.open,
            high        = EXCLUDED.high,
            low         = EXCLUDED.low,
            close       = EXCLUDED.close,
            volume      = EXCLUDED.volume,
            is_valid    = EXCLUDED.is_valid,
            ingested_at = NOW();
    """

    rows_written = 0
    for rec in records:
        hook.run(insert_sql, parameters=(
            rec["ticker"], rec["trade_date"],
            rec["open"],   rec["high"],
            rec["low"],    rec["close"],
            rec["volume"], rec["is_valid"],
        ))
        rows_written += 1

    logger.info(f"Wrote {rows_written} rows to raw.stock_prices")
    context["ti"].xcom_push(key="rows_written", value=rows_written)


# ------------------------------------------------------------------ #
# Task 5 — log pipeline run                                           #
# ------------------------------------------------------------------ #
def log_pipeline_run(**context):
    hook = PostgresHook(postgres_conn_id="postgres_etl")
    ti = context["ti"]
    rows_written  = ti.xcom_pull(key="rows_written",  task_ids="write_to_raw")
    valid_count   = ti.xcom_pull(key="valid_count",   task_ids="validate_data")
    invalid_count = ti.xcom_pull(key="invalid_count", task_ids="validate_data")

    hook.run("""
        INSERT INTO raw.pipeline_runs (dag_id, run_id, rows_ingested, status, finished_at)
        VALUES (%s, %s, %s, 'success', NOW())
        ON CONFLICT (dag_id, run_id) DO UPDATE SET
            rows_ingested = EXCLUDED.rows_ingested,
            status        = 'success',
            finished_at   = NOW();
    """, parameters=(context["dag"].dag_id, context["run_id"], rows_written or 0))

    logger.info(f"Run logged — rows: {rows_written}, valid: {valid_count}, invalid: {invalid_count}")


# ------------------------------------------------------------------ #
# DAG definition                                                       #
# ------------------------------------------------------------------ #
with DAG(
    dag_id="ingest_stock_prices",
    description="Fetch daily OHLCV from Alpha Vantage → raw.stock_prices",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 21 * * 1-5",
    catchup=False,
    tags=["ingestion", "stocks"],
) as dag:

    t1 = PythonOperator(task_id="get_tickers",   python_callable=get_tickers)
    t2 = PythonOperator(task_id="fetch_prices",  python_callable=fetch_prices)
    t3 = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t4 = PythonOperator(task_id="write_to_raw",  python_callable=write_to_raw)
    t5 = PythonOperator(task_id="log_run",       python_callable=log_pipeline_run)

    t1 >> t2 >> t3 >> t4 >> t5