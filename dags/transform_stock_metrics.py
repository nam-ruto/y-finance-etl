# dags/transform_stock_metrics.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ------------------------------------------------------------------ #
# Task 1 — read raw data (last 35 days to have enough for MA30)       #
# ------------------------------------------------------------------ #
def get_raw_data(**context):
    hook = PostgresHook(postgres_conn_id="postgres_etl")

    rows = hook.get_records("""
        SELECT ticker, trade_date, open, high, low, close, volume
        FROM raw.stock_prices
        WHERE is_valid = TRUE
          AND trade_date >= CURRENT_DATE - INTERVAL '35 days'
        ORDER BY ticker, trade_date ASC
    """)

    # Group by ticker
    data = {}
    for ticker, trade_date, open_, high, low, close, volume in rows:
        if ticker not in data:
            data[ticker] = []
        data[ticker].append({
            "trade_date": str(trade_date),
            "open":   float(open_)  if open_  else None,
            "high":   float(high)   if high   else None,
            "low":    float(low)    if low    else None,
            "close":  float(close)  if close  else None,
            "volume": int(volume)   if volume else None,
        })

    total_rows = sum(len(v) for v in data.values())
    logger.info(f"Loaded {total_rows} raw rows for {len(data)} tickers")
    context["ti"].xcom_push(key="raw_data", value=data)


# ------------------------------------------------------------------ #
# Task 2 — compute moving averages and daily returns                  #
# ------------------------------------------------------------------ #
def compute_metrics(**context):
    data = context["ti"].xcom_pull(key="raw_data", task_ids="get_raw_data")
    results = []

    for ticker, rows in data.items():
        closes = [r["close"] for r in rows if r["close"] is not None]
        dates  = [r["trade_date"] for r in rows if r["close"] is not None]

        for i, row in enumerate(rows):
            if row["close"] is None:
                continue

            # Moving averages — only calculate if enough history exists
            ma_7  = round(sum(closes[max(0, i-6):i+1])  / min(i+1, 7),  4) if i >= 0  else None
            ma_30 = round(sum(closes[max(0, i-29):i+1]) / min(i+1, 30), 4) if i >= 0  else None

            # Daily return = (close - prev_close) / prev_close
            if i > 0 and closes[i-1]:
                daily_return = round((closes[i] - closes[i-1]) / closes[i-1], 6)
            else:
                daily_return = None

            results.append({
                "ticker":       ticker,
                "trade_date":   row["trade_date"],
                "close":        row["close"],
                "ma_7":         ma_7,
                "ma_30":        ma_30,
                "daily_return": daily_return,
                "is_anomaly":   False,  # set in next task
            })

    logger.info(f"Computed metrics for {len(results)} rows")
    context["ti"].xcom_push(key="metrics", value=results)


# ------------------------------------------------------------------ #
# Task 3 — flag anomalies (daily return > 3 standard deviations)     #
# ------------------------------------------------------------------ #
def flag_anomalies(**context):
    metrics = context["ti"].xcom_pull(key="metrics", task_ids="compute_metrics")

    # Group returns by ticker to calculate per-ticker std dev
    from collections import defaultdict
    import math

    returns_by_ticker = defaultdict(list)
    for row in metrics:
        if row["daily_return"] is not None:
            returns_by_ticker[row["ticker"]].append(row["daily_return"])

    # Calculate mean and std dev per ticker
    stats = {}
    for ticker, returns in returns_by_ticker.items():
        if len(returns) < 2:
            stats[ticker] = (0, float("inf"))  # not enough data, never flag
            continue
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
        std = math.sqrt(variance)
        stats[ticker] = (mean, std)

    # Flag rows where |daily_return - mean| > 3 * std
    anomaly_count = 0
    for row in metrics:
        if row["daily_return"] is None:
            continue
        mean, std = stats.get(row["ticker"], (0, float("inf")))
        if std > 0 and abs(row["daily_return"] - mean) > 3 * std:
            row["is_anomaly"] = True
            anomaly_count += 1
            logger.warning(
                f"Anomaly: {row['ticker']} on {row['trade_date']} "
                f"return={row['daily_return']:.4f} mean={mean:.4f} std={std:.4f}"
            )

    logger.info(f"Flagged {anomaly_count} anomalies out of {len(metrics)} rows")
    context["ti"].xcom_push(key="flagged_metrics", value=metrics)
    context["ti"].xcom_push(key="anomaly_count",   value=anomaly_count)


# ------------------------------------------------------------------ #
# Task 4 — write to transformed.stock_metrics                         #
# ------------------------------------------------------------------ #
def write_to_transformed(**context):
    metrics = context["ti"].xcom_pull(key="flagged_metrics", task_ids="flag_anomalies")
    hook = PostgresHook(postgres_conn_id="postgres_etl")

    insert_sql = """
        INSERT INTO transformed.stock_metrics
            (ticker, trade_date, close, ma_7, ma_30, daily_return, is_anomaly)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
            close         = EXCLUDED.close,
            ma_7          = EXCLUDED.ma_7,
            ma_30         = EXCLUDED.ma_30,
            daily_return  = EXCLUDED.daily_return,
            is_anomaly    = EXCLUDED.is_anomaly,
            calculated_at = NOW();
    """

    rows_written = 0
    for row in metrics:
        hook.run(insert_sql, parameters=(
            row["ticker"],
            row["trade_date"],
            row["close"],
            row["ma_7"],
            row["ma_30"],
            row["daily_return"],
            row["is_anomaly"],
        ))
        rows_written += 1

    logger.info(f"Wrote {rows_written} rows to transformed.stock_metrics")
    context["ti"].xcom_push(key="rows_written", value=rows_written)


# ------------------------------------------------------------------ #
# DAG definition                                                       #
# ------------------------------------------------------------------ #
with DAG(
    dag_id="transform_stock_metrics",
    description="Compute MA7, MA30, daily return and anomaly flags → transformed.stock_metrics",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 21 * * 1-5",  # 30 min after ingest DAG
    catchup=False,
    tags=["transform", "stocks"],
) as dag:

    t1 = PythonOperator(task_id="get_raw_data",         python_callable=get_raw_data)
    t2 = PythonOperator(task_id="compute_metrics",      python_callable=compute_metrics)
    t3 = PythonOperator(task_id="flag_anomalies",       python_callable=flag_anomalies)
    t4 = PythonOperator(task_id="write_to_transformed", python_callable=write_to_transformed)

    t1 >> t2 >> t3 >> t4