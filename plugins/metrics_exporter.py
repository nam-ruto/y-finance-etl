# plugins/metrics_exporter.py

import time
import os
import psycopg2
from prometheus_client import start_http_server, Gauge, Counter

# ------------------------------------------------------------------ #
# Metric definitions                                                   #
# ------------------------------------------------------------------ #

# Total DAG runs by status (success / failed)
dag_runs_total = Gauge(
    "etl_dag_runs_total",
    "Total number of DAG runs",
    ["dag_id", "status"]
)

# Rows ingested in the most recent run per DAG
rows_ingested_last_run = Gauge(
    "etl_rows_ingested_last_run",
    "Rows ingested in the most recent successful run",
    ["dag_id"]
)

# Unix timestamp of the last successful run per DAG
last_successful_run_timestamp = Gauge(
    "etl_last_successful_run_timestamp",
    "Unix timestamp of the last successful DAG run",
    ["dag_id"]
)

# Success rate over last 7 days (0.0 to 1.0)
dag_success_rate_7d = Gauge(
    "etl_dag_success_rate_7d",
    "DAG success rate over the last 7 days",
    ["dag_id"]
)

# Total rows in raw.stock_prices
raw_rows_total = Gauge(
    "etl_raw_stock_prices_total",
    "Total rows in raw.stock_prices"
)

# Total rows in transformed.stock_metrics
transformed_rows_total = Gauge(
    "etl_transformed_stock_metrics_total",
    "Total rows in transformed.stock_metrics"
)

# Anomalies flagged in last 7 days
anomalies_7d = Gauge(
    "etl_anomalies_flagged_7d",
    "Anomaly rows flagged in transformed.stock_metrics in last 7 days"
)


# ------------------------------------------------------------------ #
# DB connection                                                        #
# ------------------------------------------------------------------ #
def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "airflow"),
        user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
        port=5432,
        connect_timeout=5,
    )


# ------------------------------------------------------------------ #
# Metric collection                                                    #
# ------------------------------------------------------------------ #
def collect_metrics():
    try:
        conn = get_connection()
        cur = conn.cursor()

        # DAG run counts by status
        cur.execute("""
            SELECT dag_id, status, COUNT(*)
            FROM raw.pipeline_runs
            GROUP BY dag_id, status
        """)
        for dag_id, status, count in cur.fetchall():
            dag_runs_total.labels(dag_id=dag_id, status=status).set(count)

        # Rows ingested in most recent successful run
        cur.execute("""
            SELECT DISTINCT ON (dag_id)
                dag_id, rows_ingested, finished_at
            FROM raw.pipeline_runs
            WHERE status = 'success'
            ORDER BY dag_id, finished_at DESC
        """)
        for dag_id, rows, finished_at in cur.fetchall():
            rows_ingested_last_run.labels(dag_id=dag_id).set(rows or 0)
            if finished_at:
                last_successful_run_timestamp.labels(dag_id=dag_id).set(
                    finished_at.timestamp()
                )

        # Success rate over last 7 days
        cur.execute("""
            SELECT
                dag_id,
                COUNT(*) FILTER (WHERE status = 'success') AS successes,
                COUNT(*) AS total
            FROM raw.pipeline_runs
            WHERE started_at >= NOW() - INTERVAL '7 days'
            GROUP BY dag_id
        """)
        for dag_id, successes, total in cur.fetchall():
            rate = round(successes / total, 4) if total > 0 else 0
            dag_success_rate_7d.labels(dag_id=dag_id).set(rate)

        # Raw table row count
        cur.execute("SELECT COUNT(*) FROM raw.stock_prices")
        raw_rows_total.set(cur.fetchone()[0])

        # Transformed table row count
        cur.execute("SELECT COUNT(*) FROM transformed.stock_metrics")
        transformed_rows_total.set(cur.fetchone()[0])

        # Anomalies in last 7 days
        cur.execute("""
            SELECT COUNT(*) FROM transformed.stock_metrics
            WHERE is_anomaly = TRUE
              AND calculated_at >= NOW() - INTERVAL '7 days'
        """)
        anomalies_7d.set(cur.fetchone()[0])

        cur.close()
        conn.close()

    except Exception as e:
        print(f"[exporter] Error collecting metrics: {e}")


# ------------------------------------------------------------------ #
# Main loop                                                            #
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    port = int(os.environ.get("EXPORTER_PORT", 8000))
    scrape_interval = int(os.environ.get("SCRAPE_INTERVAL", 15))

    print(f"[exporter] Starting metrics server on port {port}")
    start_http_server(port)

    while True:
        collect_metrics()
        time.sleep(scrape_interval)