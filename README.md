Project structure
```
etl-pipeline/
├── docker-compose.yml
├── dags/
│   ├── ingest_stock_prices.py     # Daily OHLCV ingestion
│   ├── ingest_fundamentals.py     # Weekly earnings/P/E data
│   └── pipeline_health_check.py  # DAG that emits metrics
├── plugins/
│   └── metrics_exporter.py       # Pushes stats to Prometheus
├── sql/
│   └── schema.sql                 # Raw + transformed tables
└── grafana/
    └── dashboards/
        └── pipeline_health.json
```