-- sql/schema.sql

-- ============================================================
-- Schemas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS transformed;

-- ============================================================
-- raw.stock_prices
-- Stores exactly what comes back from yfinance. Never modified.
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.stock_prices (
    id            SERIAL PRIMARY KEY,
    ticker        VARCHAR(10)    NOT NULL,
    trade_date    DATE           NOT NULL,
    open          NUMERIC(12, 4),
    high          NUMERIC(12, 4),
    low           NUMERIC(12, 4),
    close         NUMERIC(12, 4),
    volume        BIGINT,
    ingested_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    source        VARCHAR(50)    NOT NULL DEFAULT 'yfinance',
    is_valid      BOOLEAN        NOT NULL DEFAULT TRUE,

    CONSTRAINT uq_raw_ticker_date UNIQUE (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_raw_ticker      ON raw.stock_prices (ticker);
CREATE INDEX IF NOT EXISTS idx_raw_trade_date  ON raw.stock_prices (trade_date);

-- ============================================================
-- transformed.stock_metrics
-- Calculated metrics written by the transform DAG (Phase 4).
-- ============================================================
CREATE TABLE IF NOT EXISTS transformed.stock_metrics (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10)    NOT NULL,
    trade_date      DATE           NOT NULL,
    close           NUMERIC(12, 4),
    ma_7            NUMERIC(12, 4),   -- 7-day moving average
    ma_30           NUMERIC(12, 4),   -- 30-day moving average
    daily_return    NUMERIC(8, 6),    -- (close - prev_close) / prev_close
    is_anomaly      BOOLEAN        NOT NULL DEFAULT FALSE,
    calculated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_transformed_ticker_date UNIQUE (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_tf_ticker      ON transformed.stock_metrics (ticker);
CREATE INDEX IF NOT EXISTS idx_tf_trade_date  ON transformed.stock_metrics (trade_date);

-- ============================================================
-- pipeline_runs
-- Used by the metrics exporter in Phase 5 to track DAG health.
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    id            SERIAL PRIMARY KEY,
    dag_id        VARCHAR(100)   NOT NULL,
    run_id        VARCHAR(200)   NOT NULL,
    ticker        VARCHAR(10),
    rows_ingested INTEGER        DEFAULT 0,
    status        VARCHAR(20)    NOT NULL DEFAULT 'running',
    started_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    error_message TEXT,

    CONSTRAINT uq_pipeline_run_id UNIQUE (dag_id, run_id)
);

-- Ticker watchlist — edit this to add/remove symbols
CREATE TABLE IF NOT EXISTS raw.tickers (
    ticker       VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100),
    sector       VARCHAR(50),
    active       BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO raw.tickers (ticker, company_name, sector) VALUES
    ('SPY',  'SPDR S&P 500 ETF',       'ETF'),
    ('AAPL', 'Apple Inc.',             'Technology'),
    ('MSFT', 'Microsoft Corporation',  'Technology'),
    ('GOOGL','Alphabet Inc.',          'Technology'),
    ('AMZN', 'Amazon.com Inc.',        'Consumer Discretionary')
ON CONFLICT DO NOTHING;