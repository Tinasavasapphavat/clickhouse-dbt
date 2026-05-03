# ClickHouse dbt Project

A dbt project for processing and analyzing cryptocurrency trading data from Binance in ClickHouse. This project transforms raw trade data into analytics-ready tables using a three-layer architecture (staging → intermediate → marts).

## Project Overview

This dbt project ingests raw BTCUSDT and ETHUSDT trade data from Binance, cleanses it, and builds aggregated analytics tables optimized for ClickHouse's columnar storage and performance characteristics.

## Architecture

### Data Pipeline Layers

#### 1. **Staging Layer** (Views)
Raw data transformation and cleansing from Binance sources:

- **[stg_btcusdt_trades.sql](clickhouse_dbt/models/staging/stg_btcusdt_trades.sql)** - Pulls raw Bitcoin (BTCUSDT) trades from the Binance source
  - Removes duplicates using `distinct`
  - Calculates `trade_value` (price × quantity)
  - Extracts date and hour components from datetime
  - Configured for incremental loads (only pulls new trades from the previous day)
  - Uses MergeTree engine with monthly partitioning and trade_date ordering

- **[stg_ethusdt_trades.sql](clickhouse_dbt/models/staging/stg_ethusdt_trades.sql)** - Pulls raw Ethereum (ETHUSDT) trades from the Binance source
  - Same transformation logic as Bitcoin staging
  - Handles incremental updates efficiently
  - Maintains data integrity with unique_key constraint

#### 2. **Intermediate Layer** (View)

- **[int_trades.sql](clickhouse_dbt/models/intermediate/int_trades.sql)** - Unified trade data model
  - Combines Bitcoin and Ethereum trades using UNION ALL
  - Adds a standardized `instrument` column ('BTCUSDT' or 'ETHUSDT')
  - Creates unique trade_ids with instrument prefix (e.g., 'btcusdt-12345', 'ethusdt-54321')
  - Converts numeric fields to Float64 for consistency
  - Enables cross-instrument analysis

#### 3. **Marts Layer** (Tables - ClickHouse MergeTree)
Aggregated, analysis-ready tables optimized for reporting:

- **[mart_daily_ohlcv.sql](clickhouse_dbt/models/marts/mart_daily_ohlcv.sql)** - Daily OHLCV (Open, High, Low, Close, Volume) data
  - Daily aggregation per instrument
  - Calculates: opening price, high/low prices, closing price, total quantity, total value, trade count
  - Materialized as table with daily partitioning
  - Key for technical analysis and candlestick charting

- **[mart_daily_instrument_comparison.sql](clickhouse_dbt/models/marts/mart_daily_instrument_comparison.sql)** - Cross-instrument comparison metrics
  - Daily comparison between BTC and ETH
  - Metrics: volume (quantity), trade count, average price, and volume ratio (BTC/ETH)
  - Useful for analyzing market dynamics and instrument correlation
  - Single row per day combining both instruments

- **[mart_volatility.sql](clickhouse_dbt/models/marts/mart_volatility.sql)** - Volatility and price dispersion metrics
  - Daily volatility analysis per instrument
  - Calculates: price standard deviation, price range (high-low), price range percentage, average trade size
  - Measures market activity and price stability
  - Key for risk management and trading strategy development

### Data Sources

- **Source**: [sources.yml](clickhouse_dbt/models/staging/sources.yml)
- **Schema**: `binance`
- **Tables**:
  - `btcusdt_trades` - Raw Bitcoin USDT trading data
  - `ethusdt_trades` - Raw Ethereum USDT trading data

## Key Features

### ClickHouse Optimizations
- **MergeTree Engine**: All tables use ClickHouse's MergeTree engine for optimal performance
- **Partitioning**: Monthly partitions (`toYYYYMM(trade_date)`) for efficient querying and management
- **Ordering**: Tables ordered by `trade_date` to improve query performance
- **Incremental Loading**: Uses `delete+insert` strategy to efficiently update only changed data
- **Unique Keys**: Ensures data integrity and prevents duplicates during incremental loads

### dbt Configuration
All models are configured in [dbt_project.yml](clickhouse_dbt/dbt_project.yml):
- **Staging models**: Materialized as views for flexibility
- **Intermediate models**: Materialized as views to reduce storage
- **Marts models**: Materialized as tables for query performance
- **Auto-incremental processing**: Models skip full recomputation by detecting new data

## Running the Project

```bash
# Install dependencies
dbt deps

# Run all models (full refresh)
dbt run

# Run specific model
dbt run -s stg_btcusdt_trades

# Run with incremental updates (only processes new data)
dbt run --full-refresh

# Test data quality
dbt test
```

## Model Dependencies

```
Binance Sources
  ├── stg_btcusdt_trades (Staging View)
  ├── stg_ethusdt_trades (Staging View)
  │
  └─→ int_trades (Intermediate View - UNION)
       │
       ├─→ mart_daily_ohlcv (Marts Table)
       ├─→ mart_daily_instrument_comparison (Marts Table)
       └─→ mart_volatility (Marts Table)
```

## Incremental Processing Details

All models use the following pattern for efficient incremental loads:
```sql
{% if is_incremental() %}
    where datetime = (
        select max(trade_date) from source_table
    ) - 1
{% endif %}
```
This ensures only the previous day's trades are reprocessed, making daily runs fast and cost-effective.

## Resources 
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- [ClickHouse dbt adapter documentation](https://github.com/clickhouse-community/dbt-clickhouse)
- Check out [dbt Discourse](https://discourse.getdbt.com/) for commonly asked questions
- Join the [dbt community chat](https://community.getdbt.com/) on Slack 
