# Fintech Data Platform

A real-time data engineering platform that ingests synthetic trade events and live financial news, enriches with NLP sentiment analysis, transforms via dbt, and visualises in Metabase.

## Architecture

```
Python Generator → Redpanda (Kafka) → Snowflake (Raw)
NewsAPI          → Airbyte          → Snowflake (Raw)
                                           ↓
                                    FinBERT (NLP)
                                           ↓
                                    dbt (Staging → Marts)
                                           ↓
                                       Metabase
```

## Stack

| Layer | Tool |
|-------|------|
| Streaming | Redpanda (Kafka-compatible) |
| Ingestion | Airbyte Cloud + Python |
| Warehouse | Snowflake |
| NLP | FinBERT (ProsusAI) |
| Transformation | dbt |
| BI | Metabase |

## Data Sources

- **Synthetic trades** — Python generator producing realistic buy/sell events at 100 events/sec across 8 tickers (AAPL, TSLA, MSFT, GOOGL, AMZN, NVDA, SPY, META)
- **Live news** — NewsAPI top headlines ingested via Airbyte every 15 minutes
- **Sentiment scores** — FinBERT financial NLP model scoring each headline as positive/negative/neutral

## dbt Models

```
staging/
  stg_trades       — cleaned trade events from Kafka
  stg_news         — scored news headlines
  stg_prices       — 1-min OHLCV bars derived from trades

marts/
  mart_stock_sentiment   — sentiment vs price movement
  mart_trade_activity    — trading volume and behavior by ticker/hour
  mart_ticker_health     — overview per ticker with signals
  mart_ai_sector_pulse   — AI sector news impact (Anthropic, OpenAI, NVDA)
```

## Key Questions Answered

- Does negative news sentiment precede price drops?
- Which tickers are most sentiment-sensitive?
- How does AI sector news (Anthropic, OpenAI) move NVDA, MSFT, AMZN?
- What is the algo vs retail vs institutional trading ratio per ticker?

## Setup

### Prerequisites
- Python 3.11
- Podman
- Snowflake account
- dbt CLI
- NewsAPI key

### Run

```bash
# Start Redpanda
podman pod start redpanda-pod

# Activate venv
source .venv/bin/activate

# Start trade generator
python producer/trade_generator.py

# Start Snowflake consumer
python consumer/snowflake_writer.py

# Start sentiment scorer
python sentiment/finbert_scorer.py

# Run dbt
cd dbt_project/fintech
dbt run
dbt test
```

### Environment Variables

```bash
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
NEWS_API_KEY=
ALPACA_API_KEY=
ALPACA_SECRET_KEY=
```

## Data Volume

Running at 100 events/sec the trades table grows at ~360k rows/hour. After 24 hours expect ~8M rows.
