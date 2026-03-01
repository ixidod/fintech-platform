with source as (
    select * from {{ source('kafka_ingest', 'trades') }}
),

-- derive price bars from trade data since we don't have Alpaca
-- aggregate trades into 1-minute OHLCV bars per ticker
price_bars as (
    select
        upper(ticker)                           as ticker,
        date_trunc('minute', event_timestamp)   as bar_timestamp,
        date(event_timestamp)                   as price_date,
        hour(event_timestamp)                   as price_hour,

        first_value(price) over (
            partition by ticker, date_trunc('minute', event_timestamp)
            order by event_timestamp
        )                                       as open,

        max(price) over (
            partition by ticker, date_trunc('minute', event_timestamp)
        )                                       as high,

        min(price) over (
            partition by ticker, date_trunc('minute', event_timestamp)
        )                                       as low,

        last_value(price) over (
            partition by ticker, date_trunc('minute', event_timestamp)
            order by event_timestamp
            rows between unbounded preceding and unbounded following
        )                                       as close,

        sum(volume) over (
            partition by ticker, date_trunc('minute', event_timestamp)
        )                                       as total_volume,

        sum(price * volume) over (
            partition by ticker, date_trunc('minute', event_timestamp)
        ) / nullif(sum(volume) over (
            partition by ticker, date_trunc('minute', event_timestamp)
        ), 0)                                   as vwap,

        count(*) over (
            partition by ticker, date_trunc('minute', event_timestamp)
        )                                       as trade_count

    from source
    where price > 0 and volume > 0
)

select distinct
    ticker,
    bar_timestamp,
    price_date,
    price_hour,
    open,
    high,
    low,
    close,
    total_volume,
    vwap,
    trade_count

from price_bars
