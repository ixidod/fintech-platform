with source as (
    select * from {{ source('kafka_ingest', 'trades') }}
),

cleaned as (
    select
        trade_id,
        upper(ticker)                           as ticker,
        lower(action)                           as action,
        price::float                            as price,
        volume::integer                         as volume,
        bid::float                              as bid,
        ask::float                              as ask,
        ask - bid                               as spread_calculated,
        trader_id,
        lower(trader_type)                      as trader_type,
        lower(order_type)                       as order_type,
        execution_latency_ms::integer           as execution_latency_ms,
        lower(session)                          as session,
        event_timestamp::timestamp_tz           as event_timestamp,
        ingested_at::timestamp_tz               as ingested_at,

        -- derived
        date(event_timestamp)                   as trade_date,
        hour(event_timestamp)                   as trade_hour,
        minute(event_timestamp)                 as trade_minute,
        price * volume                          as trade_value

    from source
    where
        price > 0
        and volume > 0
        and trade_id is not null
        and ticker is not null
)

select * from cleaned
