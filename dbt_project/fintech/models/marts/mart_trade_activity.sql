with trades as (
    select * from {{ ref('stg_trades') }}
),

aggregated as (
    select
        ticker,
        trade_date,
        trade_hour,

        -- volume metrics
        count(*)                                            as total_trades,
        sum(volume)                                         as total_volume,
        avg(volume)                                         as avg_trade_size,
        max(volume)                                         as max_trade_size,
        sum(trade_value)                                    as total_trade_value,

        -- price metrics
        avg(price)                                          as avg_price,
        min(price)                                          as min_price,
        max(price)                                          as max_price,
        max(price) - min(price)                             as price_range,

        -- vwap
        div0(sum(price * volume), sum(volume))              as vwap,

        -- trader behavior
        div0(count(case when trader_type = 'retail' then 1 end),
            count(*)) * 100                                 as retail_pct,
        div0(count(case when trader_type = 'institutional' then 1 end),
            count(*)) * 100                                 as institutional_pct,
        div0(count(case when trader_type = 'algo' then 1 end),
            count(*)) * 100                                 as algo_pct,

        -- order types
        div0(count(case when order_type = 'market' then 1 end),
            count(*)) * 100                                 as market_order_pct,
        div0(count(case when order_type = 'limit' then 1 end),
            count(*)) * 100                                 as limit_order_pct,
        div0(count(case when order_type = 'stop' then 1 end),
            count(*)) * 100                                 as stop_order_pct,

        -- buy vs sell pressure
        sum(case when action = 'buy' then volume else 0 end)    as buy_volume,
        sum(case when action = 'sell' then volume else 0 end)   as sell_volume,
        div0(
            sum(case when action = 'buy' then volume else 0 end),
            sum(case when action = 'sell' then volume else 0 end)
        )                                                       as buy_sell_ratio,

        -- latency
        avg(execution_latency_ms)                           as avg_latency_ms,
        percentile_cont(0.95) within group (
            order by execution_latency_ms
        )                                                   as p95_latency_ms

    from trades
    group by ticker, trade_date, trade_hour
)

select * from aggregated
