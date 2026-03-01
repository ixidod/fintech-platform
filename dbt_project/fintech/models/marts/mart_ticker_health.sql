with trades as (
    select * from {{ ref('mart_trade_activity') }}
),
news as (
    select * from {{ ref('stg_news') }}
),

prices as (
    select * from {{ ref('stg_prices') }}
),

ticker_prices as (
    select
        ticker,
        price_date,
        price_hour,
        first_value(open) over (
            partition by ticker, price_date, price_hour
            order by bar_timestamp
        )                                               as hour_open,
        max(high) over (
            partition by ticker, price_date, price_hour
        )                                               as hour_high,
        min(low) over (
            partition by ticker, price_date, price_hour
        )                                               as hour_low,
        last_value(close) over (
            partition by ticker, price_date, price_hour
            order by bar_timestamp
            rows between unbounded preceding and unbounded following
        )                                               as hour_close,
        sum(total_volume) over (
            partition by ticker, price_date, price_hour
        )                                               as hour_volume
    from prices
),

ticker_sentiment as (
    select
        ticker,
        news_date                                       as sentiment_date,
        news_hour                                       as sentiment_hour,
        avg(sentiment_numeric)                          as avg_sentiment,
        avg(sentiment_score)                            as avg_sentiment_score,
        count(case when sentiment = 'positive' then 1 end) as positive_count,
        count(case when sentiment = 'negative' then 1 end) as negative_count,
        count(case when sentiment = 'neutral' then 1 end)  as neutral_count,
        count(*)                                           as total_articles
    from news
    group by ticker, news_date, news_hour
),

final as (
    select
        p.ticker,
        p.price_date,
        p.price_hour,

        -- price
        p.hour_open,
        p.hour_high,
        p.hour_low,
        p.hour_close,
        p.hour_volume,

        -- sentiment
        s.avg_sentiment,
        s.avg_sentiment_score,
        s.positive_count,
        s.negative_count,
        s.neutral_count,
        s.total_articles,

        -- signals
        case when p.hour_volume > avg(p.hour_volume) over (
            partition by p.ticker
        ) * 2 then true else false end                  as volume_spike,

        case when p.ticker in (
            'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META'
        ) then true else false end                      as is_ai_stock

    from ticker_prices p
    left join ticker_sentiment s
        on p.ticker = s.ticker
        and p.price_date = s.sentiment_date
        and p.price_hour = s.sentiment_hour
)

select distinct * from final
