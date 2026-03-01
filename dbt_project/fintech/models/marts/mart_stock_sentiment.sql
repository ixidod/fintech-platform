with news as (
    select * from {{ ref('stg_news') }}
),

prices as (
    select * from {{ ref('stg_prices') }}
),

-- join news with price at time of article
news_with_prices as (
    select
        n.article_id,
        n.ticker,
        n.headline,
        n.news_source,
        n.sentiment,
        n.sentiment_score,
        n.sentiment_numeric,
        n.published_at,
        n.news_date,
        n.news_hour,

        -- price at time of article
        p.close                                     as price_at_news,
        p.vwap                                      as vwap_at_news,
        p.total_volume                              as volume_at_news,

        -- price 1hr later
        p1.close                                    as price_1hr_later,
        -- price 4hr later
        p4.close                                    as price_4hr_later

    from news n

    -- price at same minute as article
    left join prices p
        on n.ticker = p.ticker
        and date_trunc('minute', n.published_at) = p.bar_timestamp

    -- price 1 hour later
    left join prices p1
        on n.ticker = p1.ticker
        and dateadd('hour', 1, date_trunc('minute', n.published_at)) = p1.bar_timestamp

    -- price 4 hours later
    left join prices p4
        on n.ticker = p4.ticker
        and dateadd('hour', 4, date_trunc('minute', n.published_at)) = p4.bar_timestamp
),

final as (
    select
        *,
        -- price changes
        div0(price_1hr_later - price_at_news, price_at_news) * 100    as pct_change_1hr,
        div0(price_4hr_later - price_at_news, price_at_news) * 100    as pct_change_4hr,

        -- was sentiment directionally correct?
        case
            when sentiment = 'positive' and price_1hr_later > price_at_news then true
            when sentiment = 'negative' and price_1hr_later < price_at_news then true
            else false
        end                                                             as sentiment_correct_1hr,

        case
            when sentiment = 'positive' and price_4hr_later > price_at_news then true
            when sentiment = 'negative' and price_4hr_later < price_at_news then true
            else false
        end                                                             as sentiment_correct_4hr

    from news_with_prices
)

select * from final
