with news as (
    select * from {{ ref('stg_news') }}
),

prices as (
    select * from {{ ref('stg_prices') }}
),

ai_news as (
    select
        news_date,
        news_hour,
        count(*)                                                as total_ai_articles,
        count(case when headline ilike '%anthropic%' then 1 end) as anthropic_mentions,
        count(case when headline ilike '%openai%' then 1 end)    as openai_mentions,
        count(case when headline ilike '%nvidia%' then 1 end)    as nvidia_mentions,
        count(case when headline ilike '%chatgpt%' then 1 end)   as chatgpt_mentions,
        avg(sentiment_numeric)                                   as avg_ai_sentiment,
        avg(sentiment_score)                                     as avg_confidence
    from news
    where ticker in ('NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META')
        or headline ilike '%ai%'
        or headline ilike '%artificial intelligence%'
        or headline ilike '%openai%'
        or headline ilike '%anthropic%'
    group by news_date, news_hour
),

ai_stock_prices as (
    select
        price_date,
        price_hour,
        max(case when ticker = 'NVDA' then close end)   as nvda_close,
        max(case when ticker = 'MSFT' then close end)   as msft_close,
        max(case when ticker = 'GOOGL' then close end)  as googl_close,
        max(case when ticker = 'AMZN' then close end)   as amzn_close,
        max(case when ticker = 'META' then close end)   as meta_close,
        max(case when ticker = 'SPY' then close end)    as spy_close
    from prices
    group by price_date, price_hour
)

select
    coalesce(n.news_date, p.price_date)         as date,
    coalesce(n.news_hour, p.price_hour)         as hour,

    -- news signals
    n.total_ai_articles,
    n.anthropic_mentions,
    n.openai_mentions,
    n.nvidia_mentions,
    n.chatgpt_mentions,
    n.avg_ai_sentiment,
    n.avg_confidence,

    -- prices
    p.nvda_close,
    p.msft_close,
    p.googl_close,
    p.amzn_close,
    p.meta_close,
    p.spy_close,

    -- major ai event flag
    case when n.total_ai_articles > 3 then true else false end  as major_ai_event

from ai_news n
full outer join ai_stock_prices p
    on n.news_date = p.price_date
    and n.news_hour = p.price_hour
