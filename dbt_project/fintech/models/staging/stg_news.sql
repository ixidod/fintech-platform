with source as (
    select * from {{ source('airbyte_ingest', 'news') }}
),

cleaned as (
    select
        article_id,
        upper(ticker)                           as ticker,
        trim(headline)                          as headline,
        source                                  as news_source,
        lower(sentiment)                        as sentiment,
        sentiment_score::float                  as sentiment_score,
        published_at::timestamp_tz              as published_at,
        ingested_at::timestamp_tz               as ingested_at,

        -- derived
        date(published_at)                      as news_date,
        hour(published_at)                      as news_hour,

        -- sentiment as number for easier aggregation
        case sentiment
            when 'positive' then 1
            when 'negative' then -1
            else 0
        end                                     as sentiment_numeric

    from source
    where
        article_id is not null
        and headline is not null
        and sentiment_score >= 0.5  -- filter low confidence
)

select * from cleaned
