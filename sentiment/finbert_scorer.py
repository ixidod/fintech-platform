import os
import json
import snowflake.connector
from transformers import pipeline
from datetime import datetime, timezone
from time import sleep
from dotenv import load_dotenv

load_dotenv()

POLL_INTERVAL_SEC = 60  # check for new articles every minute

print(" Loading FinBERT model...")
finbert = pipeline(
    "text-classification",
    model="ProsusAI/finbert",
    truncation=True,
    max_length=512
)
print(" FinBERT ready\n")

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role='FINTECH_ADMIN',
        warehouse='FINTECH_WH',
        database='FINTECH_RAW_DB',
        schema='AIRBYTE_INGEST',
    )

def fetch_unscored(cursor) -> list:
    cursor.execute("""
        SELECT 
            _AIRBYTE_RAW_ID,
            TITLE,
            SOURCE:name::STRING as source_name,
            PUBLISHEDAT
        FROM AIRBYTE_INGEST.TOP_HEADLINES
        WHERE TITLE IS NOT NULL
    """)
    return cursor.fetchall()

def score_headline(headline: str) -> tuple:
    result = finbert(headline)[0]
    return result['label'], round(result['score'], 4)

TICKER_KEYWORDS = {
    'AAPL':  ['apple', 'iphone', 'ipad', 'mac', 'tim cook'],
    'TSLA':  ['tesla', 'elon musk', 'electric vehicle', 'ev'],
    'MSFT':  ['microsoft', 'azure', 'openai', 'copilot', 'bing'],
    'GOOGL': ['google', 'alphabet', 'gemini', 'deepmind', 'waymo'],
    'AMZN':  ['amazon', 'aws', 'bezos', 'prime'],
    'NVDA':  ['nvidia', 'jensen huang', 'gpu', 'cuda', 'chips'],
    'SPY':   ['s&p', 'federal reserve', 'fed', 'inflation', 'interest rate', 'market'],
    'META':  ['meta', 'facebook', 'instagram', 'whatsapp', 'zuckerberg', 'llama'],
}

def map_ticker(headline: str) -> str:
    headline_lower = headline.lower()
    for ticker, keywords in TICKER_KEYWORDS.items():
        if any(kw in headline_lower for kw in keywords):
            return ticker
    return 'GENERAL'

def write_scores(cursor, scored: list):
    if not scored:
        return

    cursor.executemany("""
        INSERT INTO AIRBYTE_INGEST.NEWS (
            article_id,
            ticker,
            headline,
            source,
            published_at,
            ingested_at,
            sentiment,
            sentiment_score
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, scored)

    print(f"📥 Written {len(scored)} scored articles to NEWS table")

def main():
    print(f"🚀 FinBERT scorer started")
    print(f"⏱  Polling every {POLL_INTERVAL_SEC}s for new headlines\n")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    while True:
        print(f"🔄 Checking for headlines — {datetime.now().strftime('%H:%M:%S')}")

        rows = fetch_unscored(cursor)

        if not rows:
            print("⏳ No headlines found, waiting...\n")
            sleep(POLL_INTERVAL_SEC)
            continue

        print(f"📰 Scoring {len(rows)} headlines...")

        scored = []
        for row in rows:
            raw_id, title, source, published_at = row
            sentiment, score = score_headline(title)
            ticker = map_ticker(title)

            scored.append((
                raw_id,
                ticker,
                title,
                source,
                published_at,
                datetime.now(timezone.utc).isoformat(),
                sentiment,
                score,
            ))

            print(f"  {ticker:6} | {sentiment:8} {score:.2f} | {title[:60]}")

        write_scores(cursor, scored)
        conn.commit()

        print(f"\n✅ Done — sleeping {POLL_INTERVAL_SEC}s\n")
        sleep(POLL_INTERVAL_SEC)


if __name__ == '__main__':
    main()
