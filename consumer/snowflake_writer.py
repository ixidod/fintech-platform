import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ============================================
# CONFIG
# ============================================
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'trades'
GROUP_ID = 'snowflake-writer'
BATCH_SIZE = 100        # write to snowflake every 100 messages
BATCH_TIMEOUT_SEC = 10  # or every 10 seconds, whichever comes first

# ============================================
# SNOWFLAKE CONNECTION
# ============================================
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE', 'FINTECH_ADMIN'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FINTECH_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'FINTECH_RAW_DB'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'KAFKA_INGEST'),
    )

# ============================================
# WRITE BATCH TO SNOWFLAKE
# ============================================
def write_batch(cursor, batch: list):
    if not batch:
        return

    rows = [
        (
            trade['trade_id'],
            trade['ticker'],
            trade['action'],
            trade['price'],
            trade['volume'],
            trade['bid'],
            trade['ask'],
            trade['spread'],
            trade['trader_id'],
            trade['trader_type'],
            trade['order_type'],
            trade['execution_latency_ms'],
            trade['session'],
            trade['event_timestamp'],
            datetime.now(timezone.utc).isoformat(),
        )
        for trade in batch
    ]

    cursor.executemany("""
        INSERT INTO KAFKA_INGEST.TRADES (
            trade_id, ticker, action, price, volume,
            bid, ask, spread, trader_id, trader_type,
            order_type, execution_latency_ms, session,
            event_timestamp, ingested_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """, rows)

    print(f"✅ Written {len(rows)} trades to Snowflake | {datetime.now().strftime('%H:%M:%S')}")

# ============================================
# CONSUMER
# ============================================
def main():
    print(f"👂 Consumer started — listening to topic: {TOPIC}")
    print(f"📦 Batch size: {BATCH_SIZE} | Timeout: {BATCH_TIMEOUT_SEC}s\n")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',   # read from beginning
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=BATCH_TIMEOUT_SEC * 1000,
    )

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    batch = []

    try:
        while True:
            try:
                for message in consumer:
                    batch.append(message.value)

                    if len(batch) >= BATCH_SIZE:
                        write_batch(cursor, batch)
                        conn.commit()
                        batch = []

            except StopIteration:
                # timeout hit — flush whatever is in batch
                if batch:
                    write_batch(cursor, batch)
                    conn.commit()
                    batch = []
                print(f"⏳ Waiting for new messages...")

    except KeyboardInterrupt:
        if batch:
            write_batch(cursor, batch)
            conn.commit()
        print("\n🛑 Consumer stopped")

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
