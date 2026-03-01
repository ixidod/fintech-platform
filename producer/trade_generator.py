import json
import random
import uuid
from datetime import datetime, timezone
from time import sleep
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# ============================================
# CONFIG
# ============================================
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'trades'
EVENTS_PER_SECOND = 100  # crank this up later for big tables

TICKERS = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'SPY', 'META']

# Realistic base prices per ticker
BASE_PRICES = {
    'AAPL':  182.0,
    'TSLA':  201.0,
    'MSFT':  415.0,
    'GOOGL': 175.0,
    'AMZN':  205.0,
    'NVDA':  875.0,
    'SPY':   498.0,
    'META':  505.0,
}

TRADER_TYPES = ['retail', 'institutional', 'algo']
TRADER_WEIGHTS = [0.5, 0.2, 0.3]  # algo trades a lot in real markets

ORDER_TYPES = ['market', 'limit', 'stop']
ORDER_WEIGHTS = [0.6, 0.3, 0.1]

SESSIONS = ['pre_market', 'regular', 'after_hours']


# ============================================
# HELPERS
# ============================================
def get_session() -> str:
    hour = datetime.now().hour
    if 4 <= hour < 9:
        return 'pre_market'
    elif 9 <= hour < 16:
        return 'regular'
    else:
        return 'after_hours'


def generate_price(ticker: str) -> tuple:
    base = BASE_PRICES[ticker]
    # random walk — price drifts realistically
    price = round(base * random.uniform(0.995, 1.005), 2)
    spread = round(random.uniform(0.01, 0.05), 4)
    bid = round(price - spread / 2, 2)
    ask = round(price + spread / 2, 2)
    return price, bid, ask, round(spread, 4)


def generate_trade() -> dict:
    ticker = random.choice(TICKERS)
    price, bid, ask, spread = generate_price(ticker)
    trader_type = random.choices(TRADER_TYPES, TRADER_WEIGHTS)[0]

    return {
        'trade_id':             str(uuid.uuid4()),
        'ticker':               ticker,
        'action':               random.choice(['buy', 'sell']),
        'price':                price,
        'volume':               random.randint(1, 1000),
        'bid':                  bid,
        'ask':                  ask,
        'spread':               spread,
        'trader_id':            str(uuid.uuid4()),
        'trader_type':          trader_type,
        'order_type':           random.choices(ORDER_TYPES, ORDER_WEIGHTS)[0],
        'execution_latency_ms': random.randint(1, 50) if trader_type == 'algo' else random.randint(50, 500),
        'session':              get_session(),
        'event_timestamp':      datetime.now(timezone.utc).isoformat(),
    }


# ============================================
# PRODUCER
# ============================================
def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"🚀 Trade generator started — {EVENTS_PER_SECOND} events/sec")
    print(f"📡 Publishing to topic: {TOPIC}")
    print("Press Ctrl+C to stop\n")

    count = 0
    while True:
        trade = generate_trade()
        producer.send(TOPIC, value=trade)
        count += 1

        if count % 100 == 0:
            print(f"✅ {count} trades sent | latest: {trade['ticker']} {trade['action']} {trade['price']} ({trade['trader_type']})")

        sleep(1 / EVENTS_PER_SECOND)


if __name__ == '__main__':
    main()
