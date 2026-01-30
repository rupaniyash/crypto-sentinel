import json
import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092") 
TOPIC_NAME = "crypto_trades"
COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"

producer = None
print(f"🔌 Connecting to Kafka at {KAFKA_BROKER}...")

for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connected to Kafka!")
        break
    except NoBrokersAvailable:
        print("⏳ Kafka not ready. Retrying in 2s...")
        time.sleep(2)

if not producer:
    raise Exception("❌ Failed to connect to Kafka. Is Docker running?")

def on_open(ws):
    print("📡 Connected to Coinbase. Subscribing to BTC-USD...")
    msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker"]
    }
    ws.send(json.dumps(msg))

def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "ticker":
        trade = {
            "symbol": "BTC-USD",
            "price": float(data.get("price", 0)),
            "side": data.get("side", "neutral"),
            "timestamp": data.get("time")
        }
        
        producer.send(TOPIC_NAME, trade)
        
        print(f"📨 Sent to Kafka: ${trade['price']}")

def on_error(ws, error):
    print(f"❌ Error: {error}")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        COINBASE_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error
    )
    ws.run_forever()