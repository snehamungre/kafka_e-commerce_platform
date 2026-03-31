import json
import random
import uuid

from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "shipments",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Shipments is running and subscribed to orders topic")

DELIVERY_DAYS = ["2-3 business days", "3-5 business days", "next day delivery"]

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)

        tracking_number = str(uuid.uuid4())[:8].upper()
        delivery_estimate = random.choice(DELIVERY_DAYS)

        print(
            f"Shipment scheduled for {order['user']} | "
            f"Item: {order['quantity']}x {order['item']} | "
            f"Tracking: {tracking_number} | "
            f"ETA: {delivery_estimate}"
        )

except KeyboardInterrupt:
    print("\n🔴 Stopping Shipping")

finally:
    consumer.close()
