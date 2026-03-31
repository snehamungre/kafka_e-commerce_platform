import json
import random

from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "payments",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Payments is running and subscribed to orders topic")

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

        total = order["price"] * order["quantity"]

        payment_success = random.random() < 0.8

        if payment_success:
            print(
                f"✅ Payment of ₹{total} accepted for order {order['order_id']} by {order['user']}"
            )
        else:
            print(
                f"❌ Payment of ₹{total} FAILED for order {order['order_id']} by {order['user']}"
            )


except KeyboardInterrupt:
    print("\n🔴 Stopping Payment Service")

finally:
    consumer.close()
