import json
import uuid
from confluent_kafka import Producer

producer_config = {"bootstrap.servers": "localhost:9092"}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.value().decode("utf-8")}")
        print(f"Delivered to: {msg.topic()} and partition {msg.partition()}")


order = {"order_id": str(uuid.uuid4()), "user": "man", "item": "pen", "quantity": 1}

value = json.dumps(order).encode("utf-8")

producer.produce(topic="order", value=value, callback=delivery_report)

producer.flush()
