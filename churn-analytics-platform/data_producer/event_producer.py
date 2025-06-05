from kafka import KafkaProducer
import json
import time
from faker import Faker
import random

faker = Faker()

# Kafka producer with JSON serializer
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_event():
    return {
        "customer_id": random.randint(1000, 1100),
        "event_type": random.choice(["login", "purchase", "support_call", "cancel"]),
        "timestamp": faker.iso8601()
    }

if __name__ == "__main__":
    print("Starting Kafka producer. Sending customer events...")
    while True:
        event = generate_event()
        print(f"Sending event: {event}")
        producer.send("customer_events", event)
        time.sleep(1)  # send 1 message per second
  