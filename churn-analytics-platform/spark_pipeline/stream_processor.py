from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import sql, OperationalError
import logging

# Optional: Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection setup
try:
    conn = psycopg2.connect(
        dbname="postgres", user="postgres", password="admin", host="localhost", port="5432"
    )
    cur = conn.cursor()
    logger.info("Connected to PostgreSQL database.")

    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_events (
            customer_id INT,
            event_type TEXT,
            timestamp TIMESTAMP
        )
    """)
    conn.commit()
    logger.info("Ensured customer_events table exists.")

except OperationalError as e:
    logger.error(f"Database connection failed: {e}")
    exit(1)

# Kafka Consumer
try:
    consumer = KafkaConsumer(
        'customer_events',
        bootstrap_servers='localhost:9094',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info("Kafka consumer started. Listening for messages...")

except Exception as e:
    logger.error(f"Kafka consumer could not be started: {e}")
    exit(1)

# Message loop
for message in consumer:
    try:
        data = message.value
        logger.info(f"Received: {data}")

        cur.execute(
            "INSERT INTO customer_events (customer_id, event_type, timestamp) VALUES (%s, %s, %s)",
            (data['customer_id'], data['event_type'], data['timestamp'])
        )
        conn.commit()

    except (Exception, psycopg2.Error) as e:
        logger.error(f"Failed to insert message: {e}")
        conn.rollback()  # prevent broken transactions