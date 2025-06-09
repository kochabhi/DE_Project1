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
        dbname="postgres", user="postgres", password="admin", host="postgres_kafka", port="5432"
    )
    cur = conn.cursor()
    logger.info("Connected to PostgreSQL database.")

    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_events.telecom_customer_events_raw (
            event_id SERIAL PRIMARY KEY,
            gender VARCHAR(10),
            age INTEGER,
            married VARCHAR(5),
            number_of_dependents INTEGER,
            number_of_referrals INTEGER,
            tenure_in_months INTEGER,
            offer VARCHAR(50),
            phone_service VARCHAR(5),
            avg_monthly_long_distance_charges NUMERIC(10, 2),
            multiple_lines VARCHAR(5),
            internet_service VARCHAR(5),
            internet_type VARCHAR(50),
            avg_monthly_gb_download NUMERIC(10, 2),
            online_security VARCHAR(5),
            online_backup VARCHAR(5),
            device_protection_plan VARCHAR(5),
            premium_tech_support VARCHAR(5),
            streaming_tv VARCHAR(5),
            streaming_movies VARCHAR(5),
            streaming_music VARCHAR(5),
            unlimited_data VARCHAR(5),
            contract VARCHAR(50),
            paperless_billing VARCHAR(5),
            payment_method VARCHAR(50),
            monthly_charge NUMERIC(10, 2),
            total_charges NUMERIC(10, 2),
            total_refunds NUMERIC(10, 2),
            total_extra_data_charges INTEGER,
            total_long_distance_charges NUMERIC(10, 2),
            total_revenue NUMERIC(10, 2),
            customer_status VARCHAR(50),
            event_timestamp TIMESTAMP WITH TIME ZONE
        )
    """)
    conn.commit()
    logger.info("Ensured telecom_customer_events table exists.")

except OperationalError as e:
    logger.error(f"Database connection failed: {e}")
    exit(1)

# Kafka Consumer
try:
    consumer = KafkaConsumer(
        'customer_events',
        bootstrap_servers='kafka:9092',
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
            """
            INSERT INTO customer_events.telecom_customer_events_raw (
                gender, age, married, number_of_dependents, number_of_referrals,
                tenure_in_months, offer, phone_service, avg_monthly_long_distance_charges,
                multiple_lines, internet_service, internet_type, avg_monthly_gb_download,
                online_security, online_backup, device_protection_plan, premium_tech_support,
                streaming_tv, streaming_movies, streaming_music, unlimited_data,
                contract, paperless_billing, payment_method, monthly_charge,
                total_charges, total_refunds, total_extra_data_charges,
                total_long_distance_charges, total_revenue, customer_status,
                event_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                data.get('Gender'),
                data.get('Age'),
                data.get('Married'),
                data.get('Number of Dependents'),
                data.get('Number of Referrals'),
                data.get('Tenure in Months'),
                data.get('Offer'),
                data.get('Phone Service'),
                data.get('Avg Monthly Long Distance Charges'),
                data.get('Multiple Lines'),
                data.get('Internet Service'),
                data.get('Internet Type'),
                data.get('Avg Monthly GB Download'),
                data.get('Online Security'),
                data.get('Online Backup'),
                data.get('Device Protection Plan'),
                data.get('Premium Tech Support'),
                data.get('Streaming TV'),
                data.get('Streaming Movies'),
                data.get('Streaming Music'),
                data.get('Unlimited Data'),
                data.get('Contract'),
                data.get('Paperless Billing'),
                data.get('Payment Method'),
                data.get('Monthly Charge'),
                data.get('Total Charges'),
                data.get('Total Refunds'),
                data.get('Total Extra Data Charges'),
                data.get('Total Long Distance Charges'),
                data.get('Total Revenue'),
                data.get('Customer Status'),
                data.get('timestamp')
            )
        )
        conn.commit() # --- FIX: Corrected indentation ---

    except (Exception, psycopg2.Error) as e:
        logger.error(f"Failed to insert message: {e}")
        conn.rollback()  # prevent broken transactions