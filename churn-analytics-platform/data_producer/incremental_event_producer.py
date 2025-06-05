from kafka import KafkaProducer
import pandas as pd
import json
import random
from datetime import datetime
import time
from faker import Faker
import os

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

CSV_PATH = "ml_model/telecom_customer_churn.csv"

COLUMNS = [
    "Gender","Age","Married","Number of Dependents","Number of Referrals","Tenure in Months","Offer","Phone Service","Avg Monthly Long Distance Charges",
    "Multiple Lines","Internet Service","Internet Type","Avg Monthly GB Download","Online Security","Online Backup",
    "Device Protection Plan","Premium Tech Support","Streaming TV","Streaming Movies","Streaming Music",
    "Unlimited Data","Contract","Paperless Billing","Payment Method","Monthly Charge","Total Charges",
    "Total Refunds","Total Extra Data Charges","Total Long Distance Charges","Total Revenue","Customer Status"
]

# ──────────────────────────────────────────────────────────────────────────────
# Step 1: Load only the needed columns
# ──────────────────────────────────────────────────────────────────────────────

print(f"Loading columns {len(COLUMNS)} from {CSV_PATH} …")
df = pd.read_csv(CSV_PATH, usecols=COLUMNS)
print(f"Loaded {df.shape[0]} rows × {df.shape[1]} columns.\n")

# ──────────────────────────────────────────────────────────────────────────────
# Step 2: Compute unique-value frequencies for each column
# ──────────────────────────────────────────────────────────────────────────────

distributions = {}

for col in COLUMNS:
    series = df[col]

    # value_counts (include NaN)
    vc = series.value_counts(dropna=False)

    # unique values and frequencies
    unique_vals = vc.index.tolist()
    freqs = vc.values.tolist()

    # Replace NaN with the sentinel "__MISSING__"
    cleaned_vals = []
    for v in unique_vals:
        if pd.isna(v):
            cleaned_vals.append("__MISSING__")
        else:
            cleaned_vals.append(v)

    # Store in the dictionary
    distributions[col] = {
        "values": cleaned_vals,
        "weights": freqs
    }



# ──────────────────────────────────────────────────────────────────────────────
# Step 3: Generate sample fake rows using those distributions
# ──────────────────────────────────────────────────────────────────────────────

def generate_event():
    record = {}
    for col, dist in distributions.items():
        choice = random.choices(dist["values"], weights=dist["weights"], k=1)[0]
        record[col] = None if choice == "__MISSING__" else choice
    record["timestamp"] = datetime.utcnow().isoformat()
    return record

# ─────────────────────────────────────────────────────────────
# Kafka Producer Setup
# ─────────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ─────────────────────────────────────────────────────────────
# Main loop: send fake records every second
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting Kafka producer. Sending customer events...\n")
    try:
        while True:
            event = generate_event()
            print(f"Sending event: {event}")
            producer.send("customer_events", event)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Kafka producer stopped manually.")
