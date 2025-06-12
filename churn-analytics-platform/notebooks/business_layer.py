import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "user": "postgres",
    "password": "admin",
    "host": "postgres",
    "port": 5432,
    "database": "postgres"
}

TABLE_INPUT = "customer_events.telecom_customer_events_prediction"
TABLE_OUTPUT = "customer_events.telecom_customer_events_prediction_gold"

# ✅ 1. Connect to PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ✅ 2. Read data
df = pd.read_sql(f"SELECT * FROM {TABLE_INPUT}", con=conn)

# ✅ 3. Apply transformations
def reverse_one_hot(df, prefix):
    cols = [col for col in df.columns if col.startswith(prefix)]
    if cols:
        df[prefix.rstrip('_')] = df[cols].idxmax(axis=1).str.replace(prefix, '', regex=False)
        df.drop(columns=cols, inplace=True)
    return df

# Decode one-hot encoded columns
for prefix in ["offer_", "contract_", "internet_type_", "payment_method_"]:
    df = reverse_one_hot(df, prefix)

# Decode scaled log columns
def reverse_log_std(series, mean, std):
    return np.exp(series * std + mean)

scaler_params = {
    'monthly_charge_log_scaled': (3.9748, 0.7779),
    'total_charges_log_scaled': (6.9417, 1.5460),
    'total_refunds_log_scaled': (0.2318, 0.8403),
    'total_extra_data_charges_log_scaled': (0.4009, 1.2160),
    'total_long_distance_charges_log_scaled': (5.3417, 2.2878),
    'total_revenue_log_scaled': (7.2924, 1.4860),
    'avg_monthly_long_distance_charges_log_scaled': (2.7608, 1.1530),
    'avg_monthly_gb_download_log_scaled': (2.3787, 1.4243),
    'age_log_scaled': (3.8394, 0.3586),
    'number_of_referrals_log_scaled': (0.5785, 1.2037),
    'tenure_in_months_log_scaled': (3.3920, 0.8034)
}

for col, (mean, std) in scaler_params.items():
    if col in df.columns:
        df[col.replace("_log_scaled", "")] = reverse_log_std(df[col], mean, std).round(2)
        df.drop(columns=col, inplace=True)

# Drop any remaining *_scaled columns
df = df[[col for col in df.columns if not col.endswith('_scaled')]]

# Binary mappings
binary_mappings = {
    'gender': {0: 'Male', 1: 'Female'},
    'married': {0: 'No', 1: 'Yes'},
    'phone_service': {0: 'No', 1: 'Yes'},
    'multiple_lines': {0: 'No', 1: 'Yes'},
    'internet_service': {0: 'DSL', 1: 'Fiber Optic', 2: 'No'},
    'online_security': {0: 'No', 1: 'Yes'},
    'online_backup': {0: 'No', 1: 'Yes'},
    'device_protection_plan': {0: 'No', 1: 'Yes'},
    'premium_tech_support': {0: 'No', 1: 'Yes'},
    'streaming_tv': {0: 'No', 1: 'Yes'},
    'streaming_movies': {0: 'No', 1: 'Yes'},
    'streaming_music': {0: 'No', 1: 'Yes'},
    'unlimited_data': {0: 'No', 1: 'Yes'},
    'paperless_billing': {0: 'No', 1: 'Yes'},
    'churn_prediction': {0: 'No', 1: 'Yes'}
}

for col, mapping in binary_mappings.items():
    if col in df.columns:
        df[col] = df[col].map(mapping)

# Round probability columns
if 'churn_probability_class_1' in df.columns:
    df['churn_probability'] = (df['churn_probability_class_1'] * 100).round(2)
    df['churn_probability_class_0'] = (df['churn_probability_class_0'] * 100).round(2)
    df['churn_probability_class_1'] = (df['churn_probability_class_1'] * 100).round(2)

# Add timestamp
df['uploaded_date'] = datetime.utcnow()

# ✅ 4. Overwrite Table
cur.execute(f"DROP TABLE IF EXISTS {TABLE_OUTPUT};")
columns_sql = ', '.join([f"{col} TEXT" for col in df.columns])
create_sql = f"CREATE TABLE {TABLE_OUTPUT} ({columns_sql});"
cur.execute(create_sql)
conn.commit()

# ✅ 5. Insert Data
insert_query = f"""
INSERT INTO {TABLE_OUTPUT} ({', '.join(df.columns)})
VALUES ({', '.join(['%s'] * len(df.columns))});
"""
cur.executemany(insert_query, df.astype(str).values.tolist())
conn.commit()

# ✅ 6. Clean up
cur.close()
conn.close()
print("✅ Cleaned and overwritten data to table successfully.")
