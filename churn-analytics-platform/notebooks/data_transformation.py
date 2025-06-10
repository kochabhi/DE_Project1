import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, when, log1p
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.functions import max as spark_max

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("PostgresToPostgresChurnPipeline") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: PostgreSQL connection info
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
db_properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Step 3: Define read logic (read full table each time)
def load_source_table():
    # Load latest timestamp from target
    try:
        latest_ts_df = spark.read.jdbc(
            url=jdbc_url,
            table="customer_events.telecom_customer_events_transformed",
            properties=db_properties
        ).select(spark_max("event_timestamp").alias("latest_ts"))
        
        latest_ts = latest_ts_df.collect()[0]["latest_ts"]

        # Read only new records
        df = spark.read.jdbc(
            url=jdbc_url,
            table="customer_events.telecom_customer_events_raw",
            properties=db_properties
        ).filter(col("event_timestamp") > latest_ts)

    except:
        # If target table is empty, load all
        df = spark.read.jdbc(
            url=jdbc_url,
            table="customer_events.telecom_customer_events_raw",
            properties=db_properties
        )

    return df

# Step 4: Define transformation logic
def transform(df: DataFrame) -> DataFrame:
    # Convert binary categorical columns
    binary_mappings = {
        'gender': {'Male': 1, 'Female': 0},
        'married': {'Yes': 1, 'No': 0},
        'multiple_lines': {'Yes': 1, 'No': 0},
        'internet_service': {'Yes': 1, 'No': 0},
        'phone_service': {'Yes': 1, 'No': 0},
        'online_security': {'Yes': 1, 'No': 0},
        'online_backup': {'Yes': 1, 'No': 0},
        'device_protection_plan': {'Yes': 1, 'No': 0},
        'premium_tech_support': {'Yes': 1, 'No': 0},
        'streaming_tv': {'Yes': 1, 'No': 0},
        'streaming_movies': {'Yes': 1, 'No': 0},
        'streaming_music': {'Yes': 1, 'No': 0},
        'unlimited_data': {'Yes': 1, 'No': 0},
        'paperless_billing': {'Yes': 1, 'No': 0},
        'customer_status': {'Churned': 1, 'Stayed': 0, 'Joined': 0}
    }

    for col_name, mapping in binary_mappings.items():
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0))
        for key, val in mapping.items():
            df = df.withColumn(col_name, when(col(col_name) == key, val).otherwise(col(col_name)))

    # One-hot encode multi-category columns with null handling
    ohe_columns = {
        'offer': 'offer',
        'contract': 'contract',
        'internet_type': 'internet_type',
        'payment_method': 'payment_method'
    }

    for col_name, prefix in ohe_columns.items():
        categories = [row[col_name] for row in df.select(col_name).distinct().collect()]
        for category in categories:
            label = str(category) if category is not None else 'nan'
            df = df.withColumn(f"{prefix}_{label}", (col(col_name) == category).cast('int'))
        df = df.drop(col_name)

    # Replace missing numeric values
    revenue_cols = [
        'monthly_charge', 'total_charges', 'total_refunds',
        'total_extra_data_charges', 'total_long_distance_charges',
        'total_revenue', 'avg_monthly_long_distance_charges', 'avg_monthly_gb_download'
    ]
    num_cols = ['age', 'number_of_referrals', 'tenure_in_months']

    for col_name in revenue_cols:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
        df = df.withColumn(col_name, when(col(col_name) < 0, 0).otherwise(col(col_name)))
        df = df.withColumn(f"{col_name}_log", log1p(col(col_name)))

    for col_name in num_cols:
        median_val = df.approxQuantile(col_name, [0.5], 0.01)[0]
        df = df.withColumn(col_name, when(col(col_name).isNull(), median_val).otherwise(col(col_name)))

    # Standard scale numerical columns
    log_cols = [f"{col}_log" for col in revenue_cols]
    scale_cols = log_cols + num_cols

    vec_assembler = VectorAssembler(inputCols=scale_cols, outputCol="features_vector")
    scaler = StandardScaler(inputCol="features_vector", outputCol="scaled_features", withMean=True, withStd=True)

    df = vec_assembler.transform(df)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # Extract scaled features back to columns (optional if model takes vector input)
    from pyspark.ml.functions import vector_to_array
    df = df.withColumn("scaled_array", vector_to_array("scaled_features"))

    for idx, col_name in enumerate(scale_cols):
        df = df.withColumn(f"{col_name}_scaled", col("scaled_array")[idx])

    df = df.drop("features_vector", "scaled_features", "scaled_array")
    df = df.drop(*revenue_cols, *log_cols, *num_cols)

    df = df.fillna(0)

    return df

# Step 5: Write logic to save to cleaned table
def write_to_postgres(df, batch_id):
    df.write \
        .jdbc(url=jdbc_url, table="customer_events.telecom_customer_events_transformed", mode="append", properties=db_properties)

# Step 6: Start micro-batch loop (simulating streaming)
if __name__ == "__main__":
    print("Starting micro-batch pipeline every 30 seconds...")

    while True:
        raw_df = load_source_table()

        if not raw_df.head(1):
            print("No new data to process. Sleeping for 30 seconds...")
        else:
            transformed_df = transform(raw_df)
            write_to_postgres(transformed_df, batch_id=None)
            print("Batch written. Sleeping for 30 seconds...")
        time.sleep(30)
