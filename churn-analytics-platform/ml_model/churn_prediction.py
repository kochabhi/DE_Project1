import pyspark
import pickle
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, max as spark_max

# --- Step 1: Create Spark session ---
# Builds and retrieves a SparkSession.
# Configures a Spark package for PostgreSQL JDBC driver, necessary for connecting to Postgres.
spark = SparkSession.builder \
    .appName("PostgresToPostgresChurnPipeline") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

# Set Spark log level to WARN to reduce the amount of log output, focusing on important messages.
spark.sparkContext.setLogLevel("WARN")
print("Spark Session created successfully.")

# --- Step 2: Load churn model (Python-native model) and extract expected features ---
script_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(script_dir, 'churn_model.pkl')

# --- Step 2: Define PostgreSQL connection properties ---
# JDBC URL for connecting to the PostgreSQL database.
# Assumes PostgreSQL is running on 'postgres' host at port 5432, with database 'postgres'.
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"

# Dictionary containing database authentication and driver details.
db_properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}
print("PostgreSQL connection properties defined.")

# --- Step 4: Define data loading logic (load all relevant columns for prediction) ---
def load_transformed_data_for_prediction(spark_session, jdbc_url, db_properties, table_name):
    """
    Loads ALL relevant columns from the transformed table into a Spark DataFrame.
    
    This function constructs a SQL subquery to explicitly select and alias columns
    from the specified PostgreSQL table. This subquery is then passed to Spark's
    JDBC reader, pushing down the column selection to the database, which can be
    more efficient than loading the entire table and then selecting columns in Spark.
    All column names are explicitly double-quoted to handle case sensitivity in PostgreSQL.

    Args:
        spark_session (SparkSession): The active SparkSession.
        jdbc_url (str): The JDBC URL for the PostgreSQL database.
        db_properties (dict): A dictionary containing database connection properties (user, password, driver).
        table_name (str): The full name of the transformed table (e.g., "schema.table_name").

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame containing the selected data.

    Raises:
        Exception: If there's an error during data loading.
    """
    print(f"Attempting to load data from {table_name} for prediction using a subquery.")
    try:
        query_string = f"""
        (SELECT
            "gender" AS gender,
            "married" AS married,
            "number_of_dependents" AS number_of_dependents,
            "phone_service" AS phone_service,
            "multiple_lines" AS multiple_lines,
            "internet_service" AS internet_service,
            "online_security" AS online_security,
            "online_backup" AS online_backup,
            "device_protection_plan" AS device_protection_plan,
            "premium_tech_support" AS premium_tech_support,
            "streaming_tv" AS streaming_tv,
            "streaming_movies" AS streaming_movies,
            "streaming_music" AS streaming_music,
            "unlimited_data" AS unlimited_data,
            "paperless_billing" AS paperless_billing,
            "offer_nan" AS offer_nan,
            "internet_type_Cable" AS internet_type_Cable,
            "internet_type_DSL" AS internet_type_DSL,
            "internet_type_nan" AS internet_type_nan,
            "monthly_charge_log_scaled" AS monthly_charge_log_scaled,
            "total_charges_log_scaled" AS total_charges_log_scaled,
            "total_refunds_log_scaled" AS total_refunds_log_scaled,
            "total_extra_data_charges_log_scaled" AS total_extra_data_charges_log_scaled,
            "total_long_distance_charges_log_scaled" AS total_long_distance_charges_log_scaled,
            "total_revenue_log_scaled" AS total_revenue_log_scaled,
            "avg_monthly_long_distance_charges_log_scaled" AS avg_monthly_long_distance_charges_log_scaled,
            "avg_monthly_gb_download_log_scaled" AS avg_monthly_gb_download_log_scaled,
            "age_scaled" AS age_scaled,
            "number_of_referrals_scaled" AS number_of_referrals_scaled,
            "tenure_in_months_scaled" AS tenure_in_months_scaled
        FROM {table_name}) AS data_to_predict
        """
        
        # Use the more explicit `format("jdbc").option(...)` syntax for reading data.
        # This approach is often more robust and flexible for JDBC connections.
        df_spark = spark_session.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query_string) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .option("fetchsize", "10000") \
            .load()

        print(f"Successfully loaded data from {table_name}.")
        return df_spark

    except Exception as e:
        # Catch and log any exceptions that occur during data loading.
        print(f"Error loading data from {table_name}: {e}")
        # Re-raise the exception after logging, allowing the calling code to handle it further.
        raise

# --- Step 5: Load transformed data for prediction ---
# Call the function to load data from the specified transformed table.
transformed_df_spark = load_transformed_data_for_prediction(
    spark, jdbc_url, db_properties, "customer_events.telecom_customer_events_transformed"
)

transformed_df_pandas = transformed_df_spark.toPandas()
transformed_df_pandas = transformed_df_pandas.rename(columns={
    'internet_type_cable': 'internet_type_Cable',
    'internet_type_dsl': 'internet_type_DSL'
})

import joblib
import numpy as np # Often needed for data for prediction

model_file_path = model_path

try:
    # 1. Load the model
    # It's good practice to assign the loaded object to a new, clear variable name
    loaded_model = joblib.load(model_file_path)
    print(f"Model loaded successfully from '{model_file_path}'.")

    # 2. Verify the type of the loaded object
    print(f"Type of loaded object: {type(loaded_model)}")

    predictions = loaded_model.predict(transformed_df_pandas)
    print(f"Predictions: {predictions}")

    # You can also get prediction probabilities if your model supports it
    if hasattr(loaded_model, 'predict_proba'):
        probabilities = loaded_model.predict_proba(transformed_df_pandas)
        print(f"Prediction Probabilities: {probabilities}")

except FileNotFoundError:
    print(f"Error: The file '{model_file_path}' was not found. Please ensure the file exists in the correct directory.")
except AttributeError as e:
    print(f"AttributeError: {e}. This likely means the loaded object is not a valid model.")
    print("Please ensure that 'clf' (your classifier) was the object dumped to the file, and not a NumPy array or other data.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")


# --- Step 9: Stop Spark session ---
# Terminates the SparkSession, releasing all associated resources.
spark.stop()
print("Spark Session stopped.")