import pyspark
import pickle
import pandas as pd
import joblib
import numpy as np
import os
import datetime
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

# --- Step 3: Define PostgreSQL connection properties ---
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
def load_transformed_data_for_prediction(spark_session, jdbc_url, db_properties, table_name, last_processed_date=None):
    """
    Loads ALL relevant columns from the transformed table into a Spark DataFrame.
    Includes 'uploaded_date' for batch filtering and output.
    
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
        last_processed_date (datetime.date, optional): The date of the last processed batch.
                                                      Data newer than this date will be loaded.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame containing the selected data.

    Raises:
        Exception: If there's an error during data loading.
    """
    print(f"Attempting to load data from {table_name} for prediction using a subquery.")
    try:
        # 'uploaded_date' is included here as it's the batch identifier from the source table.
        query_string = f"""
        (SELECT
            CURRENT_TIMESTAMP AS uploaded_date,
            gender,
            married,
            number_of_dependents,
            phone_service,
            multiple_lines,
            internet_service,
            online_security,
            online_backup,
            device_protection_plan,
            premium_tech_support,
            streaming_tv,
            streaming_movies,
            streaming_music,
            unlimited_data,
            paperless_billing,
            "offer_Offer A",
            "offer_Offer B",
            "offer_Offer C",
            "offer_Offer D",
            "offer_Offer E",
            offer_nan,
            "contract_Month-to-Month",
            "contract_One Year",
            "contract_Two Year",
            "internet_type_Cable",
            "internet_type_DSL",
            "internet_type_Fiber Optic", 
            internet_type_nan,
            "payment_method_Bank Withdrawal", 
            "payment_method_Credit Card",     
            "payment_method_Mailed Check",
            monthly_charge_log_scaled,
            total_charges_log_scaled,
            total_refunds_log_scaled,
            total_extra_data_charges_log_scaled,
            total_long_distance_charges_log_scaled,
            total_revenue_log_scaled,
            avg_monthly_long_distance_charges_log_scaled,
            avg_monthly_gb_download_log_scaled,
            age_scaled,
            number_of_referrals_scaled,
            tenure_in_months_scaled
        FROM {table_name}
        """
        # Add WHERE clause for batch processing if last_processed_date is provided
        if last_processed_date:
            # Ensure the date format matches how it's stored in PostgreSQL
            query_string += f"""
            WHERE "uploaded_date" > '{last_processed_date.strftime('%Y-%m-%d')}'
            """
        query_string += f""") AS data_to_predict""" # Close the subquery alias
        
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

# Helper function to check if a table exists in PostgreSQL
def does_table_exist(spark_session, jdbc_url, db_properties, table_name):
    """
    Checks if a given table exists in the PostgreSQL database.
    """
    try:
        # Query information_schema.tables to check for table existence
        schema_name, table_only_name = table_name.split('.')
        check_query = f"""
        (SELECT 1
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}' AND table_name = '{table_only_name}') AS table_exists_check
        """
        df = spark_session.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", check_query) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .load()
        return not df.rdd.isEmpty()
    except Exception as e:
        # If there's any error (e.g., schema doesn't exist or permissions), assume table doesn't exist or can't be accessed.
        print(f"Error checking table existence for {table_name}: {e}")
        return False


# --- Step 5: Determine last processed date and load transformed data for prediction ---
# Determine the last processed date from the prediction table for incremental loading
last_processed_date = None
prediction_output_table = "customer_events.customer_events_prediction"

print(f"Attempting to determine last processed date from {prediction_output_table}.")

# Check if the prediction output table already exists
if does_table_exist(spark, jdbc_url, db_properties, prediction_output_table):
    try:
        # Query the max uploaded_date only if the table exists
        max_date_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"(SELECT MAX(uploaded_date) AS max_date FROM {prediction_output_table}) AS max_date_query") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .load()

        # If the DataFrame is not empty and max_date is not None, use it
        if not max_date_df.rdd.isEmpty() and max_date_df.collect()[0]['max_date'] is not None:
            last_processed_date = max_date_df.collect()[0]['max_date']
            print(f"Found last processed date: {last_processed_date}")
        else:
            print(f"Prediction table '{prediction_output_table}' exists but is empty. Loading all available data.")
    except Exception as e:
        # This catch is for errors *during* querying max_date, not table existence itself
        print(f"Error querying max_date from {prediction_output_table}: {e}")
        print("Proceeding to load all available data from source table (no incremental filter applied).")
else:
    print(f"Prediction table '{prediction_output_table}' does not exist. This is likely the first run. Loading all available data.")

# Call the function to load data, passing the determined last_processed_date
transformed_df_spark = load_transformed_data_for_prediction(
    spark, jdbc_url, db_properties, "customer_events.telecom_customer_events_transformed", last_processed_date
)

# Convert Spark DataFrame to Pandas DataFrame for model prediction
transformed_df_pandas = transformed_df_spark.toPandas()

# If no new data was loaded, stop the pipeline early
if transformed_df_pandas.empty:
    print("No new data to process based on uploaded_date. Exiting pipeline.")
    spark.stop()
    exit() # Exit the script

transformed_df_pandas = transformed_df_pandas.rename(columns={
    'internet_type_cable': 'internet_type_Cable',
    'internet_type_dsl': 'internet_type_DSL'
})

# Create a 'row_id' from the Pandas DataFrame index to act as a unique identifier.
# This is generated *after* loading, not retrieved from the source table.
transformed_df_pandas['row_id'] = transformed_df_pandas.index

# Separate 'row_id' and 'uploaded_date' from the DataFrame before prediction.
# 'uploaded_date' is from the source data and is now used for batch identification.
# Both are dropped before feeding to the model as they are not features.
df_for_prediction = transformed_df_pandas.drop(columns=['row_id', 'uploaded_date'])

# --- Step 6: Load the model and make predictions ---
loaded_model = None
predictions = np.array([])
probabilities = np.array([])

model_file_path = model_path

try:
    # 1. Load the model
    # It's good practice to assign the loaded object to a new, clear variable name
    loaded_model = joblib.load(model_file_path)
    print(f"Model loaded successfully from '{model_file_path}'.")

    # 2. Verify the type of the loaded object
    print(f"Type of loaded object: {type(loaded_model)}")

    predictions = loaded_model.predict(df_for_prediction)
    print(f"Predictions: {predictions}")

    # You can also get prediction probabilities if your model supports it
    if hasattr(loaded_model, 'predict_proba'):
        # For binary classification, predict_proba returns probabilities for both classes.
        # We store both.
        probabilities = loaded_model.predict_proba(df_for_prediction) 
        print(f"Prediction Probabilities: {probabilities}")
    else:
        print("Model does not support predict_proba.")
        # If probabilities are not available, we can set them to NaN or handle as appropriate.
        probabilities = np.full((len(predictions), 2), np.nan) # Placeholder if predict_proba is not available


except FileNotFoundError:
    print(f"Error: The file '{model_file_path}' was not found. Please ensure the file exists in the correct directory.")
except AttributeError as e:
    print(f"AttributeError: {e}. This likely means the loaded object is not a valid model.")
    print("Please ensure that 'clf' (your classifier) was the object dumped to the file, and not a NumPy array or other data.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

# --- Step 7: Prepare data for saving to PostgreSQL ---
# Only proceed if the model was loaded and predictions were made successfully
if loaded_model is not None and len(predictions) > 0:

    # The 'uploaded_date' used here is from the source table,
    # representing the batch's original upload date.
    results_df_pandas = pd.DataFrame({
        'row_id': transformed_df_pandas['row_id'], 
        'churn_prediction': predictions,
        'churn_probability_class_0': probabilities[:, 0], # Probability of negative class
        'churn_probability_class_1': probabilities[:, 1]  # Probability of positive class
    })

    # Join the original transformed data (with its features and generated row_id)
    # with the prediction results.
    final_output_df_pandas = transformed_df_pandas.merge(
        results_df_pandas,
        on='row_id',
        how='left'
    )

    # Convert the final Pandas DataFrame back to a Spark DataFrame
    final_output_df_spark = spark.createDataFrame(final_output_df_pandas)

    # Define the target table name for saving predictions
    prediction_table_name = "customer_events.telecom_customer_events_prediction"

    # --- Step 8: Save predictions to PostgreSQL ---
    print(f"Attempting to save predictions to {prediction_table_name}.")
    try:
        final_output_df_spark.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", prediction_table_name) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .mode("overwrite") \
            .save()
        print(f"Successfully saved predictions to {prediction_table_name}.")
    except Exception as e:
        print(f"Error saving predictions to {prediction_table_name}: {e}")
        # Re-raise the exception after logging to ensure failures are propagated.
        raise 
else:
    print("Skipping saving predictions as model loading or prediction failed or no data was processed.")


# --- Step 9: Stop Spark session ---
# Terminates the SparkSession, releasing all associated resources.
spark.stop()
print("Spark Session stopped.")