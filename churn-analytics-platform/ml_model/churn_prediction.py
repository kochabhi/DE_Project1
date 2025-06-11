import pandas as pd
import pickle
import os # Import the os module for path manipulation

# (Keep your other imports like time, pyspark.sql, etc.)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, max as spark_max
from pyspark.sql.types import DoubleType, IntegerType


# --- Fix for FileNotFoundError ---
# Get the absolute path to the directory where the current script (churn_prediction.py) is located
# __file__ gives the path to the current script
# os.path.abspath ensures it's a full path
# os.path.dirname extracts the directory part from the full path
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to the churn_model.pkl file
# os.path.join safely concatenates path components (handles slashes correctly across OS)
model_path = os.path.join(script_dir, 'churn_model.pkl')

# --- Step 2: Load model ---
try:
    # Use the constructed absolute path to open the pickle file
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    print(f"Churn model loaded successfully from: {model_path}")
    
    # Basic check if the model has predict_proba
    if not hasattr(model, 'predict_proba'):
        print("Warning: Loaded model does not have a 'predict_proba' method. Will attempt 'predict' instead.")
        use_proba = False
    else:
        use_proba = True

except FileNotFoundError:
    print(f"Error: 'churn_model.pkl' not found at expected path: {model_path}")
    print("Please ensure 'churn_model.pkl' exists in the same directory as 'churn_prediction.py'.")
    # If the model is critical, stop execution
    # Assuming 'spark' is defined globally in the full script
    if 'spark' in globals() and spark is not None:
        spark.stop()
    exit() # Exit the script if the model can't be loaded
except Exception as e:
    print(f"An unexpected error occurred while loading the model: {e}")
    if 'spark' in globals() and spark is not None:
        spark.stop()
    exit()


# --- Rest of your code (no changes needed for path here) ---

# Step 1: Create Spark session (should be before model loading if it handles exit)
spark = SparkSession.builder \
    .appName("PostgresToPostgresChurnPipeline") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created successfully.")


# Step 3: PostgreSQL connection info
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
db_properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}
print("PostgreSQL connection properties defined.")

# --- Step 4: Define data loading logic (to get data for prediction) ---
def load_transformed_data_for_prediction(spark_session, jdbc_url, db_properties, table_name):
    """
    Loads transformed customer data from PostgreSQL into a Spark DataFrame.
    """
    print(f"Attempting to load data from {table_name} for prediction.")
    try:
        query_string = f"""
        (SELECT
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
            contract_nan,
            internet_type_Cable,
            internet_type_DSL,
            "internet_type_Fiber Optic",
            internet_type_nan,
            "payment_method_Bank Withdrawal",
            "payment_method_Credit Card",
            "payment_method_Mailed Check",
            payment_method_nan,
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
        FROM {table_name}) as data_to_predict
        """
        
        df_spark = spark_session.read.jdbc(
            url=jdbc_url,
            table=query_string,
            properties=db_properties
        )
        print(f"Successfully loaded data from {table_name}.")
        return df_spark

    except Exception as e:
        print(f"Error loading data from {table_name}: {e}")
        return None

# --- Main execution flow ---

# Load the transformed data for prediction
transformed_df_spark = load_transformed_data_for_prediction(
    spark, jdbc_url, db_properties, "customer_events.telecom_customer_events_transformed"
)

if transformed_df_spark is not None:
    print("\nTransformed Spark DataFrame schema:")
    transformed_df_spark.printSchema()
    print(f"Transformed Spark DataFrame count: {transformed_df_spark.count()}")

    # --- Step 5: Convert Spark DataFrame to Pandas DataFrame for model inference ---
    try:
        data_for_prediction_pd = transformed_df_spark.toPandas()
        print("Converted Spark DataFrame to Pandas DataFrame.")
        print(f"Pandas DataFrame shape: {data_for_prediction_pd.shape}")

        # --- Step 6: Make predictions (probabilities) using the loaded model ---
        if model is not None:
            if use_proba:
                print("Making probability predictions with the loaded model...")
                probabilities = model.predict_proba(data_for_prediction_pd)[:, 1]
                data_for_prediction_pd['churn_probability'] = probabilities
                print("Probability predictions made successfully.")
                print(data_for_prediction_pd[['churn_probability']].head())
            else:
                print("Model does not have 'predict_proba', making binary predictions instead.")
                predictions = model.predict(data_for_prediction_pd)
                data_for_prediction_pd['churn_prediction'] = predictions
                print("Binary predictions made successfully.")
                print(data_for_prediction_pd[['churn_prediction']].head())
            
            data_for_prediction_pd['prediction_timestamp'] = pd.to_datetime('now', utc=True)

            # --- Step 7: Store predictions data in PostgreSQL ---
            predictions_df_spark = spark.createDataFrame(data_for_prediction_pd)
            print("Converted Pandas DataFrame with predictions back to Spark DataFrame.")
            predictions_df_spark.printSchema()

            target_table_name = "customer_events.telecom_customer_events_prediction"
            write_mode = "append" 

            print(f"Attempting to write predictions to {target_table_name} in {write_mode} mode.")
            predictions_df_spark.write \
                .jdbc(url=jdbc_url, table=target_table_name, mode=write_mode, properties=db_properties)
            
            print(f"Predictions successfully written to {target_table_name}.")

        else:
            print("Model not loaded, skipping prediction and storage.")

    except Exception as e:
        print(f"Error during data conversion, prediction, or storage: {e}")

else:
    print("No transformed data loaded to proceed with prediction and storage.")

# --- Step 8: Stop Spark session ---
spark.stop()
print("Spark Session stopped.")