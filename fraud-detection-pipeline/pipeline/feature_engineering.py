from utils.spark_session import create_spark_session
from pyspark.sql.functions import col, when, count,avg,count,to_date
from pyspark.sql.window import Window


def run_feature_engineering():
    print("Starting feature engineering...")

    spark = create_spark_session()

    print("Loading clean dataset...")
    df = spark.read.parquet("data/clean/transactions_clean")

    # -----------------------------
    # Convert timestamp
    # -----------------------------

    df = df.withColumn("transaction_date", to_date(col("timestamp")))

     # --------------------------------
    # Average transaction per user
    # --------------------------------
    window_user = Window.partitionBy("user_id")
    df = df.withColumn("avg_transaction_amount_per_user", 
    avg(col("amount")).over(window_user))

      # --------------------------------
    # Transactions per user per day
    # --------------------------------

    window_user_day = Window.partitionBy("user_id", "transaction_date")
    df = df.withColumn("transactions_per_user_per_day",
    count("transaction_id").over(window_user_day))

    # --------------------------------
    # High amount flag
    # --------------------------------

    df = df.withColumn(
        "high_amount_flag",
        when(col("amount") > 1000000, 1).otherwise(0)
    )

   
    print("Feature Engineering completed")

    df.show(10)
    
    return df

if __name__ == "__main__":
    df_features = run_feature_engineering()

    df_features.write \
        .mode("overwrite") \
        .parquet("data/curated/transactions_features")

    print("Features saved successfully")
