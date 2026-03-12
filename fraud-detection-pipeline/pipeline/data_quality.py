from utils.spark_session import create_spark_session
from utils.config import RAW_DATA_PATH
from pyspark.sql.functions import col, count, when

def run_data_quality_checks():

    spark = create_spark_session()
    print("Loading dataset...")


    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_DATA_PATH)
    )

    # Check total row
    print("Total number of rows:")
    print(df.count())

     # -----------------------------
    # Null values check
    # -----------------------------

    print("\nChecking null values...")
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])

    null_counts.show()

     # -----------------------------
    # Duplicate transactions IDs check
    # -----------------------------

    print("\nChecking for duplicate transactions IDs...")
    
    duplicate_count = df.groupBy("transaction_id").count().filter(col("count") > 1).count()
    print(f"Number of duplicate transaction IDs: {duplicate_count}")


     # -------------------------------
    # Negative transaction amounts check
    # -------------------------------
    print("\nChecking for negative transaction amounts...")
    negative_amount_count = df.filter(col("amount") < 0).count()
    print(f"Number of transactions with negative amounts: {negative_amount_count}")

    # -------------------------------
    # Basic statistics
    # -------------------------------

    print("\nTransaction amounts statistics:")
    df.select("amount").describe().show()

if __name__ == "__main__":
    run_data_quality_checks()

    