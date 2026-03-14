from utils.spark_session import create_spark_session
from utils.config import RAW_DATA_PATH
from utils.logger import setup_logger
from pyspark.sql.functions import col, count, when

def run_data_quality_checks():
    logger = setup_logger()

    spark = create_spark_session()
    logger.info("Loading dataset...")


    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_DATA_PATH)
    )

    # Check total row
    logger.info("Checking total number of rows...")
    logger.info(f"Total number of rows: {df.count()}")

     # -----------------------------
    # Null values check
    # -----------------------------

    logger.info("Checking for null values...")
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])

    null_counts.show()

     # -----------------------------
    # Duplicate transactions IDs check
    # -----------------------------

    logger.info("Checking for duplicate transactions IDs...")
    
    duplicate_count = df.groupBy("transaction_id").count().filter(col("count") > 1).count()
    logger.info(f"Number of duplicate transaction IDs: {duplicate_count}")


     # -------------------------------
    # Negative transaction amounts check
    # -------------------------------
    logger.info("Checking for negative transaction amounts...")
    negative_amount_count = df.filter(col("amount") < 0).count()
    logger.info(f"Number of transactions with negative amounts: {negative_amount_count}")

    # -------------------------------
    # Basic statistics
    # -------------------------------

    logger.info("Calculating transaction amounts statistics...")
    df.select("amount").describe().show()

if __name__ == "__main__":
    run_data_quality_checks()

    