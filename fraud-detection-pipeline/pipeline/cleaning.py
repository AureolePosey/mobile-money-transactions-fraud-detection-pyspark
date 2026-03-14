from utils.spark_session import create_spark_session
from utils.logger import setup_logger
from utils.config import RAW_DATA_PATH
from pyspark.sql.functions import col


def clean_transactions():
    spark = create_spark_session()
    logger = setup_logger()
    logger.info("Loading raw dataset")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_DATA_PATH)
    )

    logger.info("Initial number of rows: %d", df.count())

   #------------------------------
   # Remove duplicates
   #------------------------------


    df = df.dropDuplicates(["transaction_id"])
    logger.info("Number of rows after removing duplicates: %d", df.count())

    #------------------------------
    # Remove transactions with negative amounts
    #------------------------------
    df = df.filter(col("amount") >= 0)
    logger.info("Number of rows after filtering out negative amounts: %d", df.count())

    #------------------------------
    # handle null values - for simplicity, we will drop rows with any nulls
    #------------------------------
    df = df.dropna()
    logger.info("Number of rows after removing nulls: %d", df.count())

    return df

if __name__ == "__main__":
    cleaned_df = clean_transactions()

    cleaned_df.write \
        .mode("overwrite") \
        .parquet("data/clean/transactions_clean")

    logger.info("Clean dataset saved successfully")
    