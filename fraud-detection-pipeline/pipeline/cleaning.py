from utils.spark_session import create_spark_session
from utils.config import RAW_DATA_PATH
from pyspark.sql.functions import col


def clean_transactions():
    spark = create_spark_session()
    print("Loading dataset...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_DATA_PATH)
    )

    print("Initial number of rows:", df.count())

   #------------------------------
   # Remove duplicates
   #------------------------------


    df = df.dropDuplicates(["transaction_id"])
    print("Number of rows after removing duplicates:", df.count())

    #------------------------------
    # Remove transactions with negative amounts
    #------------------------------
    df = df.filter(col("amount") >= 0)
    print("Number of rows after filtering out negative amounts:", df.count())

    #------------------------------
    # handle null values - for simplicity, we will drop rows with any nulls
    #------------------------------
    df = df.dropna()
    print("After removing nulls values:", df.count())

    return df

if __name__ == "__main__":
    cleaned_df = clean_transactions()

    cleaned_df.write \
        .mode("overwrite") \
        .parquet("data/clean/transactions_clean")

    print("Clean dataset saved successfully")
    