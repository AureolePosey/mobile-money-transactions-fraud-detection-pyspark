from utils.spark_session import create_spark_session
from utils.config import RAW_DATA_PATH


def ingest_transactions_data():
    spark = create_spark_session()
    print("Reading raw transactions dataset...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_DATA_PATH)
    )
    print("Schema of dataset:")
    df.printSchema()

    print("Preview of data:")
    df.show(5)

    print("Number of rows:")
    print(df.count())
    return df


if __name__ == "__main__":
  df =  ingest_transactions_data()