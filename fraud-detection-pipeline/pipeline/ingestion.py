from utils.spark_session import create_spark_session
from utils.logger import setup_logger
from utils.config import RAW_DATA_PATH


def ingest_transactions_data():
    logger = setup_logger()
    spark = create_spark_session()
    logger.info("Reading raw transactions dataset...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_DATA_PATH)
    )
    logger.info("Schema of dataset:")
    df.printSchema()

    logger.info("Preview of data:")
    df.show(5)

    logger.info("Number of rows:")
    logger.info(df.count())
    return df


if __name__ == "__main__":
  df =  ingest_transactions_data()