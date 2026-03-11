from pyspark.sql import SparkSession


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("MobileMoneyFraudDetectionPipeline")
        .master("local[*]")
        .getOrCreate()
    )

    return spark