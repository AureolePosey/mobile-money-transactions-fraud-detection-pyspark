from utils.spark_session import create_spark_session
from utils.logger import setup_logger
from pyspark.sql.functions import col, when


def detect_fraud():

    spark = create_spark_session()
    logger = setup_logger()

    logger.info("Loading features dataset...")

    df = spark.read.parquet("data/curated/transactions_features")

    # --------------------------------
    # Rule 1 : very high transaction
    # --------------------------------

    df = df.withColumn(
        "rule_high_amount",
        when(col("amount") > 1000000, 1).otherwise(0)
    )

    # --------------------------------
    # Rule 2 : too many transactions
    # --------------------------------

    df = df.withColumn(
        "rule_too_many_transactions",
        when(col("transactions_per_user_per_day") > 10, 1).otherwise(0)
    )

    # --------------------------------
    # Rule 3 : abnormal amount
    # --------------------------------

    df = df.withColumn(
        "rule_abnormal_amount",
        when(col("amount") > (col("avg_transaction_amount_per_user") * 5), 1).otherwise(0)
    )

    # --------------------------------
    # Final fraud flag
    # --------------------------------

    df = df.withColumn(
        "fraud_flag",
        when(
            (col("rule_high_amount") == 1) |
            (col("rule_too_many_transactions") == 1) |
            (col("rule_abnormal_amount") == 1),
            1
        ).otherwise(0)
    )

    logger.info("Fraud detection completed")

    df.show(10)

    return df


if __name__ == "__main__":

    df_fraud = detect_fraud()

    df_fraud.write \
        .mode("overwrite") \
        .parquet("data/analytics/fraud_transactions")

    logger.info("Fraud dataset saved")