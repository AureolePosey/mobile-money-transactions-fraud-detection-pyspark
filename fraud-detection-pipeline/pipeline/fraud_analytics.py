from utils.spark_session import create_spark_session
from utils.logger import setup_logger # Import du logger
from pyspark.sql.functions import col, count

def run_fraud_analytics():
    spark = create_spark_session()
    logger = setup_logger() # Initialisation

    logger.info("--- Starting Fraud Analytics Report ---")

    df = spark.read.parquet("data/analytics/fraud_transactions")

    # Fraud rate
    total_transactions = df.count()
    fraud_transactions = df.filter(col("fraud_flag") == 1).count()
    
    # Sécurité pour éviter la division par zéro si le dataset est vide
    fraud_rate = (fraud_transactions / total_transactions) * 100 if total_transactions > 0 else 0

    logger.info(f"Total transactions analyzed: {total_transactions}")
    logger.info(f"Fraud transactions detected: {fraud_transactions}")
    logger.info(f"Current Fraud Rate: {fraud_rate:.2f}%")

    # Top fraud cities
    logger.info("Top cities by fraud volume:")
    # On peut convertir le résultat en texte pour le logger si besoin, 
    # mais le .show() reste le plus lisible pour toi en direct
    df.filter(col("fraud_flag") == 1) \
        .groupBy("city") \
        .count() \
        .orderBy("count", ascending=False) \
        .show()

    # Top fraud operators
    logger.info("Fraud distribution by operator:")
    df.filter(col("fraud_flag") == 1) \
        .groupBy("operator") \
        .count() \
        .orderBy("count", ascending=False) \
        .show()

    logger.info("--- Analytics Report Completed ---")

if __name__ == "__main__":
    run_fraud_analytics()

    # ------------------------------
    # Top fraud cities
    # ------------------------------

    print("Top fraud cities")

    df.filter(col("fraud_flag") == 1) \
        .groupBy("city") \
        .count() \
        .orderBy("count", ascending=False) \
        .show()

    # ------------------------------
    # Top fraud operators
    # ------------------------------

    print("Top fraud operators")

    df.filter(col("fraud_flag") == 1) \
        .groupBy("operator") \
        .count() \
        .orderBy("count", ascending=False) \
        .show()


if __name__ == "__main__":
    run_fraud_analytics()