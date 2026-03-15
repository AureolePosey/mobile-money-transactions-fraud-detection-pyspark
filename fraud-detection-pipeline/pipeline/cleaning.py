from utils.spark_session import create_spark_session
from utils.logger import setup_logger
from utils.config import RAW_DATA_PATH
from pyspark.sql.functions import col, to_date # Ajout de to_date

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

    # Création de la colonne de date pour le partitionnement futur
    # On transforme le timestamp en date simple (YYYY-MM-DD)
    df = df.withColumn("transaction_date", to_date(col("timestamp")))
    ## Nettoyage des données : suppression des doublons, des valeurs nulles et des montants négatifs
    df = df.dropDuplicates(["transaction_id"])
    df = df.filter(col("amount") >= 0)
    df = df.dropna()

    return df

if __name__ == "__main__":
    logger = setup_logger() # Pour éviter l'erreur NameError dans le main
    cleaned_df = clean_transactions()

    cleaned_df.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet("data/clean/transactions_clean")

    logger.info("Clean dataset saved and partitioned by date")