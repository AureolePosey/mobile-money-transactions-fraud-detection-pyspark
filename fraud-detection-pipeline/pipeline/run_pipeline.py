from pipeline.ingestion import ingest_transactions_data
from pipeline.feature_engineering import run_feature_engineering
from pipeline.fraud_detection import detect_fraud
from pipeline.data_quality import run_data_quality_checks
from pipeline.cleaning import clean_transactions
from pipeline.fraud_analytics import run_fraud_analytics  # Ajout de l'import
from utils.logger import setup_logger

# Initialisation du logger global
logger = setup_logger()

def run_pipeline():
    logger.info("==================================================")
    logger.info(" STARTING END-TO-END FRAUD DETECTION PIPELINE")
    logger.info("==================================================")

    # Étape 1 : Ingestion
    logger.info("Step 1/6 : Ingestion - Loading raw data...")
    ingest_transactions_data()

    # Étape 2 : Audit de Qualité
    logger.info("Step 2/6 : Data Quality Audit - Checking for inconsistencies...")
    run_data_quality_checks()

    # Étape 3 : Nettoyage
    logger.info("Step 3/6 : Data Cleaning - Removing duplicates and nulls...")
    clean_transactions()

    # Étape 4 : Feature Engineering
    logger.info("Step 4/6 : Feature Engineering - Generating behavioral metrics...")
    run_feature_engineering()

    # Étape 5 : Détection de Fraude
    logger.info("Step 5/6 : Fraud Detection - Applying business rules...")
    detect_fraud()

    # Étape 6 : Analyse et Reporting Final
    logger.info("Step 6/6 : Fraud Analytics - Generating final report...")
    run_fraud_analytics()

    logger.info("==================================================")
    logger.info(" PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("==================================================")

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f" Pipeline failed with error: {str(e)}")
        raise