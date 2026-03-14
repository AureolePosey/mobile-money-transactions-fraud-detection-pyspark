from pipeline.ingestion import ingest_transactions_data
from pipeline.feature_engineering import run_feature_engineering
from pipeline.fraud_detection import detect_fraud
from pipeline.data_quality import run_data_quality_checks
from pipeline.cleaning import clean_transactions
from utils.logger import setup_logger

logger = setup_logger()


def run_pipeline():
    logger.info("Starting fraud detection pipeline...")

    logger.info("Step 1 : Ingestion")
    ingest_transactions_data()


    logger.info("Step 2 : Data Quality Audit")
    run_data_quality_checks()

    logger.info("Step 3 : Data Cleaning")
    clean_transactions()

    logger.info("Step 4 : Feature Engineering")
    run_feature_engineering()

    logger.info("Step 5 : Fraud Detection")
    detect_fraud()

    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()