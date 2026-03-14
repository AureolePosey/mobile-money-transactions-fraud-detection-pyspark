from pipeline.ingestion import ingest_transactions_data
from pipeline.feature_engineering import run_feature_engineering
from pipeline.fraud_detection import detect_fraud
from pipeline.data_quality import run_data_quality_checks
from pipeline.cleaning import clean_transactions


def run_pipeline():
    print("Starting fraud detection pipeline...")

    print("Step 1 : Ingestion")
    ingest_transactions_data()


    print("Step 2 : Data Quality Audit")
    run_data_quality_checks()

    print("Step 3 : Data Cleaning")
    clean_transactions()

    print("Step 4 : Feature Engineering")
    run_feature_engineering()

    print("Step 5 : Fraud Detection")
    detect_fraud()

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()