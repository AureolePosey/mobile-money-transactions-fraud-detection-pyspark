*Mobile Money Fraud Detection Pipeline (PySpark)*

    This project implements an end-to-end data pipeline for detecting fraudulent activities in simulated Mobile Money transactions. It processes 1,000,000+ transactions using a Medallion Architecture to ensure data quality, reliability, and traceability.

*Tech Stack*

    -Compute Engine: Apache Spark (PySpark)

    -Storage Format: Apache Parquet (Partitioned by date)

    -Environment: WSL (Windows Subsystem for Linux) / Ubuntu

    -Tools: Python, Logging, OS Path Management

*Project Architecture*

The project is built with a modular structure to ensure maintainability and scalability:

    Plaintext
    fraud-detection-pipeline/
    ├── data/                  # Data Lake layers (Raw, Clean, Curated, Analytics)
    ├── pipeline/              # Core ETL logic (Ingestion, Cleaning, Features, etc.)
    ├── utils/                 # Utilities (Logger, SparkSession, Config)
    ├── logs/                  # Pipeline execution history & monitoring
    ├── run_pipeline.py        # Central Orchestrator (Main Entry Point)
    ├── generate_dataset.py    # Synthetic transaction generator
    └── README.md

 *Pipeline Workflow*
    The pipeline is orchestrated sequentially to transform raw data into actionable business insights:

    -Ingestion: Reads raw CSV files and converts them to the optimized Parquet format.

    -Data Quality: Automated audit of data types and missing values (null checks).

    -Cleaning: Deduplication and filtering of inconsistent or negative transaction amounts.

    -Feature Engineering: Generation of behavioral features (rolling averages, daily transaction frequency per user).

    -Fraud Detection: Application of business rules (critical thresholds and relative anomalies).

    -Fraud Analytics: Final reporting on fraud rates segmented by city and operator in Benin.

 *Key Engineering Concepts*

    Parquet Partitioning: Storage optimization using transaction_date to enable high-performance queries via predicate pushdown.

    -Professional Logging System: Full execution traceability within pipeline.log, moving beyond basic print statements for production readiness.

    -Idempotence: The pipeline is designed to be re-run safely without data duplication using Spark's overwrite mode.

    -Advanced Feature Engineering: Utilizing Window Functions to calculate standard deviations and user-specific behavior shifts.

*Sample Final Report*
    Upon execution, the pipeline generates key metrics directly in the logs:

    -Total Transactions Processed: 1,000,000+

    -Detected Fraud Rate: ~1.5%

    -High-Risk Locations: Cotonou, Parakou, Abomey-Calavi.

*Installation & Usage*
    Clone the repository:

    Bash
    git clone https://github.com/AureolePosey/fraud-detection-pipeline.git
    Run the full pipeline:

    Bash
    python run_pipeline.py
