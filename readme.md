# A Simple ETL-Pipeline : For a Health Data Warehouse

A python and SQL based pipeline for using raw data from an AWS bucket and transforms it using a medallion-based warehouse pipeline system (Bronze, Silver, Gold) and exports the final curated data result to another AWS bucket for easier access.

## Medallion Architecture

![Diagram](Medallion Arch-(Warehouse).drawio.svg)

The Architecture follows the basic 3 step process of Bronze, Silver and Gold using DuckDB for implementing SQL transformations and AWS S3 buckets for storage.

**Bronze Layer - Ingests the raw data stored as a CSV file in a separate S3 bucket and creates a table using that data, making further operations on it much easier.

**Silver Layer - Using the created table from the Bronze layer, it performs Data cleaning and Standardization in a 3 stage process of typecasting and column renaming, value standardization and data quality validation against adjustable threshholds.

**Gold Layer - By combining the work done during the Bronze and Silver layers, business-level aggregations like Risk factor analysis, Severity Distribution, Clinical metrics and a fact table which is helpful for visualizations is created in this layer.

## File Structure

```
Simple-ETL-pipeline/
├── config/
│   ├── __init__.py
│   └── config.py          # AWS credentials, S3 paths, data quality thresholds
├── sql/
│   ├── __init__.py
│   └── transformations.py  # All SQL for Bronze, Silver, and Gold layers
├── src/
│   ├── Bronze.py           # Raw ingestion from S3
│   ├── Silver.py           # Cleaning, standardization, quality checks
│   ├── Gold.py             # Aggregations and PowerBI export
│   └── pipeline.py         # Orchestrator — runs all layers end-to-end
├── .env.example
├── .gitignore
└── requirements.txt
```

## Setup

** Step 1: Install Dependencies from the requirements.txt file
        
        ```bash
        pip install -r requirements.txt
        ```
** Step 2: Configure AWS credentials by following the .env example given

        Copy `.env.example` to `.env` and fill in your AWS credentials and bucket details:

        ```
        AWS_ACCESS_KEY_ID=your-access-key
        AWS_SECRET_ACCESS_KEY=your-secret-key
        AWS_REGION=us-east-1
        SOURCE_BUCKET=your-source-bucket
        SOURCE_KEY=heart_disease_uci.csv
        TARGET_BUCKET=your-target-bucket
        ```
** Step 3: Run the pipeline.py file from src folder

        ```bash
        python src/pipeline.py
        ```
    Or

    you can run the Layers individually like

        ```bash
        python python src/pipeline.py --layer bronze  # -- for individual Bronze Layer
        ```

        ```bash
        python python src/pipeline.py --layer silver  # -- for individual Silver Layer
        ```

        ```bash
        python python src/pipeline.py --no-s3  # -- for skipping S3 export and creates only local files(mostly for testing, as the files are created temporarily)
        ```

        ```bash
        python python src/pipeline.py --no-powerbi  # -- for skipping creation of the fact table for visualizations
        ```

## Tech Stack

- **DuckDB** — In-process SQL engine for transformations
- **AWS S3** — Source and target storage (via boto3 and DuckDB's httpfs extension)
- **Pandas** — Data display and inspection
- **Python-dotenv** — Environment variable management