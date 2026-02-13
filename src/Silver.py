# Silver Layer -- Data Cleaning and Standardization

import duckdb
import boto3
from dotenv import load_dotenv
from Bronze import BronzeLayer
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from sql.transformations import ( SILVER_STAGE1_CAST_TYPES, SILVER_STAGE2_STANDARDIZATION, SILVER_STAGE3_QUALITY_CHECK, SILVER_FINAL_TABLE, DATA_QUALITY_REPORT)

class SilverLayer:

    def __init__(self, conn):
        self.conn = conn

    def _quality_rules_validation(self):
        rules = [
            ('min_age', config.MIN_AGE),
            ('max_age', config.MAX_AGE),
            ('min_chol', config.MIN_CHOLESTEROL),
            ('max_chol', config.MAX_CHOLESTEROL),
            ('min_trestbps', config.MIN_BLOOD_PRESSURE),
            ('max_trestbps', config.MAX_BLOOD_PRESSURE),
            ('min_thalch', config.MIN_HEART_RATE),
            ('max_thalch', config.MAX_HEART_RATE),
            ('min_oldpeak', config.MIN_ST_DEPRESSION),
            ('max_oldpeak', config.MAX_ST_DEPRESSION)
        ]

        for name, value in rules:
            if value is None:
                raise ValueError("Data quality rule is not set in the config.")
            if not isinstance(value, (int, float)):
                raise ValueError("Data quality rule" + name + "must be a numeric.")
            if not (0 <= value <= 1000):
                raise ValueError("Data quality rule" + name + "is out of reasonable range.")
        return True
    
    def data_cleaning_and_standardization(self):
        print("\n Starting Silver Layer transformations: Data Cleaning and Standardization")
        print("\n Stage 1: Type Casting")
        self.conn.execute(SILVER_STAGE1_CAST_TYPES)
        stage1_count = self.conn.execute("SELECT COUNT(*) FROM silver_stage1_typed").fetchone()[0]
        print("\n Stage 1 completed. Records in silver_stage1_typed: " + str(stage1_count))

        print("\n Stage 2: Standardization")
        self.conn.execute(SILVER_STAGE2_STANDARDIZATION)
        stage2_count = self.conn.execute("SELECT COUNT(*) FROM silver_stage2_standardized").fetchone()[0]
        print("\n Stage 2 completed. Records in silver_stage2_standardized: " + str(stage2_count))

        print("\n Sample records after Standardization:")
        sample = self.conn.execute("""
                                   SELECT sex, chest_pain_type, resting_ecg, thalassemia,has_heart_disease
                                   FROM silver_stage2_standardized LIMIT 5
                                   """).fetchdf()
        
        print(sample)

        print("\n Stage 3: Data Quality Checks")
        self._quality_rules_validation()
        quality_rules = config.get_quality_rules()
        self.conn.execute(SILVER_STAGE3_QUALITY_CHECK, quality_rules)

        quality_stats = self.conn.execute("""
            SELECT
                COUNT(*) AS total_records,
                SUM(CASE WHEN has_quality_issues THEN 1 ELSE 0 END) AS records_with_issues,
                COUNT(*) - SUM(CASE WHEN has_quality_issues THEN 1 ELSE 0 END) AS clean_records
            FROM silver_stage3_validated
        """).fetchone()

        print("\n Stage 3 completed. Data Quality Summary:")
        print("Total Records: " + str(quality_stats[0]))
        print("Records with Quality Issues: " + str(quality_stats[1]))  
        print("Clean Records: " + str(quality_stats[2]))    

        print("\n Creating final silver_heart_disease table with clean records only")
        self.conn.execute(SILVER_FINAL_TABLE)
        silver_count = self.conn.execute("SELECT COUNT(*) FROM silver_heart_disease").fetchone()[0]
        print("\n Silver Layer processing completed. Records in silver_heart_disease: " + str(silver_count))

        return self.conn
    
    def display_quality_report(self):
        print("\n Data Quality Report")
        report = self.conn.execute(DATA_QUALITY_REPORT).fetchdf()
        print(report)

    def display_age_group_distribution(self):
        print("\n Age Group Distribution")
        age_distribution = self.conn.execute("""
            SELECT 
                CASE 
                    WHEN age < 30 THEN '<30'
                    WHEN age BETWEEN 30 AND 39 THEN '30-39'
                    WHEN age BETWEEN 40 AND 49 THEN '40-49'
                    WHEN age BETWEEN 50 AND 59 THEN '50-59'
                    WHEN age >= 60 THEN '60+'
                END AS age_group,
                COUNT(*) AS count
            FROM silver_heart_disease
            GROUP BY age_group
            ORDER BY age_group
        """).fetchdf()
        print(age_distribution)

    def save_to_S3(self, local_path="/tmp/silver_heart_disease.parquet"):
        print("\n Preparing to export Silver layer to S3")

        self.conn.execute(
            "COPY silver_heart_disease TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)", [local_path])

        S3_client = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
        )
        S3_key = config.SILVER_PREFIX + "silver_layer_heart_data.parquet"

        try:
            S3_client.upload_file(local_path, config.TARGET_BUCKET, S3_key)
            print("Silver layer successfully exported to S3 at: s3://{}/{}".format(config.TARGET_BUCKET, S3_key))
        except Exception as e:
            print("Error uploading Silver layer to S3: {}".format(str(e)))
            print("Silver Layer data saved locally at: {}".format(local_path))

        if os.path.exists(local_path):
            os.remove(local_path)

def main():
    load_dotenv()
    config.validate_config()

    bronze = BronzeLayer()
    conn = bronze.raw_data_ingestion()

    silver = SilverLayer(conn)
    silver.data_cleaning_and_standardization()
    silver.display_quality_report()
    silver.display_age_group_distribution()
    silver.save_to_S3()

    print("\n Silver Layer processing completed successfully.")
    bronze.close()

if __name__ == "__main__":
    main()