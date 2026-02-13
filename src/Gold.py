#Gold Layer -- Final Curated data, ready for analysis and reporting

import duckdb
import boto3
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from Bronze import BronzeLayer
from Silver import SilverLayer
from dotenv import load_dotenv
from sql.transformations import (
    GOLD_DEMO_SUMMARY,
    GOLD_RISK_FACTORS,
    GOLD_SEVERITY_DISTRIBUTION,
    GOLD_CLINICAL_METRICS,
    GOLD_POWERBI_FACT_TABLE,
    GET_RECORD_COUNTS
)

class GoldLayer:
    def __init__(self, conn):
        self.conn = conn
        self.gold_tables = []

    def validate_table_name(self, table_name):
        if not table_name.replace('_', '') .isalnum():
            raise ValueError("Invalid table name: " + table_name)
        if len(table_name) > 100:
            raise ValueError("Table name is too long: " + table_name)
        return table_name

    def create_aggregations(self):
        print("\n Gold Layer: Final Curated Data for Analysis")
        aggregations = [
            ("Demographics Summary", GOLD_DEMO_SUMMARY),
            ("Risk Factor Analysis", GOLD_RISK_FACTORS),
            ("Severity Distribution in Patients", GOLD_SEVERITY_DISTRIBUTION),
            ("Clinical Metrics", GOLD_CLINICAL_METRICS),
            ("PowerBI Fact Table", GOLD_POWERBI_FACT_TABLE)
        ]

        for name, sql in aggregations:
            print("\n Processing aggregations " + name)
            self.conn.execute(sql)
            table_name = sql.split("CREATE TABLE IF NOT EXISTS ")[1].split("AS")[0].strip()
            self.gold_tables.append(table_name)
            validated_name = self.validate_table_name(table_name)
            count = self.conn.execute("SELECT COUNT(*) FROM {}".format(validated_name)).fetchone()[0]
            print("\n Created Table: " + validated_name + " with " + str(count) + " records.")

    def display_demo(self):
        print("\n Demographics Summary Sample:")
        print(self.conn.execute("""
                          SELECT sex,
                                 age_group,
                                patient_count,
                                heart_disease_percentage,
                                ROUND(avg_cholesterol, 1) AS avg_cholesterol,
                                ROUND(avg_blood_pressure, 1) AS avg_bp, 
                          FROM gold_demographics_summary
                          ORDER BY sex, age_group 
                          LIMIT 10
                          """).fetchdf())
        
    def display_top_risk(self):
        print("\n Displaying Top Risk factors: ")
        print(self.conn.execute("""
                          SELECT chest_pain_type, exercise_induced_angina, patient_count, heart_disease_count, risk_percentage
                          FROM gold_risk_factors
                          WHERE patient_count >= 10
                          ORDER BY risk_percentage DESC
                          LIMIT 10
                          """).fetchdf())
    
    def display_severity_distribution(self):
        print("\n Displaying Severity Distribution among patients: ")
        print(self.conn.execute("""
                          SELECT severity_label, patient_count, percentage, ROUND(avg_age, 1) AS avg_age
                          FROM gold_severity_distribution
                          ORDER BY heart_disease_severity
                          """).fetchdf())

    def display_all_records(self):
        print("\n Displaying all records")
        counts = self.conn.execute(GET_RECORD_COUNTS).fetchdf()
        print(counts)

    def save_to_S3(self):
        print("\n Saving Gold Layer tables locally and uploading to S3 as Parquet")

        S3_client = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
        )

        for table_name in self.gold_tables:
            validated_name = self.validate_table_name(table_name)
            local_path = "/tmp/" + validated_name + ".parquet"
            self.conn.execute("COPY {} TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)".format(validated_name), [local_path])
            S3_key = config.GOLD_PREFIX + validated_name + ".parquet"

            try:
                S3_client.upload_file(local_path, config.TARGET_BUCKET, S3_key)
                print("Successfully uploaded " + validated_name + " to s3://" + config.TARGET_BUCKET + "/" + S3_key)
            except Exception as e:
                print("Error uploading " + validated_name + " to S3: " + str(e))
                print("The Gold layer table " + validated_name + " is saved locally at: " + local_path)

            if os.path.exists(local_path):
                os.remove(local_path)
                print("Local file for " + validated_name + " removed after upload.")

    def for_powerbi(self, output_path="/tmp/powerbi_fact_table.parquet"):
        print("\n Preparing curated data for PowerBI visualization")
        os.makedirs(output_path, exist_ok=True)
        for table_name in self.gold_tables:
            validated_name = self.validate_table_name(table_name)
            csv_path = os.path.join(output_path, validated_name + ".csv")
        
        self.conn.execute("COPY {} TO ? (HEADER, DELIMITER ',')".format(validated_name), [csv_path])
        print("PowerBI fact table saved locally at: " + csv_path)
        return output_path

def main():
    load_dotenv()
    config.validate_config()
    bronze = BronzeLayer()
    conn = bronze.raw_data_ingestion()
    silver = SilverLayer(conn)
    silver.data_cleaning_and_standardization()

    gold = GoldLayer(conn)
    gold.create_aggregations()
    gold.display_demo()
    gold.display_top_risk()
    gold.display_severity_distribution()
    gold.display_all_records()
    gold.save_to_S3()
    gold.for_powerbi()
    
    bronze.close()

if __name__ == "__main__":
    main()

