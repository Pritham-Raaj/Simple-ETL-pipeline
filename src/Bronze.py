#Bronze Layer -- Raw data Ingestion

import duckdb
import boto3
from datetime import datetime
from dotenv import load_dotenv
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from sql.transformations import BRONZE_CREATE_TABLE


class BronzeLayer:
    def __init__(self):
        self.conn = None
    
    def validation_of_S3_path(self, bucket, key):
        s3 = boto3.client('s3')
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            print("Error accessing S3 path: {}".format(str(e)))
            return False
        
    def _init_duckdb(self):
        self.conn = duckdb.connect(':memory:')
        self.conn.execute('INSTALL httpfs')
        self.conn.execute('LOAD httpfs')

        if not config.AWS_ACCESS_KEY_ID or not isinstance(config.AWS_ACCESS_KEY_ID, str):
            raise ValueError("AWS_ACCESS_KEY_ID must be set in the config file.")
        if not config.AWS_SECRET_ACCESS_KEY or not isinstance(config.AWS_SECRET_ACCESS_KEY, str):
            raise ValueError("AWS_SECRET_ACCESS_KEY must be set in the config file.")
        
        self.conn.execute("""
            CREATE SECRET AWS_credentials (
                          TYPE S3,
                          KEY_ID ?,
                          SECRET ?,
            )
        """, [config.AWS_ACCESS_KEY_ID, config.AWS_SECRET_ACCESS_KEY])
        print("DuckDB initialized with AWS credentials.")
    
    def raw_data_ingestion(self):
        self._init_duckdb()
        if not self.validation_of_S3_path(config.SOURCE_BUCKET, config.SOURCE_KEY):
            raise ValueError("Invalid S3 path for source data.")
        
        s3_path = config.get_s3_path()
        print("Reading Raw data from S3 path: " + s3_path)

        self.conn.execute(
            BRONZE_CREATE_TABLE,
            {
                'csv_path': s3_path,
                'source_file': config.SOURCE_KEY
            }
        )

        result = self.conn.execute("SELECT COUNT(*) AS record_count FROM bronze_heart_disease").fetchone()
        record_count = result[0] if result else 0
        print("Raw data ingestion completed. Total records ingested: {}".format(str(record_count)))
        print("Sample records from the first 5 rows:" + str(self.conn.execute("SELECT * FROM bronze_heart_disease LIMIT 5").fetchdf()))
        return self.conn
    
    def save_to_S3(self, local_path="/tmp/bronze_heart_disease.parquet"):
        print("\n Preparing to export Bronze layer to S3")

        self.conn.execute(
            "COPY bronze_heart_disease TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)",
            [local_path]
        )

        S3_client = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
            )
        
        S3_key = config.BRONZE_PREFIX + "bronze_layer_heart_data.parquet"
        try:
            S3_client.upload_file(local_path, config.TARGET_BUCKET, S3_key)
            print("Bronze layer successfully exported to s3://{}/{}".format(config.TARGET_BUCKET, S3_key))
        except Exception as e:
            print("Error uploading Bronze layer to S3: {}".format(str(e)))
            print("The Bronze layer data is saved locally at: {}".format(local_path))

        if os.path.exists(local_path):
            os.remove(local_path)
            print("Local Bronze layer file removed after upload.")
    
    def get_connection(self):
        if self.conn is None:
            raise ValueError("DuckDB connection not initialized. Please run raw_data_ingestion() first.")
        return self.conn
    
    def close(self):
        if self.conn:
            self.conn.close()
            print("DuckDB connection closed.")

def main():

    load_dotenv()
    config.validate_config()
    config.print_config_summary()

    bronze = BronzeLayer()
    conn = bronze.raw_data_ingestion()
    bronze.save_to_S3()

    print("\n Bronze Layer Stats:")
    result = conn.execute("""
            SELECT COUNT(*) AS Total_Records,
            COUNT(DISTINCT id) AS Unique_Patients,
            MIN(ingestion_timestamp) AS Ingestion_Time
            FROM bronze_heart_disease
            """).fetchdf()
    print(result)
    bronze.close()

if __name__ == "__main__":
    main()