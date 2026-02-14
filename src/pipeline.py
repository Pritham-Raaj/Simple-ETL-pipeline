import sys
import os
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from Bronze import BronzeLayer
from Silver import SilverLayer  
from Gold import GoldLayer
import config

class Warehouse_Pipeline:
    def __init__(self):
        self.bronze = None
        self.silver = None
        self.gold = None
        self.start_time = None
        self.end_time = None
    
    def run(self, save_to_S3=True, export_to_powerbi=True):
        self.start_time = datetime.now()
        print("\n the warehouse pipeline is starting...")
        print("\n Start Time: " + self.start_time.strftime("%Y-%m-%d %H:%M:%S"))
        print("\n Warehouse location: s3://" + config.TARGET_BUCKET + "/" + config.TARGET_BASE_FILE + "/")

        try:
            print("\n Stage1: Executing Bronze Layer")
            self.bronze = BronzeLayer()
            conn = self.bronze.raw_data_ingestion()
            if save_to_S3:
                self.bronze.save_to_S3()
                print("\n Bronze Layer data is being saved to S3.")

            print("\n Stage 2: Executing Silver Layer")
            self.silver = SilverLayer(conn)
            self.silver.data_cleaning_and_standardization()
            self.silver.display_quality_report()
            self.silver.display_age_group_distribution()
            if save_to_S3:
                self.silver.save_to_S3()
                print("\n Silver Layer data is being saved to S3.")
            
            print("\n Stage 3: Executing Gold Layer")
            self.gold = GoldLayer(conn)
            self.gold.create_aggregations()
            self.gold.display_demo()
            self.gold.display_top_risk()
            self.gold.display_severity_distribution()
            if save_to_S3:
                self.gold.save_to_S3()
                print("\n Gold Layer data is being saved to S3.")

            if export_to_powerbi:
                powerbi_path = self.gold.for_powerbi()
                print("\n Curated data for PowerBI is being saved to S3. " + powerbi_path)

            self.gold.display_all_records()

            self.end_time = datetime.now()
            duration = self.end_time - self.start_time
            print("\n Warehouse pipeline completed successfully.")
            print("\n Start Time: " + self.start_time.strftime("%Y-%m-%d %H:%M:%S"))
            print("\n End Time: " + self.end_time.strftime("%Y-%m-%d %H:%M:%S"))
            print("\n Duration: " + str(duration))

            return True

        except Exception as e:
            print("\n Error during warehouse pipeline execution: " + str(e))
            print("\n The pipeline terminated with errors. Please check the logs for details.")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            if self.bronze:
                self.bronze.close()
            print("\n Warehouse pipeline execution finished.")

    def run_bronze_layer(self):
        print("\n Running Bronze Layer independently...")
        self.bronze = BronzeLayer()
        conn = self.bronze.raw_data_ingestion()
        self.bronze.save_to_S3()
        self.bronze.close()

    def run_silver_layer(self):
        self.bronze = BronzeLayer()
        conn = self.bronze.raw_data_ingestion()
        print("\n Running Silver Layer independently...")
        self.silver = SilverLayer(conn)
        self.silver.data_cleaning_and_standardization()
        self.silver.display_quality_report()
        self.silver.display_age_group_distribution()
        self.silver.save_to_S3()
        self.bronze.close()

def main():
    import argparse
    load_dotenv()

    parser = argparse.ArgumentParser(description="Running an ETL pipeline for the heart disease dataset")

    parser.add_argument('--layer', choices=['bronze', 'silver', 'full'], default = 'full', 
                        help="Which layer to run: 'bronze' for just the Bronze layer, 'silver' for Bronze + Silver, 'full' for the entire pipeline")
    parser.add_argument('--no-s3', action='store_true', help="Skip S3 upload steps and save all outputs locally")
    parser.add_argument('--no-powerbi', action='store_true', help="Skip exporting curated data for PowerBI")

    args = parser.parse_args()
    config.validate_config()
    pipeline = Warehouse_Pipeline()

    if args.layer == 'bronze':
        pipeline.run_bronze_layer()
    elif args.layer == 'silver':
        pipeline.run_silver_layer()
    else: 
        pipeline.run(save_to_s3=not args.no_s3, export_to_powerbi=not args.no_powerbi)

if __name__ == "__main__":
    main()