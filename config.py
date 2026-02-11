# Configuration file for the usage of AWS S3 services and constraints for the data

import os

#AWS Credentials and Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "pritham-heartdata")
SOURCE_KEY = os.getenv("SOURCE_KEY", "heart_disease_uci.csv")
TARGET_BUCKET = os.getenv("TARGET_BUCKET", "data-endpoint")
TARGET_BASE_FILE = os.getenv("TARGET_BASE_FILE", "Health_data")

BRONZE_PREFIX = TARGET_BASE_FILE + "/Bronze/"
SILVER_PREFIX = TARGET_BASE_FILE + "/Silver/"
GOLD_PREFIX = TARGET_BASE_FILE + "/Gold/"

#Data Quality Constraints

MIN_AGE = 18
MAX_AGE = 100
MIN_BLOOD_PRESSURE = 80
MAX_BLOOD_PRESSURE = 250
MIN_CHOLESTEROL = 100
MAX_CHOLESTEROL = 600
MIN_HEART_RATE = 60
MAX_HEART_RATE = 220
MIN_ST_DEPRESSION = 0.0
MAX_ST_DEPRESSION = 10.0

#Column Descriptions

COLUMN_DESCRIPTIONS = {
    'id': 'Patient unique identifier',
    'age': 'Age of the patient in years',
    'sex': 'Gender of the patient (Male/female)',
    'dataset': 'location of the data sourced',
    'cp': 'Chest pain type',
    'trestbps': 'Resting blood pressure (mmHg)',
    'chol': 'Serum cholesterol (mg/dl)',
    'fbs': 'Fasting blood sugar > 120 mg/dl',
    'restecg': 'Resting electrocardiographic results',
    'thalch': 'Maximum heart rate achieved',
    'exang': 'Exercise induced angina',
    'oldpeak': 'ST depression induced by exercise relative to rest',
    'slope': 'Slope of the peak exercise ST segment',
    'ca': 'Number of major vessels (0-3)',
    'thal': 'Thalassemia Type',
    'num': 'Diagnosis of heart disease (0=no disease, 1-4=disease severity)'
}

#Validation

def validate_config():
    """Check if all required configuration values are set"""
    errors = []
    
    if not AWS_ACCESS_KEY_ID:
        errors.append("AWS_ACCESS_KEY_ID not set")
    if not AWS_SECRET_ACCESS_KEY:
        errors.append("AWS_SECRET_ACCESS_KEY not set")
    if not SOURCE_BUCKET:
        errors.append("SOURCE_BUCKET not set")
    if not TARGET_BUCKET:
        errors.append("TARGET_BUCKET not set")
    
    if MIN_AGE < 0 or MAX_AGE > 150:
        errors.append("Age constraints out of reasonable range")
    if MIN_BLOOD_PRESSURE < 0 or MAX_BLOOD_PRESSURE > 300:
        errors.append("Blood pressure constraints out of reasonable range")
    
    if errors:
        error_msg = "Configuration errors:\n  - " + "\n  - ".join(errors)
        raise ValueError(error_msg)
    
    return True


def get_s3_path():
    """Get full S3 path to source data"""
    return "s3://" + SOURCE_BUCKET + "/" + SOURCE_KEY


def get_quality_rules():
    """Get all data quality rules as a dictionary"""
    return {
        'min_age': MIN_AGE,
        'max_age': MAX_AGE,
        'min_trestbps': MIN_BLOOD_PRESSURE,
        'max_trestbps': MAX_BLOOD_PRESSURE,
        'min_chol': MIN_CHOLESTEROL,
        'max_chol': MAX_CHOLESTEROL,
        'min_thalch': MIN_HEART_RATE,
        'max_thalch': MAX_HEART_RATE,
        'min_oldpeak': MIN_ST_DEPRESSION,
        'max_oldpeak': MAX_ST_DEPRESSION
    }


def print_config_summary():
    """Print configuration summary"""
    print("\n" + "="*70)
    print("CONFIGURATION SUMMARY")
    print("="*70)
    print("Source: s3://" + SOURCE_BUCKET + "/" + SOURCE_KEY)
    print("Warehouse: s3://" + TARGET_BUCKET + "/" + TARGET_BASE_FILE + "/")
    print("  - Bronze: " + BRONZE_PREFIX)
    print("  - Silver: " + SILVER_PREFIX)
    print("  - Gold: " + GOLD_PREFIX)
    print("\nFull S3 Paths:")
    print("  - Bronze: s3://" + TARGET_BUCKET + "/" + BRONZE_PREFIX)
    print("  - Silver: s3://" + TARGET_BUCKET + "/" + SILVER_PREFIX)
    print("  - Gold: s3://" + TARGET_BUCKET + "/" + GOLD_PREFIX)
    print("\nData Quality Rules:")
    print("  - Age: " + str(MIN_AGE) + "-" + str(MAX_AGE) + " years")
    print("  - Blood Pressure: " + str(MIN_BLOOD_PRESSURE) + "-" + str(MAX_BLOOD_PRESSURE) + " mmHg")
    print("  - Cholesterol: " + str(MIN_CHOLESTEROL) + "-" + str(MAX_CHOLESTEROL) + " mg/dL")
    print("="*70 + "\n")