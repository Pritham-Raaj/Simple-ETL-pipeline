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