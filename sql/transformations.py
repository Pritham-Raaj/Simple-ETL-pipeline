# SQL transformations for all the Layers (Bronze, Silver, Gold)

#Bronze Layer

BRONZE_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS bronze_heart_disease AS
SELECT
    *,
    CURRENT_TIMESTAMP AS ingestion_timestamp,
    $source_file AS source_file
FROM read_csv_auto($csv_path, Header=True)"""

#Silver Layer

# Stage 1: Type Casting
SILVER_STAGE1_CAST_TYPES = """
CREATE VIEW IF NOT EXISTS silver_stage1_typed AS
SELECT 
    CAST(id AS INTEGER) AS patient_id,
    CAST(age AS INTEGER) AS age,
    CAST(sex AS VARCHAR) AS sex,
    CAST(dataset AS VARCHAR) AS dataset,
    CAST(cp AS VARCHAR) AS chest_pain_type,
    CAST(trestbps AS INTEGER) AS resting_blood_pressure,
    CAST(chol AS INTEGER) AS cholesterol,
    CAST(fbs AS BOOLEAN) AS fasting_blood_sugar_high,
    CAST(restecg AS VARCHAR) AS resting_ecg,
    CAST(thalch AS INTEGER) AS max_heart_rate,
    CAST(exang AS BOOLEAN) AS exercise_induced_angina,
    CAST(oldpeak AS DOUBLE) AS st_depression,
    CAST(slope AS VARCHAR) AS st_slope,
    CAST(ca AS INTEGER) AS num_major_vessels,
    CAST(thal AS VARCHAR) AS thalassemia,
    CAST(num AS INTEGER) AS heart_disease_severity,
    ingestion_timestamp,
    source_file
FROM bronze_heart_disease """

# Stage 2:Cleaning and Standardization

SILVER_STAGE2_STANDARDIZATION = """
CREATE OR REPLACE VIEW silver_stage2_standardized AS
SELECT 
    patient_id,
    age,
    CASE 
        WHEN LOWER(TRIM(sex)) IN ('male', 'm', '1') THEN 'Male'
        WHEN LOWER(TRIM(sex)) IN ('female', 'f', '0') THEN 'Female'
        ELSE 'Unknown'
    END AS sex,
    TRIM(dataset) AS dataset,
    
    CASE 
        WHEN LOWER(chest_pain_type) LIKE '%typical%' THEN 'Typical Angina'
        WHEN LOWER(chest_pain_type) LIKE '%atypical%' THEN 'Atypical Angina'
        WHEN LOWER(chest_pain_type) LIKE '%non%' THEN 'Non-Anginal Pain'
        WHEN LOWER(chest_pain_type) LIKE '%asymp%' THEN 'Asymptomatic'
        ELSE 'Unknown'
    END AS chest_pain_type,
    
    resting_blood_pressure,
    cholesterol,
    fasting_blood_sugar_high,
    
    CASE 
        WHEN LOWER(resting_ecg) LIKE '%normal%' THEN 'Normal'
        WHEN LOWER(resting_ecg) LIKE '%hypertrophy%' THEN 'LV Hypertrophy'
        WHEN LOWER(resting_ecg) LIKE '%abnormal%' OR LOWER(resting_ecg) LIKE '%wave%' THEN 'ST-T Abnormality'
        ELSE 'Unknown'
    END AS resting_ecg,
    
    max_heart_rate,
    exercise_induced_angina,
    st_depression,
    
    CASE 
        WHEN LOWER(st_slope) LIKE '%up%' THEN 'Upsloping'
        WHEN LOWER(st_slope) LIKE '%flat%' THEN 'Flat'
        WHEN LOWER(st_slope) LIKE '%down%' THEN 'Downsloping'
        ELSE 'Unknown'
    END AS st_slope,
    
    num_major_vessels,
    
    CASE 
        WHEN LOWER(thalassemia) LIKE '%normal%' THEN 'Normal'
        WHEN LOWER(thalassemia) LIKE '%fixed%' THEN 'Fixed Defect'
        WHEN LOWER(thalassemia) LIKE '%revers%' THEN 'Reversible Defect'
        ELSE 'Unknown'
    END AS thalassemia,
    
    heart_disease_severity,
    
    CASE 
        WHEN heart_disease_severity = 0 THEN FALSE
        WHEN heart_disease_severity > 0 THEN TRUE
        ELSE NULL
    END AS has_heart_disease,
    
    ingestion_timestamp,
    source_file
FROM silver_stage1_typed """

# Stage 3: Data Qulaity Checks

SILVER_STAGE3_QUALITY_CHECK = """
CREATE OR REPLACE VIEW silver_stage3_validated AS
SELECT 
    *,
    CASE 
        WHEN age < $min_age OR age > $max_age THEN TRUE
        WHEN resting_blood_pressure < $min_trestbps OR resting_blood_pressure > $max_trestbps THEN TRUE
        WHEN cholesterol < $min_chol OR cholesterol > $max_chol THEN TRUE
        WHEN max_heart_rate < $min_thalch OR max_heart_rate > $max_thalch THEN TRUE
        WHEN st_depression < $min_oldpeak OR st_depression > $max_oldpeak THEN TRUE
        ELSE FALSE
    END AS has_quality_issues,
    
    CASE 
        WHEN age < 40 THEN '< 40'
        WHEN age >= 40 AND age < 50 THEN '40-49'
        WHEN age >= 50 AND age < 60 THEN '50-59'
        WHEN age >= 60 AND age < 70 THEN '60-69'
        WHEN age >= 70 THEN '70+'
        ELSE 'Unknown'
    END AS age_group,
    
    CASE 
        WHEN max_heart_rate >= 180 THEN 'Very High'
        WHEN max_heart_rate >= 160 THEN 'High'
        WHEN max_heart_rate >= 140 THEN 'Moderate'
        ELSE 'Low'
    END AS max_heart_rate_category,
    
    CURRENT_TIMESTAMP AS validation_timestamp
FROM silver_stage2_standardized """

# Final Silver Layer

SILVER_FINAL_TABLE = """
CREATE TABLE IF NOT EXISTS silver_heart_disease AS
SELECT * FROM silver_stage3_validated
WHERE has_quality_issues = FALSE """

#Gold Layer

GOLD_DEMO_SUMMARY = """
CREATE TABLE IF NOT EXISTS gold_demographics_summary AS
SELECT 
    sex,
    age_group,
    COUNT(*) AS patient_count,
    AVG(age) AS avg_age,
    COUNT(CASE WHEN has_heart_disease THEN 1 END) AS heart_disease_count,
    ROUND(COUNT(CASE WHEN has_heart_disease THEN 1 END) * 100.0 / COUNT(*), 2) AS heart_disease_percentage,
    AVG(resting_blood_pressure) AS avg_blood_pressure,
    AVG(cholesterol) AS avg_cholesterol,
    AVG(max_heart_rate) AS avg_max_heart_rate,
    CURRENT_TIMESTAMP AS created_at
FROM silver_heart_disease
GROUP BY sex, age_group
ORDER BY sex, age_group
"""

GOLD_RISK_FACTORS = """
CREATE TABLE IF NOT EXISTS gold_risk_factors AS
SELECT 
    chest_pain_type,
    resting_ecg,
    exercise_induced_angina,
    st_slope,
    thalassemia,
    COUNT(*) AS patient_count,
    COUNT(CASE WHEN has_heart_disease THEN 1 END) AS heart_disease_count,
    ROUND(COUNT(CASE WHEN has_heart_disease THEN 1 END) * 100.0 / COUNT(*), 2) AS risk_percentage,
    CURRENT_TIMESTAMP AS created_at
FROM silver_heart_disease
GROUP BY chest_pain_type, resting_ecg, exercise_induced_angina, st_slope, thalassemia
ORDER BY risk_percentage DESC """

GOLD_SEVERITY_DISTRIBUTION = """
CREATE TABLE IF NOT EXISTS gold_severity_distribution AS
SELECT 
    heart_disease_severity,
    CASE 
        WHEN heart_disease_severity = 0 THEN 'No Disease'
        WHEN heart_disease_severity = 1 THEN 'Mild'
        WHEN heart_disease_severity = 2 THEN 'Moderate'
        WHEN heart_disease_severity = 3 THEN 'Severe'
        WHEN heart_disease_severity = 4 THEN 'Very Severe'
    END AS severity_label,
    COUNT(*) AS patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    AVG(age) AS avg_age,
    AVG(cholesterol) AS avg_cholesterol,
    AVG(resting_blood_pressure) AS avg_blood_pressure,
    CURRENT_TIMESTAMP AS created_at
FROM silver_heart_disease
GROUP BY heart_disease_severity
ORDER BY heart_disease_severity """

GOLD_CLINICAL_METRICS = """
CREATE TABLE IF NOT EXISTS gold_clinical_metrics AS
SELECT 
    dataset,
    sex,
    COUNT(*) AS total_patients,
    
    AVG(resting_blood_pressure) AS avg_resting_bp,
    MIN(resting_blood_pressure) AS min_resting_bp,
    MAX(resting_blood_pressure) AS max_resting_bp,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resting_blood_pressure) AS median_resting_bp,
    
    AVG(cholesterol) AS avg_cholesterol,
    MIN(cholesterol) AS min_cholesterol,
    MAX(cholesterol) AS max_cholesterol,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cholesterol) AS median_cholesterol,
    
    AVG(max_heart_rate) AS avg_max_heart_rate,
    MIN(max_heart_rate) AS min_max_heart_rate,
    MAX(max_heart_rate) AS max_max_heart_rate,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY max_heart_rate) AS median_max_heart_rate,
    
    AVG(st_depression) AS avg_st_depression,
    MAX(st_depression) AS max_st_depression,
    
    COUNT(CASE WHEN fasting_blood_sugar_high THEN 1 END) AS high_fasting_sugar_count,
    COUNT(CASE WHEN exercise_induced_angina THEN 1 END) AS exercise_angina_count,
    
    CURRENT_TIMESTAMP AS created_at
FROM silver_heart_disease
GROUP BY dataset, sex
ORDER BY dataset, sex """

GOLD_POWERBI_FACT_TABLE = """
CREATE TABLE IF NOT EXISTS gold_powerbi_fact_table AS
SELECT 
    patient_id,
    age,
    sex,
    age_group,
    dataset,
    chest_pain_type,
    resting_blood_pressure,
    cholesterol,
    fasting_blood_sugar_high,
    resting_ecg,
    max_heart_rate,
    max_heart_rate_category,
    exercise_induced_angina,
    st_depression,
    st_slope,
    num_major_vessels,
    thalassemia,
    heart_disease_severity,
    has_heart_disease,
    (CASE WHEN age > 60 THEN 2 ELSE 0 END + CASE WHEN sex = 'Male' THEN 1 ELSE 0 END + 
    CASE WHEN chest_pain_type = 'Asymptomatic' THEN 2 ELSE 0 END +
    CASE WHEN resting_blood_pressure > 140 THEN 2 ELSE 0 END + CASE WHEN cholesterol > 240 THEN 2 ELSE 0 END +
    CASE WHEN fasting_blood_sugar_high THEN 1 ELSE 0 END +
    CASE WHEN max_heart_rate < 120 THEN 2 ELSE 0 END +
    CASE WHEN exercise_induced_angina THEN 2 ELSE 0 END +
    CASE WHEN st_depression > 2.0 THEN 2 ELSE 0 END +
    CASE WHEN num_major_vessels >= 2 THEN 2 ELSE 0 END 
    ) AS calculated_risk_score,
    
    CURRENT_TIMESTAMP AS created_at
FROM silver_heart_disease """

#Utility Queries

GET_RECORD_COUNTS = """
SELECT 
    'bronze_heart_disease' AS layer,
     COUNT(*) AS record_count
FROM bronze_heart_disease

UNION ALL

SELECT 
    'silver_heart_disease' AS layer,
     COUNT(*) AS record_count
FROM silver_heart_disease

UNION ALL

SELECT 
    'gold_demographics_summary' AS table_name,
     COUNT(*) AS record_count
FROM gold_demographics_summary

UNION ALL

SELECT 
    'gold_risk_factors' AS table_name,
     COUNT(*) AS record_count
FROM gold_risk_factors

UNION ALL

SELECT 
    'gold_severity_distribution' AS table_name,
     COUNT(*) AS record_count
FROM gold_severity_distribution

UNION ALL

SELECT 
    'gold_clinical_metrics' AS table_name,
     COUNT(*) AS record_count
FROM gold_clinical_metrics

UNION ALL

SELECT 
    'gold_powerbi_fact_table' as table_name,
     COUNT(*) AS record_count
FROM gold_powerbi_fact_table
"""

DATA_QUALITY_REPORT = """
SELECT 
    'Total Records' AS metric,
     COUNT(*) AS value
FROM bronze_heart_disease

UNION ALL

SELECT 
    'Records with Quality Issues' AS metric,
     COUNT(*) AS value
FROM silver_stage3_validated
WHERE has_quality_issues = TRUE

UNION ALL

SELECT 
    'Clean Records in Silver' AS metric,
     COUNT(*) AS value
FROM silver_heart_disease

UNION ALL

SELECT 
    'Null Values in Critical Fields' AS metric,
     COUNT(*) AS value
FROM bronze_heart_disease
WHERE age IS NULL 
   OR sex IS NULL 
   OR trestbps IS NULL 
   OR chol IS NULL
"""