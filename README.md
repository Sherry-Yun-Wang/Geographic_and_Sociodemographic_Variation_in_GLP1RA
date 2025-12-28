# Geographic and Sociodemographic Variation in GLP1-RA Prescribing Patterns

This repository contains code for analyzing geographic and sociodemographic variation in GLP1-RA prescribing patterns using IQVIA claims data.

## Overview

This project processes large-scale pharmaceutical claims data to examine how GLP1-RA prescribing rates vary across geographic regions and sociodemographic factors in the United States from 2010-2022. The analysis includes data extraction, cleaning, merging with census/demographic data, and statistical modeling using various regression approaches.

## Repository Structure

### Data Processing Pipeline

The data processing pipeline consists of sequential scripts that transform raw IQVIA claims data into a final analytical dataset:

#### **0_pull_all_T2Dobese_pats.py**
Extracts patients with Type 2 Diabetes (T2D) and/or Obesity diagnoses from IQVIA claims data. Filters claims by diagnosis codes and saves patient-level data by year.

#### **1_count_pat_across_state_year.py**
Counts unique patients across states and years from the extracted patient files. Generates state-level patient counts for downstream analysis.

#### **2_pull_payment_info_GLP_pats.py**
Extracts payment and insurance information for GLP1-RA patients from enrollment data. Pulls payer type and enrollment details.

#### **3_compile_demo_info_GLP.py**
Compiles demographic information for GLP1-RA patients, including age, sex, and geographic location.

#### **4_add_zip_info.py**
Merges ZIP code-level demographic and socioeconomic data from the `uszips` dataset. Adds neighborhood characteristics including income, education, race/ethnicity, housing, and other census variables.

#### **5_adjust_dataframe.py**
Adjusts and restructures the dataframe for time-series analysis. Expands prescriptions to patient-years and handles temporal data alignment.

#### **6_fill_in_enroll_data.py**
Fills missing enrollment data by reading enrollment files from IQVIA data. Adds enrollment and geographic information for patients with missing data.

#### **7_fill_in_payment.py**
Fills missing payment/insurance type information by querying enrollment records.

#### **8_fill_in_condition.py**
Fills missing condition (T2D/Obesity) information by merging with original patient condition files.

#### **9_fill_in_zip_data.py**
Fills missing ZIP code information using weighted ZIP mappings and the `uszips` dataset.

#### **10_finalize_data.py**
Finalizes the dataset by performing final cleaning, validation, and preparing the data for modeling.

### Data Enhancement Scripts

#### **12_add_rural_urban.py**
Adds rural/urban classification using RUCA (Rural-Urban Commuting Area) codes. Maps county-level RUCA codes to create rural/urban binary variables.

#### **13_add_diabetes_rate_to_merged_data.py**
Adds diabetes patient counts and diabetes rate (per population) to the merged dataset. Calculates diabetes prevalence at the ZIP code level.

### Analysis and Modeling Scripts

#### **11_lr_model.py**
Trains a basic linear regression model to predict GLP1-RA prescribing rates. Performs initial modeling with demographic and socioeconomic predictors.

#### **14_final_modeling.py**
Comprehensive modeling script that implements multiple regression approaches:
- **OLS (Ordinary Least Squares)** - Baseline linear regression
- **Poisson GLM** - Generalized linear model with Poisson distribution for count data
- **Negative Binomial GLM** - Handles overdispersion in count data
- **Regularized Models** - Ridge, Lasso, and Elastic Net regression
- **Tree-based Models** - Random Forest and Gradient Boosting

The script includes:
- Multicollinearity reduction using VIF (Variance Inflation Factor)
- Overdispersion diagnostics
- Feature importance analysis
- Model performance comparison

### Utility Scripts

#### **read_iqvia.py**
Utility functions for reading IQVIA data files:
- `read_ndc_codes()` - Reads NDC (National Drug Code) codes for GLP1-RA medications
- `read_iqvia_header()` - Reads IQVIA file headers by year
- `read_iqvia_claims()` - Reads and filters IQVIA claims data

#### **calculate_glp1ra_rate_by_state_yearly.py**
Calculates GLP1-RA prescribing rates per 1,000 residents by state for each year (2010-2022). Outputs separate CSV files for each year and a summary file.

#### **calculate_glp1ra_rate_by_diabetes_patients.py**
Calculates GLP1-RA prescribing rates among diabetes patients by state and year. Normalizes rates by diabetes patient population.

### Data Files

#### **MERGED_DATA_example.csv**
Example dataset with synthetic data matching the structure of the full analytical dataset. Contains 100 rows and 100 columns including:
- Patient counts and prescribing rates
- ZIP code and geographic information
- Demographic variables (age, race/ethnicity, education)
- Socioeconomic variables (income, poverty, employment)
- Health-related variables (insurance, disability)
- Rural/urban classification
- Diabetes prevalence rates

**Note:** This is synthetic data generated for demonstration purposes. All values are randomly generated and do not represent real patient data.

#### **final_result.log** / **urban_result.log**
Model output logs containing regression results and diagnostics.

## Requirements

### Python Packages

```
pandas
numpy
statsmodels
scikit-learn
tqdm
```

### Data Dependencies

The full pipeline requires access to:
- IQVIA claims data (not included in repository)
- `uszips.csv` - ZIP code demographic dataset
- `RUCA-codes-2020-tract.csv` - Rural-Urban Commuting Area codes
- `state_pop_estimates.csv` - State population estimates by year
- Obesity and T2D diagnosis code files

**Note:** Due to data privacy and licensing restrictions, the original IQVIA data files and large reference datasets are not included in this repository.

## Running the Regression Models with Example Data

The `14_final_modeling.py` script is configured to work with the example dataset. Follow these steps to run the regression models:

### Step 1: Ensure Dependencies are Installed

```bash
pip install pandas numpy statsmodels scikit-learn
```

### Step 2: Run the Modeling Script

```bash
python 14_final_modeling.py
```

The script will:
1. Load `MERGED_DATA_example.csv`
2. Prepare predictors and outcome variables
3. Reduce multicollinearity using VIF threshold of 10.0
4. Fit multiple regression models:
   - OLS regression
   - Poisson GLM with population offset
   - Negative Binomial GLM
   - Regularized models (Ridge, Lasso, Elastic Net)
   - Tree-based models (Random Forest, Gradient Boosting)
5. Display model summaries, diagnostics, and feature importance

### Expected Output

The script will print:
- Data loading and preprocessing messages
- VIF reduction process (if multicollinearity is detected)
- OLS regression summary (coefficients, p-values, R²)
- Poisson and Negative Binomial GLM summaries
- Overdispersion diagnostics
- R² scores for regularized models
- Feature importance rankings for tree-based models


## Data Processing Workflow

For the full data processing pipeline (requires access to IQVIA data):

1. **Extract Patients:** Run `0_pull_all_T2Dobese_pats.py` to extract T2D/Obesity patients
2. **Count Patients:** Run `1_count_pat_across_state_year.py` for state-level counts
3. **Extract Payment Info:** Run `2_pull_payment_info_GLP_pats.py` for insurance data
4. **Compile Demographics:** Run `3_compile_demo_info_GLP.py` for patient demographics
5. **Add ZIP Data:** Run `4_add_zip_info.py` to merge neighborhood characteristics
6. **Fill Missing Data:** Run scripts 6-9 to fill in missing enrollment, payment, condition, and ZIP data
7. **Finalize:** Run `10_finalize_data.py` to create the final dataset
8. **Add Variables:** Run `12_add_rural_urban.py` and `13_add_diabetes_rate_to_merged_data.py` for additional variables
9. **Model:** Run `14_final_modeling.py` for regression analysis

## Notes

- All scripts contain hardcoded file paths that need to be updated for your environment
- The example dataset (`MERGED_DATA_example.csv`) contains synthetic data for demonstration only
- Original data files are excluded due to privacy/licensing restrictions
- Log files contain model output and can be reviewed for detailed results


