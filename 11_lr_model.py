'''
Train a LR model to predict GLP1 usage
'''

import pandas as pd
import statsmodels.api as sm

# Load data
final_df = pd.read_csv("FINAL_DATA.csv")
uszips = pd.read_csv("uszips.csv")
state_pop = pd.read_csv("state_pop_estimates.csv")

print('Loaded data!')

# Merge state population estimates on state + year
final_df = final_df.merge(
    state_pop,
    left_on=["pat_state", "year"],
    right_on=["state_abbrev", "year"],
    how="left"
)

# Aggregate to zip × year
df_grouped = final_df.groupby(['weighted_zip', 'year']).agg(
    glp1ra_count=('pat_id', 'nunique'),   # number of unique patients on GLP1
    state_population=('population', 'mean')  # from state_pop_estimates.csv
).reset_index()

# Merge with neighborhood data
merged = pd.merge(
    df_grouped,
    uszips,
    left_on="weighted_zip",
    right_on="zip",
    how="left",
    suffixes=("_final", "_uszips")
)
print('Merged all data!')

# Drop FINAL_DATA duplicates if also in uszips
drop_cols = [col for col in merged.columns if col.endswith("_final")]
merged = merged.drop(columns=drop_cols)

# Normalize outcome (rate per 1,000)
merged['glp1ra_rate'] = merged['glp1ra_count'] / merged['state_population'] * 1000

print('Filtering and Normalize outcome done!')

# Predictor set
predictors = [
    'age_median', 'age_40s', 'age_50s', 'age_60s',
    'income_household_median', 'poverty', 'unemployment_rate',
    'education_highschool', 'education_some_college',
    'education_bachelors', 'education_graduate',
    'race_white', 'race_black', 'race_asian', 'hispanic',
    'health_uninsured', 'family_size', 'family_dual_income'
]
predictors = [p for p in predictors if p in merged.columns]

import numpy as np

# Drop rows with NaN or inf in predictors or outcome
X = merged[predictors]
y = merged['glp1ra_rate']

# Combine X and y to drop together
data = pd.concat([X, y], axis=1)
data = data.replace([np.inf, -np.inf], np.nan).dropna()

X = data[predictors]
y = data['glp1ra_rate']

X = sm.add_constant(X)

print(f"Final training set shape: {X.shape}, {y.shape}")

print('Training Linear Regression model...')
model = sm.OLS(y, X).fit()
print('Done!')
print(model.summary())

merged.to_csv('MERGED_DATA.csv', index = False)
