import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import glm
from sklearn.linear_model import RidgeCV, LassoCV, ElasticNetCV
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
import numpy as np

# --- Load merged dataset ---
df = pd.read_csv("MERGED_DATA_FOR_LR_NEW.csv")

# Rename diabetes_rate to diabetes_obesity_population_rate for clarity
# This represents (diabetes+obesity patients) / population rate
if 'diabetes_rate' in df.columns:
    df = df.rename(columns={'diabetes_rate': 'diabetes_obesity_population_rate'})

# Define predictors & outcome
predictors = [
    'age_median', 'age_40s', 'age_50s', 'age_60s',
    'income_household_median', 'poverty', 'unemployment_rate',
    'education_highschool', 'education_some_college',
    'education_bachelors', 'education_graduate',
    'race_white', 'race_black', 'race_asian', 'hispanic',
    'health_uninsured', 'family_size', 'family_dual_income',
    'rural_urban',  # Added rural/urban variable
    'diabetes_obesity_population_rate'  # (diabetes+obesity)/population_rate
]

# Keep only valid predictors that exist
predictors = [p for p in predictors if p in df.columns]

# Build clean dataset
# Separate numeric and categorical predictors
numeric_predictors = [p for p in predictors if p != 'rural_urban']
categorical_predictors = [p for p in predictors if p == 'rural_urban']

# Convert numeric predictors to numeric
df_model = df[numeric_predictors + categorical_predictors + ["glp1ra_count", "glp1ra_rate", "population"]].copy()
df_model[numeric_predictors] = df_model[numeric_predictors].apply(pd.to_numeric, errors="coerce")

# Create single dummy variable for rural_urban (Urban=1, Rural=0)
if 'rural_urban' in df_model.columns:
    # Create is_urban variable: Urban=1, Rural=0, Unknown=NaN (will be dropped)
    df_model['is_urban'] = df_model['rural_urban'].map({'Urban': 1, 'Rural': 0, 'Unknown': np.nan})
    # Remove original rural_urban column
    df_model = df_model.drop(columns=['rural_urban'])
    # Add is_urban to predictors list
    predictors = numeric_predictors + ['is_urban']
    print(f"\nCreated binary variable: is_urban (Urban=1, Rural=0)")
    print(f"  Urban: {(df_model['is_urban'] == 1).sum()} observations")
    print(f"  Rural: {(df_model['is_urban'] == 0).sum()} observations")
    print(f"  Unknown (excluded): {df_model['is_urban'].isna().sum()} observations")

# Drop rows with NaN
df_model = df_model.dropna().reset_index(drop=True)

# Ensure all predictors are numeric for VIF calculation
X = df_model[predictors].copy()
# Convert all columns to numeric (dummy variables should already be 0/1)
for col in X.columns:
    X[col] = pd.to_numeric(X[col], errors='coerce')

# Drop any rows that became NaN after conversion
valid_rows = ~X.isna().any(axis=1)
X = X[valid_rows].reset_index(drop=True)
df_model = df_model[valid_rows].reset_index(drop=True)

y_count = df_model["glp1ra_count"]
y_rate = df_model["glp1ra_rate"]


from statsmodels.stats.outliers_influence import variance_inflation_factor

def reduce_multicollinearity(X, predictors, thresh=10.0):
    """
    Iteratively remove predictors with highest VIF until all are below threshold.
    """
    # Ensure X contains only numeric data (include bool as it's numeric for VIF)
    X_numeric = X[predictors].select_dtypes(include=[np.number, bool])
    predictors_numeric = [p for p in predictors if p in X_numeric.columns]
    
    if len(predictors_numeric) != len(predictors):
        excluded = [p for p in predictors if p not in predictors_numeric]
        print(f"Warning: {len(predictors) - len(predictors_numeric)} non-numeric predictors excluded from VIF calculation: {excluded}")
    
    while True:
        if len(predictors_numeric) < 2:
            break
            
        vif = pd.DataFrame()
        vif["feature"] = predictors_numeric
        # Convert to numpy array and ensure float type
        X_vif = X_numeric[predictors_numeric].values.astype(float)
        vif["VIF"] = [variance_inflation_factor(X_vif, i) for i in range(len(predictors_numeric))]

        max_vif = vif["VIF"].max()
        if max_vif > thresh:
            worst_feature = vif.sort_values("VIF", ascending=False).iloc[0]["feature"]
            print(f"Dropping '{worst_feature}' with VIF={max_vif:.2f}")
            predictors_numeric.remove(worst_feature)
        else:
            break

    return predictors_numeric

# --- Reduce collinearity ---
predictors = reduce_multicollinearity(X, predictors, thresh=10.0)

print("\nFinal predictors after VIF reduction:\n", predictors)

# Rebuild clean dataset with reduced predictors
X = df_model[predictors]
y_count = df_model["glp1ra_count"]
y_rate = df_model["glp1ra_rate"]



# --------------------
# 1. OLS (baseline)
# --------------------
X_const = sm.add_constant(X)
ols_model = sm.OLS(y_rate, X_const).fit()
print("\nOLS Results:\n", ols_model.summary())

# --------------------
# 2. GLM - Poisson with population offset
# --------------------
poisson_model = glm(
    formula="glp1ra_count ~ " + " + ".join(predictors),
    data=df_model,
    family=sm.families.Poisson(),
    offset=np.log(df_model["population"])
).fit()
print("\nPoisson GLM Results:\n", poisson_model.summary())

# --------------------
# 3. GLM - Negative Binomial
# --------------------
nb_model = glm(
    formula="glp1ra_count ~ " + " + ".join(predictors),
    data=df_model,
    family=sm.families.NegativeBinomial(),
    offset=np.log(df_model["population"])
).fit()
print("\nNegative Binomial GLM Results:\n", nb_model.summary())


# --- Overdispersion checks ---
mean_count = y_count.mean()
var_count = y_count.var()
print("\n[Overdispersion Check]")
print("Mean of outcome:", mean_count)
print("Variance of outcome:", var_count)
print("Variance/Mean:", var_count / mean_count)

poisson_deviance = poisson_model.deviance
df_resid = poisson_model.df_resid
dispersion_deviance = poisson_deviance / df_resid
print("Dispersion (Deviance/df):", dispersion_deviance)

pearson_chi2 = poisson_model.pearson_chi2
dispersion_pearson = pearson_chi2 / df_resid
print("Dispersion (Pearson/df):", dispersion_pearson)

from statsmodels.discrete.discrete_model import NegativeBinomial

# --------------------
# 3b. Negative Binomial (discrete model, estimates alpha)
# --------------------
X_nb = sm.add_constant(X)
nb_discrete = NegativeBinomial(y_count, X_nb, loglike_method='nb2').fit()
print("\nNegative Binomial (discrete model) Results:\n", nb_discrete.summary())

# Show alpha (dispersion parameter)
print("\nEstimated alpha (overdispersion):", nb_discrete.params.get('alpha', 'Not estimated'))

# --------------------
# 4. Regularized Linear Models
# --------------------
X_train, X_test, y_train, y_test = train_test_split(X, y_rate, test_size=0.2, random_state=42)

ridge = RidgeCV(alphas=np.logspace(-6, 6, 13), cv=5).fit(X_train, y_train)
lasso = LassoCV(alphas=np.logspace(-6, 1, 50), cv=5, random_state=42).fit(X_train, y_train)
elastic = ElasticNetCV(l1_ratio=[.1, .5, .9], cv=5, random_state=42).fit(X_train, y_train)

print("\nRidge R^2:", ridge.score(X_test, y_test))
print("Lasso R^2:", lasso.score(X_test, y_test))
print("ElasticNet R^2:", elastic.score(X_test, y_test))

# --------------------
# 5. Tree-based Models
# --------------------
rf = RandomForestRegressor(n_estimators=500, random_state=42, n_jobs=-1)
gb = GradientBoostingRegressor(n_estimators=500, learning_rate=0.05, random_state=42)

rf.fit(X_train, y_train)
gb.fit(X_train, y_train)

print("\nRandom Forest R^2:", r2_score(y_test, rf.predict(X_test)))
print("Gradient Boosting R^2:", r2_score(y_test, gb.predict(X_test)))

# Feature importance analysis
print("\n" + "="*80)
print("Random Forest Feature Importance:")
print("="*80)
feature_importance = pd.DataFrame({
    'feature': predictors,
    'importance': rf.feature_importances_
}).sort_values('importance', ascending=False)

for idx, row in feature_importance.iterrows():
    print(f"{row['feature']:25s} {row['importance']:8.4f} ({row['importance']/feature_importance['importance'].sum()*100:5.2f}%)")

print("\n" + "="*80)
print("Gradient Boosting Feature Importance:")
print("="*80)
gb_importance = pd.DataFrame({
    'feature': predictors,
    'importance': gb.feature_importances_
}).sort_values('importance', ascending=False)

for idx, row in gb_importance.iterrows():
    print(f"{row['feature']:25s} {row['importance']:8.4f} ({row['importance']/gb_importance['importance'].sum()*100:5.2f}%)")
