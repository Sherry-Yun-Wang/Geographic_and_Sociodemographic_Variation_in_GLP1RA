import pandas as pd
import numpy as np

print("Loading data files...")

# Load the main data file
merged_df = pd.read_csv("MERGED_DATA_FOR_LR.csv")
print(f"Loaded MERGED_DATA_FOR_LR.csv: {len(merged_df)} rows")

# Load RUCA codes file (skip first row which is header description)
ruca_df = pd.read_csv("RUCA-codes-2020-tract.csv", skiprows=1)
print(f"Loaded RUCA-codes-2020-tract.csv: {len(ruca_df)} rows")

# Check if county_fips exists in merged_df
if 'county_fips' not in merged_df.columns:
    print("Error: 'county_fips' column not found in MERGED_DATA_FOR_LR.csv")
    print("Available columns:", merged_df.columns.tolist()[:10])
    exit(1)

# Convert county_fips to string and remove decimals for matching
# Need to pad to 5 digits to match RUCA format (e.g., 6037 -> 06037)
merged_df['county_fips_str'] = merged_df['county_fips'].astype(str).str.replace('.0', '', regex=False).str.zfill(5)

# Convert CountyFIPS20 to string in RUCA file (it might be integer)
ruca_df['CountyFIPS20_str'] = ruca_df['CountyFIPS20'].astype(str).str.zfill(5)

# Create a mapping from CountyFIPS20 to PrimaryRUCADescription
# Since one county can have multiple tracts with potentially different RUCA codes,
# we'll take the most common one for each county
print("\nCreating county to RUCA mapping...")
county_ruca = ruca_df.groupby('CountyFIPS20_str')['PrimaryRUCADescription'].agg(
    lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.iloc[0]
).to_dict()

print(f"Created mapping for {len(county_ruca)} counties")

# Map RUCA descriptions to rural/urban
def classify_rural_urban(ruca_desc):
    """
    Classify RUCA description as Rural or Urban
    """
    if pd.isna(ruca_desc) or ruca_desc == 'Not coded':
        return 'Unknown'
    
    ruca_desc_str = str(ruca_desc).lower()
    
    # Urban classifications
    urban_keywords = [
        'metropolitan core',
        'metropolitan high commuting',
        'metropolitan low commuting',
        'micropolitan core',  # Added: Micropolitan areas are urban
        'micropolitan high commuting',
        'micropolitan low commuting'
    ]
    
    # Rural classifications
    rural_keywords = [
        'rural area',
        'small town core',  # Added: Small town core is rural
        'small town high commuting',
        'small town low commuting'
    ]
    
    # Check for urban
    for keyword in urban_keywords:
        if keyword in ruca_desc_str:
            return 'Urban'
    
    # Check for rural
    for keyword in rural_keywords:
        if keyword in ruca_desc_str:
            return 'Rural'
    
    # Check for numeric codes (1-3 are typically urban, 4-10 are rural)
    try:
        ruca_code = int(ruca_desc)
        if ruca_code in [1, 2, 3]:
            return 'Urban'
        elif ruca_code in [4, 5, 6, 7, 8, 9, 10]:
            return 'Rural'
        else:
            return 'Unknown'
    except (ValueError, TypeError):
        pass
    
    return 'Unknown'

# Apply the mapping
print("\nMapping RUCA codes to counties...")
merged_df['PrimaryRUCADescription'] = merged_df['county_fips_str'].map(county_ruca)

# Classify as Rural/Urban
print("Classifying as Rural/Urban...")
merged_df['rural_urban'] = merged_df['PrimaryRUCADescription'].apply(classify_rural_urban)

# Check the results
print("\n Rural/Urban Classification Summary:")
print(merged_df['rural_urban'].value_counts())
print("\n Missing values:")
print(f"  PrimaryRUCADescription missing: {merged_df['PrimaryRUCADescription'].isna().sum()}")
print(f"  rural_urban = Unknown: {(merged_df['rural_urban'] == 'Unknown').sum()}")

# Drop temporary columns
merged_df = merged_df.drop(columns=['county_fips_str'])

# Save the result
output_file = "MERGED_DATA_FOR_LR_NEW.csv"
merged_df.to_csv(output_file, index=False)
print(f"\n Saved to {output_file}")
print(f"   Total rows: {len(merged_df)}")
print(f"   Total columns: {len(merged_df.columns)}")
print(f"   New columns added: 'PrimaryRUCADescription', 'rural_urban'")

