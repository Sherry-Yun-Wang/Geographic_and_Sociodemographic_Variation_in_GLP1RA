"""
Add diabetes/obesity patient rate (diabetes_patients / population) as a new variable
to MERGED_DATA_FOR_LR_NEW.csv
"""

import pandas as pd
import os

def add_diabetes_rate(input_file, output_file):
    """
    Add diabetes patient rate to merged data
    
    Parameters:
        input_file: Input CSV file (MERGED_DATA_FOR_LR.csv or MERGED_DATA_FOR_LR_NEW.csv)
        output_file: Output CSV file (MERGED_DATA_FOR_LR_NEW.csv)
    """
    print("="*80)
    print("Add Diabetes/Obesity Patient Rate to Merged Data")
    print("="*80)
    
    # Read input data
    print(f"\nReading {input_file}...")
    df = pd.read_csv(input_file)
    print(f"Input data shape: {df.shape}")
    print(f"Columns: {len(df.columns)}")
    
    # Read diabetes patient counts
    print("\nReading diabetes patient counts...")
    diabetes_counts = pd.read_csv('data/updated_state_counts.csv')
    print(f"Diabetes counts shape: {diabetes_counts.shape}")
    print(f"Year range: {diabetes_counts['year'].min()} - {diabetes_counts['year'].max()}")
    
    # Rename columns for merge
    diabetes_counts = diabetes_counts.rename(columns={
        'pat_state': 'state_abbrev',
        'count': 'diabetes_patients_count'
    })
    
    # Check merge keys
    print("\nChecking merge keys...")
    print(f"Input data has 'state_id': {'state_id' in df.columns}")
    print(f"Input data has 'year': {'year' in df.columns}")
    print(f"Input data has 'state_population': {'state_population' in df.columns}")
    
    if 'state_id' not in df.columns:
        print("Error: 'state_id' column not found in input data")
        return
    
    # Merge diabetes counts with input data
    print("\nMerging diabetes patient counts...")
    df_merged = df.merge(
        diabetes_counts[['state_abbrev', 'year', 'diabetes_patients_count']],
        left_on=['state_id', 'year'],
        right_on=['state_abbrev', 'year'],
        how='left'
    )
    
    # Check merge results
    missing_diabetes = df_merged['diabetes_patients_count'].isna().sum()
    print(f"Rows with missing diabetes data: {missing_diabetes} ({missing_diabetes/len(df_merged)*100:.2f}%)")
    
    # Calculate diabetes rate (diabetes_patients / state_population)
    print("\nCalculating diabetes rate...")
    if 'state_population' in df_merged.columns:
        df_merged['diabetes_rate'] = (
            df_merged['diabetes_patients_count'] / df_merged['state_population']
        )
        print(f"Diabetes rate range: {df_merged['diabetes_rate'].min():.6f} - {df_merged['diabetes_rate'].max():.6f}")
        print(f"Mean diabetes rate: {df_merged['diabetes_rate'].mean():.6f}")
    else:
        print("Warning: 'state_population' column not found, cannot calculate rate")
        print("Available columns:", df_merged.columns.tolist()[:20])
        return
    
    # Drop the temporary state_abbrev column from merge (keep state_id)
    if 'state_abbrev' in df_merged.columns and 'state_abbrev' != 'state_id':
        df_merged = df_merged.drop(columns=['state_abbrev'])
    
    # Save to output file
    print(f"\nSaving to {output_file}...")
    df_merged.to_csv(output_file, index=False)
    
    print(f"   Saved! Output shape: {df_merged.shape}")
    print(f"   New columns added: 'diabetes_patients_count', 'diabetes_rate'")
    
    # Display summary
    print("\n" + "="*80)
    print("Summary Statistics")
    print("="*80)
    print(f"Total rows: {len(df_merged):,}")
    print(f"Rows with diabetes data: {df_merged['diabetes_patients_count'].notna().sum():,}")
    print(f"Rows with diabetes rate: {df_merged['diabetes_rate'].notna().sum():,}")
    
    if df_merged['diabetes_rate'].notna().sum() > 0:
        valid_rate = df_merged['diabetes_rate'].dropna()
        print(f"\nDiabetes rate statistics:")
        print(f"  Mean: {valid_rate.mean():.6f}")
        print(f"  Median: {valid_rate.median():.6f}")
        print(f"  Min: {valid_rate.min():.6f}")
        print(f"  Max: {valid_rate.max():.6f}")
        print(f"  Std: {valid_rate.std():.6f}")
    
    # Show sample
    print("\nSample data (first 5 rows with diabetes data):")
    sample = df_merged[df_merged['diabetes_rate'].notna()][
        ['state_id', 'year', 'glp1ra_count', 'state_population', 
         'diabetes_patients_count', 'diabetes_rate']
    ].head()
    print(sample.to_string(index=False))
    
    print("\n" + "="*80)
    print(" Done!")
    print("="*80)
    
    return df_merged


def main():
    """Main function"""
    # Check which input file exists
    input_file = None
    if os.path.exists('MERGED_DATA_FOR_LR_NEW.csv'):
        input_file = 'MERGED_DATA_FOR_LR_NEW.csv'
        print("Found MERGED_DATA_FOR_LR_NEW.csv, will update it")
    elif os.path.exists('MERGED_DATA_FOR_LR.csv'):
        input_file = 'MERGED_DATA_FOR_LR.csv'
        print("Found MERGED_DATA_FOR_LR.csv, will create MERGED_DATA_FOR_LR_NEW.csv")
    else:
        print("Error: Neither MERGED_DATA_FOR_LR.csv nor MERGED_DATA_FOR_LR_NEW.csv found")
        return
    
    output_file = 'MERGED_DATA_FOR_LR_NEW.csv'
    
    result = add_diabetes_rate(input_file, output_file)
    
    if result is not None:
        print(f"\n  Successfully created/updated {output_file}")
        print(f"   You can now use 'diabetes_rate' as a predictor in your models")


if __name__ == "__main__":
    main()
