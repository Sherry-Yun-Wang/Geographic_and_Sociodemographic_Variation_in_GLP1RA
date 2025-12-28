"""
Calculate GLP1-RA prescribing rate per 1000 diabetes patients (T2D + Obesity) by state for each year from 2010-2022
Save results to separate CSV files for each year

Denominator: All diabetes patients (T2D + Obesity) by state and year
Numerator: GLP1-RA patients by state and year
"""

import pandas as pd
import os

def calculate_glp1ra_rate_by_diabetes_patients(year, final_data, diabetes_state_counts, output_dir='.'):
    """
    Calculate GLP1-RA prescribing rate per 1000 diabetes patients by state for a given year
    
    Parameters:
        year: Year to calculate
        final_data: DataFrame from FINAL_DATA.csv (contains GLP1-RA patients with state info)
        diabetes_state_counts: DataFrame from updated_state_counts.csv (contains diabetes patient counts by state and year)
        output_dir: Output directory
    
    Returns:
        DataFrame containing results
    """
    print(f"\nProcessing year {year}...")
    
    # Filter GLP1-RA patients for the specified year
    glp1_data_year = final_data[final_data['year'] == year].copy()
    
    if len(glp1_data_year) == 0:
        print(f"  Warning: No GLP1-RA data for year {year}")
        return None
    
    print(f"  GLP1-RA records for {year}: {len(glp1_data_year):,}")
    
    # Count unique GLP1-RA patients by state
    glp1_state_counts = glp1_data_year.groupby('pat_state')['pat_id'].nunique().reset_index()
    glp1_state_counts.columns = ['state_abbrev', 'glp1ra_patients']
    
    # Get diabetes patient counts for the specified year from updated_state_counts.csv
    diabetes_year = diabetes_state_counts[diabetes_state_counts['year'] == year].copy()
    
    if len(diabetes_year) == 0:
        print(f"  Warning: No diabetes patient count data for year {year}")
        return None
    
    # Rename columns for consistency
    diabetes_year = diabetes_year.rename(columns={'pat_state': 'state_abbrev', 'count': 'total_diabetes_patients'})
    diabetes_year = diabetes_year[['state_abbrev', 'total_diabetes_patients']]
    
    print(f"  States with diabetes patient data: {len(diabetes_year)}")
    print(f"  Total diabetes patients for {year}: {diabetes_year['total_diabetes_patients'].sum():,}")
    
    # Merge GLP1-RA counts and diabetes patient counts
    result = glp1_state_counts.merge(diabetes_year, on='state_abbrev', how='outer')
    
    # Fill missing values
    result['glp1ra_patients'] = result['glp1ra_patients'].fillna(0).astype(int)
    result['total_diabetes_patients'] = result['total_diabetes_patients'].fillna(0).astype(int)
    
    # Calculate prescribing rate (per 1000 diabetes patients)
    # Avoid division by zero
    result['prescribing_rate_per_1000'] = (
        result['glp1ra_patients'] / result['total_diabetes_patients'].replace(0, pd.NA) * 1000
    )
    
    # Add year column
    result['year'] = year
    
    # Reorder columns
    result = result[['year', 'state_abbrev', 'glp1ra_patients', 'total_diabetes_patients', 'prescribing_rate_per_1000']]
    
    # Sort by prescribing rate (descending)
    result = result.sort_values('prescribing_rate_per_1000', ascending=False, na_position='last')
    
    # Save to CSV
    output_file = os.path.join(output_dir, f'glp1ra_rate_per_1000_diabetes_patients_by_state_{year}.csv')
    result.to_csv(output_file, index=False)
    
    # Display summary statistics
    total_glp1 = result['glp1ra_patients'].sum()
    total_diabetes = result['total_diabetes_patients'].sum()
    states_with_data = len(result[result['total_diabetes_patients'] > 0])
    states_with_glp1 = len(result[result['glp1ra_patients'] > 0])
    
    # Calculate average rate (excluding NaN)
    valid_rates = result['prescribing_rate_per_1000'].dropna()
    avg_rate = valid_rates.mean() if len(valid_rates) > 0 else 0
    max_rate = valid_rates.max() if len(valid_rates) > 0 else 0
    max_state = result.loc[result['prescribing_rate_per_1000'].idxmax(), 'state_abbrev'] if len(valid_rates) > 0 else 'N/A'
    
    print(f"  Saved to: {output_file}")
    print(f"  Summary: GLP1-RA patients={total_glp1:,}, Total diabetes patients={total_diabetes:,}, "
          f"States with data={states_with_data}, States with GLP1-RA={states_with_glp1}")
    print(f"  Rate: Avg={avg_rate:.4f}, Max={max_rate:.4f} ({max_state})")
    
    return result




def main():
    """Main function"""
    print("="*80)
    print("Calculate GLP1-RA Prescribing Rate per 1000 Diabetes Patients by State (2010-2022)")
    print("="*80)
    
    # Read data
    print("\nReading data...")
    final_data = pd.read_csv('FINAL_DATA.csv')
    
    print(f"FINAL_DATA.csv total records: {len(final_data):,}")
    print(f"Year range: {final_data['year'].min()} - {final_data['year'].max()}")
    
    # Read diabetes patient counts from updated_state_counts.csv
    diabetes_state_counts_file = 'data/updated_state_counts.csv'
    if not os.path.exists(diabetes_state_counts_file):
        print(f"Error: File not found: {diabetes_state_counts_file}")
        return
    
    print(f"\nReading {diabetes_state_counts_file}...")
    diabetes_state_counts = pd.read_csv(diabetes_state_counts_file)
    print(f"Diabetes state counts total records: {len(diabetes_state_counts):,}")
    print(f"Year range: {diabetes_state_counts['year'].min()} - {diabetes_state_counts['year'].max()}")
    print(f"Total diabetes patients (all years): {diabetes_state_counts['count'].sum():,}")
    
    years = range(2010, 2023)
    
    # Create output directory if it doesn't exist
    output_dir = 'glp1ra_rate_by_diabetes_patients_yearly'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"\nCreated output directory: {output_dir}")
    
    # Process years 2010-2022
    all_results = []
    
    for year in years:
        result = calculate_glp1ra_rate_by_diabetes_patients(
            year, final_data, diabetes_state_counts, output_dir
        )
        if result is not None:
            all_results.append(result)
    
    # Create summary file (all years combined)
    if all_results:
        print("\n" + "="*80)
        print("Creating summary file (all years combined)...")
        all_years = pd.concat(all_results, ignore_index=True)
        summary_file = os.path.join(output_dir, 'glp1ra_rate_per_1000_diabetes_patients_by_state_all_years.csv')
        all_years.to_csv(summary_file, index=False)
        print(f"   Summary file saved to: {summary_file}")
        print(f"   Total records: {len(all_years):,}")
        
        # Display yearly trends
        print("\nYearly trends:")
        yearly_summary = all_years.groupby('year').agg({
            'glp1ra_patients': 'sum',
            'total_diabetes_patients': 'sum'
        }).reset_index()
        yearly_summary['overall_rate'] = (yearly_summary['glp1ra_patients'] / 
                                         yearly_summary['total_diabetes_patients'] * 1000)
        yearly_summary.columns = ['year', 'total_glp1_patients', 'total_diabetes_patients', 'overall_rate_per_1000']
        print(yearly_summary.to_string(index=False))
    
    print("\n" + "="*80)
    print("  All files generated successfully!")
    print(f"   Output directory: {output_dir}/")
    print(f"   Generated {len(all_results)} year files + 1 summary file")
    print("="*80)


if __name__ == "__main__":
    main()
