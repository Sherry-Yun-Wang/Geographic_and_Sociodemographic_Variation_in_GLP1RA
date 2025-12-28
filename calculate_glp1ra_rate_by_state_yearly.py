"""
Calculate GLP1-RA prescribing rate per 1000 residents by state for each year from 2010-2022
Save results to separate CSV files for each year
"""

import pandas as pd
import os

def calculate_glp1ra_rate_by_state_year(year, final_data, state_pop, output_dir='.'):
    """
    Calculate GLP1-RA prescribing rate by state for a given year
    
    Parameters:
        year: Year to calculate
        final_data: DataFrame from FINAL_DATA.csv
        state_pop: DataFrame from state_pop_estimates.csv
        output_dir: Output directory
    
    Returns:
        DataFrame containing results
    """
    print(f"\nProcessing year {year}...")
    
    # Filter data for the specified year
    data_year = final_data[final_data['year'] == year].copy()
    
    if len(data_year) == 0:
        print(f"  Warning: No data for year {year}")
        return None
    
    print(f"  Total records for {year}: {len(data_year):,}")
    
    # Count unique GLP1-RA patients by state
    state_counts = data_year.groupby('pat_state')['pat_id'].nunique().reset_index()
    state_counts.columns = ['state_abbrev', 'glp1ra_patients']
    print(f"  States with data: {len(state_counts)}")
    
    # Get state population for the specified year
    pop_year = state_pop[state_pop['year'] == year][['state_abbrev', 'population']].copy()
    
    # Merge data
    result = state_counts.merge(pop_year, on='state_abbrev', how='outer')
    
    # Fill missing values (states with no patients set to 0)
    result['glp1ra_patients'] = result['glp1ra_patients'].fillna(0).astype(int)
    
    # Handle DC (District of Columbia) population data if missing
    if 'DC' in result['state_abbrev'].values:
        dc_pop = result[result['state_abbrev'] == 'DC']['population'].values[0]
        if pd.isna(dc_pop):
            # DC population data for 2010-2022 (approximate values, update as needed)
            dc_populations = {
                2010: 601723, 2011: 617996, 2012: 632323, 2013: 646449,
                2014: 658893, 2015: 672228, 2016: 681170, 2017: 693972,
                2018: 702455, 2019: 705749, 2020: 689545, 2021: 670050,
                2022: 671803
            }
            if year in dc_populations:
                result.loc[result['state_abbrev'] == 'DC', 'population'] = dc_populations[year]
                print(f"  Fixed DC population data: {dc_populations[year]:,}")
    
    # Calculate prescribing rate (per 1000 residents)
    result['prescribing_rate_per_1000'] = (result['glp1ra_patients'] / result['population']) * 1000
    
    # Add year column
    result['year'] = year
    
    # Reorder columns
    result = result[['year', 'state_abbrev', 'glp1ra_patients', 'population', 'prescribing_rate_per_1000']]
    
    # Sort by prescribing rate (descending)
    result = result.sort_values('prescribing_rate_per_1000', ascending=False)
    
    # Save to CSV
    output_file = os.path.join(output_dir, f'glp1ra_prescribing_rate_by_state_{year}.csv')
    result.to_csv(output_file, index=False)
    
    # Display summary statistics
    total_patients = result['glp1ra_patients'].sum()
    states_with_patients = len(result[result['glp1ra_patients'] > 0])
    avg_rate = result['prescribing_rate_per_1000'].mean()
    max_rate = result['prescribing_rate_per_1000'].max()
    max_state = result.loc[result['prescribing_rate_per_1000'].idxmax(), 'state_abbrev']
    
    print(f"   Saved to: {output_file}")
    print(f"  Summary: Total patients={total_patients:,}, States with patients={states_with_patients}, "
          f"Avg rate={avg_rate:.4f}, Max={max_rate:.4f} ({max_state})")
    
    return result


def main():
    """Main function"""
    print("="*80)
    print("Calculate GLP1-RA Prescribing Rate per 1000 Residents by State (2010-2022)")
    print("="*80)
    
    # Read data
    print("\nReading data...")
    final_data = pd.read_csv('FINAL_DATA.csv')
    state_pop = pd.read_csv('state_pop_estimates.csv')
    
    print(f"FINAL_DATA.csv total records: {len(final_data):,}")
    print(f"Year range: {final_data['year'].min()} - {final_data['year'].max()}")
    print(f"State population data year range: {state_pop['year'].min()} - {state_pop['year'].max()}")
    
    # Create output directory if it doesn't exist
    output_dir = 'glp1ra_rate_by_state_yearly'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"\nCreated output directory: {output_dir}")
    
    # Process years 2010-2022
    years = range(2010, 2023)
    all_results = []
    
    for year in years:
        result = calculate_glp1ra_rate_by_state_year(year, final_data, state_pop, output_dir)
        if result is not None:
            all_results.append(result)
    
    # Create summary file (all years combined)
    if all_results:
        print("\n" + "="*80)
        print("Creating summary file (all years combined)...")
        all_years = pd.concat(all_results, ignore_index=True)
        summary_file = os.path.join(output_dir, 'glp1ra_prescribing_rate_by_state_all_years.csv')
        all_years.to_csv(summary_file, index=False)
        print(f"   Summary file saved to: {summary_file}")
        print(f"   Total records: {len(all_years):,}")
        
        # Display yearly total patients trend
        print("\nYearly total patients trend:")
        yearly_totals = all_years.groupby('year')['glp1ra_patients'].sum().reset_index()
        yearly_totals.columns = ['year', 'total_patients']
        print(yearly_totals.to_string(index=False))
    
    print("\n" + "="*80)
    print("   All files generated successfully!")
    print(f"   Output directory: {output_dir}/")
    print(f"   Generated {len(all_results)} year files + 1 summary file")
    print("="*80)


if __name__ == "__main__":
    main()
