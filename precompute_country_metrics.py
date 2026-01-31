#!/usr/bin/env python3
"""
Script to calculate only country-level metrics (faster than full precalculation).
"""
import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from performance_metrics import (
    calculate_country_metrics_chunked,
    get_year_range,
    get_cache_dir
)
import pandas as pd
import pyarrow.parquet as pq

def main():
    # Paths
    data_dir = Path(__file__).parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    
    if not works_file.exists():
        print(f"❌ Works file not found: {works_file}")
        return 1
    
    if not journals_file.exists():
        print(f"❌ Journals file not found: {journals_file}")
        return 1
    
    print("=" * 70)
    print("COUNTRY METRICS PRECALCULATION")
    print("=" * 70)
    print()
    
    # Load journals
    print("⚙️  Loading journals data...")
    journals_df = pd.read_parquet(journals_file)
    print(f"✓ Loaded {len(journals_df):,} journals")
    
    # Verify works file
    parquet_file = pq.ParquetFile(works_file)
    total_works = parquet_file.metadata.num_rows
    print(f"✓ Works file contains {total_works:,} articles")
    
    # Detect year range
    print("\n⚙️  Detecting year range...")
    start_year, end_year = get_year_range(str(works_file))
    print(f"✓ Year range: {start_year}-{end_year}")
    
    # Calculate country metrics
    print("\n📊 Calculating country metrics...")
    cache_dir = get_cache_dir()
    
    country_annual_list = []
    country_period_list = []
    
    countries = sorted(journals_df['country_code'].unique())
    print(f"  Found {len(countries)} countries: {', '.join(countries)}")
    print()
    
    for idx, country_code in enumerate(countries, 1):
        print(f"  [{idx}/{len(countries)}] Processing {country_code}...")
        
        try:
            annual, period, _ = calculate_country_metrics_chunked(
                str(works_file), 
                journals_df, 
                country_code, 
                start_year, 
                end_year
            )
            
            if annual is not None:
                country_annual_list.append(annual)
                print(f"      ✓ Annual metrics: {len(annual)} years")
            
            if period is not None:
                country_period_list.append(period)
                print(f"      ✓ Period metrics calculated")
        except Exception as e:
            print(f"      ❌ Error: {e}")
    
    # Save results
    print("\n⚙️  Saving results...")
    
    if country_annual_list:
        country_annual_df = pd.concat(country_annual_list, ignore_index=True)
        country_annual_df.to_parquet(cache_dir / 'metrics_country_annual.parquet', index=False)
        print(f"  ✓ Saved country annual metrics: {len(country_annual_df)} rows")
    else:
        print("  ⚠️  No annual metrics to save")
    
    if country_period_list:
        country_period_df = pd.DataFrame(country_period_list)
        country_period_df.to_parquet(cache_dir / 'metrics_country_period.parquet', index=False)
        print(f"  ✓ Saved country period metrics: {len(country_period_df)} countries")
    else:
        print("  ⚠️  No period metrics to save")
    
    print()
    print("=" * 70)
    print("✅ Country metrics calculation complete!")
    print("=" * 70)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
