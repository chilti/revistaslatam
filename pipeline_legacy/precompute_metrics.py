#!/usr/bin/env python3
"""
Standalone script to precompute and cache performance metrics.

This script should be run after downloading works data to precalculate
all performance indicators (FWCI, percentiles, etc.) and cache them for
faster dashboard loading.

Usage:
    python precompute_metrics.py [--force]
    
Options:
    --force    Force recalculation even if cache is valid
"""

import os
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from performance_metrics import compute_and_cache_all_metrics

def main():
    parser = argparse.ArgumentParser(description='Precompute and cache performance metrics')
    parser.add_argument('--force', action='store_true', 
                       help='Force recalculation even if cache is valid')
    args = parser.parse_args()
    
    # Define paths
    data_dir = Path(__file__).parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    
    # Check if files exist
    if not works_file.exists():
        print(f"❌ Error: Works file not found: {works_file}")
        print("   Please run data_collector.py first to download article data.")
        sys.exit(1)
    
    if not journals_file.exists():
        print(f"❌ Error: Journals file not found: {journals_file}")
        print("   Please run data_collector.py first to download journal data.")
        sys.exit(1)
    
    print("=" * 70)
    print("📊 METRICS PRECALCULATION")
    print("=" * 70)
    print(f"Works file: {works_file}")
    print(f"Journals file: {journals_file}")
    print(f"Force recalculation: {args.force}")
    print("=" * 70)
    print()
    
    try:
        result = compute_and_cache_all_metrics(
            str(works_file),
            str(journals_file),
            force_recalculate=args.force
        )
        
        if result:
            print()
            print("=" * 70)
            print("✅ SUCCESS: Metrics precalculation completed!")
            print("=" * 70)
            print()
            print("Summary:")
            print(f"  • Journal annual metrics: {len(result['journal_annual']):,} rows")
            print(f"  • Journal period metrics: {len(result['journal_period']):,} journals")
            print(f"  • Country annual metrics: {len(result['country_annual']):,} rows")
            print(f"  • Country period metrics: {len(result['country_period']):,} countries")
            print(f"  • LATAM annual metrics: {len(result['latam_annual']):,} years")
            print(f"  • LATAM period metrics: Computed")
            print()
            print("Metrics calculated:")
            print("  - No. Documents (by year and 2021-2025)")
            print("  - FWCI Average")
            print("  - % Top 10% (highly cited)")
            print("  - % Top 1% (highly cited)")
            print("  - Average Percentile")
            print("  - OA Types: % Gold, Green, Hybrid, Bronze, Closed")
            print("  - Journal indicators: % Scopus, CORE, DOAJ")
            print()
            print("Cache location: data/cache/")
            print()
            print("The dashboard will now load metrics from cache for faster performance.")
        else:
            print()
            print("⚠️  Warning: Precalculation returned no results.")
            print("   Check if works data is available.")
            sys.exit(1)
            
    except Exception as e:
        print()
        print("=" * 70)
        print("❌ ERROR during metrics precalculation")
        print("=" * 70)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
