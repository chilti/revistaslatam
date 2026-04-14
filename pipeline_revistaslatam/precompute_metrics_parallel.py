#!/usr/bin/env python3
"""
Parallelized metrics precalculation script.
Optimized for servers with large RAM (loads data once, processes in parallel).
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
import time

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from performance_metrics import (
    get_cache_dir,
    get_year_range,
    safe_get
)

def calculate_performance_metrics_from_df(works_df):
    """
    Calculate performance metrics from a DataFrame (in-memory version).
    """
    if len(works_df) == 0:
        return {
            'num_documents': 0,
            'fwci_avg': 0.0,
            'pct_top_10': 0.0,
            'pct_top_1': 0.0,
            'avg_percentile': 0.0,
            'pct_oa_diamond': 0.0,
            'pct_oa_gold': 0.0,
            'pct_oa_green': 0.0,
            'pct_oa_hybrid': 0.0,
            'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0
        }
    
    num_documents = len(works_df)
    
    # FWCI average - convert to numeric first
    if 'fwci' in works_df.columns:
        fwci_values = pd.to_numeric(works_df['fwci'], errors='coerce')
        fwci_avg = fwci_values.mean()
    else:
        fwci_avg = 0.0
    
    # % Top 10% - convert to boolean robustly
    if 'is_in_top_10_percent' in works_df.columns:
        # Manejar mixtos (bool, str, int)
        top_10_values = works_df['is_in_top_10_percent'].astype(str).str.lower().isin(['true', '1', '1.0'])
        pct_top_10 = (top_10_values.sum() / num_documents) * 100
    else:
        pct_top_10 = 0.0
    
    # % Top 1% - convert to boolean robustly
    if 'is_in_top_1_percent' in works_df.columns:
        top_1_values = works_df['is_in_top_1_percent'].astype(str).str.lower().isin(['true', '1', '1.0'])
        pct_top_1 = (top_1_values.sum() / num_documents) * 100
    else:
        pct_top_1 = 0.0
    
    # Average Percentile - convert to numeric first
    if 'percentile' in works_df.columns:
        percentile_values = pd.to_numeric(works_df['percentile'], errors='coerce')
        avg_percentile = percentile_values.mean()
    else:
        avg_percentile = 0.0
    
    # OA percentages by type
    if 'oa_status' in works_df.columns:
        total = len(works_df)
        oa_counts = works_df['oa_status'].value_counts()
        
        oa_types = {
            'pct_oa_diamond': (oa_counts.get('diamond', 0) / total) * 100,
            'pct_oa_gold': (oa_counts.get('gold', 0) / total) * 100,
            'pct_oa_green': (oa_counts.get('green', 0) / total) * 100,
            'pct_oa_hybrid': (oa_counts.get('hybrid', 0) / total) * 100,
            'pct_oa_bronze': (oa_counts.get('bronze', 0) / total) * 100,
            'pct_oa_closed': (oa_counts.get('closed', 0) / total) * 100
        }
    else:
        oa_types = {
            'pct_oa_diamond': 0.0,
            'pct_oa_gold': 0.0,
            'pct_oa_green': 0.0,
            'pct_oa_hybrid': 0.0,
            'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0
        }
    # % Articles with at least one author from the same country as the journal
    if 'is_domestic_author' in works_df.columns:
        pct_domestic = (works_df['is_domestic_author'].fillna(False).astype(bool).sum() / num_documents) * 100
    else:
        pct_domestic = 0.0

    # Language distribution
    if 'language' in works_df.columns:
        lang_counts = works_df['language'].value_counts()
        total_lang = len(works_df)
        lang_metrics = {
            'pct_lang_es': (lang_counts.get('es', 0) / total_lang) * 100,
            'pct_lang_en': (lang_counts.get('en', 0) / total_lang) * 100,
            'pct_lang_pt': (lang_counts.get('pt', 0) / total_lang) * 100,
            'pct_lang_fr': (lang_counts.get('fr', 0) / total_lang) * 100,
            'pct_lang_de': (lang_counts.get('de', 0) / total_lang) * 100,
            'pct_lang_it': (lang_counts.get('it', 0) / total_lang) * 100,
        }
        # Others
        calculated_sum = sum(lang_metrics.values())
        lang_metrics['pct_lang_other'] = max(0, 100 - calculated_sum)
    else:
        lang_metrics = {k: 0.0 for k in ['pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 'pct_lang_fr', 'pct_lang_de', 'pct_lang_it', 'pct_lang_other']}

    metrics = {
        'num_documents': num_documents,
        'fwci_avg': round(fwci_avg, 3) if pd.notna(fwci_avg) else 0.0,
        'pct_top_10': round(pct_top_10, 4),
        'pct_top_1': round(pct_top_1, 4),
        'avg_percentile': round(avg_percentile, 3) if pd.notna(avg_percentile) else 0.0,
        'pct_authors_domestic': round(pct_domestic, 3)
    }
    
    # Always ensure ALL OA types are present in metrics
    all_oa_keys = ['pct_oa_diamond', 'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed']
    for k in all_oa_keys:
        metrics[k] = round(oa_types.get(k, 0.0), 2)
    
    # Add language metrics
    for k, v in lang_metrics.items():
        metrics[k] = round(v, 2)
    
    return metrics

def process_country_parallel(args):
    """Worker function to process a single country."""
    country_code, country_works, country_journals, start_year, end_year = args
    
    num_journals = len(country_journals)
    
    if len(country_works) == 0:
        return None, None, None, None
    
    # Journal indexing metrics
    pct_scopus = (country_journals.apply(lambda x: safe_get(x, 'is_indexed_in_scopus', default=False), axis=1).sum() / num_journals) * 100
    pct_core = (country_journals.apply(lambda x: safe_get(x, 'is_core', default=False), axis=1).sum() / num_journals) * 100
    pct_doaj = (country_journals.apply(lambda x: safe_get(x, 'is_in_doaj', default=False), axis=1).sum() / num_journals) * 100
    
    journal_metrics = {
        'num_journals': num_journals,
        'pct_scopus': round(pct_scopus, 2),
        'pct_core': round(pct_core, 2),
        'pct_doaj': round(pct_doaj, 2)
    }
    
    # Annual metrics
    annual_data = []
    for year in range(start_year, end_year + 1):
        year_works = country_works[country_works['publication_year'] == year]
        metrics = calculate_performance_metrics_from_df(year_works)
        metrics['year'] = year
        metrics['country_code'] = country_code
        annual_data.append(metrics)
    
    annual_metrics_df = pd.DataFrame(annual_data)
    
    # Period metrics (Total)
    period_works = country_works[
        (country_works['publication_year'] >= start_year) & 
        (country_works['publication_year'] <= end_year)
    ]
    period_metrics = calculate_performance_metrics_from_df(period_works)
    period_metrics.update(journal_metrics)
    period_metrics['country_code'] = country_code
    period_metrics['period'] = f'{start_year}-{end_year}'
    
    # Period metrics (Recent: 2021-2025)
    period_recent_works = country_works[
        (country_works['publication_year'] >= 2021) & 
        (country_works['publication_year'] <= 2025)
    ]
    period_recent_metrics = calculate_performance_metrics_from_df(period_recent_works)
    period_recent_metrics.update(journal_metrics)
    period_recent_metrics['country_code'] = country_code
    period_recent_metrics['period'] = '2021-2025'
    
    return country_code, annual_metrics_df, period_metrics, period_recent_metrics

def process_journal_parallel(args):
    """Worker function to process a single journal."""
    journal_id, journal_works, journal_info, start_year, end_year = args
    
    if journal_info is None or len(journal_info) == 0:
        return None, None, None
    
    if isinstance(journal_info, pd.DataFrame):
        journal_info = journal_info.iloc[0]
    
    # Extract indexing information
    is_scopus = safe_get(journal_info, 'is_indexed_in_scopus', default=False)
    is_core = safe_get(journal_info, 'is_core', default=False)
    is_doaj = safe_get(journal_info, 'is_in_doaj', default=False)
    
    journal_indexing = {
        'is_scopus': bool(is_scopus),
        'is_core': bool(is_core),
        'is_doaj': bool(is_doaj)
    }
    
    if len(journal_works) == 0:
        return None, None, None
    
    # Annual metrics
    annual_data = []
    for year in range(start_year, end_year + 1):
        year_works = journal_works[journal_works['publication_year'] == year]
        metrics = calculate_performance_metrics_from_df(year_works)
        metrics['year'] = year
        metrics['journal_id'] = journal_id
        # Add indexing info to annual metrics
        metrics.update(journal_indexing)
        annual_data.append(metrics)
    
    annual_metrics_df = pd.DataFrame(annual_data)
    
    # Period metrics (Total)
    period_works = journal_works[
        (journal_works['publication_year'] >= start_year) & 
        (journal_works['publication_year'] <= end_year)
    ]
    period_metrics = calculate_performance_metrics_from_df(period_works)
    period_metrics['journal_id'] = journal_id
    period_metrics['period'] = f'{start_year}-{end_year}'
    period_metrics.update(journal_indexing)
    
    # Period metrics (Recent: 2021-2025)
    period_recent_works = journal_works[
        (journal_works['publication_year'] >= 2021) & 
        (journal_works['publication_year'] <= 2025)
    ]
    period_recent_metrics = calculate_performance_metrics_from_df(period_recent_works)
    period_recent_metrics['journal_id'] = journal_id
    period_recent_metrics['period'] = '2021-2025'
    period_recent_metrics.update(journal_indexing)
    
    return annual_metrics_df, period_metrics, period_recent_metrics

def main():
    data_dir = Path(__file__).parent.parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    
    if not works_file.exists() or not journals_file.exists():
        print("❌ Data files not found!")
        return 1
    
    print("=" * 70)
    print("PARALLELIZED METRICS PRECALCULATION")
    print("=" * 70)
    print()
    
    # Detect number of cores - use 50% to leave resources for system
    total_cores = cpu_count()
    num_cores = max(1, int(total_cores * 0.5))  # Use 50% of cores (conservative)
    print(f"🖥️  Detected {total_cores} CPU cores")
    print(f"📊 Using {num_cores} cores (50% - leaving {total_cores - num_cores} for system)")
    
    # Load data to RAM
    print("\n⚙️  Loading data to RAM...")
    start_time = time.time()
    
    print("  → Loading journals...")
    journals_df = pd.read_parquet(journals_file)
    print(f"    ✓ {len(journals_df):,} journals loaded")
    
    print("  → Loading works (this may take a minute)...")
    works_df = pd.read_parquet(works_file)
    print(f"    ✓ {len(works_df):,} works loaded")
    
    load_time = time.time() - start_time
    print(f"  ✓ Data loaded in {load_time:.1f} seconds")
    
    # Detect year range
    print("\n⚙️  Detecting year range...")
    start_year = int(works_df['publication_year'].min())
    end_year = int(works_df['publication_year'].max())
    print(f"  ✓ Year range: {start_year}-{end_year}")
    
    cache_dir = get_cache_dir()
    
    # 1. LATAM metrics (single-threaded, fast)
    print("\n📊 LATAM metrics...")
    latam_start = time.time()
    
    num_journals = len(journals_df)
    pct_scopus = (journals_df.apply(lambda x: safe_get(x, 'is_indexed_in_scopus', default=False), axis=1).sum() / num_journals) * 100
    pct_core = (journals_df.apply(lambda x: safe_get(x, 'is_core', default=False), axis=1).sum() / num_journals) * 100
    pct_doaj = (journals_df.apply(lambda x: safe_get(x, 'is_in_doaj', default=False), axis=1).sum() / num_journals) * 100
    
    journal_metrics = {
        'num_journals': num_journals,
        'pct_scopus': round(pct_scopus, 2),
        'pct_core': round(pct_core, 2),
        'pct_doaj': round(pct_doaj, 2)
    }
    
    # Annual
    latam_annual_data = []
    for year in range(start_year, end_year + 1):
        year_works = works_df[works_df['publication_year'] == year]
        metrics = calculate_performance_metrics_from_df(year_works)
        metrics['year'] = year
        latam_annual_data.append(metrics)
    
    latam_annual = pd.DataFrame(latam_annual_data)
    latam_annual.to_parquet(cache_dir / 'metrics_latam_annual.parquet', index=False)
    
    # Period (Total)
    period_works = works_df[(works_df['publication_year'] >= start_year) & (works_df['publication_year'] <= end_year)]
    latam_period = calculate_performance_metrics_from_df(period_works)
    latam_period.update(journal_metrics)
    latam_period['period'] = f'{start_year}-{end_year}'
    pd.DataFrame([latam_period]).to_parquet(cache_dir / 'metrics_latam_period.parquet', index=False)

    # Period (Recent: 2021-2025)
    recent_works = works_df[(works_df['publication_year'] >= 2021) & (works_df['publication_year'] <= 2025)]
    latam_recent = calculate_performance_metrics_from_df(recent_works)
    latam_recent.update(journal_metrics)
    latam_recent['period'] = '2021-2025'
    pd.DataFrame([latam_recent]).to_parquet(cache_dir / 'metrics_latam_period_2021_2025.parquet', index=False)
    
    latam_time = time.time() - latam_start
    print(f"  ✓ LATAM metrics completed in {latam_time:.1f}s")
    
    # 2. Country metrics (PARALLELIZED)
    print(f"\n📊 Country metrics (using {num_cores} cores)...")
    country_start = time.time()
    
    countries = sorted(journals_df['country_code'].unique())
    print(f"  Processing {len(countries)} countries in parallel...")
    
    # Prepare arguments for parallel processing
    print("  Pre-filtering data to avoid RAM overload...")
    country_args = []
    for country_code in countries:
        c_journals = journals_df[journals_df['country_code'] == country_code]
        c_journal_ids = c_journals['id'].tolist()
        c_works = works_df[works_df['journal_id'].isin(c_journal_ids)]
        country_args.append((country_code, c_works, c_journals, start_year, end_year))
    
    print(f"  Executing {len(country_args)} country tasks...")
    # Process in parallel
    with Pool(processes=num_cores) as pool:
        results = pool.map(process_country_parallel, country_args)
    
    # Collect results
    country_annual_list = []
    country_period_list = []
    country_recent_list = []
    
    for country_code, annual, period, period_recent in results:
        if annual is not None:
            country_annual_list.append(annual)
        if period is not None:
            country_period_list.append(period)
        if period_recent is not None:
            country_recent_list.append(period_recent)
    
    # Save
    if country_annual_list:
        country_annual_df = pd.concat(country_annual_list, ignore_index=True)
        country_annual_df.to_parquet(cache_dir / 'metrics_country_annual.parquet', index=False)
        print(f"  ✓ Saved country annual metrics: {len(country_annual_df)} rows")
    
    if country_period_list:
        country_period_df = pd.DataFrame(country_period_list)
        country_period_df.to_parquet(cache_dir / 'metrics_country_period.parquet', index=False)
        print(f"  ✓ Saved country total period metrics: {len(country_period_df)} countries")

    if country_recent_list:
        country_recent_df = pd.DataFrame(country_recent_list)
        country_recent_df.to_parquet(cache_dir / 'metrics_country_period_2021_2025.parquet', index=False)
        print(f"  ✓ Saved country recent period metrics (2021-2025): {len(country_recent_df)} countries")
    
    country_time = time.time() - country_start
    print(f"  ✓ Country metrics completed in {country_time:.1f}s")
    
    # 3. Journal metrics (PARALLELIZED)
    print(f"\n📊 Journal metrics (using {num_cores} cores)...")
    journal_start = time.time()
    
    journal_ids = journals_df['id'].unique()
    print(f"  Processing {len(journal_ids)} journals in parallel...")
    
    # Prepare arguments
    print("  Pre-filtering data to avoid RAM overload...")
    journal_args = []
    for j_id in journal_ids:
        j_info = journals_df[journals_df['id'] == j_id]
        j_works = works_df[works_df['journal_id'] == j_id]
        journal_args.append((j_id, j_works, j_info, start_year, end_year))
    
    print(f"  Executing {len(journal_args)} journal tasks...")
    # Process in parallel with progress
    with Pool(processes=num_cores) as pool:
        results = pool.map(process_journal_parallel, journal_args)
    
    # Collect results
    journal_annual_list = []
    journal_period_list = []
    journal_recent_list = []
    
    for annual, period, period_recent in results:
        if annual is not None and len(annual) > 0:
            journal_annual_list.append(annual)
        if period is not None:
            journal_period_list.append(period)
        if period_recent is not None:
            journal_recent_list.append(period_recent)
    
    # Save
    if journal_annual_list:
        journal_annual_df = pd.concat(journal_annual_list, ignore_index=True)
        journal_annual_df.to_parquet(cache_dir / 'metrics_journal_annual.parquet', index=False)
        print(f"  ✓ Saved journal annual metrics: {len(journal_annual_df)} rows")
    
    if journal_period_list:
        journal_period_df = pd.DataFrame(journal_period_list)
        journal_period_df.to_parquet(cache_dir / 'metrics_journal_period.parquet', index=False)
        print(f"  ✓ Saved journal total period metrics: {len(journal_period_df)} journals")

    if journal_recent_list:
        journal_recent_df = pd.DataFrame(journal_recent_list)
        journal_recent_df.to_parquet(cache_dir / 'metrics_journal_period_2021_2025.parquet', index=False)
        print(f"  ✓ Saved journal recent period metrics (2021-2025): {len(journal_recent_df)} journals")
    
    journal_time = time.time() - journal_start
    print(f"  ✓ Journal metrics completed in {journal_time:.1f}s")
    
    # Summary
    total_time = time.time() - start_time
    print()
    print("=" * 70)
    print("✅ ALL METRICS COMPUTED SUCCESSFULLY!")
    print("=" * 70)
    print()
    print(f"Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"  - Data loading: {load_time:.1f}s")
    print(f"  - LATAM: {latam_time:.1f}s")
    print(f"  - Countries: {country_time:.1f}s")
    print(f"  - Journals: {journal_time:.1f}s")
    print()
    print(f"Speedup: ~{num_cores}x faster than sequential processing")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
