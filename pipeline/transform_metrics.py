#!/usr/bin/env python3
"""
Optimized parallelized metrics precalculation script.
Uses chunked processing to avoid memory exhaustion.
Supports incremental processing to skip already computed metrics.
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
import time
import argparse

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from performance_metrics import (
    get_cache_dir,
    get_year_range,
    safe_get
)

# Global variables for worker processes (loaded once per process)
_works_df = None
_journals_df = None
_start_year = None
_end_year = None

def init_worker(works_file, journals_file, start_year, end_year):
    """Initialize worker process with data (loaded once per worker)."""
    global _works_df, _journals_df, _start_year, _end_year
    _works_df = pd.read_parquet(works_file)
    _journals_df = pd.read_parquet(journals_file)
    _start_year = start_year
    _end_year = end_year

def calculate_performance_metrics_from_df(works_df):
    """
    Calculate performance metrics from a DataFrame (in-memory version).
    Matches the logic from performance_metrics.py MetricsAccumulator.
    """
    if len(works_df) == 0:
        return {
            'num_documents': 0,
            'fwci_avg': 0.0,
            'pct_top_10': 0.0,
            'pct_top_1': 0.0,
            'avg_percentile': 0.0,
            'pct_oa_gold': 0.0,
            'pct_oa_diamond': 0.0,
            'pct_oa_green': 0.0,
            'pct_oa_hybrid': 0.0,
            'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0
        }
    
    num_documents = len(works_df)
    
    # FWCI average - convert to numeric, fillna(0), then calculate mean
    # This matches MetricsAccumulator logic: sum(fillna(0)) / count
    if 'fwci' in works_df.columns:
        fwci_values = pd.to_numeric(works_df['fwci'], errors='coerce').fillna(0)
        fwci_avg = fwci_values.sum() / num_documents
    else:
        fwci_avg = 0.0
    
    # % Top 10% - fillna(False) then convert to bool (matches original)
    if 'is_in_top_10_percent' in works_df.columns:
        top_10_values = works_df['is_in_top_10_percent'].fillna(False).astype(bool)
        pct_top_10 = (top_10_values.sum() / num_documents) * 100
    else:
        pct_top_10 = 0.0
    
    # % Top 1% - fillna(False) then convert to bool (matches original)
    if 'is_in_top_1_percent' in works_df.columns:
        top_1_values = works_df['is_in_top_1_percent'].fillna(False).astype(bool)
        pct_top_1 = (top_1_values.sum() / num_documents) * 100
    else:
        pct_top_1 = 0.0
    
    # Average Percentile - convert to numeric, fillna(0), then calculate mean
    # This matches MetricsAccumulator logic: sum(fillna(0)) / count
    if 'citation_normalized_percentile' in works_df.columns:
        percentile_values = pd.to_numeric(works_df['citation_normalized_percentile'], errors='coerce').fillna(0)
        avg_percentile = percentile_values.sum() / num_documents
    else:
        avg_percentile = 0.0
    
    # OA percentages by type
    if 'oa_status' in works_df.columns:
        total = len(works_df)
        oa_counts = works_df['oa_status'].value_counts()
        
        oa_types = {
            'pct_oa_gold': (oa_counts.get('gold', 0) / total) * 100,
            'pct_oa_diamond': (oa_counts.get('diamond', 0) / total) * 100,
            'pct_oa_green': (oa_counts.get('green', 0) / total) * 100,
            'pct_oa_hybrid': (oa_counts.get('hybrid', 0) / total) * 100,
            'pct_oa_bronze': (oa_counts.get('bronze', 0) / total) * 100,
            'pct_oa_closed': (oa_counts.get('closed', 0) / total) * 100
        }
    else:
        oa_types = {
            'pct_oa_gold': 0.0,
            'pct_oa_green': 0.0,
            'pct_oa_hybrid': 0.0,
            'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0
        }
    
    metrics = {
        'num_documents': num_documents,
        'fwci_avg': round(fwci_avg, 6),
        'pct_top_10': round(pct_top_10, 6),
        'pct_top_1': round(pct_top_1, 6),
        'avg_percentile': round(avg_percentile, 6)
    }
    
    metrics.update({k: round(v, 6) for k, v in oa_types.items()})
    
    return metrics

def process_country_worker(country_code):
    """Worker function to process a single country (uses global data)."""
    global _works_df, _journals_df, _start_year, _end_year
    
    # Get journals for this country
    country_journals = _journals_df[_journals_df['country_code'] == country_code]
    num_journals = len(country_journals)
    journal_ids = country_journals['id'].tolist()
    
    # Filter works for this country
    country_works = _works_df[_works_df['journal_id'].isin(journal_ids)].copy()
    
    if len(country_works) == 0:
        return None, None, None, None
    
    # Journal indexing metrics
    pct_scopus = (country_journals.apply(lambda x: safe_get(x, 'is_indexed_in_scopus', default=False), axis=1).sum() / num_journals) * 100
    pct_core = (country_journals.apply(lambda x: safe_get(x, 'is_core', default=False), axis=1).sum() / num_journals) * 100
    pct_doaj = (country_journals.apply(lambda x: safe_get(x, 'is_in_doaj', default=False), axis=1).sum() / num_journals) * 100
    
    journal_metrics = {
        'num_journals': num_journals,
        'pct_scopus': round(pct_scopus, 6),
        'pct_core': round(pct_core, 6),
        'pct_doaj': round(pct_doaj, 6)
    }
    
    # Annual metrics
    annual_data = []
    for year in range(_start_year, _end_year + 1):
        year_works = country_works[country_works['publication_year'] == year]
        metrics = calculate_performance_metrics_from_df(year_works)
        metrics['year'] = year
        metrics['country_code'] = country_code
        annual_data.append(metrics)
    
    annual_metrics_df = pd.DataFrame(annual_data)
    
    # Period metrics
    period_works = country_works[
        (country_works['publication_year'] >= _start_year) & 
        (country_works['publication_year'] <= _end_year)
    ]
    period_metrics = calculate_performance_metrics_from_df(period_works)
    period_metrics.update(journal_metrics)
    period_metrics['country_code'] = country_code
    period_metrics['period'] = f'{_start_year}-{_end_year}'
    
    period_metrics['period'] = f'{_start_year}-{_end_year}'
    
    # Recent Period metrics (2021-2025)
    period_recent_works = country_works[
        (country_works['publication_year'] >= 2021) & 
        (country_works['publication_year'] <= 2025)
    ]
    period_recent_metrics = calculate_performance_metrics_from_df(period_recent_works)
    period_recent_metrics.update(journal_metrics)
    period_recent_metrics['country_code'] = country_code
    period_recent_metrics['period'] = '2021-2025'
    
    return country_code, annual_metrics_df, period_metrics, period_recent_metrics

def process_journal_worker(journal_id):
    """Worker function to process a single journal (uses global data)."""
    global _works_df, _journals_df, _start_year, _end_year
    
    # Get journal metadata
    journal_info = _journals_df[_journals_df['id'] == journal_id]
    
    if len(journal_info) == 0:
        return None, None, None, None
    
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
    
    # Filter works for this journal
    journal_works = _works_df[_works_df['journal_id'] == journal_id].copy()
    
    if len(journal_works) == 0:
        return None, None, None, None
    
    # Annual metrics
    annual_data = []
    for year in range(_start_year, _end_year + 1):
        year_works = journal_works[journal_works['publication_year'] == year]
        metrics = calculate_performance_metrics_from_df(year_works)
        metrics['year'] = year
        metrics['journal_id'] = journal_id
        # Add indexing info to annual metrics
        metrics.update(journal_indexing)
        annual_data.append(metrics)
    
    annual_metrics_df = pd.DataFrame(annual_data)
    
    # Period metrics
    period_works = journal_works[
        (journal_works['publication_year'] >= _start_year) & 
        (journal_works['publication_year'] <= _end_year)
    ]
    period_metrics = calculate_performance_metrics_from_df(period_works)
    period_metrics['journal_id'] = journal_id
    period_metrics['period'] = f'{_start_year}-{_end_year}'
    # Add indexing info to period metrics
    period_metrics.update(journal_indexing)
    
    # Add indexing info to period metrics
    period_metrics.update(journal_indexing)
    
    # Recent Period metrics (2021-2025)
    period_recent_works = journal_works[
        (journal_works['publication_year'] >= 2021) & 
        (journal_works['publication_year'] <= 2025)
    ]
    period_recent_metrics = calculate_performance_metrics_from_df(period_recent_works)
    period_recent_metrics['journal_id'] = journal_id
    period_recent_metrics['period'] = '2021-2025'
    # Add indexing info to recent period metrics
    period_recent_metrics.update(journal_indexing)
    
    return journal_id, annual_metrics_df, period_metrics, period_recent_metrics
    


def process_in_chunks(items, worker_func, num_cores, chunk_size, desc="items"):
    """Process items in chunks to control memory usage."""
    all_results = []
    total_items = len(items)
    num_chunks = (total_items + chunk_size - 1) // chunk_size
    
    print(f"  Processing {total_items} {desc} in {num_chunks} chunks of {chunk_size}...")
    
    for i in range(0, total_items, chunk_size):
        chunk = items[i:i + chunk_size]
        chunk_num = i // chunk_size + 1
        print(f"    Chunk {chunk_num}/{num_chunks}: processing {len(chunk)} {desc}...", end=' ')
        
        chunk_start = time.time()
        with Pool(processes=num_cores, initializer=init_worker, 
                  initargs=(works_file, journals_file, start_year, end_year)) as pool:
            results = pool.map(worker_func, chunk)
        
        all_results.extend(results)
        chunk_time = time.time() - chunk_start
        print(f"✓ ({chunk_time:.1f}s)")
    
    return all_results

def load_existing_metrics(cache_dir, metric_type):
    """Load existing metrics if they exist."""
    file_map = {
        'country_annual': 'metrics_country_annual.parquet',
        'country_period': 'metrics_country_period.parquet',
        'country_period_recent': 'metrics_country_period_2021_2025.parquet',
        'journal_annual': 'metrics_journal_annual.parquet',
        'journal_period': 'metrics_journal_period.parquet',
        'journal_period_recent': 'metrics_journal_period_2021_2025.parquet'
    }
    
    file_path = cache_dir / file_map.get(metric_type)
    if file_path.exists():
        return pd.read_parquet(file_path)
    return None

def get_items_to_process(all_items, existing_df, id_column, force=False):
    """
    Determine which items need to be processed.
    
    Args:
        all_items: List of all item IDs (countries or journals)
        existing_df: DataFrame with existing metrics (or None)
        id_column: Column name containing the ID ('country_code' or 'journal_id')
        force: If True, process all items regardless of existing metrics
    
    Returns:
        Tuple of (items_to_process, existing_df)
    """
    if force or existing_df is None:
        return all_items, existing_df
    
    existing_ids = set(existing_df[id_column].unique())
    items_to_process = [item for item in all_items if item not in existing_ids]
    
    return items_to_process, existing_df

def main():
    global works_file, journals_file, start_year, end_year
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Precompute metrics for Latin American journals (optimized with incremental processing)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force recalculation of all metrics, even if they already exist'
    )
    args = parser.parse_args()
    
    data_dir = Path(__file__).parent.parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    
    if not works_file.exists() or not journals_file.exists():
        print("❌ Data files not found!")
        return 1
    
    print("=" * 70)
    print("OPTIMIZED PARALLELIZED METRICS PRECALCULATION")
    if args.force:
        print("MODE: FORCE (recalculating all metrics)")
    else:
        print("MODE: INCREMENTAL (skipping existing metrics)")
    print("=" * 70)
    print()
    
    # Detect number of cores - use fewer cores to reduce memory pressure
    total_cores = cpu_count()
    # Use 25% of cores (more conservative) or max 8 cores
    num_cores = min(4, max(1, int(total_cores * 0.10)))
    print(f"🖥️  Detected {total_cores} CPU cores")
    print(f"📊 Using {num_cores} cores (conservative to manage memory)")
    
    # Load data to detect year range (main process only)
    print("\n⚙️  Loading metadata...")
    start_time = time.time()
    
    print("  → Loading journals metadata...")
    journals_df = pd.read_parquet(journals_file)
    print(f"    ✓ {len(journals_df):,} journals")
    
    print("  → Loading works metadata...")
    works_df = pd.read_parquet(works_file)
    print(f"    ✓ {len(works_df):,} works")
    
    # Estimate memory usage
    works_memory_mb = works_df.memory_usage(deep=True).sum() / 1024 / 1024
    journals_memory_mb = journals_df.memory_usage(deep=True).sum() / 1024 / 1024
    total_memory_mb = works_memory_mb + journals_memory_mb
    estimated_total_mb = total_memory_mb * (num_cores + 1)  # Each worker + main
    
    print(f"\n💾 Memory usage estimate:")
    print(f"  - Works DataFrame: {works_memory_mb:.1f} MB")
    print(f"  - Journals DataFrame: {journals_memory_mb:.1f} MB")
    print(f"  - Per process: {total_memory_mb:.1f} MB")
    print(f"  - Estimated total ({num_cores} workers): {estimated_total_mb:.1f} MB ({estimated_total_mb/1024:.1f} GB)")
    
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
        'pct_scopus': round(pct_scopus, 6),
        'pct_core': round(pct_core, 6),
        'pct_doaj': round(pct_doaj, 6)
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
    
    # Period
    period_works = works_df[(works_df['publication_year'] >= start_year) & (works_df['publication_year'] <= end_year)]
    latam_period = calculate_performance_metrics_from_df(period_works)
    latam_period.update(journal_metrics)
    latam_period['period'] = f'{start_year}-{end_year}'
    pd.DataFrame([latam_period]).to_parquet(cache_dir / 'metrics_latam_period.parquet', index=False)
    
    # Recent Period (2021-2025)
    period_recent_works = works_df[(works_df['publication_year'] >= 2021) & (works_df['publication_year'] <= 2025)]
    latam_period_recent = calculate_performance_metrics_from_df(period_recent_works)
    latam_period_recent.update(journal_metrics)
    latam_period_recent['period'] = '2021-2025'
    pd.DataFrame([latam_period_recent]).to_parquet(cache_dir / 'metrics_latam_period_2021_2025.parquet', index=False)
    
    latam_time = time.time() - latam_start
    print(f"  ✓ LATAM metrics completed in {latam_time:.1f}s")
    
    # Free memory from main process
    del works_df, journals_df
    
    # 2. Country metrics (CHUNKED PARALLEL)
    print(f"\n📊 Country metrics (chunked processing)...")
    country_start = time.time()
    
    # Load just to get country list
    temp_journals = pd.read_parquet(journals_file)
    countries = sorted(temp_journals['country_code'].unique())
    del temp_journals
    
    # Load existing metrics and determine what to process
    existing_country_annual = load_existing_metrics(cache_dir, 'country_annual')
    existing_country_period = load_existing_metrics(cache_dir, 'country_period')
    existing_country_period_recent = load_existing_metrics(cache_dir, 'country_period_recent')
    
    countries_to_process, _ = get_items_to_process(
        countries, existing_country_period, 'country_code', force=args.force
    )
    
    if len(countries_to_process) == 0:
        print(f"  ℹ️  All {len(countries)} countries already processed (use --force to recalculate)")
        country_time = 0
    else:
        if not args.force and existing_country_period is not None:
            print(f"  ℹ️  Found existing metrics for {len(countries) - len(countries_to_process)} countries")
            print(f"  📝 Processing {len(countries_to_process)} new countries...")
        else:
            print(f"  📝 Processing all {len(countries_to_process)} countries...")
        
        # Process in chunks (smaller chunks = less memory per batch)
        chunk_size = max(1, len(countries_to_process) // 4)  # Process ~4 batches
        results = process_in_chunks(countries_to_process, process_country_worker, num_cores, 
                                    chunk_size, desc="countries")
        
        # Collect results
        country_annual_list = []
        country_period_list = []
        country_period_recent_list = []
        
        for country_code, annual, period, period_recent in results:
            if annual is not None:
                country_annual_list.append(annual)
            if period is not None:
                country_period_list.append(period)
            if period_recent is not None:
                country_period_recent_list.append(period_recent)
        
        # Combine with existing metrics if in incremental mode
        if country_annual_list:
            new_country_annual = pd.concat(country_annual_list, ignore_index=True)
            if not args.force and existing_country_annual is not None:
                country_annual_df = pd.concat([existing_country_annual, new_country_annual], ignore_index=True)
                print(f"  ✓ Combined {len(new_country_annual)} new rows with {len(existing_country_annual)} existing rows")
            else:
                country_annual_df = new_country_annual
            country_annual_df.to_parquet(cache_dir / 'metrics_country_annual.parquet', index=False)
            print(f"  ✓ Saved country annual metrics: {len(country_annual_df)} total rows")
        
        if country_period_list:
            new_country_period = pd.DataFrame(country_period_list)
            if not args.force and existing_country_period is not None:
                country_period_df = pd.concat([existing_country_period, new_country_period], ignore_index=True)
                print(f"  ✓ Combined {len(new_country_period)} new countries with {len(existing_country_period)} existing")
            else:
                country_period_df = new_country_period
            country_period_df.to_parquet(cache_dir / 'metrics_country_period.parquet', index=False)
            print(f"  ✓ Saved country period metrics: {len(country_period_df)} total countries")
        
        if country_period_recent_list:
            new_country_period_recent = pd.DataFrame(country_period_recent_list)
            if not args.force and existing_country_period_recent is not None:
                country_period_recent_df = pd.concat([existing_country_period_recent, new_country_period_recent], ignore_index=True)
                print(f"  ✓ Combined {len(new_country_period_recent)} new countries (recent) with {len(existing_country_period_recent)} existing")
            else:
                country_period_recent_df = new_country_period_recent
            country_period_recent_df.to_parquet(cache_dir / 'metrics_country_period_2021_2025.parquet', index=False)
            print(f"  ✓ Saved country recent period metrics: {len(country_period_recent_df)} total countries")
        
        country_time = time.time() - country_start
        print(f"  ✓ Country metrics completed in {country_time:.1f}s")

    
    # 3. Journal metrics (CHUNKED PARALLEL)
    print(f"\n📊 Journal metrics (chunked processing)...")
    journal_start = time.time()
    
    # Load just to get journal IDs
    temp_journals = pd.read_parquet(journals_file)
    journal_ids = temp_journals['id'].unique().tolist()
    del temp_journals
    
    # Load existing metrics and determine what to process
    existing_journal_annual = load_existing_metrics(cache_dir, 'journal_annual')
    existing_journal_period = load_existing_metrics(cache_dir, 'journal_period')
    existing_journal_period_recent = load_existing_metrics(cache_dir, 'journal_period_recent')
    
    journals_to_process, _ = get_items_to_process(
        journal_ids, existing_journal_period, 'journal_id', force=args.force
    )
    
    if len(journals_to_process) == 0:
        print(f"  ℹ️  All {len(journal_ids)} journals already processed (use --force to recalculate)")
        journal_time = 0
    else:
        if not args.force and existing_journal_period is not None:
            print(f"  ℹ️  Found existing metrics for {len(journal_ids) - len(journals_to_process)} journals")
            print(f"  📝 Processing {len(journals_to_process)} new journals...")
        else:
            print(f"  📝 Processing all {len(journals_to_process)} journals...")
        
        # Process in chunks (smaller chunks for journals since there are more)
        chunk_size = max(10, len(journals_to_process) // 20)  # Process ~20 batches
        results = process_in_chunks(journals_to_process, process_journal_worker, num_cores, 
                                    chunk_size, desc="journals")
        
        # Collect results
        journal_annual_list = []
        journal_period_list = []
        journal_period_recent_list = []
        
        for annual, period, period_recent in results:
            if annual is not None and len(annual) > 0:
                journal_annual_list.append(annual)
            if period is not None:
                journal_period_list.append(period)
            if period_recent is not None:
                journal_period_recent_list.append(period_recent)
        
        # Combine with existing metrics if in incremental mode
        if journal_annual_list:
            new_journal_annual = pd.concat(journal_annual_list, ignore_index=True)
            if not args.force and existing_journal_annual is not None:
                journal_annual_df = pd.concat([existing_journal_annual, new_journal_annual], ignore_index=True)
                print(f"  ✓ Combined {len(new_journal_annual)} new rows with {len(existing_journal_annual)} existing rows")
            else:
                journal_annual_df = new_journal_annual
            journal_annual_df.to_parquet(cache_dir / 'metrics_journal_annual.parquet', index=False)
            print(f"  ✓ Saved journal annual metrics: {len(journal_annual_df)} total rows")
        
        if journal_period_list:
            new_journal_period = pd.DataFrame(journal_period_list)
            if not args.force and existing_journal_period is not None:
                journal_period_df = pd.concat([existing_journal_period, new_journal_period], ignore_index=True)
                print(f"  ✓ Combined {len(new_journal_period)} new journals with {len(existing_journal_period)} existing")
            else:
                journal_period_df = new_journal_period
            journal_period_df.to_parquet(cache_dir / 'metrics_journal_period.parquet', index=False)
            print(f"  ✓ Saved journal period metrics: {len(journal_period_df)} total journals")
        
        if journal_period_recent_list:
            new_journal_period_recent = pd.DataFrame(journal_period_recent_list)
            if not args.force and existing_journal_period_recent is not None:
                journal_period_recent_df = pd.concat([existing_journal_period_recent, new_journal_period_recent], ignore_index=True)
                print(f"  ✓ Combined {len(new_journal_period_recent)} new journals (recent) with {len(existing_journal_period_recent)} existing")
            else:
                journal_period_recent_df = new_journal_period_recent
            journal_period_recent_df.to_parquet(cache_dir / 'metrics_journal_period_2021_2025.parquet', index=False)
            print(f"  ✓ Saved journal recent period metrics: {len(journal_period_recent_df)} total journals")
        
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
    print(f"  - LATAM: {latam_time:.1f}s")
    print(f"  - Countries: {country_time:.1f}s")
    print(f"  - Journals: {journal_time:.1f}s")
    print()
    print(f"Memory-optimized: chunked processing with {num_cores} cores")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
