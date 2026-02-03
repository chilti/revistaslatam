#!/usr/bin/env python3
"""
Optimized parallelized metrics precalculation script.
Uses chunked processing to avoid memory exhaustion.
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
import time

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

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
    """
    if len(works_df) == 0:
        return {
            'num_documents': 0,
            'fwci_avg': 0.0,
            'pct_top_10': 0.0,
            'pct_top_1': 0.0,
            'avg_percentile': 0.0,
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
    
    # % Top 10% - convert to boolean
    if 'is_in_top_10_percent' in works_df.columns:
        top_10_values = pd.to_numeric(works_df['is_in_top_10_percent'], errors='coerce').fillna(0).astype(bool)
        pct_top_10 = (top_10_values.sum() / num_documents) * 100
    else:
        pct_top_10 = 0.0
    
    # % Top 1% - convert to boolean
    if 'is_in_top_1_percent' in works_df.columns:
        top_1_values = pd.to_numeric(works_df['is_in_top_1_percent'], errors='coerce').fillna(0).astype(bool)
        pct_top_1 = (top_1_values.sum() / num_documents) * 100
    else:
        pct_top_1 = 0.0
    
    # Average Percentile - convert to numeric first
    if 'citation_normalized_percentile' in works_df.columns:
        percentile_values = pd.to_numeric(works_df['citation_normalized_percentile'], errors='coerce')
        avg_percentile = percentile_values.mean()
    else:
        avg_percentile = 0.0
    
    # OA percentages by type
    if 'oa_status' in works_df.columns:
        total = len(works_df)
        oa_counts = works_df['oa_status'].value_counts()
        
        oa_types = {
            'pct_oa_gold': (oa_counts.get('gold', 0) / total) * 100,
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
        'fwci_avg': round(fwci_avg, 2) if pd.notna(fwci_avg) else 0.0,
        'pct_top_10': round(pct_top_10, 2),
        'pct_top_1': round(pct_top_1, 2),
        'avg_percentile': round(avg_percentile, 2) if pd.notna(avg_percentile) else 0.0
    }
    
    metrics.update({k: round(v, 2) for k, v in oa_types.items()})
    
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
        return None, None, None
    
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
    
    return country_code, annual_metrics_df, period_metrics

def process_journal_worker(journal_id):
    """Worker function to process a single journal (uses global data)."""
    global _works_df, _start_year, _end_year
    
    # Filter works for this journal
    journal_works = _works_df[_works_df['journal_id'] == journal_id].copy()
    
    if len(journal_works) == 0:
        return None, None
    
    # Annual metrics
    annual_data = []
    for year in range(_start_year, _end_year + 1):
        year_works = journal_works[journal_works['publication_year'] == year]
        metrics = calculate_performance_metrics_from_df(year_works)
        metrics['year'] = year
        metrics['journal_id'] = journal_id
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
    
    return annual_metrics_df, period_metrics

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

def main():
    global works_file, journals_file, start_year, end_year
    
    data_dir = Path(__file__).parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    
    if not works_file.exists() or not journals_file.exists():
        print("❌ Data files not found!")
        return 1
    
    print("=" * 70)
    print("OPTIMIZED PARALLELIZED METRICS PRECALCULATION")
    print("=" * 70)
    print()
    
    # Detect number of cores - use fewer cores to reduce memory pressure
    total_cores = cpu_count()
    # Use 25% of cores (more conservative) or max 8 cores
    num_cores = min(8, max(1, int(total_cores * 0.25)))
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
    
    # Period
    period_works = works_df[(works_df['publication_year'] >= start_year) & (works_df['publication_year'] <= end_year)]
    latam_period = calculate_performance_metrics_from_df(period_works)
    latam_period.update(journal_metrics)
    latam_period['period'] = f'{start_year}-{end_year}'
    pd.DataFrame([latam_period]).to_parquet(cache_dir / 'metrics_latam_period.parquet', index=False)
    
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
    
    # Process in chunks (smaller chunks = less memory per batch)
    chunk_size = max(1, len(countries) // 4)  # Process ~4 batches
    results = process_in_chunks(countries, process_country_worker, num_cores, 
                                chunk_size, desc="countries")
    
    # Collect results
    country_annual_list = []
    country_period_list = []
    
    for country_code, annual, period in results:
        if annual is not None:
            country_annual_list.append(annual)
        if period is not None:
            country_period_list.append(period)
    
    # Save
    if country_annual_list:
        country_annual_df = pd.concat(country_annual_list, ignore_index=True)
        country_annual_df.to_parquet(cache_dir / 'metrics_country_annual.parquet', index=False)
        print(f"  ✓ Saved country annual metrics: {len(country_annual_df)} rows")
    
    if country_period_list:
        country_period_df = pd.DataFrame(country_period_list)
        country_period_df.to_parquet(cache_dir / 'metrics_country_period.parquet', index=False)
        print(f"  ✓ Saved country period metrics: {len(country_period_df)} countries")
    
    country_time = time.time() - country_start
    print(f"  ✓ Country metrics completed in {country_time:.1f}s")
    
    # 3. Journal metrics (CHUNKED PARALLEL)
    print(f"\n📊 Journal metrics (chunked processing)...")
    journal_start = time.time()
    
    # Load just to get journal IDs
    temp_journals = pd.read_parquet(journals_file)
    journal_ids = temp_journals['id'].unique().tolist()
    del temp_journals
    
    # Process in chunks (smaller chunks for journals since there are more)
    chunk_size = max(10, len(journal_ids) // 20)  # Process ~20 batches
    results = process_in_chunks(journal_ids, process_journal_worker, num_cores, 
                                chunk_size, desc="journals")
    
    # Collect results
    journal_annual_list = []
    journal_period_list = []
    
    for annual, period in results:
        if annual is not None and len(annual) > 0:
            journal_annual_list.append(annual)
        if period is not None:
            journal_period_list.append(period)
    
    # Save
    if journal_annual_list:
        journal_annual_df = pd.concat(journal_annual_list, ignore_index=True)
        journal_annual_df.to_parquet(cache_dir / 'metrics_journal_annual.parquet', index=False)
        print(f"  ✓ Saved journal annual metrics: {len(journal_annual_df)} rows")
    
    if journal_period_list:
        journal_period_df = pd.DataFrame(journal_period_list)
        journal_period_df.to_parquet(cache_dir / 'metrics_journal_period.parquet', index=False)
        print(f"  ✓ Saved journal period metrics: {len(journal_period_df)} journals")
    
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
