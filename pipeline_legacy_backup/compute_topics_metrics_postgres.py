#!/usr/bin/env python3
"""
Topic-level metrics calculation for Postgres pipeline.
Aggregates performance metrics (FWCI, Percentile, OA) at each level 
of the topic hierarchy (Domain, Field, Subfield).
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
import time

# Add src to path if needed (assuming structure)
# sys.path.append(str(Path(__file__).parent / 'src'))

def calculate_metrics_for_group(group_df):
    """
    Calculates the standard suite of metrics for a given subset of works.
    Matches the logic used in other pipeline scripts.
    """
    total = len(group_df)
    if total == 0:
        return pd.Series({
            'count': 0,
            'fwci_avg': 0.0,
            'avg_percentile': 0.0,
            'pct_top_10': 0.0,
            'pct_top_1': 0.0,
            'pct_oa_gold': 0.0,
            'pct_oa_green': 0.0,
            'pct_oa_hybrid': 0.0,
            'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0
        })

    # Numeric metrics
    fwci = pd.to_numeric(group_df['fwci'], errors='coerce').mean()
    percentile = pd.to_numeric(group_df['citation_normalized_percentile'], errors='coerce').mean()
    
    # Booleans / Binary counts
    top10 = (pd.to_numeric(group_df['is_in_top_10_percent'], errors='coerce').fillna(0).astype(bool).sum() / total) * 100
    top1 = (pd.to_numeric(group_df['is_in_top_1_percent'], errors='coerce').fillna(0).astype(bool).sum() / total) * 100
    
    # OA Status
    oa_counts = group_df['oa_status'].value_counts()
    
    return pd.Series({
        'count': total,
        'fwci_avg': round(fwci, 3) if pd.notna(fwci) else 0.0,
        'avg_percentile': round(percentile, 1) if pd.notna(percentile) else 0.0,
        'pct_top_10': round(top10, 2),
        'pct_top_1': round(top1, 2),
        'pct_oa_gold': round((oa_counts.get('gold', 0) / total) * 100, 2),
        'pct_oa_green': round((oa_counts.get('green', 0) / total) * 100, 2),
        'pct_oa_hybrid': round((oa_counts.get('hybrid', 0) / total) * 100, 2),
        'pct_oa_bronze': round((oa_counts.get('bronze', 0) / total) * 100, 2),
        'pct_oa_closed': round((oa_counts.get('closed', 0) / total) * 100, 2)
    })

def aggregate_hierarchy(df, group_cols):
    """
    Performs the hierarchical aggregation.
    group_cols: list of columns identifying the entity (e.g., ['country_code'] or ['journal_id'])
    """
    levels = ['domain', 'field', 'subfield']
    all_results = []
    
    # 1. Level: Subfield (Deepest)
    print(f"  → Aggregating level: Subfield...")
    res_sub = df.groupby(group_cols + levels).apply(calculate_metrics_for_group).reset_index()
    res_sub['level'] = 'subfield'
    all_results.append(res_sub)
    
    # 2. Level: Field
    print(f"  → Aggregating level: Field...")
    res_field = df.groupby(group_cols + ['domain', 'field']).apply(calculate_metrics_for_group).reset_index()
    res_field['subfield'] = 'ALL' # Filler for hierarchy
    res_field['level'] = 'field'
    all_results.append(res_field)
    
    # 3. Level: Domain
    print(f"  → Aggregating level: Domain...")
    res_domain = df.groupby(group_cols + ['domain']).apply(calculate_metrics_for_group).reset_index()
    res_domain['field'] = 'ALL'
    res_domain['subfield'] = 'ALL'
    res_domain['level'] = 'domain'
    all_results.append(res_domain)

    return pd.concat(all_results, ignore_index=True)

def main():
    data_dir = Path(__file__).parent.parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    topics_file = data_dir / 'journals_topics_sunburst.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    
    output_dir = data_dir / 'cache'
    output_dir.mkdir(exist_ok=True)

    print("=" * 70)
    print("POSTGRES TOPIC METRICS ENGINE (PANDAS)")
    print("=" * 70)

    # 1. Load Data
    print("\n⚙️  Loading data...")
    works_df = pd.read_parquet(works_file, columns=[
        'id', 'journal_id', 'fwci', 'citation_normalized_percentile', 
        'is_in_top_10_percent', 'is_in_top_1_percent', 'oa_status', 'publication_year'
    ])
    topics_df = pd.read_parquet(topics_file)
    journals_df = pd.read_parquet(journals_file, columns=['id', 'country_code'])
    
    print(f"  ✓ {len(works_df):,} works loaded")
    
    # 2. Map Topics to Works
    # We use primary_topic association. Since 'latin_american_works.parquet' might 
    # not have 'domain'/'field' columns directly, we join with topics_df.
    # Note: topics_df has journal_id, domain, field, subfield.
    # We need a per-work topic mapping. If it's not in works_df, we use the journal's main topics.
    # BUT, the goal is topic-level indicators.
    
    # Check if works_df has topic info
    # ... assuming for now we need to join journal_id with topics hierarchy ...
    
    print("\n⚙️  Preparing hierarchy...")
    # journals_topics_sunburst has journal_id, topic_name, subfield, field, domain, count
    # Usually, a journal has multiple topics. To simplify "Topic Sunburst" with indicators,
    # we assign each work to its journal's primary topics.
    
    # For a truer representation, we'd need work -> topic mapping from PostgreSQL.
    # Let's assume topics_df represents the distribution of topics in journals.
    
    # Merge with journals to get country_code
    print("  → Merging works with journals...")
    works_df = pd.merge(works_df, journals_df, left_on='journal_id', right_on='id')
    
    # PRE-AGGREGATE at (journal_id, country_code) level to reduce rows significantly
    # instead of 3.4M work rows, we'll have (num_journals * country_code) rows
    print("  → Pre-aggregating works at journal level...")
    
    # For numeric averages, we need to track sum and count to do weighted averages later if needed,
    # or just pre-calculate journal-level metrics.
    # To keep it simple and accurate, let's group by journal_id.
    
    # Journal-level pre-aggregation
    # Using include_groups=False if pandas >= 2.2.0, else standard apply
    try:
        journal_agg = works_df.groupby(['journal_id', 'country_code']).apply(calculate_metrics_for_group, include_groups=False).reset_index()
    except TypeError:
        # Fallback for older pandas
        journal_agg = works_df.groupby(['journal_id', 'country_code']).apply(calculate_metrics_for_group).reset_index()
    
    # Get hierarchy and share per topic
    # Hierarchy and share metadata
    journal_hierarchy = topics_df[['journal_id', 'domain', 'field', 'subfield', 'share']].copy()
    
    # We need to Ensure share is a float explicitly to avoid TypeError during assignment
    journal_hierarchy['share'] = pd.to_numeric(journal_hierarchy['share'], errors='coerce').fillna(0.0).astype(float)
    
    # FALLBACK: If a journal has 0 total share, distribute 1.0 equally among its topics
    print("  → Normalizing topic shares...")
    shares_sum = journal_hierarchy.groupby('journal_id')['share'].transform('sum')
    mask_zero = (shares_sum <= 0)
    
    if mask_zero.any():
        print(f"    (Fixing {mask_zero.sum()} topics with zero share)")
        # For journals with 0 total share, count how many topics they have
        topic_counts = journal_hierarchy.groupby('journal_id')['journal_id'].transform('count')
        journal_hierarchy.loc[mask_zero, 'share'] = 1.0 / topic_counts[mask_zero]
    
    # Final normalization to ensure sum = 1.0 for all journals
    shares_sum = journal_hierarchy.groupby('journal_id')['share'].transform('sum')
    journal_hierarchy['share'] = journal_hierarchy['share'] / shares_sum
    
    # Merge pre-aggregated metrics with hierarchy
    print("  → Merging aggregated works with topic hierarchy...")
    # journal_agg had a 'count' column, hierarchies have 'share'.
    # Double check no 'count' in journal_hierarchy
    if 'count' in journal_hierarchy.columns:
        journal_hierarchy = journal_hierarchy.drop(columns=['count'])
        
    enriched_agg = pd.merge(journal_agg, journal_hierarchy, on='journal_id')
    
    # Partition the count
    enriched_agg['count'] = enriched_agg['count'] * enriched_agg['share']
    
    # Debug: Check if we have positive counts now
    pos_count = (enriched_agg['count'] > 0).sum()
    print(f"  ✓ Intermediate enriched records with positive count: {pos_count}")
    
# Helper to calculate metrics from PRE-AGGREGATED data
def calculate_from_agg(df):
    total_docs = df['count'].sum()
    if total_docs == 0:
        return pd.Series({
            'count': 0, 'fwci_avg': 0.0, 'avg_percentile': 0.0,
            'pct_top_10': 0.0, 'pct_top_1': 0.0, 'pct_oa_gold': 0.0,
            'pct_oa_green': 0.0, 'pct_oa_hybrid': 0.0, 'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0
        })
    
    # Weighted averages for FWCI and Percentile
    fwci = (df['fwci_avg'] * df['count']).sum() / total_docs
    perc = (df['avg_percentile'] * df['count']).sum() / total_docs
    
    # Weighted averages for percentages
    res = {
        'count': total_docs,
        'fwci_avg': round(fwci, 3),
        'avg_percentile': round(perc, 1)
    }
    for col in ['pct_top_10', 'pct_top_1', 'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed']:
        res[col] = round((df[col] * df['count']).sum() / total_docs, 2)
        
    return pd.Series(res)

def aggregate_hierarchy_from_agg(df, group_cols, suffix=""):
    levels = ['domain', 'field', 'subfield']
    all_results = []
    
    # Helper for modern pandas compatibility
    def apply_with_groups_fix(obj, func):
        try:
            return obj.apply(func, include_groups=False)
        except TypeError:
            return obj.apply(func)

    print(f"  → Aggregating level: Subfield ({suffix})...")
    res_sub = apply_with_groups_fix(df.groupby(group_cols + levels), calculate_from_agg).reset_index()
    res_sub['level'] = 'subfield'
    all_results.append(res_sub)
    
    print(f"  → Aggregating level: Field ({suffix})...")
    res_field = apply_with_groups_fix(df.groupby(group_cols + ['domain', 'field']), calculate_from_agg).reset_index()
    res_field['subfield'] = 'ALL'
    res_field['level'] = 'field'
    all_results.append(res_field)
    
    print(f"  → Aggregating level: Domain ({suffix})...")
    res_domain = apply_with_groups_fix(df.groupby(group_cols + ['domain']), calculate_from_agg).reset_index()
    res_domain['field'] = 'ALL'
    res_domain['subfield'] = 'ALL'
    res_domain['level'] = 'domain'
    all_results.append(res_domain)

    final_df = pd.concat(all_results, ignore_index=True)
    
    # Apply suffix to metric columns
    if suffix:
        cols_to_rename = {col: f"{col}_{suffix}" for col in final_df.columns if col not in (group_cols + levels + ['level'])}
        final_df = final_df.rename(columns=cols_to_rename)
    
    return final_df

def main():
    data_dir = Path(__file__).parent.parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    topics_file = data_dir / 'journals_topics_sunburst.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    
    output_dir = data_dir / 'cache'
    output_dir.mkdir(exist_ok=True)

    print("=" * 70)
    print("POSTGRES TOPIC METRICS ENGINE (PANDAS) - DUAL PERIOD")
    print("=" * 70)

    # 1. Load Data
    print("\n⚙️  Loading data...")
    works_df = pd.read_parquet(works_file, columns=[
        'id', 'journal_id', 'fwci', 'citation_normalized_percentile', 
        'is_in_top_10_percent', 'is_in_top_1_percent', 'oa_status', 'publication_year'
    ])
    topics_df = pd.read_parquet(topics_file)
    journals_df = pd.read_parquet(journals_file, columns=['id', 'country_code'])
    
    print(f"  ✓ {len(works_df):,} works loaded")
    
    # Merge with journals to get country_code
    print("  → Merging works with journals...")
    works_df = pd.merge(works_df, journals_df, left_on='journal_id', right_on='id')
    
    # Function to process a specific dataframe subset
    def process_period(df_subset, period_suffix):
        print(f"\n📑 Processing Period: {period_suffix.upper()}...")
        
        # Aggregation at journal level
        try:
            j_agg = df_subset.groupby(['journal_id', 'country_code']).apply(calculate_metrics_for_group, include_groups=False).reset_index()
        except TypeError:
            j_agg = df_subset.groupby(['journal_id', 'country_code']).apply(calculate_metrics_for_group).reset_index()
            
        # Topic hierarchy metadata
        j_h = topics_df[['journal_id', 'domain', 'field', 'subfield', 'share']].copy()
        j_h['share'] = pd.to_numeric(j_h['share'], errors='coerce').fillna(0.0).astype(float)
        
        # Share normalization
        s_sum = j_h.groupby('journal_id')['share'].transform('sum')
        mask_z = (s_sum <= 0)
        if mask_z.any():
            t_counts = j_h.groupby('journal_id')['journal_id'].transform('count')
            j_h.loc[mask_z, 'share'] = 1.0 / t_counts[mask_z]
        s_sum = j_h.groupby('journal_id')['share'].transform('sum')
        j_h['share'] = j_h['share'] / s_sum
        
        # Merge metrics + topic hierarchy
        enr = pd.merge(j_agg, j_h, on='journal_id')
        enr['count'] = enr['count'] * enr['share']
        
        # Aggregate at hierarchy levels
        c_m = aggregate_hierarchy_from_agg(enr, ['country_code'], period_suffix)
        l_m = aggregate_hierarchy_from_agg(enr, [], period_suffix)
        l_m['country_code'] = 'LATAM'
        j_m = aggregate_hierarchy_from_agg(enr, ['journal_id'], period_suffix)
        
        return c_m, l_m, j_m

    # PERIOD 1: Full
    c_full, l_full, j_full = process_period(works_df, "full")
    
    # PERIOD 2: Recent (2021-2025)
    recent_mask = (works_df['publication_year'] >= 2021)
    c_recent, l_recent, j_recent = process_period(works_df[recent_mask], "recent")

    # Combine results
    merge_cols = ['country_code', 'domain', 'field', 'subfield', 'level']
    final_country = pd.merge(c_full, c_recent, on=merge_cols, how='outer').fillna(0)
    final_latam = pd.merge(l_full, l_recent, on=merge_cols, how='outer').fillna(0)
    
    merge_cols_j = ['journal_id', 'domain', 'field', 'subfield', 'level']
    final_journal = pd.merge(j_full, j_recent, on=merge_cols_j, how='outer').fillna(0)

    # Save
    final_country.to_parquet(output_dir / 'sunburst_metrics_country.parquet', index=False)
    final_latam.to_parquet(output_dir / 'sunburst_metrics_latam.parquet', index=False)
    final_journal.to_parquet(output_dir / 'sunburst_metrics_journal.parquet', index=False)

    print(f"\n✅ Topic Metrics (Dual Period) Saved to {output_dir}")

if __name__ == "__main__":
    main()
