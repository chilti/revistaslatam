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
    # A journal can have multiple topics, each with a 'share' (0.0 to 1.0)
    journal_hierarchy = topics_df[['journal_id', 'domain', 'field', 'subfield', 'share']].copy()
    
    # FALLBACK: If share is 0 or missing, distribute equally among the journal's topics
    def distribute_shares(df):
        s_sum = df['share'].sum()
        if s_sum <= 0:
            df.loc[:, 'share'] = 1.0 / len(df)
        else:
            df.loc[:, 'share'] = df['share'] / s_sum # Normalize just in case
        return df
    
    print("  → Normalizing topic shares...")
    journal_hierarchy = journal_hierarchy.groupby('journal_id', group_keys=False).apply(distribute_shares)
    
    # Merge pre-aggregated metrics with hierarchy
    print("  → Merging aggregated works with topic hierarchy (using shares)...")
    enriched_agg = pd.merge(journal_agg, journal_hierarchy, on='journal_id')
    
    # ADJUST COUNT: Partition the journal's total works according to the topic's share
    # This prevents duplication of counts in higher levels of the sunburst
    enriched_agg['count'] = enriched_agg['count'] * enriched_agg['share']
    
    # Helper to calculate metrics from PRE-AGGREGATED data
    def calculate_from_agg(df):
        total_docs = df['count'].sum()
        if total_docs == 0: return pd.Series({'count':0, 'fwci_avg':0.0}) # ...
        
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

    def aggregate_hierarchy_from_agg(df, group_cols):
        levels = ['domain', 'field', 'subfield']
        all_results = []
        
        # Helper for modern pandas compatibility
        def apply_with_groups_fix(obj, func):
            try:
                return obj.apply(func, include_groups=False)
            except TypeError:
                return obj.apply(func)

        print(f"  → Aggregating level: Subfield...")
        res_sub = apply_with_groups_fix(df.groupby(group_cols + levels), calculate_from_agg).reset_index()
        res_sub['level'] = 'subfield'
        all_results.append(res_sub)
        
        print(f"  → Aggregating level: Field...")
        res_field = apply_with_groups_fix(df.groupby(group_cols + ['domain', 'field']), calculate_from_agg).reset_index()
        res_field['subfield'] = 'ALL'
        res_field['level'] = 'field'
        all_results.append(res_field)
        
        print(f"  → Aggregating level: Domain...")
        res_domain = apply_with_groups_fix(df.groupby(group_cols + ['domain']), calculate_from_agg).reset_index()
        res_domain['field'] = 'ALL'
        res_domain['subfield'] = 'ALL'
        res_domain['level'] = 'domain'
        all_results.append(res_domain)

        return pd.concat(all_results, ignore_index=True)

    # 3. Calculation for Country Level
    print("\n📊 Computing Country-Topic Metrics...")
    country_metrics = aggregate_hierarchy_from_agg(enriched_agg, ['country_code'])
    country_metrics.to_parquet(output_dir / 'sunburst_metrics_country.parquet', index=False)
    print(f"  ✓ Saved: {len(country_metrics)} records")

    # 4. Calculation for Global Level (LATAM)
    print("\n📊 Computing LATAM-Topic Metrics...")
    latam_metrics = aggregate_hierarchy_from_agg(enriched_agg, [])
    latam_metrics['country_code'] = 'LATAM'
    latam_metrics.to_parquet(output_dir / 'sunburst_metrics_latam.parquet', index=False)
    print(f"  ✓ Saved: {len(latam_metrics)} records")

    # 5. Calculation for Journal Level
    print("\n📊 Computing Journal-Topic Metrics...")
    journal_metrics = aggregate_hierarchy_from_agg(enriched_agg, ['journal_id'])
    journal_metrics.to_parquet(output_dir / 'sunburst_metrics_journal.parquet', index=False)
    print(f"  ✓ Saved: {len(journal_metrics)} records")

    print("\n✅ Topic Metrics Computation Completed!")

if __name__ == "__main__":
    main()
