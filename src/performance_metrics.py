import pandas as pd
import numpy as np
import json

def safe_get(obj, *keys, default=None):
    """Safely navigate nested dictionaries."""
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key, default)
        else:
            return default
    return obj if obj is not None else default

def load_works_data(filepath):
    """
    Loads works (articles) data from parquet file.
    """
    try:
        df = pd.read_parquet(filepath)
        
        # Parse potential JSON columns (works data)
        json_cols = ['open_access', 'primary_topic', 'topics', 'authorships']
        for col in json_cols:
            if col in df.columns and df[col].dtype == 'object':
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample, str) and (sample.startswith('{') or sample.startswith('[')):
                    try:
                        df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                    except:
                        pass
                        
        return df
    except Exception as e:
        print(f"Error loading works data: {e}")
        return pd.DataFrame()

def get_topic_baselines(works_df):
    """
    Pre-calculates average citations per topic for FWCI.
    Returns: dict {topic_name: avg_citations}
    """
    # Extract primary topic name safely
    def get_topic_name(row):
        return safe_get(row, 'primary_topic', 'display_name')

    # Create a temporary column for grouping
    # We use apply because data might be mixed dict/json-string/None
    # But for speed, we should rely on the fact that load_works_data already parses JSON
    # So we assume dicts or None.
    
    # Reset index to ensure clean operations
    df = works_df.copy()
    
    # Extract topic names into a list to avoid slow apply if possible, or just use apply
    # Check if 'primary_topic_name' already exists (optimization)
    if 'primary_topic_name' not in df.columns:
        df['primary_topic_name'] = df.apply(get_topic_name, axis=1)
    
    # Group by topic
    topic_stats = df.groupby('primary_topic_name')['cited_by_count'].mean()
    return topic_stats.to_dict()

def enrich_works_data(works_df, topic_baselines=None):
    """
    Adds calculated columns to works_df:
    - citation_percentile (0-100)
    - fwci (Field-Weighted Citation Impact)
    """
    df = works_df.copy()
    
    # 1. Percentiles (Global rank)
    df['citation_percentile'] = df['cited_by_count'].rank(pct=True) * 100
    
    # 2. FWCI
    if topic_baselines:
        # Extract topic name if not present
        if 'primary_topic_name' not in df.columns:
            df['primary_topic_name'] = df.apply(lambda x: safe_get(x, 'primary_topic', 'display_name'), axis=1)
        
        # Map average citations
        df['expected_citations'] = df['primary_topic_name'].map(topic_baselines)
        
        # Calculate FWCI (Observed / Expected)
        # Handle division by zero or missing baseline
        df['fwci'] = df.apply(
            lambda x: x['cited_by_count'] / x['expected_citations'] if pd.notnull(x['expected_citations']) and x['expected_citations'] > 0 else 1.0, 
            axis=1
        )
    else:
        df['fwci'] = 1.0
        
    return df

def calculate_metrics_from_enriched(enriched_df, total_works_count_for_percentile=None):
    """
    Calculates summary metrics from an already enriched DataFrame (subset of works).
    """
    if len(enriched_df) == 0:
        return {
            'num_documents': 0, 'pct_oa_articles': 0, 'avg_citations': 0,
            'fwci_approx': 0, 'pct_top10': 0, 'avg_percentile': 0
        }
    
    num_documents = len(enriched_df)
    
    # OA
    oa_values = enriched_df.apply(lambda x: safe_get(x, 'open_access', 'is_oa', default=safe_get(x, 'is_oa', default=False)), axis=1)
    pct_oa_articles = (oa_values.sum() / num_documents) * 100
    
    # Citations
    avg_citations = enriched_df['cited_by_count'].mean()
    
    # Percentiles
    avg_percentile = enriched_df['citation_percentile'].mean()
    
    # Top 10% (Global definition)
    # Ideally, we should check if percentile >= 90
    pct_top10 = (len(enriched_df[enriched_df['citation_percentile'] >= 90]) / num_documents) * 100
    
    # FWCI
    fwci_approx = enriched_df['fwci'].mean()
    
    return {
        'num_documents': num_documents,
        'pct_oa_articles': round(pct_oa_articles, 2),
        'avg_citations': round(avg_citations, 2),
        'fwci_approx': round(fwci_approx, 2),
        'pct_top10': round(pct_top10, 2),
        'avg_percentile': round(avg_percentile, 2)
    }

def calculate_journal_performance_metrics(works_df, journal_id):
    # 1. Pre-calculate baselines (this is expensive to do per-call, ideally done once in Dashboard)
    # But for backward compatibility we do it here if not passed. 
    # NOTE: In optimal flow, dashboard pre-calculates global baselines. 
    # Here we will just calculate local baselines which is wrong for FWCI but acceptable if no global context.
    # BETTER: Just filter works_df for journal, but we need global rankings for percentiles.
    
    # Assuming works_df is the GLOBAL dataset
    # We should calculate percentiles visually relative to global.
    
    # Let's check if 'citation_percentile' exists, if so use it, else calculate on fly
    if 'citation_percentile' not in works_df.columns:
        # Full recalculation (slow)
        topic_baselines = get_topic_baselines(works_df)
        enriched_global = enrich_works_data(works_df, topic_baselines)
        journal_works = enriched_global[enriched_global['journal_id'] == journal_id]
    else:
        # Already enriched
        journal_works = works_df[works_df['journal_id'] == journal_id]
        
    return calculate_metrics_from_enriched(journal_works)

def calculate_country_performance_metrics(works_df, journals_df, country_code):
    country_journals = journals_df[journals_df['country_code'] == country_code]
    
    # Journal mets
    num_journals = len(country_journals)
    if num_journals == 0: return {}
    
    pct_scopus = (country_journals.apply(lambda x: safe_get(x, 'is_indexed_in_scopus', default=False), axis=1).sum() / num_journals) * 100
    pct_core = (country_journals.apply(lambda x: safe_get(x, 'is_core', default=False), axis=1).sum() / num_journals) * 100
    pct_doaj = (country_journals.apply(lambda x: safe_get(x, 'is_in_doaj', default=False), axis=1).sum() / num_journals) * 100
    
    # Works mets
    journal_ids = country_journals['id'].tolist()
    
    if 'citation_percentile' not in works_df.columns:
        topic_baselines = get_topic_baselines(works_df)
        enriched_global = enrich_works_data(works_df, topic_baselines)
        country_works = enriched_global[enriched_global['journal_id'].isin(journal_ids)]
    else:
        country_works = works_df[works_df['journal_id'].isin(journal_ids)]
        
    metrics = calculate_metrics_from_enriched(country_works)
    
    # Merge
    metrics.update({
        'num_journals': num_journals,
        'pct_scopus': round(pct_scopus, 2),
        'pct_core': round(pct_core, 2),
        'pct_doaj': round(pct_doaj, 2)
    })
    return metrics

def calculate_latam_performance_metrics(works_df, journals_df):
    # Journal mets
    num_journals = len(journals_df)
    if num_journals == 0: return {}
    
    pct_scopus = (journals_df.apply(lambda x: safe_get(x, 'is_indexed_in_scopus', default=False), axis=1).sum() / num_journals) * 100
    pct_core = (journals_df.apply(lambda x: safe_get(x, 'is_core', default=False), axis=1).sum() / num_journals) * 100
    pct_doaj = (journals_df.apply(lambda x: safe_get(x, 'is_in_doaj', default=False), axis=1).sum() / num_journals) * 100
    
    if 'citation_percentile' not in works_df.columns:
        topic_baselines = get_topic_baselines(works_df)
        enriched_global = enrich_works_data(works_df, topic_baselines)
    else:
        enriched_global = works_df
        
    metrics = calculate_metrics_from_enriched(enriched_global)
    
    # Merge
    metrics.update({
        'num_journals': num_journals,
        'pct_scopus': round(pct_scopus, 2),
        'pct_core': round(pct_core, 2),
        'pct_doaj': round(pct_doaj, 2)
    })
    return metrics
