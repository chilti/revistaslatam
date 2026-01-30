import pandas as pd
import numpy as np
import json

def safe_get(obj, *keys, default=None):
    """Safely navigate nested dictionaries/columns."""
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key, default)
        else:
            return default
    return obj if obj is not None else default

def extract_nested_field(df, column, *keys, default=None):
    """Helper to extract nested fields from a DataFrame column that contains dicts."""
    if column not in df.columns:
        return default
    # If column is all nulls/NaNs, apply might fail or return NaN
    # We should handle potential strings if they were saved as json strings
    def extractor(x):
        if isinstance(x, str):
            try:
                x = json.loads(x)
            except:
                return default
        return safe_get(x, *keys, default=default)
        
    return df[column].apply(extractor)

def load_data(filepath):
    """
    Loads journal data from a parquet file.
    """
    try:
        df = pd.read_parquet(filepath)
        
        # Identify columns that are JSON strings and parse them back to dicts/lists
        # This is needed because DataCollector now saves complex types as JSON strings
        json_cols = ['summary_stats', 'topics', 'counts_by_year', 'open_access', 'primary_topic']
        for col in json_cols:
            if col in df.columns and df[col].dtype == 'object':
                # Check first element to see if it is a string looking like json
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample, str) and (sample.startswith('{') or sample.startswith('[')):
                    try:
                        df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                    except:
                        pass # Keep as is if parsing fails
        
        # Flatten critical nested fields for easier access in Dashboard
        # 1. Impact Metrics from summary_stats
        if 'summary_stats' in df.columns:
            # Re-ensure it is dict (in case parsing above failed or wasn't triggered)
            # Actually extract_nested_field handles json strings too now (see helper above)
            df['2yr_mean_citedness'] = extract_nested_field(df, 'summary_stats', '2yr_mean_citedness', default=0)
            df['h_index'] = extract_nested_field(df, 'summary_stats', 'h_index', default=0)
            
        # 2. Open Access Status
        # If is_oa is not top-level, try to extract from open_access dict
        if 'is_oa' not in df.columns:
            if 'open_access' in df.columns:
                 df['is_oa'] = extract_nested_field(df, 'open_access', 'is_oa', default=False)
            
        # Ensure default columns exist if extraction failed (to avoid KeyErrors)
        required_cols = ['2yr_mean_citedness', 'h_index', 'is_oa', 'works_count', 'cited_by_count']
        for col in required_cols:
            if col not in df.columns:
                if col == 'is_oa':
                    df[col] = False
                else:
                    df[col] = 0

        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame()

def get_latam_kpis(df):
    """
    Calculates general KPIs for Latin America.
    """
    if df.empty:
        return {}
    
    total_journals = len(df)
    total_articles = df['works_count'].sum()
    total_citations = df['cited_by_count'].sum()
    
    # Open Access Analysis
    # Ensure is_oa exists (should be handled by load_data but duplicate check safe)
    if 'is_oa' in df.columns:
        oa_journals = df[df['is_oa'] == True]
        percent_oa = (len(oa_journals) / total_journals) * 100 if total_journals > 0 else 0
    else:
        percent_oa = 0
        
    # Impact 
    avg_impact = df['2yr_mean_citedness'].mean() if '2yr_mean_citedness' in df.columns else 0
    
    return {
        'total_journals': total_journals,
        'total_articles': total_articles,
        'total_citations': total_citations,
        'percent_oa': percent_oa,
        'avg_impact': avg_impact
    }

def get_country_stats(df):
    """
    Aggregates data by country.
    """
    if df.empty:
        return pd.DataFrame()

    # Pre-process nested fields if necessary for aggregation
    # load_data already flattens critical fields
    df_processed = df.copy()
        
    country_stats = df_processed.groupby('country_code').agg({
        'id': 'count', # Number of journals
        'works_count': 'sum',
        'cited_by_count': 'sum',
        '2yr_mean_citedness': 'mean',
        'is_oa': lambda x: (x.sum() / len(x)) * 100 # % OA
    }).reset_index()
    
    country_stats = country_stats.rename(columns={
        'id': 'num_journals',
        '2yr_mean_citedness': 'avg_impact_factor',
        'is_oa': 'percent_oa'
    })
    
    return country_stats

def analyze_oa_vs_impact(df):
    """
    Prepares data for analyzing the relationship between OA and Impact.
    Returns a DataFrame suitable for plotting.
    """
    if df.empty:
        return pd.DataFrame()
    
    # We focus on fields relevant for scatter plots or box plots
    # Ensure columns exist
    analysis_df = df.copy()
    
    cols = ['display_name', 'country_code', 'is_oa', '2yr_mean_citedness', 'h_index', 'works_count']
    available_cols = [c for c in cols if c in analysis_df.columns]
    
    analysis_df = analysis_df[available_cols].copy()
    
    # Normalize or clean if necessary
    if 'is_oa' in analysis_df.columns:
        analysis_df['oa_status'] = analysis_df['is_oa'].map({True: 'Acceso Abierto (OA)', False: 'Suscripción/Híbrido'})
    
    return analysis_df
