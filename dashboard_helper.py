
# Helper to load metrics and enforce 0-100 scale for Average Percentile
def load_metric_df(entity_type, period):
    df = load_cached_metrics(entity_type, period)
    if df is not None and not df.empty and 'avg_percentile' in df.columns:
        # Auto-detect if scale is 0-1 (max <= 1.0) and convert to 0-100
        if df['avg_percentile'].max() <= 1.0 and df['avg_percentile'].max() > 0:
            df['avg_percentile'] = df['avg_percentile'] * 100
    return df
