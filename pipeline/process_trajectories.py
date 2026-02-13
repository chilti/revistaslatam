#!/usr/bin/env python3
"""
Trajectory Analysis Pipeline v1.1
---------------------------------
1. Loads annual metrics for Journals, Countries, and LATAM.
2. Applies exponential smoothing with two settings:
   - Variant 1 (Default): Window=3, Tau=1
   - Variant 2 (Heavy): Window=5, Tau=1
3. Computes UMAP 2D projection (based on Variant 1).
4. Saves 4 parquet files for Dashboard consumption.
"""
import pandas as pd
import numpy as np
import os
from pathlib import Path
import logging
from sklearn.preprocessing import StandardScaler
import warnings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
CACHE_DIR = Path(__file__).parent.parent / 'data' / 'cache'
METRICS_COLS = ['num_documents', 'fwci_avg', 'avg_percentile', 'pct_top_1', 'pct_top_10']
ID_VARS = ['id', 'name', 'type', 'year']

def apply_smoothing(df, group_col, value_cols, window_size=3):
    """
    Applies exponential smoothing to the specified columns for each group.
    """
    if df.empty:
        return df
    
    # Ensure data is sorted by year
    df = df.sort_values(by=[group_col, 'year'])
    
    smoothed_df = df.copy()
    
    for col in value_cols:
        try:
            # win_type='exponential' requires scipy
            smoothed_df[col] = df.groupby(group_col)[col].transform(
                lambda x: x.rolling(window_size, min_periods=1, win_type='exponential', center=True).mean(tau=1)
            )
        except Exception as e:
            logger.warning(f"Smoothing failed for {col} (scipy might be missing?): {e}. Using simple mean.")
            smoothed_df[col] = df.groupby(group_col)[col].transform(
                lambda x: x.rolling(window_size, min_periods=1, center=True).mean()
            )
            
    return smoothed_df

def run_umap_projection(combined_df):
    """
    Runs UMAP dimensionality reduction on the metrics columns.
    Returns DataFrame with 'x' and 'y' coordinates.
    """
    try:
        import umap
    except ImportError:
        logger.error("❌ 'umap-learn' library not installed. Cannot calculate trajectories.")
        logger.error("Please run: pip install umap-learn")
        return None

    # Filter only year/id metadata and metrics
    # We use fillna(0) to handle missing values, which is necessary for UMAP
    data_for_projection = combined_df[METRICS_COLS].fillna(0)
    
    # Standardize data (Z-score) - Critical for UMAP/PCA
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(data_for_projection)
    
    print("Running UMAP projection on {} samples...".format(len(scaled_data)))
    
    # UMAP parameters tailored for trajectory preservation
    reducer = umap.UMAP(
        n_neighbors=30,
        min_dist=0.1,
        n_components=2,
        metric='euclidean',
        random_state=42 # Ensure reproducibility
    )
    
    embedding = reducer.fit_transform(scaled_data)
    
    result_df = combined_df[['id', 'name', 'type', 'year']].copy()
    result_df['x'] = embedding[:, 0]
    result_df['y'] = embedding[:, 1]
    
    return result_df

def load_and_prep_data():
    """Lengths and combines data from different parquet files."""
    
    # 1. Journals
    j_path = CACHE_DIR / 'metrics_journal_annual.parquet'
    if not j_path.exists(): 
        logger.warning(f"Journal metrics not found at {j_path}")
        return None
        
    df_j = pd.read_parquet(j_path)
    # Load journals metadata for names
    j_meta_path = CACHE_DIR.parent / 'latin_american_journals.parquet'
    if j_meta_path.exists():
        journals_meta = pd.read_parquet(j_meta_path)[['id', 'display_name']]
        df_j = df_j.merge(journals_meta, left_on='journal_id', right_on='id', how='left')
        if 'id' in df_j.columns:
            df_j = df_j.drop(columns=['id'])
        df_j['type'] = 'journal'
        df_j = df_j.rename(columns={'journal_id': 'id', 'display_name': 'name'})
    else:
        logger.warning("Journals metadata not found, using IDs as names")
        df_j['type'] = 'journal'
        df_j['name'] = df_j['journal_id']
        df_j = df_j.rename(columns={'journal_id': 'id'})
    
    # 2. Countries
    c_path = CACHE_DIR / 'metrics_country_annual.parquet'
    if c_path.exists():
        df_c = pd.read_parquet(c_path)
        df_c['type'] = 'country'
        df_c['name'] = df_c['country_code'] # Use code as name
        df_c = df_c.rename(columns={'country_code': 'id'})
    else:
        df_c = pd.DataFrame()

    # 3. LATAM (Iberoamerica)
    l_path = CACHE_DIR / 'metrics_latam_annual.parquet'
    if l_path.exists():
        df_l = pd.read_parquet(l_path)
        df_l['type'] = 'region'
        df_l['id'] = 'LATAM'
        df_l['name'] = 'Iberoamérica'
    else:
        df_l = pd.DataFrame()

    # Combine
    combined = pd.concat([df_j, df_c, df_l], ignore_index=True)
    
    if combined.empty:
        return None
        
    # Keep strictly necessary cols
    keep_cols = ['id', 'name', 'type', 'year'] + METRICS_COLS
    
    # Check for missing columns (e.g. if parquet is old)
    for c in keep_cols:
        if c not in combined.columns:
            logger.warning(f"Column {c} missing in metrics data. Filling with 0.")
            combined[c] = 0
            
    return combined[keep_cols]

def main():
    if not CACHE_DIR.exists():
        logger.error(f"Cache directory not found: {CACHE_DIR}")
        return

    logger.info("Loading metrics data...")
    raw_data = load_and_prep_data()
    
    if raw_data is None or raw_data.empty:
        logger.error("No data found to process. Please run the earlier pipeline steps first.")
        return

    logger.info(f"Loaded {len(raw_data)} rows.")
    
    # 1. Apply Smoothing (Variant 1: Window 3)
    logger.info("Applying Smoothing (Window=3)...")
    smoothed_w3 = apply_smoothing(raw_data, 'id', METRICS_COLS, window_size=3)
    
    # 2. Apply Smoothing (Variant 2: Window 5)
    logger.info("Applying Smoothing (Window=5)...")
    smoothed_w5 = apply_smoothing(raw_data, 'id', METRICS_COLS, window_size=5)
    
    # Save processed tables
    logger.info("Saving data tables...")
    raw_data.to_parquet(CACHE_DIR / 'trajectory_data_raw.parquet', index=False)
    smoothed_w3.to_parquet(CACHE_DIR / 'trajectory_data_smoothed.parquet', index=False)
    smoothed_w5.to_parquet(CACHE_DIR / 'trajectory_data_smoothed_w5.parquet', index=False)
    
    # 3. Calculate UMAP Projection
    # We project the SMOOTHED data (Variant 1 is generally better for visualization)
    logger.info("Calculating UMAP projection...")
    
    # Filter out empty or NaN rows before UMAP to avoid crash
    data_to_project = smoothed_w3.dropna(subset=METRICS_COLS)
    
    if len(data_to_project) < 10:
        logger.error("Not enough data points for UMAP.")
        return

    coords_df = run_umap_projection(data_to_project)
    
    if coords_df is not None:
        out_file = CACHE_DIR / 'trajectory_coordinates.parquet'
        coords_df.to_parquet(out_file, index=False)
        logger.info(f"✅ Trajectory coordinates saved to: {out_file}")
    else:
        logger.warning("Skipping visualization data generation.")

if __name__ == "__main__":
    main()
