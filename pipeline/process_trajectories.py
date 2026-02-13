#!/usr/bin/env python3
"""
Trajectory Analysis Pipeline v2.0 (Multi-Map)
---------------------------------------------
1. Loads annual metrics.
2. Applies smoothing.
3. Generates Two Types of UMAP Projections:
   A. Global Countries Map: Only Countries and Region (LATAM).
   B. Country Context Maps: For each country, project its Journals + the Country itself.
4. Saves coordinates to separate parquet files.
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
MAP_COUNTRIES_FILE = CACHE_DIR / 'trajectory_countries_coords.parquet'
MAP_JOURNALS_FILE = CACHE_DIR / 'trajectory_journals_coords.parquet'

def apply_smoothing(df, group_col, value_cols, window_size=3):
    if df.empty: return df
    df = df.sort_values(by=[group_col, 'year'])
    smoothed_df = df.copy()
    for col in value_cols:
        try:
            # win_type='exponential' requires scipy
            smoothed_df[col] = df.groupby(group_col)[col].transform(
                lambda x: x.rolling(window_size, min_periods=1, win_type='exponential', center=True).mean(tau=1)
            )
        except:
            smoothed_df[col] = df.groupby(group_col)[col].transform(
                lambda x: x.rolling(window_size, min_periods=1, center=True).mean()
            )
    return smoothed_df

def run_umap_projection(df, metrics_cols, n_neighbors=15, min_dist=0.1):
    try:
        import umap
    except ImportError:
        logger.error("❌ 'umap-learn' library not installed.")
        return None

    # Filter data and fill NaN
    data = df[metrics_cols].fillna(0)
    
    # Standardize
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(data)
    
    # UMAP
    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        n_components=2,
        metric='euclidean',
        random_state=42
    )
    
    try:
        embedding = reducer.fit_transform(scaled_data)
        result_df = df.copy()
        result_df['x'] = embedding[:, 0]
        result_df['y'] = embedding[:, 1]
        return result_df[['id', 'name', 'type', 'year', 'country_code', 'x', 'y']] # Keep metadata
    except Exception as e:
        logger.warning(f"UMAP failed: {e}")
        return None

def load_and_prep_data():
    # 1. Journals
    j_path = CACHE_DIR / 'metrics_journal_annual.parquet'
    if not j_path.exists(): return None
    
    df_j = pd.read_parquet(j_path)
    # Load metadata with country_code
    j_meta_path = CACHE_DIR.parent / 'latin_american_journals.parquet'
    if j_meta_path.exists():
        meta = pd.read_parquet(j_meta_path)
        # Check if country_code exists
        cols_to_use = ['id', 'display_name']
        if 'country_code' in meta.columns:
            cols_to_use.append('country_code')
            
        journals_meta = meta[cols_to_use]
        
        # Merge
        df_j = df_j.merge(journals_meta, left_on='journal_id', right_on='id', how='left', suffixes=('', '_meta'))
        
        # Cleanup ID column collision
        if 'id_meta' in df_j.columns:
            df_j = df_j.drop(columns=['id_meta'])
        if 'id' in df_j.columns and 'journal_id' in df_j.columns:
             # If 'id' came from somewhere else and conflicts
             df_j = df_j.drop(columns=['id'])
             
        df_j['type'] = 'journal'
        df_j = df_j.rename(columns={'journal_id': 'id', 'display_name': 'name'})
        
        if 'country_code' not in df_j.columns:
            df_j['country_code'] = 'Unknown'
            
    else:
        df_j['type'] = 'journal'
        df_j['name'] = df_j['journal_id']
        df_j = df_j.rename(columns={'journal_id': 'id'})
        df_j['country_code'] = 'Unknown'
    
    # 2. Countries
    c_path = CACHE_DIR / 'metrics_country_annual.parquet'
    if c_path.exists():
        df_c = pd.read_parquet(c_path)
        df_c['type'] = 'country'
        df_c['name'] = df_c['country_code']
        df_c = df_c.rename(columns={'country_code': 'id'})
        df_c['country_code'] = df_c['id'] # Self reference
    else:
        df_c = pd.DataFrame()

    # 3. LATAM
    l_path = CACHE_DIR / 'metrics_latam_annual.parquet'
    if l_path.exists():
        df_l = pd.read_parquet(l_path)
        df_l['type'] = 'region'
        df_l['id'] = 'LATAM'
        df_l['name'] = 'Iberoamérica'
        df_l['country_code'] = 'LATAM'
    else:
        df_l = pd.DataFrame()

    # Combine
    combined = pd.concat([df_j, df_c, df_l], ignore_index=True)
    if combined.empty: return None

    # Keep necessary cols
    keep_cols = ['id', 'name', 'type', 'year', 'country_code'] + METRICS_COLS
    
    # Fill missing cols
    for c in keep_cols:
        if c not in combined.columns:
            combined[c] = 0 if c in METRICS_COLS else ''
            
    return combined[keep_cols]

def main():
    if not CACHE_DIR.exists(): return
    logger.info("Loading metrics data...")
    raw_data = load_and_prep_data()
    if raw_data is None or raw_data.empty:
        logger.error("No data found.")
        return

    # Apply Smoothing (Window 3)
    logger.info("Applying Smoothing (Window=3)...")
    smoothed = apply_smoothing(raw_data, 'id', METRICS_COLS, window_size=3)
    
    # Apply Smoothing (Window 5) - For heavy smoothing tab
    logger.info("Applying Smoothing (Window=5)...")
    smoothed_w5 = apply_smoothing(raw_data, 'id', METRICS_COLS, window_size=5)
    
    # Save base data tables for Dashboard
    raw_data.to_parquet(CACHE_DIR / 'trajectory_data_raw.parquet', index=False)
    smoothed.to_parquet(CACHE_DIR / 'trajectory_data_smoothed.parquet', index=False)
    smoothed_w5.to_parquet(CACHE_DIR / 'trajectory_data_smoothed_w5.parquet', index=False)
    
    # Remove rows with NaN metrics for UMAP
    valid_data = smoothed.dropna(subset=METRICS_COLS).copy()
    
    # --- Map 1: Global Countries ---
    logger.info("Generating Global Countries Map (Countries + LATAM)...")
    countries_data = valid_data[valid_data['type'].isin(['country', 'region'])].copy()
    
    if len(countries_data) > 10:
        # Lower neighbors since we have fewer points (~35 countries * ~15 years = ~500 points)
        # Actually each year is a point.
        n_neighbors = min(30, len(countries_data) - 1)
        coords_countries = run_umap_projection(countries_data, METRICS_COLS, n_neighbors=n_neighbors)
        if coords_countries is not None:
            coords_countries.to_parquet(MAP_COUNTRIES_FILE, index=False)
            logger.info(f"Saved Global Countries Map to {MAP_COUNTRIES_FILE}")

    # --- Map 2: Journals by Country ---
    logger.info("Generating Maps by Country...")
    all_journal_coords = []
    
    # Iterate over each country
    countries = valid_data[valid_data['type'] == 'country']['id'].unique()
    
    for country in countries:
        # Subset: Journals of this country + The Country itself
        # Note: 'country_code' for the country row is equal to its ID (e.g. 'BR')
        # 'country_code' for journals is e.g. 'BR'.
        mask = (valid_data['country_code'] == country)
        subset = valid_data[mask].copy()
        
        if len(subset) < 10:
            continue # Skip countries with not enough data
            
        # Dynamic neighbors for local contexts
        # If we have 100 journals * 10 years = 1000 points, n_neighbors=15 is fine.
        # If we have 2 journals * 10 years = 20 points, n_neighbors=15 is fine.
        n_neighbors = min(15, len(subset) - 1)
        if n_neighbors < 2: n_neighbors = 2
        
        logger.info(f"Projecting {country} ({len(subset)} samples)...")
        coords = run_umap_projection(subset, METRICS_COLS, n_neighbors=n_neighbors)
        
        if coords is not None:
            coords['map_context'] = country
            all_journal_coords.append(coords)
    
    if all_journal_coords:
        final_df = pd.concat(all_journal_coords)
        final_df.to_parquet(MAP_JOURNALS_FILE, index=False)
        logger.info(f"Saved Journal Context Maps to {MAP_JOURNALS_FILE}")

if __name__ == "__main__":
    main()
