"""
Pipeline step: Calculate UMAP embeddings for countries and journals

This script generates 2D UMAP projections for:
1. Countries - based on their performance indicators (2021-2025)
2. Journals - based on their performance indicators (2021-2025)

The embeddings are saved as parquet files for visualization in the dashboard.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from umap import UMAP
from sklearn.preprocessing import StandardScaler

# Paths
BASE_PATH = Path(__file__).parent.parent
CACHE_DIR = BASE_PATH / 'data' / 'cache'
OUTPUT_DIR = BASE_PATH / 'data' / 'umap'
OUTPUT_DIR.mkdir(exist_ok=True)

print("="*70)
print("CALCULATING UMAP EMBEDDINGS")
print("="*70)

# ============================================================================
# 1. UMAP for Countries (Región section)
# ============================================================================
print("\n1️⃣ UMAP for Countries")
print("-"*70)

# Load country metrics for recent period (2021-2025)
country_file = CACHE_DIR / 'metrics_country_period_2021_2025.parquet'

if not country_file.exists():
    print(f"⚠️ {country_file} not found. Skipping country UMAP.")
else:
    df_country = pd.read_parquet(country_file)
    print(f"Loaded {len(df_country)} countries")
    
    # Select features for UMAP
    feature_cols = [
        'num_documents',      # Documentos
        'pct_oa_diamond',     # OA Diamante
        'fwci_avg',           # FWCI Promedio
        'pct_top_10',         # % Top 10%
        'pct_top_1',          # % Top 1%
        'avg_percentile'      # Percentil Promedio (corregido)
    ]
    
    # Check which features exist
    available_features = [col for col in feature_cols if col in df_country.columns]
    missing_features = [col for col in feature_cols if col not in df_country.columns]
    
    if missing_features:
        print(f"⚠️ Missing features: {missing_features}")
        print(f"Using available features: {available_features}")
    
    if len(available_features) < 2:
        print("❌ Not enough features for UMAP (need at least 2)")
    else:
        # Prepare data
        X = df_country[available_features].copy()
        
        # Handle missing values
        X = X.fillna(0)
        
        # Remove countries with all zeros
        mask = (X != 0).any(axis=1)
        X_filtered = X[mask]
        df_filtered = df_country[mask].copy()
        
        print(f"Countries after filtering: {len(df_filtered)}")
        
        if len(df_filtered) < 3:
            print("❌ Not enough countries for UMAP (need at least 3)")
        else:
            # Standardize features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X_filtered)
            
            # Calculate UMAP
            print("Calculating UMAP (this may take a minute)...")
            umap_model = UMAP(
                n_components=2,
                n_neighbors=min(15, len(df_filtered) - 1),
                min_dist=0.1,
                metric='euclidean',
                random_state=42
            )
            
            embeddings = umap_model.fit_transform(X_scaled)
            
            # Add coordinates to dataframe
            df_filtered['umap_x'] = embeddings[:, 0]
            df_filtered['umap_y'] = embeddings[:, 1]
            
            # Save
            output_file = OUTPUT_DIR / 'umap_countries_recent.parquet'
            df_filtered.to_parquet(output_file, index=False)
            print(f"✅ Saved to {output_file}")
            print(f"   Features used: {', '.join(available_features)}")

# ============================================================================
# 2. UMAP for Journals (País section - per country)
# ============================================================================
print("\n2️⃣ UMAP for Journals by Country")
print("-"*70)

# Load journal metrics for recent period (2021-2025)
journal_file = CACHE_DIR / 'metrics_journal_period_2021_2025.parquet'

if not journal_file.exists():
    print(f"⚠️ {journal_file} not found. Skipping journal UMAP.")
else:
    df_journal = pd.read_parquet(journal_file)
    print(f"Loaded {len(df_journal)} journals")
    
    # Load journal metadata to get country info
    journals_meta = pd.read_parquet(BASE_PATH / 'data' / 'latin_american_journals.parquet')
    df_journal = df_journal.merge(
        journals_meta[['id', 'country_code', 'display_name']], 
        left_on='journal_id', 
        right_on='id',
        how='left'
    )
    
    # Select features for UMAP (same as countries for consistency)
    feature_cols = [
        'num_documents',      # Documentos
        'pct_oa_diamond',     # OA Diamante
        'fwci_avg',           # FWCI Promedio
        'pct_top_10',         # % Top 10%
        'pct_top_1',          # % Top 1%
        'avg_percentile'      # Percentil Promedio (corregido)
    ]
    
    # Check which features exist
    available_features = [col for col in feature_cols if col in df_journal.columns]
    missing_features = [col for col in feature_cols if col not in df_journal.columns]
    
    if missing_features:
        print(f"⚠️ Missing features: {missing_features}")
        print(f"Using available features: {available_features}")
    
    if len(available_features) < 2:
        print("❌ Not enough features for UMAP (need at least 2)")
    else:
        # Process each country separately
        countries = df_journal['country_code'].dropna().unique()
        print(f"Processing {len(countries)} countries...")
        
        all_umap_data = []
        
        for country in countries:
            country_journals = df_journal[df_journal['country_code'] == country].copy()
            
            # Need at least 3 journals for UMAP
            if len(country_journals) < 3:
                print(f"  {country}: Only {len(country_journals)} journals, skipping")
                continue
            
            # Prepare data
            X = country_journals[available_features].copy()
            X = X.fillna(0)
            
            # Remove journals with all zeros
            mask = (X != 0).any(axis=1)
            X_filtered = X[mask]
            journals_filtered = country_journals[mask].copy()
            
            if len(journals_filtered) < 3:
                print(f"  {country}: Only {len(journals_filtered)} journals with data, skipping")
                continue
            
            # Standardize features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X_filtered)
            
            # Calculate UMAP
            umap_model = UMAP(
                n_components=2,
                n_neighbors=min(15, len(journals_filtered) - 1),
                min_dist=0.1,
                metric='euclidean',
                random_state=42
            )
            
            embeddings = umap_model.fit_transform(X_scaled)
            
            # Add coordinates
            journals_filtered['umap_x'] = embeddings[:, 0]
            journals_filtered['umap_y'] = embeddings[:, 1]
            
            all_umap_data.append(journals_filtered)
            print(f"  {country}: ✅ {len(journals_filtered)} journals")
        
        if all_umap_data:
            # Combine all countries
            df_all_journals = pd.concat(all_umap_data, ignore_index=True)
            
            # Save
            output_file = OUTPUT_DIR / 'umap_journals_recent.parquet'
            df_all_journals.to_parquet(output_file, index=False)
            print(f"\n✅ Saved to {output_file}")
            print(f"   Total journals: {len(df_all_journals)}")
            print(f"   Features used: {', '.join(available_features)}")
        else:
            print("\n⚠️ No countries had enough journals for UMAP")

print("\n" + "="*70)
print("UMAP CALCULATION COMPLETE")
print("="*70)
