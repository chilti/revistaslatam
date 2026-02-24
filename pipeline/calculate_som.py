"""
Pipeline step: Calculate SOM (Self-Organizing Map) for countries

This script trains a 20x15 Hexagonal SOM for countries based on their 
recent performance indicators (2021-2025), similar to UMAP.

Outputs:
1. U-Matrix (distance matrix) -> data/som/som_countries_umatrix.npy
2. Country BMUs (Best Matching Units) -> data/som/som_countries_bmus.parquet
"""
import pandas as pd
import numpy as np
import somoclu
from pathlib import Path
from sklearn.preprocessing import StandardScaler
import shutil

# Paths
BASE_PATH = Path(__file__).parent.parent
CACHE_DIR = BASE_PATH / 'data' / 'cache'
OUTPUT_DIR = BASE_PATH / 'data' / 'som'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# SOM Parameters
N_ROWS = 10
N_COLS = 15
GRID_TYPE = 'hexagonal'
MAP_TYPE = 'planar'  # or 'toroid'

print("="*70)
print("CALCULATING SOM FOR COUNTRIES")
print("="*70)

# 1. Load Data (Same as UMAP)
country_file = CACHE_DIR / 'metrics_country_period_2021_2025.parquet'

if not country_file.exists():
    print(f"⚠️ {country_file} not found. Skipping SOM.")
    exit()

df_country = pd.read_parquet(country_file)
print(f"Loaded {len(df_country)} countries")

# Select features (Same as UMAP)
feature_cols = [
    'num_journals',       # Revistas
    'pct_oa_diamond',     # OA Diamante
    'fwci_avg',           # FWCI Promedio
    'pct_top_10',         # % Top 10%
    'pct_top_1',          # % Top 1%
    'avg_percentile',     # Percentil Promedio
    'pct_lang_en'         # % Inglés
]

# Check availability
available_features = [col for col in feature_cols if col in df_country.columns]
if len(available_features) < 2:
    print("❌ Not enough features for SOM.")
    exit()

# Prepare Data
X = df_country[available_features].copy()
X = X.fillna(0)
# Convert to float32 for somoclu
data = X.values.astype(np.float32)

# Standardize
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data)

print(f"Training {N_ROWS}x{N_COLS} {GRID_TYPE} SOM on {len(data)} samples...")

# 2. Train SOM
# somoclu.Somoclu(n_columns, n_rows, data, gridtype='hexagonal', maptype='planar', ...)
som = somoclu.Somoclu(N_COLS, N_ROWS, gridtype=GRID_TYPE, maptype=MAP_TYPE)
som.train(data_scaled, epochs=10000)

# 3. Get Results
umatrix = som.umatrix
bmus = som.bmus # (idx, 2) array of (col, row) ? No, somoclu returns (col, row) usually but let's verify.
# Actually somoclu documentation says bmus is [n_samples, 2] corresponding to (x, y) coordinates.
# x is column index, y is row index.

print("SOM Trained.")
print(f"U-Matrix shape: {umatrix.shape}")

# 4. Save Outputs
# Save U-Matrix
umatrix_file = OUTPUT_DIR / 'som_countries_umatrix.npy'
np.save(umatrix_file, umatrix)

# Save BMUs with Country Info
df_bmus = df_country.copy()
# CRITICAL: somoclu.bmus returns coordinates in (x, y) format = (col_index, row_index)
# bmus shape is (n_samples, 2) where:
#   bmus[:, 0] = x-coordinate = column index (0 to n_cols-1)
#   bmus[:, 1] = y-coordinate = row index (0 to n_rows-1)
# Convert to integers to ensure proper indexing
df_bmus['bmu_col'] = bmus[:, 0].astype(int)  # First coordinate is COLUMN (x)
df_bmus['bmu_row'] = bmus[:, 1].astype(int)  # Second coordinate is ROW (y)

bmus_file = OUTPUT_DIR / 'som_countries_bmus.parquet'
df_bmus.to_parquet(bmus_file, index=False)

print(f"✅ Saved U-Matrix to {umatrix_file}")
print(f"✅ Saved BMUs to {bmus_file}")
print(f"   Features used: {', '.join(available_features)}")
