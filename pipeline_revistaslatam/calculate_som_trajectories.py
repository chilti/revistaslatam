"""
Pipeline step: Calculate SOM (Self-Organizing Map) for Latam Trajectories

This script trains a large 50x30 Hexagonal SOM using the data from 
"Trayectorias de Desempeño Latam (Global)" across all years.

Outputs:
1. U-Matrix (distance matrix) -> data/som/som_trajectories_umatrix.npy
2. Trajectory BMUs (Best Matching Units) -> data/som/som_trajectories_bmus.parquet
"""
import pandas as pd
import numpy as np
import somoclu
from pathlib import Path
from sklearn.preprocessing import StandardScaler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
BASE_PATH = Path(__file__).parent.parent
CACHE_DIR = BASE_PATH / 'data' / 'cache'
OUTPUT_DIR = BASE_PATH / 'data' / 'som'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# SOM Parameters
N_ROWS = 30
N_COLS = 50
GRID_TYPE = 'hexagonal'
MAP_TYPE = 'planar'  

print("="*70)
print(f"CALCULATING SOM {N_COLS}x{N_ROWS} FOR TRAYECTORIAS GLOBALES")
print("="*70)

# 1. Load Data
traj_file = CACHE_DIR / 'trajectory_data_smoothed.parquet'

if not traj_file.exists():
    print(f"⚠️ {traj_file} not found. Skipping SOM for trajectories.")
    exit()

df_traj = pd.read_parquet(traj_file)
print(f"Loaded {len(df_traj)} trajectory points (Entities x Years)")

# Identificamos los features que se utilizaron para UMAP en process_trajectories.py
# y los reutilizamos para que el SOM represente la misma dimensionalidad.
feature_cols = [
    'num_documents', 
    'fwci_avg', 
    'avg_percentile',
    'pct_top_1', 
    'pct_top_10', 
    'pct_lang_en',
    'pct_oa_diamond'
]

# Check availability
available_features = [col for col in feature_cols if col in df_traj.columns]
if len(available_features) < 2:
    print("❌ Not enough features for SOM.")
    exit()

# Filter data that has the variables for SOM (no NaNs in at least these columns)
df_valid_traj = df_traj.dropna(subset=available_features).copy()
print(f"Valid trajectory points mapped: {len(df_valid_traj)} out of {len(df_traj)}")

if len(df_valid_traj) < 10:
    print("❌ Not enough valid points.")
    exit()

# Prepare Data
X = df_valid_traj[available_features].copy()
X = X.fillna(0)
# Convert to float32 for somoclu
data = X.values.astype(np.float32)

# Standardize
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data)

print(f"Training {N_COLS}x{N_ROWS} {GRID_TYPE} SOM on {len(data)} samples... (mighty take several minutes)")

# 2. Train SOM
som = somoclu.Somoclu(N_COLS, N_ROWS, gridtype=GRID_TYPE, maptype=MAP_TYPE)
som.train(data_scaled, epochs=1000) # Usamos 1000 epocas para balancear con el tamaño del mapa de 1500 neuronas y miles de puntos

# 3. Get Results
umatrix = som.umatrix
bmus = som.bmus 

print("SOM Trained.")
print(f"U-Matrix shape: {umatrix.shape}")

# 4. Save Outputs
# Save U-Matrix
umatrix_file = OUTPUT_DIR / 'som_trajectories_umatrix.npy'
np.save(umatrix_file, umatrix)

# Save BMUs along with internal IDs and Years
df_bmus = df_valid_traj.copy()

# bmus[:, 0] = x-coordinate = column index (0 to n_cols-1)
# bmus[:, 1] = y-coordinate = row index (0 to n_rows-1)
df_bmus['bmu_col'] = bmus[:, 0].astype(int)  # COL (x)
df_bmus['bmu_row'] = bmus[:, 1].astype(int)  # ROW (y)

bmus_file = OUTPUT_DIR / 'som_trajectories_bmus.parquet'
df_bmus.to_parquet(bmus_file, index=False)

print(f"✅ Saved U-Matrix to {umatrix_file}")
print(f"✅ Saved BMUs to {bmus_file}")
print(f"   Features used: {', '.join(available_features)}")
