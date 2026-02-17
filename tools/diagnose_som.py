"""
Diagnostic script to analyze SOM BMU assignments and identify issues.
"""
import pandas as pd
import numpy as np
from pathlib import Path

BASE_PATH = Path(__file__).parent.parent
SOM_DIR = BASE_PATH / 'data' / 'som'

print("=" * 70)
print("SOM DIAGNOSTIC REPORT")
print("=" * 70)

# Load files
umatrix_file = SOM_DIR / 'som_countries_umatrix.npy'
bmus_file = SOM_DIR / 'som_countries_bmus.parquet'

if not umatrix_file.exists():
    print("❌ U-Matrix file not found!")
    exit(1)

if not bmus_file.exists():
    print("❌ BMUs file not found!")
    exit(1)

# Load data
U = np.load(umatrix_file)
df_bmus = pd.read_parquet(bmus_file)

print(f"\n1. U-Matrix Shape: {U.shape}")
print(f"   n_rows = {U.shape[0]}, n_cols = {U.shape[1]}")

print(f"\n2. BMUs DataFrame:")
print(f"   Total countries: {len(df_bmus)}")
print(f"   Columns: {list(df_bmus.columns)}")

print(f"\n3. BMU Coordinate Statistics:")
if 'bmu_row' in df_bmus.columns and 'bmu_col' in df_bmus.columns:
    print(f"   bmu_row: min={df_bmus['bmu_row'].min():.2f}, max={df_bmus['bmu_row'].max():.2f}")
    print(f"   bmu_col: min={df_bmus['bmu_col'].min():.2f}, max={df_bmus['bmu_col'].max():.2f}")
    
    # Check if they're integers
    print(f"\n   Data types:")
    print(f"   bmu_row: {df_bmus['bmu_row'].dtype}")
    print(f"   bmu_col: {df_bmus['bmu_col'].dtype}")
    
    # Check ranges
    n_rows, n_cols = U.shape
    print(f"\n4. Range Validation:")
    print(f"   Expected row range: [0, {n_rows-1}]")
    print(f"   Actual row range: [{df_bmus['bmu_row'].min()}, {df_bmus['bmu_row'].max()}]")
    print(f"   Expected col range: [0, {n_cols-1}]")
    print(f"   Actual col range: [{df_bmus['bmu_col'].min()}, {df_bmus['bmu_col'].max()}]")
    
    # Check if rows are out of bounds
    rows_oob = (df_bmus['bmu_row'] < 0) | (df_bmus['bmu_row'] >= n_rows)
    cols_oob = (df_bmus['bmu_col'] < 0) | (df_bmus['bmu_col'] >= n_cols)
    
    if rows_oob.any():
        print(f"   ⚠️ {rows_oob.sum()} countries have row coordinates OUT OF BOUNDS!")
    else:
        print(f"   ✅ All row coordinates are within bounds")
        
    if cols_oob.any():
        print(f"   ⚠️ {cols_oob.sum()} countries have col coordinates OUT OF BOUNDS!")
    else:
        print(f"   ✅ All col coordinates are within bounds")
    
    # Distribution of neurons used
    print(f"\n5. Neuron Usage Distribution:")
    df_bmus['neuron'] = df_bmus.apply(lambda x: (int(x['bmu_row']), int(x['bmu_col'])), axis=1)
    neuron_counts = df_bmus['neuron'].value_counts()
    
    total_neurons = n_rows * n_cols
    used_neurons = len(neuron_counts)
    
    print(f"   Total neurons in SOM: {total_neurons}")
    print(f"   Neurons with at least 1 country: {used_neurons}")
    print(f"   Empty neurons: {total_neurons - used_neurons}")
    print(f"   Coverage: {used_neurons/total_neurons*100:.1f}%")
    
    print(f"\n6. Top 10 Most Populated Neurons:")
    for neuron, count in neuron_counts.head(10).items():
        countries = df_bmus[df_bmus['neuron'] == neuron]['country_code'].tolist()
        print(f"   Neuron {neuron}: {count} countries - {', '.join(countries)}")
    
    # Check if coordinates might be swapped
    print(f"\n7. Coordinate Order Check:")
    print(f"   If bmu_row values exceed n_rows-1 ({n_rows-1}), coordinates might be swapped.")
    print(f"   If bmu_col values exceed n_cols-1 ({n_cols-1}), coordinates might be swapped.")
    
    if df_bmus['bmu_row'].max() >= n_rows:
        print(f"   ⚠️ WARNING: bmu_row max ({df_bmus['bmu_row'].max()}) >= n_rows ({n_rows})")
        print(f"   This suggests ROW and COL might be SWAPPED!")
    
    if df_bmus['bmu_col'].max() >= n_cols:
        print(f"   ⚠️ WARNING: bmu_col max ({df_bmus['bmu_col'].max()}) >= n_cols ({n_cols})")
        print(f"   This suggests ROW and COL might be SWAPPED!")
    
else:
    print("   ❌ Missing bmu_row or bmu_col columns!")

print("\n" + "=" * 70)
