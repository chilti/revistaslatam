import pandas as pd
from pathlib import Path

data_dir = Path('data/cache')
files = [
    'sunburst_metrics_country.parquet',
    'sunburst_metrics_region.parquet',
    'sunburst_metrics_journal.parquet'
]

for f_name in files:
    f_path = data_dir / f_name
    if f_path.exists():
        print(f"\n--- {f_name} ---")
        df = pd.read_parquet(f_path)
        print(f"Columnas: {df.columns.tolist()[:15]}...") # Primeras 15
        if 'count_recent' in df.columns:
            print("✅ 'count_recent' PRESENTE")
        else:
            print("❌ 'count_recent' FALTANTE")
    else:
        print(f"\n❌ {f_name} no existe.")
