import pandas as pd
from pathlib import Path

data_dir = Path('data/cache')
files = [
    'sunburst_metrics_country.parquet',
    'sunburst_metrics_region.parquet',
    'sunburst_metrics_journal.parquet'
]
output_info = []

for f_name in files:
    f_path = data_dir / f_name
    if f_path.exists():
        df = pd.read_parquet(f_path)
        output_info.append(f"--- {f_name} ---")
        output_info.append(f"Columnas: {df.columns.tolist()}")
    else:
        output_info.append(f"--- {f_name} (FALTANTE) ---")

with open('data/debug_columns.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output_info))
