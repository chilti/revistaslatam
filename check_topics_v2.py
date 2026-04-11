import pandas as pd
from pathlib import Path

parts_dir = Path('data/works_parts')
files = list(parts_dir.glob('*.parquet'))

if not files:
    print("No se encontraron archivos en data/works_parts/")
else:
    file = files[0]
    print(f"Verificando archivo: {file}")
    df = pd.read_parquet(file)
    print("Columnas encontradas:")
    print(df.columns.tolist())
    if 'topics' in df.columns:
        print("\n✅ La columna 'topics' está presente.")
    else:
        print("\n❌ La columna 'topics' NO está presente.")
