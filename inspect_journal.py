import pandas as pd
from pathlib import Path

# Cargar el parquet enriquecido
data_dir = Path('data')
enriched_file = data_dir / 'journals_enriched.parquet'

if enriched_file.exists():
    df = pd.read_parquet(enriched_file)
    # Filtrar por el ID de la revista (comparando el ID corto o largo)
    target_id = "https://openalex.org/S2737081250"
    target_short = "S2737081250"
    
    # Intentar encontrarla
    row = df[df['id'].str.contains(target_short, na=False)]
    
    if not row.empty:
        print(f"--- Datos encontrados para {target_id} ---")
        # Mostrar todas las columnas disponibles para esta fila
        print(row.iloc[0].to_dict())
    else:
        print(f"❌ La revista {target_id} no se encontró en {enriched_file}")
        # Mostrar algunas revistas para ver el formato de ID
        print("\nEjemplos de IDs en el parquet:")
        print(df['id'].head().tolist())
else:
    print(f"❌ El archivo {enriched_file} no existe.")
