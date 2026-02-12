import pandas as pd
import pyarrow.parquet as pq
import sys
from pathlib import Path

# Configuración
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
WORKS_FILE = DATA_DIR / 'latin_american_works.parquet'

print(f"Directorio de datos: {DATA_DIR}")
print("-" * 60)

if not WORKS_FILE.exists():
    print(f"❌ Error: No se encuentra el archivo {WORKS_FILE}")
    sys.exit(1)

print(f"Abriendo archivo: {WORKS_FILE.name}...")

try:
    # Usar pyarrow para leer metadatos sin cargar todo el archivo
    parquet_file = pq.ParquetFile(WORKS_FILE)
    schema = parquet_file.schema.names
    
    print(f"Total filas estimadas: {parquet_file.metadata.num_rows:,}")
    print(f"Columnas encontradas ({len(schema)}):")
    print(schema)
    print("-" * 60)
    
    # Columnas críticas para métricas
    critical_cols = [
        'fwci', 
        'citation_normalized_percentile', 
        'is_in_top_10_percent', 
        'is_in_top_1_percent',
        'open_access', 
        'publication_year'
    ]
    
    missing_cols = [c for c in critical_cols if c not in schema]
    
    if missing_cols:
        print(f"❌ FALTAN COLUMNAS CRÍTICAS: {missing_cols}")
        print("   Esto explica por qué las métricas están en cero.")
        print("   Solución: Debes volver a ejecutar pipeline/extract_postgres.py")
    else:
        print("✅ Todas las columnas críticas están presentes en el esquema.")
        
        # Leer una muestra para verificar contenido
        print("\nVerificando contenido de columnas críticas (primeras 5 filas con datos)...")
        # Leer solo columnas críticas
        df_sample = pd.read_parquet(WORKS_FILE, columns=critical_cols + ['id']).head(10)
        
        # Mostrar tipos de datos y nulos
        print(df_sample.dtypes)
        print("\nMuestra de datos:")
        print(df_sample.to_string())
        
        # Verificar si hay valores no nulos en FWCI
        # Leer un chunk más grande para estadística rápida
        print("\nAnalizando distribución de valores en primeros 10,000 registros...")
        df_chunk = next(parquet_file.iter_batches(batch_size=10000)).to_pandas()
        
        if 'fwci' in df_chunk.columns:
            fwci_valid = pd.to_numeric(df_chunk['fwci'], errors='coerce').dropna()
            print(f"   FWCI: {len(fwci_valid)} valores válidos (Media: {fwci_valid.mean():.4f})")
            print(f"   FWCI > 0: {(fwci_valid > 0).sum()} registros")
        
        if 'is_in_top_10_percent' in df_chunk.columns:
             top10 = df_chunk['is_in_top_10_percent'].fillna(False).sum()
             print(f"   Top 10%: {top10} registros marcados como True")

except Exception as e:
    print(f"❌ Error al leer el archivo: {e}")
    import traceback
    traceback.print_exc()
