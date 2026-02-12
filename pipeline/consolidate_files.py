
"""
Script para consolidar archivos Parquet parciales usando DuckDB.
Une todos los archivos en data/works_parts/ y data/latin_american_works.parquet
en un solo archivo optimizado.
"""
import duckdb
import os
from pathlib import Path
import time

DATA_DIR = Path(__file__).parent.parent / 'data'
PARTS_DIR = DATA_DIR / 'works_parts'
OUTPUT_FILE = DATA_DIR / 'latin_american_works_consolidated.parquet'
ORIGINAL_FILE = DATA_DIR / 'latin_american_works.parquet'

def consolidate():
    print("="*60)
    print("CONSOLIDANDO ARCHIVOS PARQUET DE WORKS")
    print("="*60)
    
    files_to_merge = []
    
    # 1. Agregar archivo original si existe
    if ORIGINAL_FILE.exists():
        print(f"Incluyendo archivo original: {ORIGINAL_FILE.name}")
        files_to_merge.append(str(ORIGINAL_FILE))
        
    # 2. Agregar partes
    if PARTS_DIR.exists():
        parts = list(PARTS_DIR.glob('*.parquet'))
        print(f"Incluyendo {len(parts)} archivos parciales de {PARTS_DIR.name}/")
        files_to_merge.extend([str(p) for p in parts])
        
    if not files_to_merge:
        print("No hay archivos para consolidar.")
        return

    print(f"\nTotal archivos a unir: {len(files_to_merge)}")
    print("Iniciando consolidación con DuckDB (bajo consumo de memoria)...")
    
    start_time = time.time()
    
    try:
        # Usar DuckDB para leer todos los archivos y escribir uno solo
        # read_parquet acepta una lista de archivos o glob
        
        # Construir query SQL dinámica
        # "COPY (SELECT * FROM read_parquet(['file1', 'file2'...])) TO 'output.parquet' ..."
        
        # Para evitar problemas con listas muy largas en string SQL, usamos una vista o glob si posible
        # Pero como pueden estar en diferentes carpetas (root data y works_parts), pasamos lista
        
        # Truco: DuckDB permite leer lista de archivos
        files_sql = ", ".join([f"'{f}'" for f in files_to_merge])
        files_sql = f"[{files_sql}]"
        
        con = duckdb.connect()
        con.execute("SET memory_limit='8GB'") # Límite seguro
        
        query = f"""
        COPY (
            SELECT DISTINCT * FROM read_parquet({files_sql})
        ) TO '{str(OUTPUT_FILE)}' (FORMAT PARQUET, COMPRESSION 'SNAPPY', ROW_GROUP_SIZE 100000);
        """
        
        con.execute(query)
        
        elapsed = time.time() - start_time
        print(f"\n✅ Consolidación exitosa en {elapsed:.2f} segundos")
        print(f"Archivo generado: {OUTPUT_FILE}")
        
        # Opcional: Renombrar para reemplazar el original
        print("\nPara usar este archivo como el principal, ejecuta:")
        print(f"mv {OUTPUT_FILE} {ORIGINAL_FILE}")
        
        # Opción automática:
        # os.replace(OUTPUT_FILE, ORIGINAL_FILE)
        
    except Exception as e:
        print(f"\n❌ Error durante la consolidación: {e}")

if __name__ == "__main__":
    consolidate()
