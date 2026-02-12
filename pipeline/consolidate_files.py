import os
from pathlib import Path
import time
import pandas as pd
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
except ImportError:
    print("Error: pyarrow no instalado. Ejecuta pip install pyarrow")
    exit(1)

DATA_DIR = Path(__file__).parent.parent / 'data'
PARTS_DIR = DATA_DIR / 'works_parts'
OUTPUT_FILE = DATA_DIR / 'latin_american_works.parquet'

def consolidate():
    print("="*60)
    print("CONSOLIDANDO ARCHIVOS PARQUET DE WORKS (PyArrow)")
    print("="*60)
    
    files_to_merge = []
    
    # Agregar partes
    if PARTS_DIR.exists():
        parts = list(PARTS_DIR.glob('*.parquet'))
        # Ordenar por tiempo
        parts.sort(key=os.path.getmtime)
        if parts:
            print(f"Encontrados {len(parts)} archivos parciales en {PARTS_DIR.name}/")
            files_to_merge.extend([str(p) for p in parts])
        
    if not files_to_merge:
        print("No hay archivos parciales para consolidar.")
        if OUTPUT_FILE.exists():
            print("El archivo consolidado ya existe.")
        return

    print(f"\nTotal archivos a unir: {len(files_to_merge)}")
    print(f"Destino: {OUTPUT_FILE}")
    
    start_time = time.time()
    
    try:
        # Leer esquema del primer archivo para inicializar Writer
        first_file_path = files_to_merge[0]
        schema = pq.read_schema(first_file_path)
        
        # Crear carpeta si no existe
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Escribir incrementalmente
        with pq.ParquetWriter(OUTPUT_FILE, schema=schema, compression='snappy') as writer:
            for i, f_path in enumerate(files_to_merge):
                print(f"  [{i+1}/{len(files_to_merge)}] Procesando {Path(f_path).name}...", end=" ", flush=True)
                
                try:
                    table = pq.read_table(f_path)
                    
                    # Verificar compatibilidad de esquema (básico)
                    if not table.schema.equals(schema):
                        # Intentar cast/normalizar si difieren columnas (p.ej. nulls vs int)
                        # Esto es complejo, asumimos por ahora que son iguales.
                        # Si fallan, pyarrow lanzará error.
                        pass
                        
                    writer.write_table(table)
                    print("✓")
                except Exception as e:
                    print(f"❌ Error leyendo {f_path}: {e}")
        
        elapsed = time.time() - start_time
        print(f"\n✅ Consolidación exitosa en {elapsed:.2f} segundos")
        print(f"Archivo generado: {OUTPUT_FILE}")
        
        size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
        print(f"Tamaño final: {size_mb:.2f} MB")
        
    except Exception as e:
        print(f"\n❌ Error durante la consolidación: {e}")
        # Si falla, borrar el archivo corrupto parcial
        if OUTPUT_FILE.exists():
            try:
                os.remove(OUTPUT_FILE)
                print("Archivo de salida parcial eliminado.")
            except:
                pass

if __name__ == "__main__":
    consolidate()
