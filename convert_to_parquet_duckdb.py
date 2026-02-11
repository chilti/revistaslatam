"""
Script optimizado para convertir OpenAlex Snapshot (JSONL) a Parquet usando DuckDB nativo.
Mantiene la estructura anidada (Listas/Structs) para consultas analíticas potentes.
"""
import os
import glob
import duckdb
import time
from datetime import datetime

# --- CONFIGURACIÓN ---
# Ajusta estas rutas a tu entorno Windows
SNAPSHOT_DIR = r'./openalex-snapshot/data'
# Recomendación: Usa un disco SSD rápido si es posible, o tu disco externo
OUTPUT_PARQUET_DIR = r'D:/openalex_parquet' 

# Entidades a procesar
ENTITIES = [
    'concepts', 
    'institutions', 
    'publishers', 
    'sources', 
    'authors', 
    'topics', 
    'works'
]

def convert_entity_to_parquet(entity_name):
    print(f"\n" + "="*60)
    print(f"PROCESANDO ENTIDAD: {entity_name.upper()}")
    print("="*60)
    
    # Buscar archivos fuente
    # OpenAlex estructura: data/works/updated_date=2023-XX-XX/*.gz
    # Usamos búsqueda recursiva por si hay sub-carpetas de fechas
    input_pattern = os.path.join(SNAPSHOT_DIR, entity_name, '**', '*.gz')
    files = glob.glob(input_pattern, recursive=True)
    
    if not files:
        print(f"⚠️ No se encontraron archivos en: {input_pattern}")
        return

    print(f"Found {len(files)} files found. Starting conversion...")
    
    # Crear directorio de salida
    output_path = os.path.join(OUTPUT_PARQUET_DIR, entity_name)
    os.makedirs(output_path, exist_ok=True)
    
    # Conexión persistente a DuckDB (más rápido)
    con = duckdb.connect(database=':memory:')
    
    # Configuración para memoria y velocidad
    con.execute("SET memory_limit='10GB';") # Ajusta según tu RAM
    con.execute("SET threads TO 4;")        # Ajusta según tus CPUs
    
    start_time = time.time()
    converted_count = 0
    skipped_count = 0
    
    for i, file_path in enumerate(files):
        filename = os.path.basename(file_path).replace('.gz', '.parquet')
        target_file = os.path.join(output_path, filename)
        
        # Saltar si ya existe
        if os.path.exists(target_file):
            skipped_count += 1
            print(f"[{i+1}/{len(files)}] ⏭️ Salteado (ya existe): {filename}")
            continue
            
        try:
            # MAGIA DE DUCKDB: Lee JSON Gzippeado y escribe Parquet directamente
            # read_json_auto infiere el esquema automáticamente
            # columns={'abstract_inverted_index': 'JSON'} ayuda si el esquema varía mucho, 
            # pero intentaremos la inferencia automática primero.
            
            query = f"""
            COPY (
                SELECT * 
                FROM read_json_auto('{file_path}', ignore_errors=true)
            ) TO '{target_file}' (FORMAT PARQUET, COMPRESSION 'SNAPPY', ROW_GROUP_SIZE 100000);
            """
            
            con.execute(query)
            converted_count += 1
            print(f"[{i+1}/{len(files)}] ✓ Convertido: {filename}")
            
        except Exception as e:
            print(f"[{i+1}/{len(files)}] ❌ Error en {filename}: {e}")
    
    elapsed = time.time() - start_time
    print(f"\nResumen para {entity_name}:")
    print(f"  - Convertidos: {converted_count}")
    print(f"  - Salteados: {skipped_count}")
    print(f"  - Tiempo: {elapsed:.2f} segundos")

if __name__ == "__main__":
    full_start = datetime.now()
    
    print("INICIANDO CONVERSIÓN A PARQUET (MODO DUCKDB)")
    print(f"Origen: {SNAPSHOT_DIR}")
    print(f"Destino: {OUTPUT_PARQUET_DIR}")
    
    for entity in ENTITIES:
        convert_entity_to_parquet(entity)
        
    print(f"\n" + "="*60)
    print(f"PROCESO TOTAL FINALIZADO EN: {datetime.now() - full_start}")
    print("="*60)
