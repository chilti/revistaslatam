import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import time
import sys
import gc

# Configuración
DATA_DIR = Path(__file__).parent.parent / 'data'
PARTS_DIR = DATA_DIR / 'works_parts'
OUTPUT_FILE = DATA_DIR / 'latin_american_works.parquet'

def consolidate_stream():
    print("="*70)
    print("CONSOLIDACIÓN OPTIMIZADA (STREAMING - BAJA MEMORIA)")
    print("="*70)
    
    if not PARTS_DIR.exists():
        print("No se encontró directorio de partes.")
        return

    # Listar y ordenar archivos
    files = sorted(list(PARTS_DIR.glob('*.parquet')), key=lambda x: x.name)
    if not files:
        print("No hay archivos para consolidar.")
        return
        
    print(f"Total archivos parciales: {len(files)}")
    total_size = sum(f.stat().st_size for f in files) / (1024**2)
    print(f"Tamaño total en disco: {total_size:.2f} MB")
    print("Iniciando proceso por lotes para proteger la memoria RAM...\n")
    
    # Configuración de Lotes
    BATCH_SIZE = 50  # Procesar 50 archivos por vez (aprox 300MB RAM)
    total_written = 0
    writer = None
    start_time = time.time()
    
    try:
        # Detectar Esquema del primer archivo
        # Esto es crucial para inicializar el escritor Parquet
        print("🔍 Detectando esquema base del primer archivo...")
        first_table = pq.read_table(files[0])
        schema = first_table.schema
        
        # Abrir Writer en modo overwrite
        # row_group_size ajustado para eficiencia
        writer = pq.ParquetWriter(OUTPUT_FILE, schema=schema, compression='snappy')
        
        current_batch = []
        
        for i, f in enumerate(files):
            try:
                # Leer archivo con Pandas (maneja mejor tipos mixtos/nulos)
                df = pd.read_parquet(f)
                
                # Alinear columnas con el esquema base
                # Si faltan columnas, agregar como None para evitar error de PyArrow
                for col in schema.names:
                    if col not in df.columns:
                        df[col] = None
                        
                # Reordenar columnas y descartar extras no presentes en esquema base
                # (Asumiendo que el primer archivo dicta el esquema maestro)
                df = df[schema.names]
                
                # Añadir al lote actual
                current_batch.append(df)
                
                # Procesar lote si está lleno o es el último archivo
                if len(current_batch) >= BATCH_SIZE or i == len(files) - 1:
                    batch_num = (i // BATCH_SIZE) + 1
                    print(f"  ⚡ Procesando Lote {batch_num} ({len(current_batch)} archivos)...", end=" ", flush=True)
                    
                    # Concatenar lote
                    batch_df = pd.concat(current_batch, ignore_index=True)
                    
                    # Deduplicar dentro del lote
                    batch_df.drop_duplicates(subset=['id'], keep='last', inplace=True)
                    
                    # Convertir a PyArrow Table forzando el esquema
                    # Esto maneja conversiones de tipos compatibles
                    table = pa.Table.from_pandas(batch_df, schema=schema, preserve_index=False)
                    
                    # Escribir al archivo final
                    writer.write_table(table)
                    
                    rows_in_batch = len(batch_df)
                    total_written += rows_in_batch
                    print(f"✓ Escritos {rows_in_batch:,} registros.")
                    
                    # Limpieza agresiva de memoria
                    del batch_df
                    del table
                    del current_batch
                    current_batch = []
                    gc.collect()
                    
            except Exception as e:
                print(f"  ❌ Error procesando {f.name}: {e}")
                # Continuar con el siguiente archivo
                
    except Exception as e:
        print(f"\n❌ Error Fatal durante consolidación: {e}")
    finally:
        if writer:
            writer.close()
            
    elapsed = time.time() - start_time
    print(f"\n✅ PROCESO FINALIZADO en {elapsed:.2f} s")
    print(f"Archivo generado: {OUTPUT_FILE}")
    print(f"Total registros consolidados: {total_written:,}")
    
    if OUTPUT_FILE.exists():
        final_size = OUTPUT_FILE.stat().st_size / (1024**2)
        print(f"Tamaño final del archivo: {final_size:.2f} MB")

if __name__ == "__main__":
    consolidate_stream()
