import gzip
import json
import os
import psycopg2
from io import StringIO
import time

# --- CONFIGURACIÓN ---
# Usamos la misma configuración que en load_Latam.py
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "openalex_db",
    "user": "postgres",
    "password": "tu_contasena"
}

# Ruta al snapshot en el servidor
SNAPSHOT_DIR = "./openalex-snapshot/data"

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def setup_database_schema(cur):
    """Agrega las columnas faltantes a la tabla works."""
    print("Modificando esquema de base de datos...")
    
    # Agregar columnas si no existen
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='works' AND column_name='fwci') THEN
                ALTER TABLE openalex.works ADD COLUMN fwci float;
            END IF;
            
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='works' AND column_name='citation_normalized_percentile') THEN
                ALTER TABLE openalex.works ADD COLUMN citation_normalized_percentile float;
            END IF;
        END $$;
    """)
    
    # Crear tabla temporal para carga masiva de actualizaciones
    cur.execute("""
        CREATE TEMP TABLE temp_works_metrics (
            id text PRIMARY KEY,
            fwci float,
            percentile float
        ) ON COMMIT DROP;
    """)
    print("Esquema actualizado y tabla temporal creada.")

def clean(val):
    if val is None: return '\\N'
    return str(val)

def process_and_update():
    works_path = os.path.join(SNAPSHOT_DIR, "works")
    if not os.path.exists(works_path):
        print(f"❌ Error: No se encuentra la carpeta {works_path}")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SET search_path TO openalex, public;")
    
    setup_database_schema(cur)
    
    # OPTIMIZACIÓN DE MEMORIA:
    # En lugar de cargar 3.4 millones de IDs de trabajos (que consume mucha RAM),
    # cargamos los IDs de las Fuentes (Revistas) que ya tenemos.
    # Como solo guardamos trabajos de revistas LATAM, filtrar por revista es equivalente y mucho más ligero.
    print("Cargando IDs de revistas (sources) existentes en la DB para filtrar...")
    cur.execute("SELECT DISTINCT id FROM openalex.sources")
    existing_source_ids = set(row[0] for row in cur.fetchall())
    print(f"Total revistas para filtrar: {len(existing_source_ids):,}")
    
    print("\nIniciando escaneo de archivos GZ...")
    start_time = time.time()
    
    processed_count = 0
    updated_count = 0
    buffer = StringIO()
    
    for root, _, files in os.walk(works_path):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(root, file)
                
                rows_in_file = 0
                
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        for line in f:
                            try:
                                # Lectura rápida: buscamos el ID primero
                                work = json.loads(line)
                                
                                # Filtro eficiente por Revista (Source)
                                primary_loc = work.get('primary_location') or {}
                                source = primary_loc.get('source') or {}
                                source_id = source.get('id')
                                
                                if source_id in existing_source_ids:
                                    work_id = work.get('id')
                                    # Extraer métricas
                                    fwci = work.get('fwci')
                                    
                                    # Extraer percentil (es un objeto, necesitamos 'value')
                                    percentile_obj = work.get('citation_normalized_percentile')
                                    percentile = None
                                    if isinstance(percentile_obj, dict):
                                        percentile = percentile_obj.get('value')
                                    else:
                                        # A veces viene como número directo en versiones viejas
                                        percentile = percentile_obj if isinstance(percentile_obj, (int, float)) else None
                                    
                                    # Solo guardar si tenemos algún dato nuevo
                                    if fwci is not None or percentile is not None:
                                        buffer.write(f"{work_id}\t{clean(fwci)}\t{clean(percentile)}\n")
                                        rows_in_file += 1
                                        processed_count += 1
                                    
                            except json.JSONDecodeError: continue
                            except Exception: continue
                            
                    # Cargar lote a tabla temporal
                    if rows_in_file > 0:
                        buffer.seek(0)
                        cur.copy_from(buffer, 'temp_works_metrics', columns=('id', 'fwci', 'percentile'), null='\\N')
                        buffer.seek(0)
                        buffer.truncate(0)
                        updated_count += rows_in_file
                        
                except Exception as e:
                    print(f"\nError procesando archivo {file}: {e}")

    print(f"\n\nProcesados {processed_count} trabajos con métricas encontrados en los GZ.")
    print("Aplicando UPDATE masivo a la tabla works...")
    
    # Hacer el UPDATE desde la tabla temporal
    # Esto es mucho más rápido que miles de updates individuales
    try:
        cur.execute("""
            UPDATE openalex.works w
            SET 
                fwci = t.fwci,
                citation_normalized_percentile = t.percentile
            FROM temp_works_metrics t
            WHERE w.id = t.id;
        """)
        conn.commit()
        print(f"✅ UPDATE Completado. Filas afectadas: {cur.rowcount}")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en UPDATE masivo: {e}")
    
    cur.close()
    conn.close()
    
    elapsed = time.time() - start_time
    print(f"Tiempo total: {elapsed/60:.2f} minutos")

if __name__ == "__main__":
    process_and_update()
