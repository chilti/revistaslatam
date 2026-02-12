import gzip
import json
import os
import psycopg2
import time
from pathlib import Path

# --- CONFIGURACIÓN ---
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "openalex_db",
    "user": "postgres",
    "password": "tu_contasena"
}

# Ruta al snapshot
SNAPSHOT_DIR = Path("./openalex-snapshot/data/works")

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def ensure_columns_exist(cur):
    """Asegura que las columnas métricas existan en la tabla."""
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

def get_relevant_source_ids(cur):
    """Obtiene los IDs de revistas (sources) que tenemos en la base de datos para filtrar."""
    print("Cargando IDs de revistas para filtrar...")
    cur.execute("SELECT id FROM openalex.sources")
    return set(row[0] for row in cur.fetchall())

def process_and_update_incremental():
    """
    Procesa los archivos GZ de trabajos y actualiza la base de datos archivo por archivo.
    """
    if not SNAPSHOT_DIR.exists():
        print(f"❌ Error: No se encuentra la ruta {SNAPSHOT_DIR}")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SET search_path TO openalex, public;")
        ensure_columns_exist(cur)
        conn.commit()
        
        relevant_sources = get_relevant_source_ids(cur)
        print(f"Filtrando por {len(relevant_sources):,} revistas relevantes.")
        
        files = list(SNAPSHOT_DIR.glob("**/*.gz"))
        total_files = len(files)
        print(f"Encontrados {total_files} archivos GZ para procesar.")
        
        start_time = time.time()
        total_updated = 0
        
        for i, file_path in enumerate(files, 1):
            file_updates = []
            
            try:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            work = json.loads(line)
                            
                            # Filtro rápido: ¿Es de una de nuestras revistas?
                            # La estructura es work['primary_location']['source']['id']
                            primary_loc = work.get('primary_location')
                            if not primary_loc: continue
                            
                            source = primary_loc.get('source')
                            if not source: continue
                            
                            source_id = source.get('id')
                            if source_id not in relevant_sources: continue
                            
                            # Extraer métricas
                            work_id = work.get('id')
                            fwci = work.get('fwci')
                            
                            pct_data = work.get('citation_normalized_percentile')
                            percentile = None
                            if isinstance(pct_data, dict):
                                percentile = pct_data.get('value')
                            elif isinstance(pct_data, (int, float)):
                                percentile = pct_data
                            
                            if fwci is not None or percentile is not None:
                                # Tupla para el update: (fwci, percentile, id)
                                file_updates.append((fwci, percentile, work_id))
                                
                        except (json.JSONDecodeError, AttributeError):
                            continue
                
                # Ejecutar actualización por lotes para este archivo
                if file_updates:
                    cur.executemany("""
                        UPDATE openalex.works 
                        SET fwci = %s, citation_normalized_percentile = %s 
                        WHERE id = %s
                    """, file_updates)
                    conn.commit()
                    total_updated += len(file_updates)
                    print(f"[{i}/{total_files}] Procesado {file_path.name}: {len(file_updates)} actualizaciones. (Total: {total_updated:,})")
                else:
                    print(f"[{i}/{total_files}] Procesado {file_path.name}: 0 actualizaciones relevantes.")
                    
            except Exception as e:
                conn.rollback()
                print(f"❌ Error procesando {file_path.name}: {e}")
                
    except Exception as e:
        print(f"❌ Error general: {e}")
    finally:
        cur.close()
        conn.close()
        
    elapsed = time.time() - start_time
    print(f"\n✅ Proceso finalizado en {elapsed/60:.2f} minutos.")
    print(f"Total de trabajos actualizados: {total_updated:,}")

if __name__ == "__main__":
    process_and_update_incremental()
