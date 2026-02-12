import gzip
import json
import os
import psycopg2
from io import StringIO

# --- CONFIGURACIÓN ---
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "openalex_db",
    "user": "postgres",
    "password": "tu_contasena" 
}

SNAPSHOT_DIR = "./openalex-snapshot/data"

# Lista completa de países de Latinoamérica y el Caribe (ISO 3166-1 alpha-2)
LATAM_CODES = {
    'MX', 'GT', 'SV', 'HN', 'NI', 'CR', 'PA', 'CU', 'DO', 'HT', 
    'PR', 'CO', 'VE', 'EC', 'PE', 'BO', 'CL', 'AR', 'PY', 'UY', 
    'BR', 'BZ', 'JM', 'TT', 'BB', 'BS', 'GY', 'SR', 'GF'
}

def clean(val):
    """Limpia valores para el formato TSV de Postgres, eliminando caracteres de control."""
    if val is None:
        return '\\N'
    # Eliminamos tabuladores, saltos de línea y retornos de carro (0x0d)
    return str(val).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')

def get_latam_venue_ids():
    print("Fase 1: Identificando revistas de Latinoamérica y el Caribe...")
    latam_ids = set()
    sources_path = os.path.join(SNAPSHOT_DIR, "sources")
    
    if not os.path.exists(sources_path):
        print(f"Error: No se encuentra la carpeta {sources_path}")
        return None

    for root, _, files in os.walk(sources_path):
        for file in files:
            if file.endswith(".gz"):
                with gzip.open(os.path.join(root, file), 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            if data.get('country_code') in LATAM_CODES:
                                latam_ids.add(data['id'])
                        except: continue
    print(f"Éxito: {len(latam_ids)} revistas identificadas.")
    return latam_ids

def load_data(latam_ids):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        # Forzar el esquema para evitar errores de relación no existente
        cur.execute("SET search_path TO openalex, public;")
        print("--- Conexión establecida con éxito ---")
    except Exception as e:
        print(f"Error de conexión: {e}")
        return

    works_path = os.path.join(SNAPSHOT_DIR, "works")
    print("Fase 2: Iniciando procesamiento de Works y Authorships...")

    for root, _, files in os.walk(works_path):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(root, file)
                buffer_works = StringIO()
                buffer_authors = StringIO()
                found_in_file = 0
                
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            work = json.loads(line)
                            primary_loc = work.get('primary_location')
                            
                            if primary_loc and primary_loc.get('source'):
                                source_id = primary_loc['source'].get('id')
                                
                                # FILTRO: Solo si el artículo pertenece a una revista de Latam
                                if source_id in latam_ids:
                                    found_in_file += 1
                                    
                                    # Limpieza especial para el JSON del abstract (evita error 0x0d)
                                    abstract = work.get('abstract_inverted_index')
                                    abstract_json = json.dumps(abstract).replace('\\r', '').replace('\\n', '') if abstract else None

                                    # Preparar datos para openalex.works
                                    work_row = [
                                        work['id'], work.get('doi'), work.get('title'),
                                        work.get('display_name'), work.get('publication_year'),
                                        work.get('publication_date'), work.get('type'),
                                        work.get('cited_by_count', 0), work.get('is_retracted', False),
                                        work.get('is_paratext', False), work.get('cited_by_api_url'),
                                        abstract_json, work.get('language')
                                    ]
                                    buffer_works.write('\t'.join([clean(i) for i in work_row]) + '\n')

                                    # Preparar datos para openalex.works_authorships
                                    for auth in work.get('authorships', []):
                                        author_id = auth.get('author', {}).get('id')
                                        institutions = auth.get('institutions', [])
                                        base_row = [work['id'], auth.get('author_position'), author_id]
                                        
                                        if not institutions:
                                            auth_row = base_row + [None, auth.get('raw_affiliation_string')]
                                            buffer_authors.write('\t'.join([clean(i) for i in auth_row]) + '\n')
                                        else:
                                            for inst in institutions:
                                                auth_row = base_row + [inst.get('id'), auth.get('raw_affiliation_string')]
                                                buffer_authors.write('\t'.join([clean(i) for i in auth_row]) + '\n')
                        except Exception: continue

                # Carga masiva a Postgres si se encontraron artículos en este archivo
                if found_in_file > 0:
                    try:
                        buffer_works.seek(0)
                        cur.copy_from(buffer_works, 'works', sep='\t', null='\\N')
                        
                        buffer_authors.seek(0)
                        cur.copy_from(buffer_authors, 'works_authorships', sep='\t', null='\\N')
                        
                        conn.commit()
                        print(f"Procesado: {file_path} ({found_in_file} artículos)")
                    except Exception as e:
                        conn.rollback()
                        print(f"Error en DB al procesar {file}: {e}")
                else:
                    # Opcional: imprimir si no hubo hallazgos para ver progreso
                    if "part_000" in file: print(f"Escaneado (sin hallazgos): {file}")

    cur.close()
    conn.close()
    print("--- Proceso finalizado exitosamente ---")

if __name__ == "__main__":
    ids = get_latam_venue_ids()
    if ids:
        load_data(ids)
