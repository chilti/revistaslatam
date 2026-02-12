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

LATAM_CODES = {
    'MX', 'GT', 'SV', 'HN', 'NI', 'CR', 'PA', 'CU', 'DO', 'HT', 
    'PR', 'CO', 'VE', 'EC', 'PE', 'BO', 'CL', 'AR', 'PY', 'UY', 
    'BR', 'BZ', 'JM', 'TT', 'BB', 'BS', 'GY', 'SR', 'GF'
}

def reconstruct_abstract(inverted_index):
    """Convierte el diccionario de índices invertidos de OpenAlex en texto plano."""
    if not inverted_index or not isinstance(inverted_index, dict):
        return None
    try:
        # Encontrar la longitud basándose en el índice más alto encontrado
        max_idx = 0
        for positions in inverted_index.values():
            for pos in positions:
                if pos > max_idx:
                    max_idx = pos
        
        # Crear lista vacía y rellenar
        abstract_list = [""] * (max_idx + 1)
        for word, positions in inverted_index.items():
            for pos in positions:
                abstract_list[pos] = word
        
        return " ".join(abstract_list)
    except Exception:
        return None

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
        cur.execute("SET search_path TO openalex, public;")
        print("--- Conexión establecida con éxito ---")
    except Exception as e:
        print(f"Error de conexión: {e}")
        return

    works_path = os.path.join(SNAPSHOT_DIR, "works")
    print("Fase 2: Iniciando procesamiento de Works y Authorships (Reconstruyendo Abstracts)...")

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
                                
                                if source_id in latam_ids:
                                    found_in_file += 1
                                    
                                    # RECONSTRUCCIÓN DEL ABSTRACT
                                    # Transformamos el índice invertido en un string legible
                                    raw_abstract = work.get('abstract_inverted_index')
                                    full_abstract = reconstruct_abstract(raw_abstract)

                                    # Preparar datos para openalex.works
                                    work_row = [
                                        work['id'], work.get('doi'), work.get('title'),
                                        work.get('display_name'), work.get('publication_year'),
                                        work.get('publication_date'), work.get('type'),
                                        work.get('cited_by_count', 0), work.get('is_retracted', False),
                                        work.get('is_paratext', False), work.get('cited_by_api_url'),
                                        full_abstract, # <--- Ahora es Texto Plano
                                        work.get('language')
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

                if found_in_file > 0:
                    try:
                        buffer_works.seek(0)
                        cur.copy_from(buffer_works, 'works', sep='\t', null='\\N')
                        
                        buffer_authors.seek(0)
                        cur.copy_from(buffer_authors, 'works_authorships', sep='\t', null='\\N')
                        
                        conn.commit()
                        print(f"Cargado: {file_path} ({found_in_file} registros)")
                    except Exception as e:
                        conn.rollback()
                        print(f"Error en DB al procesar {file}: {e}")

    cur.close()
    conn.close()
    print("--- Proceso finalizado exitosamente ---")

if __name__ == "__main__":
    ids = get_latam_venue_ids()
    if ids:
        load_data(ids)
