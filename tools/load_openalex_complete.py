import gzip
import json
import os
import psycopg2
from io import StringIO
import time
import datetime

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

def get_connection():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SET search_path TO openalex, public;")
    cur.execute("SET client_encoding TO 'UTF8';")
    return conn, cur

def clean(val):
    """Limpieza para formato COPY (TSV)."""
    if val is None: return '\\N'
    return str(val).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').replace('\0', '')

def clean_json(val):
    if val is None: return '\\N'
    json_str = json.dumps(val, ensure_ascii=False, separators=(',', ':'))
    json_str = json_str.replace('\\', '\\\\').replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    return json_str

# --- DEFINICIÓN DE TABLAS ---

def create_tables(cur):
    print("Creando tablas si no existen...")
    
    # 1. Sources (Revistas)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS openalex.sources (
            id text PRIMARY KEY,
            issn_l text,
            issn json,
            display_name text,
            publisher text,
            works_count integer,
            cited_by_count integer,
            is_oa boolean,
            is_in_doaj boolean,
            homepage_url text,
            works_api_url text,
            updated_date timestamp without time zone,
            country_code text,
            is_scopus boolean,
            summary_stats json
        );
    """)
    
    # 2. Institutions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS openalex.institutions (
            id text PRIMARY KEY,
            ror text,
            display_name text,
            country_code text,
            type text,
            homepage_url text,
            image_url text,
            image_thumbnail_url text,
            display_name_acronyms json,
            display_name_alternatives json,
            works_count integer,
            cited_by_count integer,
            works_api_url text,
            updated_date timestamp without time zone
        );
    """)
    
    # 3. Works (Tabla Principal) - INCLUYE METRICAS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS openalex.works (
            id text PRIMARY KEY,
            doi text,
            title text,
            display_name text,
            publication_year integer,
            publication_date text,
            type text,
            cited_by_count integer,
            is_retracted boolean,
            is_paratext boolean,
            cited_by_api_url text,
            abstract_inverted_index text,
            language text,
            fwci real,
            citation_normalized_percentile real
        );
    """)
    
    # 4. Works Authorships
    cur.execute("""
        CREATE TABLE IF NOT EXISTS openalex.works_authorships (
            work_id text,
            author_position text,
            author_id text,
            institution_id text,
            raw_affiliation_string text
        );
        CREATE INDEX IF NOT EXISTS idx_works_authorships_work_id ON openalex.works_authorships(work_id);
    """)
    
    # 5. Works Primary Location
    cur.execute("""
        CREATE TABLE IF NOT EXISTS openalex.works_primary_location (
            work_id text PRIMARY KEY,
            source_id text,
            is_oa boolean,
            landing_page_url text,
            pdf_url text,
            license text,
            version text
        );
    """)
    
    # 6. Works Open Access
    cur.execute("""
        CREATE TABLE IF NOT EXISTS openalex.works_open_access (
            work_id text PRIMARY KEY,
            is_oa boolean,
            oa_status text,
            oa_url text,
            any_repository_has_fulltext boolean
        );
    """)
    
    # 7. Works Topics (Nuevo para Sunburst)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS openalex.works_topics (
            work_id text,
            topic_id text,
            score real,
            topic_display_name text,
            field_display_name text,
            domain_display_name text
        );
        CREATE INDEX IF NOT EXISTS idx_works_topics_work_id ON openalex.works_topics(work_id);
    """)

    print("Tablas verificadas.")

# --- CARGA DE DATOS ---

def load_sources():
    print("\n>>> Procesando SOURCES (Revistas)...")
    conn, cur = get_connection()
    buffer = StringIO()
    latam_ids = set()
    count = 0
    
    path = os.path.join(SNAPSHOT_DIR, "sources")
    if not os.path.exists(path):
        print(f"No encontrado: {path}")
        return set()

    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".gz"):
                with gzip.open(os.path.join(root, file), 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            d = json.loads(line)
                            if d.get('country_code') in LATAM_CODES:
                                latam_ids.add(d['id'])
                                count += 1
                                row = [
                                    d['id'], d.get('issn_l'), clean_json(d.get('issn')),
                                    d.get('display_name'), d.get('publisher'),
                                    d.get('works_count', 0), d.get('cited_by_count', 0),
                                    d.get('is_oa', False), d.get('is_in_doaj', False),
                                    d.get('homepage_url'), d.get('works_api_url'),
                                    d.get('updated_date'), d.get('country_code'),
                                    bool((d.get('ids') or {}).get('scopus')),
                                    clean_json(d.get('summary_stats'))
                                ]
                                buffer.write('\t'.join([clean(x) for x in row]) + '\n')
                        except: continue
    
    if count > 0:
        print(f"Cargando {count} revistas...")
        cur.execute("TRUNCATE TABLE openalex.sources")
        buffer.seek(0)
        cur.copy_from(buffer, 'sources', sep='\t', null='\\N', columns=(
            'id', 'issn_l', 'issn', 'display_name', 'publisher', 'works_count', 
            'cited_by_count', 'is_oa', 'is_in_doaj', 'homepage_url', 'works_api_url', 
            'updated_date', 'country_code', 'is_scopus', 'summary_stats'
        ))
        conn.commit()
    
    conn.close()
    print(f"✓ Revistas cargadas: {count}")
    return latam_ids

def load_institutions():
    print("\n>>> Procesando INSTITUTIONS...")
    conn, cur = get_connection()
    buffer = StringIO()
    count = 0
    
    path = os.path.join(SNAPSHOT_DIR, "institutions")
    if not os.path.exists(path):
        print(f"No encontrado: {path}")
        return

    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".gz"):
                with gzip.open(os.path.join(root, file), 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            d = json.loads(line)
                            if d.get('country_code') in LATAM_CODES:
                                count += 1
                                row = [
                                    d['id'], d.get('ror'), d.get('display_name'),
                                    d.get('country_code'), d.get('type'), d.get('homepage_url'),
                                    d.get('image_url'), d.get('image_thumbnail_url'),
                                    clean_json(d.get('display_name_acronyms')),
                                    clean_json(d.get('display_name_alternatives')),
                                    d.get('works_count', 0), d.get('cited_by_count', 0),
                                    d.get('works_api_url'), d.get('updated_date')
                                ]
                                buffer.write('\t'.join([clean(x) for x in row]) + '\n')
                        except: continue

    if count > 0:
        print(f"Cargando {count} instituciones...")
        cur.execute("TRUNCATE TABLE openalex.institutions")
        buffer.seek(0)
        cur.copy_from(buffer, 'institutions', sep='\t', null='\\N', columns=(
            'id', 'ror', 'display_name', 'country_code', 'type', 'homepage_url',
            'image_url', 'image_thumbnail_url', 'display_name_acronyms',
            'display_name_alternatives', 'works_count', 'cited_by_count',
            'works_api_url', 'updated_date'
        ))
        conn.commit()
    
    conn.close()
    print(f"✓ Instituciones cargadas: {count}")

def load_works_complete(latam_ids):
    print("\n>>> Procesando WORKS (Completo con métricas y auxiliares)...")
    conn, cur = get_connection()
    
    # Truncar tablas antes de cargar (limpieza total)
    tables = ['works', 'works_authorships', 'works_primary_location', 'works_open_access', 'works_topics']
    for t in tables:
        cur.execute(f"TRUNCATE TABLE openalex.{t}")
    conn.commit()
    print("Tablas de Works truncadas. Iniciando carga masiva...")

    path = os.path.join(SNAPSHOT_DIR, "works")
    if not os.path.exists(path): return

    # Buffers
    b_works = StringIO()
    b_auth = StringIO()
    b_loc = StringIO()
    b_oa = StringIO()
    b_topic = StringIO()
    
    total_works = 0
    start_time = time.time()

    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(root, file)
                file_count = 0
                
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            w = json.loads(line)
                            
                            # Filtro: Revista LATAM
                            loc = w.get('primary_location') or {}
                            source = loc.get('source') or {}
                            s_id = source.get('id')
                            
                            if s_id in latam_ids:
                                file_count += 1
                                w_id = w['id']
                                
                                # 1. WORKS (con métricas)
                                percent_obj = w.get('citation_normalized_percentile')
                                percentile = percent_obj.get('value') if isinstance(percent_obj, dict) else None
                                
                                abstract_raw = w.get('abstract_inverted_index')
                                abstract_json = json.dumps(abstract_raw).replace('\\r', '').replace('\\n', '') if abstract_raw else None
                                
                                row_w = [
                                    w_id, w.get('doi'), w.get('title'), w.get('display_name'),
                                    w.get('publication_year'), w.get('publication_date'),
                                    w.get('type'), w.get('cited_by_count', 0),
                                    w.get('is_retracted', False), w.get('is_paratext', False),
                                    w.get('cited_by_api_url'), abstract_json, w.get('language'),
                                    w.get('fwci'), percentile
                                ]
                                b_works.write('\t'.join([clean(x) for x in row_w]) + '\n')
                                
                                # 2. LOCATIONS
                                row_loc = [
                                    w_id, s_id, loc.get('is_oa'), loc.get('landing_page_url'),
                                    loc.get('pdf_url'), loc.get('license'), loc.get('version')
                                ]
                                b_loc.write('\t'.join([clean(x) for x in row_loc]) + '\n')
                                
                                # 3. OPEN ACCESS
                                oa = w.get('open_access') or {}
                                row_oa = [
                                    w_id, oa.get('is_oa'), oa.get('oa_status'),
                                    oa.get('oa_url'), oa.get('any_repository_has_fulltext')
                                ]
                                b_oa.write('\t'.join([clean(x) for x in row_oa]) + '\n')
                                
                                # 4. AUTHORSHIPS
                                for auth in w.get('authorships', []):
                                    a_id = (auth.get('author') or {}).get('id')
                                    pos = auth.get('author_position')
                                    affil = auth.get('raw_affiliation_string')
                                    insts = auth.get('institutions', [])
                                    
                                    if not insts:
                                        row_a = [w_id, pos, a_id, None, affil]
                                        b_auth.write('\t'.join([clean(x) for x in row_a]) + '\n')
                                    else:
                                        for i in insts:
                                            row_a = [w_id, pos, a_id, i.get('id'), affil]
                                            b_auth.write('\t'.join([clean(x) for x in row_a]) + '\n')
                                            
                                # 5. TOPICS (Primeros 3 para no llenar DB)
                                for t in w.get('topics', [])[:3]:
                                    row_t = [
                                        w_id, t.get('id'), t.get('score'),
                                        t.get('display_name'), t.get('field', {}).get('display_name'),
                                        t.get('domain', {}).get('display_name')
                                    ]
                                    b_topic.write('\t'.join([clean(x) for x in row_t]) + '\n')

                        except: continue

                # Batch commit por archivo para no reventar RAM
                if file_count > 0:
                    total_works += file_count
                    try:
                        # Copy Works
                        b_works.seek(0)
                        cur.copy_from(b_works, 'works', sep='\t', null='\\N', columns=(
                            'id','doi','title','display_name','publication_year','publication_date',
                            'type','cited_by_count','is_retracted','is_paratext','cited_by_api_url',
                            'abstract_inverted_index','language','fwci','citation_normalized_percentile'
                        ))
                        # Copy others
                        b_loc.seek(0); cur.copy_from(b_loc, 'works_primary_location', sep='\t', null='\\N')
                        b_oa.seek(0); cur.copy_from(b_oa, 'works_open_access', sep='\t', null='\\N')
                        b_auth.seek(0); cur.copy_from(b_auth, 'works_authorships', sep='\t', null='\\N')
                        b_topic.seek(0); cur.copy_from(b_topic, 'works_topics', sep='\t', null='\\N', columns=(
                            'work_id','topic_id','score','topic_display_name','field_display_name','domain_display_name'
                        ))
                        
                        conn.commit()
                        print(f"  {file}: {file_count} trabajos procesados.")
                        
                        # Limpiar buffers
                        b_works.truncate(0); b_works.seek(0)
                        b_loc.truncate(0); b_loc.seek(0)
                        b_oa.truncate(0); b_oa.seek(0)
                        b_auth.truncate(0); b_auth.seek(0)
                        b_topic.truncate(0); b_topic.seek(0)
                        
                    except Exception as e:
                        conn.rollback()
                        print(f"ERROR procesando {file}: {e}")
                        # Reset buffers
                        b_works = StringIO(); b_loc = StringIO(); b_oa = StringIO(); b_auth = StringIO(); b_topic = StringIO()

    conn.close()
    elapsed = (time.time() - start_time) / 60
    print(f"\n✓ Carga Completada. Total Works: {total_works:,}. Tiempo: {elapsed:.2f} min.")


if __name__ == "__main__":
    print("="*60)
    print("CARGA COMPLETA DE OPENALEX (LATAM) v3.0")
    print("="*60)
    
    conn, cur = get_connection()
    create_tables(cur)
    conn.close()
    
    # 1. Sources
    ids = load_sources()
    
    if ids:
        # 2. Institutions
        load_institutions()
        
        # 3. Works (All tables)
        load_works_complete(ids)
    else:
        print("Error: No se encontraron revistas LATAM.")
