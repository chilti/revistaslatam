"""
Script para completar la base de datos OpenAlex con las tablas faltantes.
Complementa load2.py cargando: sources, institutions, works_primary_location, etc.
"""
import gzip
import json
import os
import psycopg2
from io import StringIO

# --- CONFIGURACIÓN ---
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "openalex_db",  # Cambiar si usas otro nombre
    "user": "postgres",
    "password": "tu_contasena" 
}

SNAPSHOT_DIR = "./openalex-snapshot/data"

LATAM_CODES = {
    'MX', 'GT', 'SV', 'HN', 'NI', 'CR', 'PA', 'CU', 'DO', 'HT', 
    'PR', 'CO', 'VE', 'EC', 'PE', 'BO', 'CL', 'AR', 'PY', 'UY', 
    'BR', 'BZ', 'JM', 'TT', 'BB', 'BS', 'GY', 'SR', 'GF'
}

def clean(val):
    """Limpia valores para el formato TSV de Postgres."""
    if val is None:
        return '\\N'
    return str(val).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')

def clean_json(val):
    """Convierte JSON a string limpio sin corromper Unicode."""
    if val is None:
        return '\\N'
    # ensure_ascii=False preserva caracteres Unicode correctamente
    # separators sin espacios para formato compacto
    json_str = json.dumps(val, ensure_ascii=False, separators=(',', ':'))
    # Escapar comillas dobles para PostgreSQL COPY (\" → \")
    # Solo reemplazar caracteres problemáticos para TSV
    json_str = json_str.replace('\\', '\\\\')  # Escapar backslashes primero
    json_str = json_str.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    return json_str


def load_sources():
    """Carga todas las revistas (sources) de LATAM."""
    print("\n" + "="*70)
    print("CARGANDO SOURCES (Revistas)")
    print("="*70)
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("SET search_path TO openalex, public;")
        print("✓ Conexión establecida")
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        return None
    
    sources_path = os.path.join(SNAPSHOT_DIR, "sources")
    
    if not os.path.exists(sources_path):
        print(f"✗ No se encuentra la carpeta {sources_path}")
        return None
    
    buffer = StringIO()
    total_loaded = 0
    latam_source_ids = set()
    
    print("Procesando archivos...")
    
    for root, _, files in os.walk(sources_path):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(root, file)
                count_in_file = 0
                
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            
                            # Filtrar solo LATAM
                            if data.get('country_code') in LATAM_CODES:
                                count_in_file += 1
                                latam_source_ids.add(data['id'])
                                
                                # Preparar fila para sources
                                row = [
                                    data['id'],
                                    data.get('issn_l'),
                                    clean_json(data.get('issn')),
                                    data.get('display_name'),
                                    data.get('publisher'),
                                    data.get('works_count', 0),
                                    data.get('cited_by_count', 0),
                                    data.get('is_oa', False),
                                    data.get('is_in_doaj', False),
                                    data.get('homepage_url'),
                                    data.get('works_api_url'),
                                    data.get('updated_date'),
                                    data.get('country_code'),  # Agregado country_code
                                    bool((data.get('ids') or {}).get('scopus')),  # Agregado is_scopus
                                    clean_json(data.get('summary_stats'))  # Agregado summary_stats
                                ]
                                
                                buffer.write('\t'.join([clean(i) for i in row]) + '\n')
                        except Exception as e:
                            continue
                
                if count_in_file > 0:
                    print(f"  {file}: {count_in_file} revistas LATAM")
                    total_loaded += count_in_file
    
    # Crear la tabla sources con country_code si no existe
    # (Nota: Si ya existe sin country_code, deberás borrarla manualmente o el copy fallará si las columnas no coinciden)
    # Pero aquí asumimos que el usuario borrará la tabla antes de correr esto
    try:
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
        conn.commit()
    except Exception as e:
        print(f"Advertencia al crear tabla sources: {e}")
        conn.rollback()

    # Cargar a la base de datos
    try:
        buffer.seek(0)
        # Especificar columnas explícitamente para evitar errores si la tabla tiene diferente orden
        columns = (
            'id', 'issn_l', 'issn', 'display_name', 'publisher', 
            'works_count', 'cited_by_count', 'is_oa', 'is_in_doaj', 
            'homepage_url', 'works_api_url', 'updated_date', 'country_code', 'is_scopus', 'summary_stats'
        )
        # Especificar encoding UTF-8 explícitamente
        cur.execute("SET client_encoding TO 'UTF8';")
        cur.copy_from(buffer, 'sources', sep='\t', null='\\N', columns=columns)
        conn.commit()
        print(f"\n✓ Cargadas {total_loaded} revistas LATAM")
    except Exception as e:
        conn.rollback()
        print(f"✗ Error al cargar sources: {e}")
        latam_source_ids = None
    
    cur.close()
    conn.close()
    
    return latam_source_ids


def load_institutions():
    """Carga todas las instituciones de LATAM."""
    print("\n" + "="*70)
    print("CARGANDO INSTITUTIONS (Instituciones)")
    print("="*70)
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("SET search_path TO openalex, public;")
        print("✓ Conexión establecida")
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        return None
    
    institutions_path = os.path.join(SNAPSHOT_DIR, "institutions")
    
    if not os.path.exists(institutions_path):
        print(f"✗ No se encuentra la carpeta {institutions_path}")
        return None
    
    buffer = StringIO()
    total_loaded = 0
    latam_inst_ids = set()
    
    print("Procesando archivos...")
    
    for root, _, files in os.walk(institutions_path):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(root, file)
                count_in_file = 0
                
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            
                            # Filtrar solo LATAM
                            if data.get('country_code') in LATAM_CODES:
                                count_in_file += 1
                                latam_inst_ids.add(data['id'])
                                
                                # Preparar fila para institutions
                                row = [
                                    data['id'],
                                    data.get('ror'),
                                    data.get('display_name'),
                                    data.get('country_code'),
                                    data.get('type'),
                                    data.get('homepage_url'),
                                    data.get('image_url'),
                                    data.get('image_thumbnail_url'),
                                    clean_json(data.get('display_name_acronyms')),
                                    clean_json(data.get('display_name_alternatives')),
                                    data.get('works_count', 0),
                                    data.get('cited_by_count', 0),
                                    data.get('works_api_url'),
                                    data.get('updated_date')
                                ]
                                
                                buffer.write('\t'.join([clean(i) for i in row]) + '\n')
                        except Exception as e:
                            continue
                
                if count_in_file > 0:
                    print(f"  {file}: {count_in_file} instituciones LATAM")
                    total_loaded += count_in_file
    
    # Crear tabla con PRIMARY KEY si no existe
    try:
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
        conn.commit()
    except Exception as e:
        print(f"Advertencia al crear tabla institutions: {e}")
        conn.rollback()

    # Cargar a la base de datos
    try:
        buffer.seek(0)
        columns = (
            'id', 'ror', 'display_name', 'country_code', 'type',
            'homepage_url', 'image_url', 'image_thumbnail_url',
            'display_name_acronyms', 'display_name_alternatives',
            'works_count', 'cited_by_count', 'works_api_url', 'updated_date'
        )
        # Especificar encoding UTF-8 explícitamente
        cur.execute("SET client_encoding TO 'UTF8';")
        cur.copy_from(buffer, 'institutions', sep='\t', null='\\N', columns=columns)
        conn.commit()
        print(f"\n✓ Cargadas {total_loaded} instituciones LATAM")
    except Exception as e:
        conn.rollback()
        print(f"✗ Error al cargar institutions: {e}")
        latam_inst_ids = None
    
    cur.close()
    conn.close()
    
    return latam_inst_ids


def load_works_primary_location(latam_source_ids):
    """
    Carga works_primary_location para los works que ya están en la BD.
    Requiere que ya se hayan cargado los works con load2.py
    """
    print("\n" + "="*70)
    print("CARGANDO WORKS_PRIMARY_LOCATION")
    print("="*70)
    
    if not latam_source_ids:
        print("✗ No hay IDs de sources LATAM. Ejecuta load_sources() primero.")
        return
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("SET search_path TO openalex, public;")
        print("✓ Conexión establecida")
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        return
    
    # Primero, crear la tabla si no existe
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS openalex.works_primary_location (
        work_id text PRIMARY KEY,
        source_id text,
        is_oa boolean,
        landing_page_url text,
        pdf_url text,
        license text,
        version text
    );
    """
    
    try:
        cur.execute(create_table_sql)
        conn.commit()
        print("✓ Tabla works_primary_location verificada/creada")
    except Exception as e:
        print(f"✗ Error al crear tabla: {e}")
        return
    
    works_path = os.path.join(SNAPSHOT_DIR, "works")
    
    if not os.path.exists(works_path):
        print(f"✗ No se encuentra la carpeta {works_path}")
        return
    
    buffer = StringIO()
    total_loaded = 0
    
    print("Procesando archivos...")
    
    for root, _, files in os.walk(works_path):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(root, file)
                count_in_file = 0
                
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            work = json.loads(line)
                            primary_loc = work.get('primary_location')
                            
                            if primary_loc and primary_loc.get('source'):
                                source_id = primary_loc['source'].get('id')
                                
                                # Solo procesar si es una revista LATAM
                                if source_id in latam_source_ids:
                                    count_in_file += 1
                                    
                                    row = [
                                        work['id'],
                                        source_id,
                                        primary_loc.get('is_oa', False),
                                        primary_loc.get('landing_page_url'),
                                        primary_loc.get('pdf_url'),
                                        primary_loc.get('license'),
                                        primary_loc.get('version')
                                    ]
                                    
                                    buffer.write('\t'.join([clean(i) for i in row]) + '\n')
                        except Exception:
                            continue
                
                if count_in_file > 0:
                    try:
                        buffer.seek(0)
                        cur.copy_from(buffer, 'works_primary_location', sep='\t', null='\\N')
                        conn.commit()
                        print(f"  {file}: {count_in_file} registros")
                        total_loaded += count_in_file
                        buffer = StringIO()  # Reset buffer
                    except Exception as e:
                        conn.rollback()
                        print(f"  ✗ Error en {file}: {e}")
                        buffer = StringIO()
    
    print(f"\n✓ Cargados {total_loaded} registros de works_primary_location")
    
    cur.close()
    conn.close()


def load_works_open_access(latam_source_ids):
    """
    Carga información de Open Access para los works LATAM.
    """
    print("\n" + "="*70)
    print("CARGANDO WORKS_OPEN_ACCESS")
    print("="*70)
    
    if not latam_source_ids:
        print("✗ No hay IDs de sources LATAM. Ejecuta load_sources() primero.")
        return
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("SET search_path TO openalex, public;")
        print("✓ Conexión establecida")
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        return
    
    # Crear la tabla si no existe
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS openalex.works_open_access (
        work_id text PRIMARY KEY,
        is_oa boolean,
        oa_status text,
        oa_url text,
        any_repository_has_fulltext boolean
    );
    """
    
    try:
        cur.execute(create_table_sql)
        conn.commit()
        print("✓ Tabla works_open_access verificada/creada")
    except Exception as e:
        print(f"✗ Error al crear tabla: {e}")
        return
    
    works_path = os.path.join(SNAPSHOT_DIR, "works")
    buffer = StringIO()
    total_loaded = 0
    
    print("Procesando archivos...")
    
    for root, _, files in os.walk(works_path):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(root, file)
                count_in_file = 0
                
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            work = json.loads(line)
                            primary_loc = work.get('primary_location')
                            
                            if primary_loc and primary_loc.get('source'):
                                source_id = primary_loc['source'].get('id')
                                
                                if source_id in latam_source_ids:
                                    oa_info = work.get('open_access', {})
                                    count_in_file += 1
                                    
                                    row = [
                                        work['id'],
                                        oa_info.get('is_oa', False),
                                        oa_info.get('oa_status'),
                                        oa_info.get('oa_url'),
                                        oa_info.get('any_repository_has_fulltext', False)
                                    ]
                                    
                                    buffer.write('\t'.join([clean(i) for i in row]) + '\n')
                        except Exception:
                            continue
                
                if count_in_file > 0:
                    try:
                        buffer.seek(0)
                        cur.copy_from(buffer, 'works_open_access', sep='\t', null='\\N')
                        conn.commit()
                        print(f"  {file}: {count_in_file} registros")
                        total_loaded += count_in_file
                        buffer = StringIO()
                    except Exception as e:
                        conn.rollback()
                        print(f"  ✗ Error en {file}: {e}")
                        buffer = StringIO()
    
    print(f"\n✓ Cargados {total_loaded} registros de works_open_access")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    import argparse
    import psycopg2
    
    parser = argparse.ArgumentParser(description='Cargar tablas faltantes de OpenAlex a PostgreSQL')
    parser.add_argument('--only-sources', action='store_true', help='Cargar solo la tabla sources')
    parser.add_argument('--only-institutions', action='store_true', help='Cargar solo la tabla institutions')
    parser.add_argument('--only-locations', action='store_true', help='Cargar solo works_primary_location')
    parser.add_argument('--only-oa', action='store_true', help='Cargar solo works_open_access')
    
    args = parser.parse_args()
    
    # Si no se especifica ninguna bandera, cargar todo (comportamiento por defecto)
    load_all = not (args.only_sources or args.only_institutions or args.only_locations or args.only_oa)
    
    print("="*70)
    print("COMPLETANDO BASE DE DATOS OPENALEX")
    print("="*70)
    
    latam_source_ids = set()

    # Paso 1: Cargar sources (o recuperar IDs si no se carga)
    if load_all or args.only_sources:
        print(">> Cargando Sources (Revistas)...")
        latam_source_ids = load_sources()
        if not latam_source_ids and (load_all or args.only_sources):
             print("❌ Error cargando sources o conjunto vacío.")
    
    # Recuperar IDs de DB si no los cargamos pero los necesitamos para filtrar otras tablas
    if not latam_source_ids and (args.only_locations or args.only_oa):
        print("Recuperando IDs de revistas LATAM existentes en DB...")
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cur = conn.cursor()
            cur.execute("SELECT id FROM openalex.sources WHERE country_code IS NOT NULL")
            rows = cur.fetchall()
            latam_source_ids = set(row[0] for row in rows)
            conn.close()
            print(f"✓ Recuperados {len(latam_source_ids)} IDs de revistas LATAM")
        except Exception as e:
            print(f"⚠️ Error recuperando IDs: {e}")

    # Paso 2: Cargar institutions
    if load_all or args.only_institutions:
        print("\n>> Cargando Institutions...")
        load_institutions()
    
    # Paso 3: Cargar works_primary_location
    if load_all or args.only_locations:
        print("\n>> Cargando Works Primary Location...")
        if latam_source_ids:
            load_works_primary_location(latam_source_ids)
        else:
            print("⚠️ Saltando locations: Se requieren IDs de revistas LATAM.")
    
    # Paso 4: Cargar works_open_access
    if load_all or args.only_oa:
        print("\n>> Cargando Works Open Access...")
        if latam_source_ids:
            load_works_open_access(latam_source_ids)
        else:
            print("⚠️ Saltando Open Access: Se requieren IDs de revistas LATAM.")
    
    print("\n" + "="*70)
    print("PROCESO COMPLETADO")
    print("="*70)
