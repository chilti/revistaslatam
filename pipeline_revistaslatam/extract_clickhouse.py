import os
import pandas as pd
import numpy as np
import json
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno (.env)
load_dotenv()

try:
    import clickhouse_connect
except ImportError:
    print("❌ Error: 'clickhouse-connect' no está instalado. Instálalo con: pip install clickhouse-connect")
    exit(1)

# Configuración de ClickHouse
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

# Lista de países LATAM para filtrado inicial
LATAM_COUNTRIES = ["AR", "BO", "BR", "CL", "CO", "CR", "CU", "DO", "EC", "SV", "GT", "HN", "MX", "NI", "PA", "PY", "PE", "PR", "UY", "VE"]

# Directorios de datos
DATA_DIR = Path(__file__).parent.parent / 'data'
JOURNALS_FILE = DATA_DIR / 'latin_american_journals.parquet'
WORKS_FILE = DATA_DIR / 'latin_american_works.parquet'
PARTS_DIR = DATA_DIR / 'works_parts'

def get_ch_client():
    """Establece conexión con ClickHouse."""
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, 
            username=CH_USER, password=CH_PASSWORD, 
            database=CH_DATABASE
        )
        return client
    except Exception as e:
        print(f"❌ Error conectando a ClickHouse: {e}")
        raise

def fetch_journals_clickhouse(client):
    """Extrae metadatos de revistas LATAM desde ClickHouse."""
    print("🚀 Extrayendo revistas latinoamericanas desde ClickHouse...")
    
    query = """
    SELECT 
        id,
        JSONExtractString(raw_data, 'issn_l') as issn_l,
        JSONExtractArrayRaw(raw_data, 'issn') as issn_array,
        JSONExtractString(raw_data, 'display_name') as display_name,
        JSONExtractString(raw_data, 'publisher') as publisher,
        JSONExtractInt(raw_data, 'works_count') as works_count,
        JSONExtractInt(raw_data, 'cited_by_count') as cited_by_count,
        JSONExtractBool(raw_data, 'is_oa') as is_oa,
        JSONExtractBool(raw_data, 'is_in_doaj') as is_in_doaj,
        JSONExtractString(raw_data, 'homepage_url') as homepage_url,
        JSONExtractString(raw_data, 'works_api_url') as works_api_url,
        JSONExtractString(raw_data, 'updated_date') as updated_date,
        JSONExtractString(raw_data, 'country_code') as country_code,
        JSONExtractInt(raw_data, 'summary_stats', 'h_index') as h_index,
        JSONExtractInt(raw_data, 'summary_stats', 'i10_index') as i10_index,
        JSONExtractFloat(raw_data, 'summary_stats', '2yr_mean_citedness') as citedness_2yr,
        -- Campos técnicos que suelen faltar en Postgres
        JSONExtractBool(raw_data, 'is_in_scielo') as is_in_scielo,
        JSONExtractBool(raw_data, 'is_ojs') as is_ojs,
        JSONExtractBool(raw_data, 'is_core') as is_core
    FROM sources
    WHERE country_code IN {countries}
      AND JSONExtractString(raw_data, 'type') = 'journal'
    ORDER BY works_count DESC
    """.format(countries=tuple(LATAM_COUNTRIES))
    
    df = client.query_df(query)
    
    # Procesar ISSN (Asegurar que manejamos tanto listas como strings JSON)
    def clean_json_list(val):
        if not val: return ""
        if isinstance(val, list): return ",".join(val)
        try:
            return ",".join(json.loads(val))
        except:
            return str(val)

    df['issn'] = df['issn_array'].apply(clean_json_list)
    df = df.drop(columns=['issn_array'])
    
    # Metadatos de descarga
    df['download_date'] = datetime.now().isoformat()
    
    print(f"✅ Encontradas {len(df)} revistas LATAM.")
    return df

def fetch_works_batch(client, journal_ids, batch_num):
    """Extrae trabajos de un lote de revistas."""
    print(f"📦 Procesando lote {batch_num} ({len(journal_ids)} revistas)...")
    
    # Query optimizada con Autoría Doméstica nativa
    # Nota: Usamos una subquery o JOIN para obtener el país de la revista
    # pero aquí es más fácil pasar los IDs y comparar contra el país del autor
    # dado que ya conocemos el país de cada revista en el DataFrame principal.
    
    query = """
    SELECT 
        w.id as id,
        JSONExtractString(raw_data, 'doi') as doi,
        JSONExtractString(raw_data, 'title') as title,
        JSONExtractInt(raw_data, 'publication_year') as publication_year,
        JSONExtractString(raw_data, 'publication_date') as publication_date,
        JSONExtractString(raw_data, 'type') as type,
        JSONExtractInt(raw_data, 'cited_by_count') as cited_by_count,
        JSONExtractBool(raw_data, 'is_retracted') as is_retracted,
        JSONExtractBool(raw_data, 'is_paratext') as is_paratext,
        JSONExtractString(raw_data, 'language') as language,
        JSONExtractFloat(raw_data, 'fwci') as fwci,
        JSONExtractFloat(raw_data, 'citation_normalized_percentile') as percentile,
        JSONExtractString(raw_data, 'primary_location', 'source', 'id') as journal_id,
        JSONExtractString(raw_data, 'open_access', 'oa_status') as oa_status,
        -- Lista de países de los autores (para cálculo de domesticidad en Python o aquí)
        JSONExtractArrayRaw(raw_data, 'authorships') as authors_raw
    FROM works w
    WHERE journal_id IN {jids}
    """.format(jids=tuple(journal_ids))
    
    df = client.query_df(query)
    return df

def process_domestic_authorship(df_works, df_journals):
    """Calcula si un artículo tiene al menos un autor del país de la revista."""
    # Crear lookup de país por revista
    journal_countries = df_journals.set_index('id')['country_code'].to_dict()
    
    def check_domestic(row):
        jid = row['journal_id']
        target_country = journal_countries.get(jid)
        if not target_country: return False
        
        try:
            val = row['authors_raw']
            authors = val if isinstance(val, list) else json.loads(val)
            for auth in authors:
                inst_country = auth.get('author_institution', {}).get('country_code')
                if inst_country == target_country:
                    return True
        except:
            pass
        return False

    df_works['is_domestic_author'] = df_works.apply(check_domestic, axis=1)
    
    # Limpiar columnas temporales
    if 'authors_raw' in df_works.columns:
        df_works = df_works.drop(columns=['authors_raw'])
    
    return df_works

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Extractor de OpenAlex desde ClickHouse (Sustituto Postgres)')
    parser.add_argument('--force', action='store_true', help='Forzar descarga de todos los artículos')
    args = parser.parse_args()

    # Directorios
    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    
    client = get_ch_client()
    
    # 1. Journals
    journals_df = fetch_journals_clickhouse(client)
    journals_df.to_parquet(JOURNALS_FILE, index=False)
    
    # 2. Determinar qué journals descargar
    downloaded_ids = set()
    if not args.force:
        print("🔍 Comprobando journals ya descargados...")
        part_files = list(PARTS_DIR.glob('*.parquet'))
        for f in part_files:
            downloaded_ids.add(f"https://openalex.org/{f.stem}")
            
    journals_to_process = journals_df[~journals_df['id'].isin(downloaded_ids)]
    
    if len(journals_to_process) == 0:
        print("✅ Todos los artículos están descargados. Usa --force para re-descargar.")
        return

    print(f"📂 Procesando {len(journals_to_process)} revistas faltantes...")
    
    # Procesar por lotes de revistas para mayor estabilidad
    batch_size = 50
    journal_list = journals_to_process['id'].tolist()
    
    for i in range(0, len(journal_list), batch_size):
        batch_ids = journal_list[i:i+batch_size]
        try:
            works_df = fetch_works_batch(client, batch_ids, (i//batch_size)+1)
            
            if not works_df.empty:
                # Calcular domesticidad
                works_df = process_domestic_authorship(works_df, journals_df)
                
                # Guardar individualmente por revista (el Dashboard espera esto)
                for jid, group in works_df.groupby('journal_id'):
                    jid_short = jid.split('/')[-1]
                    out_file = PARTS_DIR / f"{jid_short}.parquet"
                    
                    # Añadir flags de excelencia (Top 10, Top 1)
                    if 'percentile' in group.columns:
                        group['is_top_10'] = group['percentile'] >= 90.0
                        group['is_top_1'] = group['percentile'] >= 99.0
                    
                    group.to_parquet(out_file, index=False)
            
            time.sleep(0.5) # Respirar
        except Exception as e:
            print(f"❌ Error en lote {i}: {e}")

    print("\n✅ EXTRACCIÓN DESDE CLICKHOUSE FINALIZADA.")
    print(f"Recuerda correr 'python pipeline_revistaslatam/consolidate_files.py' para unir los resultados.")

if __name__ == "__main__":
    main()
