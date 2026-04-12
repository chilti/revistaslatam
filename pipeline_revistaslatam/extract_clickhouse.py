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
    """
    Extrae metadatos de revistas LATAM desde ClickHouse.
    OPTIMIZADO: Usa columnas materializadas.
    """
    print("🚀 Extrayendo revistas latinoamericanas (Optimizado)...")
    
    # Usamos las columnas físicas materializadas recientemente
    # Usamos las columnas físicas materializadas recientemente
    query = """
    SELECT 
        id,
        argMax(issn_l, updated_date) as issn_l,
        argMax(JSONExtractArrayRaw(raw_data, 'issn'), updated_date) as issn_array,
        argMax(display_name, updated_date) as display_name,
        argMax(JSONExtractString(raw_data, 'publisher'), updated_date) as publisher,
        argMax(works_count, updated_date) as works_count_val,
        argMax(cited_by_count, updated_date) as cited_by_count,
        argMax(JSONExtractBool(raw_data, 'is_oa'), updated_date) as is_oa,
        argMax(JSONExtractBool(raw_data, 'is_in_doaj'), updated_date) as is_in_doaj,
        argMax(JSONExtractString(raw_data, 'homepage_url'), updated_date) as homepage_url,
        argMax(JSONExtractString(raw_data, 'works_api_url'), updated_date) as works_api_url,
        max(updated_date) as updated_date,
        argMax(country_code, updated_date) as country_code_val,
        argMax(JSONExtractInt(raw_data, 'summary_stats', 'h_index'), updated_date) as h_index,
        argMax(JSONExtractInt(raw_data, 'summary_stats', 'i10_index'), updated_date) as i10_index,
        argMax(JSONExtractFloat(raw_data, 'summary_stats', '2yr_mean_citedness'), updated_date) as citedness_2yr,
        argMax(JSONExtractBool(raw_data, 'is_in_scielo'), updated_date) as is_in_scielo,
        argMax(JSONExtractBool(raw_data, 'is_ojs'), updated_date) as is_ojs,
        argMax(JSONExtractBool(raw_data, 'is_core'), updated_date) as is_core
    FROM sources
    WHERE country_code IN {countries}
      AND type = 'journal'
    GROUP BY id
    HAVING works_count > 0
    ORDER BY works_count DESC
    """.format(countries=tuple(LATAM_COUNTRIES))
    
    df = client.query_df(query)
    
    # Renombrar columnas para evitar el conflicto del parser de ClickHouse
    df = df.rename(columns={
        'works_count_val': 'works_count',
        'country_code_val': 'country_code'
    })
    
    # Procesar ISSN (ClickHouse devuelve lista o string JSON)
    def clean_issn(val):
        if not val: return ""
        if isinstance(val, list): return ",".join(val)
        try: return ",".join(json.loads(val))
        except: return str(val)

    df['issn'] = df['issn_array'].apply(clean_issn)
    df = df.drop(columns=['issn_array'])
    
    # Metadatos de descarga
    df['download_date'] = datetime.now().isoformat()
    
    print(f"✅ Encontradas {len(df)} revistas LATAM activas.")
    return df

def fetch_works_batch(client, journal_id_to_country, batch_num):
    """
    Extrae trabajos de un lote de revistas.
    OPTIMIZADO: Usa cálculo nativo de domesticidad y columnas físicas.
    """
    journal_ids = list(journal_id_to_country.keys())
    print(f"📦 Procesando lote {batch_num} ({len(journal_ids)} revistas)...")
    
    # Construir un CASE statement para la domesticidad según el país de cada revista del lote
    # Esto es mucho más rápido que hacerlo en el loop de Python
    domestic_cases = []
    for jid, country in journal_id_to_country.items():
        # Lógica: Si el jid coincide, ver si algún ROR de la institución del autor es de ese país
        # O usar institution_ids cruzados con la tabla institutions
        case_line = f"""
        WHEN source_id = '{jid}' THEN arrayExists(
            inst_id -> inst_id IN (SELECT id FROM institutions WHERE country_code = '{country}'), 
            institution_ids
        )
        """
        domestic_cases.append(case_line)
    
    query = """
    SELECT 
        id,
        argMax(doi, updated_date) as doi,
        argMax(title, updated_date) as title,
        argMax(publication_year, updated_date) as publication_year,
        argMax(JSONExtractString(raw_data, 'publication_date'), updated_date) as publication_date,
        argMax(type, updated_date) as type,
        argMax(cited_by_count, updated_date) as cited_by_count,
        argMax(JSONExtractBool(raw_data, 'is_retracted'), updated_date) as is_retracted,
        argMax(JSONExtractBool(raw_data, 'is_paratext'), updated_date) as is_paratext,
        argMax(JSONExtractString(raw_data, 'language'), updated_date) as language,
        argMax(JSONExtractFloat(raw_data, 'fwci'), updated_date) as fwci,
        argMax(JSONExtractFloat(raw_data, 'citation_normalized_percentile'), updated_date) as percentile,
        argMax(source_id, updated_date) as journal_id,
        argMax(JSONExtractString(raw_data, 'open_access', 'oa_status'), updated_date) as oa_status,
        -- Cálculo NATIVO de domesticidad
        argMax(
            CASE 
                {cases}
                ELSE False
            END,
            updated_date
        ) as is_domestic_author
    FROM works
    WHERE source_id IN {jids}
    GROUP BY id
    """.format(
        cases=" ".join(domestic_cases),
        jids=tuple(journal_ids)
    )
    
    df = client.query_df(query)
    return df

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Extractor ClickHouse Optimizado (V2 Materializado)')
    parser.add_argument('--force', action='store_true', help='Forzar descarga de todos los artículos')
    args = parser.parse_args()

    # Directorios
    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    
    client = get_ch_client()
    
    # 1. Journals (Usa columnas físicas)
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
        print("✅ Todos los artículos están descargados.")
        return

    print(f"📂 Procesando {len(journals_to_process)} revistas...")
    
    # Procesar por lotes
    batch_size = 30 # Reducido ligeramente por la complejidad del CASE statement
    journal_list = journals_to_process[['id', 'country_code']].to_dict('records')
    
    for i in range(0, len(journal_list), batch_size):
        batch_items = journal_list[i:i+batch_size]
        batch_map = {item['id']: item['country_code'] for item in batch_items}
        
        try:
            works_df = fetch_works_batch(client, batch_map, (i//batch_size)+1)
            
            if not works_df.empty:
                # Guardar individualmente por revista
                for jid, group in works_df.groupby('journal_id'):
                    jid_short = jid.split('/')[-1]
                    out_file = PARTS_DIR / f"{jid_short}.parquet"
                    
                    # Añadir flags de excelencia
                    if 'percentile' in group.columns:
                        group['is_top_10'] = group['percentile'] >= 90.0
                        group['is_top_1'] = group['percentile'] >= 99.0
                    
                    group.to_parquet(out_file, index=False)
            
            print(f"  ✓ Lote completado. Artículos procesados: {len(works_df)}")
        except Exception as e:
            print(f"❌ Error en lote {i}: {e}")

    print("\n✅ EXTRACCIÓN OPTIMIZADA FINALIZADA.")

if __name__ == "__main__":
    main()
