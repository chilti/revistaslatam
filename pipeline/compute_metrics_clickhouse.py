"""
Procesamiento de Métricas - OLAP en ClickHouse

Este script reemplaza el flujo anterior donde Python bajaba millones de works.
Aquí, Python solo envía la Query SQL a ClickHouse, el servidor realiza todo el
GROUP BY (conteo de documentos, % Open Access, % Idiomas, FWCI promedio),
y devuelve un DataFrame pequeño (métricas consolidadas por Revista o País por año)
que se guarda en formato Parquet para uso inmediato y reactivo en el Dashboard Global.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import argparse

import sys
import os
# Añadir src para importar los módulos de regiones
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))
from regions import get_all_country_codes, get_region_for_country

try:
    import clickhouse_connect
except ImportError:
    print("pip install clickhouse-connect")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
BASE_PATH = Path(__file__).parent.parent
CACHE_DIR = BASE_PATH / 'data' / 'cache'
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ClickHouse Env
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'openalex')

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DATABASE
    )

def _build_journal_analytics_query(country_list: list):
    """
    Construye el Query analítico a nivel Revista/Año.
    Asume un esquema dinámico inferido que contiene al menos publication_year y un primary_location.
    """
    countries_str = "', '".join(country_list)
    
    # Dado que la ingesta fue dinámica, algunas claves internas de OpenAlex pueden 
    # estar anidadas. Para simplificar, asumiremos que primary_location.source.id y 
    # authorships están estructuradas de forma que pueden extraerse.
    # En un caso real con JSONEachRow inferido, los nombres de campo son precisos.
    # Si usamos Fallback (Variant B con raw_data string), usamos JSONExtract.
    
    return f"""
    SELECT 
        JSONExtractString(raw_data, 'primary_location', 'source', 'id') AS journal_id,
        toUInt16(JSONExtractInt(raw_data, 'publication_year')) AS year,
        
        -- Volumetría
        count() AS num_documents,
        
        -- FWCI Average
        avg(toFloat32OrZero(JSONExtractString(raw_data, 'fwci'))) AS fwci_avg,
        
        -- Percentile
        avg(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value'))) AS avg_percentile,
        
        -- Top 10 y Top 1
        sum(if(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')) >= 90.0, 1, 0)) AS top_10_count,
        sum(if(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')) >= 99.0, 1, 0)) AS top_1_count,
        
        -- Open Access Status
        sum(if(JSONExtractString(raw_data, 'open_access', 'is_oa') = 'true', 1, 0)) AS oa_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'gold', 1, 0)) AS oa_gold_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'green', 1, 0)) AS oa_green_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'hybrid', 1, 0)) AS oa_hybrid_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'bronze', 1, 0)) AS oa_bronze_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'closed', 1, 0)) AS oa_closed_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'diamond', 1, 0)) AS oa_diamond_count,

        -- Language Pcts
        sum(if(JSONExtractString(raw_data, 'language') = 'en', 1, 0)) AS lang_en_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'es', 1, 0)) AS lang_es_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'pt', 1, 0)) AS lang_pt_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'fr', 1, 0)) AS lang_fr_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'de', 1, 0)) AS lang_de_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'it', 1, 0)) AS lang_it_count
        
    FROM openalex_works
    
    -- Filtro de país inyectando arrayJSONExtract
    -- En un schema limpio, podríamos cruzar contra openalex_works_authorships.
    WHERE JSONExtractString(raw_data, 'primary_location', 'source', 'id') != ''
      AND year >= 2000
    GROUP BY 
        journal_id, year
    ORDER BY 
        journal_id, year
    """


def transform_counts_to_pcts(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte las columnas '_count' agregadas por SQL en '%_top' y '%_lang' base 100."""
    res = df.copy()
    
    doc_count = res['num_documents'].replace(0, 1) # Evitar div by zero
    
    # Porcentajes de Impacto (Tops)
    res['pct_top_10'] = (res['top_10_count'] / doc_count) * 100
    res['pct_top_1']  = (res['top_1_count'] / doc_count) * 100
    
    # Porcentajes de Open Access
    res['pct_oa_total']   = (res['oa_count'] / doc_count) * 100
    res['pct_oa_gold']    = (res['oa_gold_count'] / doc_count) * 100
    res['pct_oa_green']   = (res['oa_green_count'] / doc_count) * 100
    res['pct_oa_hybrid']  = (res['oa_hybrid_count'] / doc_count) * 100
    res['pct_oa_bronze']  = (res['oa_bronze_count'] / doc_count) * 100
    res['pct_oa_diamond'] = (res['oa_diamond_count'] / doc_count) * 100
    res['pct_oa_closed']  = (res['oa_closed_count'] / doc_count) * 100
    
    # Porcentajes de Lenguaje
    res['pct_lang_en'] = (res['lang_en_count'] / doc_count) * 100
    res['pct_lang_es'] = (res['lang_es_count'] / doc_count) * 100
    res['pct_lang_pt'] = (res['lang_pt_count'] / doc_count) * 100
    res['pct_lang_fr'] = (res['lang_fr_count'] / doc_count) * 100
    res['pct_lang_de'] = (res['lang_de_count'] / doc_count) * 100
    res['pct_lang_it'] = (res['lang_it_count'] / doc_count) * 100
    
    # Dropear counts intermedios
    cols_to_drop = [c for c in res.columns if c.endswith('_count') and c != 'num_documents']
    res.drop(columns=cols_to_drop, inplace=True, errors='ignore')
    
    return res

def compute_and_save_journal_metrics(client):
    logger.info("Computando agregaciones SQL a Nivel Revista (OLAP)...")
    
    # Este array puede ser muy grande si habilitamos los 80 países. ClickHouse lo maneja bien.
    # Para efectos del query SQL usamos todo el scope posible por si la query cruzaba works -> institutions.
    countries = get_all_country_codes() 
    
    query = _build_journal_analytics_query(countries)
    
    try:
        df_journal_raw = client.query_df(query)
    except Exception as e:
        logger.error(f"Falla en la consulta ClickHouse: {e}")
        return
        
    logger.info(f"ClickHouse retornó {len(df_journal_raw)} agregaciones año-journal en segundos.")
    
    # Transformar a Porcentajes localmente (Vectorizado en Pandas es igual de rápido)
    df_metrics = transform_counts_to_pcts(df_journal_raw)
    
    # Exportar el maestro anual
    output_path_annual = CACHE_DIR / 'metrics_global_journal_annual.parquet'
    df_metrics.to_parquet(output_path_annual, index=False)
    logger.info(f"✅ Guardado metrics_global_journal_annual.parquet -> {len(df_metrics)} rows")
    
    # Agregar Período Reciente (2021-2025) en Python
    logger.info("Agregando período 2021-2025 (Local)...")
    recent = df_metrics[(df_metrics['year'] >= 2021) & (df_metrics['year'] <= 2025)]
    period_recent = recent.groupby('journal_id', as_index=False).mean(numeric_only=True)
    # Num documents should be sum in period, not average
    period_recent['num_documents'] = recent.groupby('journal_id')['num_documents'].sum().values
    
    output_path_period = CACHE_DIR / 'metrics_global_journal_period_2021_2025.parquet'
    period_recent.to_parquet(output_path_period, index=False)
    logger.info(f"✅ Guardado metrics_global_journal_period_2021_2025.parquet")

def compute_and_save_country_metrics(client):
    """
    Agrupación a nivel de País usando la metadata de los journals (sources).
    Como el dashboard Global requiere comparar las 8 regiones, lo agruparemos aquí en local 
    con las tablas de los países para construir la Métrica de Región.
    """
    logger.info("Agregando métricas a nivel País y Región...")
    
    annual_j_path = CACHE_DIR / 'metrics_global_journal_annual.parquet'
    if not annual_j_path.exists():
        logger.error("No existe el archivo de revistas base.")
        return
        
    df_annual = pd.read_parquet(annual_j_path)
    
    # Necesitamos el mapping Journal -> Country
    # Cargar los metadatos de las revistas (extraídos de sources o descargados aparte)
    # Por ahora simulamos si existe sources o usamos una tabla paralela existente
    sources_path = BASE_PATH / 'data' / 'global_journals.parquet'
    if not sources_path.exists():
        # Fallback a latam para prueba si aún no está bajado el global
        sources_path = BASE_PATH / 'data' / 'latin_american_journals.parquet'
        
    df_sources = pd.read_parquet(sources_path)[['id', 'country_code']].rename(columns={'id': 'journal_id'})
    
    # Join con country code
    df_merged = df_annual.merge(df_sources, on='journal_id', how='inner')
    
    # Agregar 'region' al dataframe
    df_merged['region'] = df_merged['country_code'].apply(get_region_for_country)
    
    # Agregado Nivel País
    df_country_annual = df_merged.groupby(['country_code', 'year'], as_index=False).mean(numeric_only=True)
    df_country_annual['num_documents'] = df_merged.groupby(['country_code', 'year'])['num_documents'].sum().values
    # Cantidad de revistas que publicaron ese año
    df_country_annual['num_journals'] = df_merged.groupby(['country_code', 'year'])['journal_id'].nunique().values
    
    df_country_annual.to_parquet(CACHE_DIR / 'metrics_global_country_annual.parquet', index=False)
    
    # Agregado Nivel Región
    df_region_annual = df_merged.groupby(['region', 'year'], as_index=False).mean(numeric_only=True)
    df_region_annual['num_documents'] = df_merged.groupby(['region', 'year'])['num_documents'].sum().values
    df_region_annual['num_journals'] = df_merged.groupby(['region', 'year'])['journal_id'].nunique().values
    
    df_region_annual.to_parquet(CACHE_DIR / 'metrics_global_region_annual.parquet', index=False)
    logger.info("✅ Generados archivos de métricas Global Country y Global Region.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-journals", action="store_true", help="Salta la consulta SQL de Journals y solo regenera Pais/Region.")
    args = parser.parse_args()

    client = get_client()

    if not args.skip_journals:
        compute_and_save_journal_metrics(client)
        
    compute_and_save_country_metrics(client)
    logger.info("¡Proceso Analítico Finalizado!")
