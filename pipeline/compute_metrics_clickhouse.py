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
        
        -- Volumetría e Impacto Básico
        count() AS num_documents,
        sum(toUInt32OrZero(JSONExtractString(raw_data, 'cited_by_count'))) AS total_citations,
        avg(toFloat32OrZero(JSONExtractString(raw_data, 'fwci'))) AS fwci_avg,
        
        -- Impacto 2yr (Aproximación por CITAS en trabajos de ese año)
        -- Nota: En OpenAlex el '2yr_mean_citedness' suele venir en el objeto Source, 
        -- pero aquí lo calculamos como citas_acumuladas/docs_acumulados si fuera necesario.
        -- Por ahora extraemos el valor reportado si existe en el works (Metadata enriquecida)
        avg(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value'))) AS avg_percentile,
        
        -- Top 10 y Top 1 (OpenAlex usa escala 0-1 para percentiles)
        sum(if(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')) >= 0.90, 1, 0)) AS top_10_count,
        sum(if(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')) >= 0.99, 1, 0)) AS top_1_count,
        
        -- Open Access Detallado
        sum(if(JSONExtractString(raw_data, 'open_access', 'is_oa') = 'true', 1, 0)) AS oa_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'gold', 1, 0)) AS oa_gold_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'green', 1, 0)) AS oa_green_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'hybrid', 1, 0)) AS oa_hybrid_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'bronze', 1, 0)) AS oa_bronze_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'closed', 1, 0)) AS oa_closed_count,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'diamond', 1, 0)) AS oa_diamond_count,

        -- Language
        sum(if(JSONExtractString(raw_data, 'language') = 'en', 1, 0)) AS lang_en_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'es', 1, 0)) AS lang_es_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'pt', 1, 0)) AS lang_pt_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'fr', 1, 0)) AS lang_fr_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'de', 1, 0)) AS lang_de_count,
        sum(if(JSONExtractString(raw_data, 'language') = 'it', 1, 0)) AS lang_it_count
        
    FROM openalex_works
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
    # Multiplicamos por 100 porque top_x_count cuenta documentos que CUMPLEN el umbral
    res['pct_top_10'] = (res['top_10_count'] / doc_count) * 100
    res['pct_top_1']  = (res['top_1_count'] / doc_count) * 100
    
    # Normalizar avg_percentile a base 100 (OpenAlex da 0.99 -> 99.0)
    res['avg_percentile'] = res['avg_percentile'] * 100
    
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
    # 1. Cargar metadatos exportados de ClickHouse (incluye revistas globales añadidas)
    ch_metadata_path = CACHE_DIR / 'global_journals_metadata.parquet'
    df_ch_meta = pd.DataFrame()
    if ch_metadata_path.exists():
        df_ch_meta = pd.read_parquet(ch_metadata_path)[['id', 'country_code']]
    
    # 2. Cargar metadatos locales de LATAM (histórico)
    latam_metadata_path = BASE_PATH / 'data' / 'latin_american_journals.parquet'
    df_latam_meta = pd.DataFrame()
    if latam_metadata_path.exists():
        df_latam_meta = pd.read_parquet(latam_metadata_path)[['id', 'country_code']]
    
    # 3. Combinar y remover duplicados (priorizando ClickHouse)
    df_sources = pd.concat([df_ch_meta, df_latam_meta]).drop_duplicates(subset=['id'], keep='first')
    df_sources = df_sources.rename(columns={'id': 'journal_id'})
    
    logger.info(f"Metadatos consolidados: {len(df_sources)} revistas identificadas.")
    
    # Join con country code
    df_merged = df_annual.merge(df_sources, on='journal_id', how='inner')
    logger.info(f"Registros después del join con metadatos: {len(df_merged)} (Métricas con país asignado)")
    
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


def compute_and_save_sunburst_metrics(client):
    logger.info("Computando agregaciones Temáticas (Sunburst) OLAP en ClickHouse...")

    query = """
    SELECT 
        JSONExtractString(raw_data, 'primary_location', 'source', 'id') AS journal_id,
        JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name') AS domain,
        JSONExtractString(raw_data, 'primary_topic', 'field', 'display_name') AS field,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name') AS subfield,
        JSONExtractString(raw_data, 'primary_topic', 'display_name') AS topic,
        
        count() AS count_full,
        avg(toFloat32OrZero(JSONExtractString(raw_data, 'fwci'))) AS fwci_avg_full,
        avg(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value'))) AS avg_percentile_full,
        
        sum(if(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')) >= 0.90, 1, 0)) AS top_10_count_full,
        sum(if(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')) >= 0.99, 1, 0)) AS top_1_count_full,
        sum(if(JSONExtractString(raw_data, 'open_access', 'oa_status') = 'gold', 1, 0)) AS oa_gold_count_full,
        
        sum(if(toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) >= 2021, 1, 0)) AS count_recent,
        
        avgIf(toFloat32OrZero(JSONExtractString(raw_data, 'fwci')), toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) >= 2021) AS fwci_avg_recent,
        avgIf(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')), toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) >= 2021) AS avg_percentile_recent,
        
        sum(if(toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) >= 2021 AND toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')) >= 0.90, 1, 0)) AS top_10_count_recent,
        sum(if(toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) >= 2021 AND toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')) >= 0.99, 1, 0)) AS top_1_count_recent,
        sum(if(toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) >= 2021 AND JSONExtractString(raw_data, 'open_access', 'oa_status') = 'gold', 1, 0)) AS oa_gold_count_recent
        
    FROM openalex_works
    WHERE JSONExtractString(raw_data, 'primary_location', 'source', 'id') != ''
      AND JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name') != ''
      AND toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) >= 2000
    GROUP BY 
        journal_id, domain, field, subfield, topic
    """
    
    try:
        df_raw = client.query_df(query)
        logger.info(f"ClickHouse retornó {len(df_raw)} combinaciones de tópicos fuente.")
    except Exception as e:
        logger.error(f"Error en consulta de sunburst temático: {e}")
        return
        
    def calc_percentages_and_weighted_avgs(df):
        tot_full = df['count_full'].sum()
        fwci_f = (df['fwci_avg_full'].fillna(0) * df['count_full']).sum() / tot_full if tot_full > 0 else 0.0
        perc_f = (df['avg_percentile_full'].fillna(0) * df['count_full']).sum() / tot_full if tot_full > 0 else 0.0
        
        tot_rec = df['count_recent'].sum()
        fwci_r = (df['fwci_avg_recent'].fillna(0) * df['count_recent']).sum() / tot_rec if tot_rec > 0 else 0.0
        perc_r = (df['avg_percentile_recent'].fillna(0) * df['count_recent']).sum() / tot_rec if tot_rec > 0 else 0.0

        return pd.Series({
            'count_full': tot_full,
            'fwci_avg_full': round(fwci_f, 3),
            'avg_percentile_full': round(perc_f * 100, 1), # Normalizar a base 100
            'pct_top_10_full': round((df['top_10_count_full'].sum() / tot_full)*100, 2) if tot_full > 0 else 0.0,
            'pct_top_1_full': round((df['top_1_count_full'].sum() / tot_full)*100, 2) if tot_full > 0 else 0.0,
            'pct_oa_gold_full': round((df['oa_gold_count_full'].sum() / tot_full)*100, 2) if tot_full > 0 else 0.0,
            
            'count_recent': tot_rec,
            'fwci_avg_recent': round(fwci_r, 3),
            'avg_percentile_recent': round(perc_r * 100, 1), # Normalizar a base 100
            'pct_top_10_recent': round((df['top_10_count_recent'].sum() / tot_rec)*100, 2) if tot_rec > 0 else 0.0,
            'pct_top_1_recent': round((df['top_1_count_recent'].sum() / tot_rec)*100, 2) if tot_rec > 0 else 0.0,
            'pct_oa_gold_recent': round((df['oa_gold_count_recent'].sum() / tot_rec)*100, 2) if tot_rec > 0 else 0.0
        })

    def safe_apply(grouped):
        try:
            return grouped.apply(calc_percentages_and_weighted_avgs, include_groups=False).reset_index()
        except TypeError:
            return grouped.apply(calc_percentages_and_weighted_avgs).reset_index()

    levels = ['domain', 'field', 'subfield', 'topic']
    
    # Nivel Topic
    df_topic = safe_apply(df_raw.groupby(['journal_id'] + levels))
    df_topic['level'] = 'topic'
    
    # Nivel Subfield
    df_sub = safe_apply(df_raw.groupby(['journal_id', 'domain', 'field', 'subfield']))
    df_sub['topic'] = 'ALL'
    df_sub['level'] = 'subfield'
    
    # Nivel Field
    df_field = safe_apply(df_raw.groupby(['journal_id', 'domain', 'field']))
    df_field['subfield'] = 'ALL'
    df_field['topic'] = 'ALL'
    df_field['level'] = 'field'
    
    # Nivel Domain
    df_domain = safe_apply(df_raw.groupby(['journal_id', 'domain']))
    df_domain['field'] = 'ALL'
    df_domain['subfield'] = 'ALL'
    df_domain['topic'] = 'ALL'
    df_domain['level'] = 'domain'
    
    df_sunburst_journal = pd.concat([df_topic, df_sub, df_field, df_domain], ignore_index=True)
    out_path = CACHE_DIR / 'sunburst_metrics_journal.parquet'
    df_sunburst_journal.to_parquet(out_path, index=False)
    logger.info(f"✅ Guardado sunburst_metrics_journal.parquet -> {len(df_sunburst_journal)} nodes")
    
    # ---------------------------------------------
    # EXPANSION A NIVEL PAIS Y REGION
    # ---------------------------------------------
    # Cargar metadatos para mapear journal_id -> country_code -> region
    try:
        sources_path = CACHE_DIR / 'global_journals_metadata.parquet'
        if not sources_path.exists():
            sources_path = BASE_PATH / 'data' / 'latin_american_journals.parquet'
            df_sources = pd.read_parquet(sources_path)[['id', 'country_code']].rename(columns={'id': 'journal_id'})
        else:
            df_sources = pd.read_parquet(sources_path)[['id', 'country_code']].rename(columns={'id': 'journal_id'})
            
        df_raw_geo = df_raw.merge(df_sources, on='journal_id', how='inner')
        df_raw_geo['region'] = df_raw_geo['country_code'].apply(get_region_for_country)
        
        # COUNTRY SUNBURST
        def process_geo_sunburst(df_base, group_key, prefix_name):
            topic = safe_apply(df_base.groupby([group_key] + levels))
            topic['level'] = 'topic'

            sub = safe_apply(df_base.groupby([group_key, 'domain', 'field', 'subfield']))
            sub['topic'] = 'ALL'
            sub['level'] = 'subfield'
            
            field = safe_apply(df_base.groupby([group_key, 'domain', 'field']))
            field['subfield'] = 'ALL'
            field['topic'] = 'ALL'
            field['level'] = 'field'
            
            domain = safe_apply(df_base.groupby([group_key, 'domain']))
            domain['field'] = 'ALL'
            domain['subfield'] = 'ALL'
            domain['topic'] = 'ALL'
            domain['level'] = 'domain'
            
            df_geo = pd.concat([topic, sub, field, domain], ignore_index=True)
            out_geo = CACHE_DIR / f'sunburst_metrics_{prefix_name}.parquet'
            df_geo.to_parquet(out_geo, index=False)
            logger.info(f"✅ Guardado sunburst_metrics_{prefix_name}.parquet -> {len(df_geo)} nodes")

        process_geo_sunburst(df_raw_geo, 'country_code', 'country')
        process_geo_sunburst(df_raw_geo, 'region', 'region')
        
    except Exception as e:
        logger.error(f"Advertencia: No se pudo generar sunburst_metrics para pais/region: {e}")



def export_journal_metadata(client):
    """Extrae nombres e ISSNs de las revistas para el buscador global y sus indicadores de indexación."""
    logger.info("Exportando metadatos robustos de revistas desde ClickHouse...")
    
    query = """
    SELECT 
        JSONExtractString(raw_data, 'id') AS id,
        JSONExtractString(raw_data, 'display_name') AS display_name,
        JSONExtractString(raw_data, 'issn_l') AS issn_l,
        JSONExtractString(raw_data, 'country_code') AS country_code,
        JSONExtractString(raw_data, 'type') AS type,
        JSONExtractBool(raw_data, 'is_oa') AS is_oa,
        JSONExtractBool(raw_data, 'is_in_doaj') AS is_in_doaj,
        
        -- H-Index e i10-Index (Extracción directa como enteros)
        JSONExtractInt(raw_data, 'summary_stats', 'h_index') AS h_index,
        JSONExtractInt(raw_data, 'summary_stats', 'i10_index') AS i10_index,
        
        -- Mean Citedness (Citas promedio 2yr)
        toFloat32OrZero(JSONExtractString(raw_data, 'summary_stats', '2yr_mean_citedness')) AS citedness_2yr,
        
        -- Indexación (Buscando en ids)
        if(JSONExtractString(raw_data, 'ids', 'scopus') != '', 1, 0) AS in_scopus,
        if(JSONExtractString(raw_data, 'ids', 'mag') != '', 1, 0) AS in_mag,
        
        -- SciELO y Redalyc (Aproximación por editorial o etiquetas en display_name si no hay campo directo)
        if(position(JSONExtractString(raw_data, 'display_name'), 'SciELO') > 0 OR position(JSONExtractString(raw_data, 'host_organization_name'), 'SciELO') > 0, 1, 0) AS in_scielo,
        if(position(JSONExtractString(raw_data, 'display_name'), 'CORE') > 0 OR JSONExtractString(raw_data, 'ids', 'fatcat') != '', 1, 0) AS in_core
        
    FROM openalex_sources
    """
    
    try:
        df_sources = client.query_df(query)
        output_path = CACHE_DIR / 'global_journals_metadata.parquet'
        df_sources.to_parquet(output_path, index=False)
        logger.info(f"✅ Guardado global_journals_metadata.parquet -> {len(df_sources)} journals")
    except Exception as e:
        logger.error(f"Error exportando metadata de revistas: {e}")

def compute_thematic_evolution(client):
    logger.info("Computando Evolución Temática Histórica (2000-2025) en ClickHouse...")

    query = """
    SELECT 
        JSONExtractString(raw_data, 'primary_location', 'source', 'id') AS journal_id,
        toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) AS year,
        JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name') AS domain,
        JSONExtractString(raw_data, 'primary_topic', 'field', 'display_name') AS field,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name') AS subfield,
        JSONExtractString(raw_data, 'primary_topic', 'display_name') AS topic,
        count() AS num_documents
    FROM openalex_works
    WHERE JSONExtractString(raw_data, 'primary_location', 'source', 'id') != ''
      AND JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name') != ''
      AND toUInt16OrZero(JSONExtractString(raw_data, 'publication_year')) BETWEEN 2000 AND 2025
    GROUP BY 
        journal_id, year, domain, field, subfield, topic
    """
    
    try:
        df_evo = client.query_df(query)
        output_path = CACHE_DIR / 'thematic_evolution_base.parquet'
        df_evo.to_parquet(output_path, index=False)
        logger.info(f"✅ Guardada evolución temática base -> {len(df_evo)} filas")
    except Exception as e:
        logger.error(f"Error computando evolución temática: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-journals", action="store_true", help="Salta la consulta SQL de Journals y solo regenera Pais/Region.")
    args = parser.parse_args()

    client = get_client()

    if not args.skip_journals:
        compute_and_save_journal_metrics(client)
        export_journal_metadata(client)
        compute_and_save_sunburst_metrics(client)
        compute_thematic_evolution(client) # Nueva llamada
        
    compute_and_save_country_metrics(client)
    logger.info("¡Proceso Analítico Finalizado!")
