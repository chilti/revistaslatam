#!/usr/bin/env python3
"""
Topic-level metrics calculation for ClickHouse pipeline.
Uses SQL native GROUP BY WITH ROLLUP for high-performance hierarchical aggregation.
"""
import pandas as pd
import os
from pathlib import Path
from clickhouse_driver import Client
import logging

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ClickHouse connection params
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'openalex')

def get_client():
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
    )

def _build_topic_hierarchy_query(group_col, min_year=None):
    """
    Builds a query that performs hierarchical aggregation of metrics 
    at Domain, Field, and Subfield levels using ROLLUP.
    """
    grouping = f"{group_col}, " if group_col else ""
    where_clause = f"AND toInt32OrZero(JSONExtractString(raw_data, 'publication_year')) >= {min_year}" if min_year else ""
    
    query = f"""
    SELECT
        {grouping}
        if(domain = '', 'Sin Clasificación', domain) as domain,
        if(field = '', 'Sin Clasificación', field) as field,
        if(subfield = '', 'Sin Clasificación', subfield) as subfield,
        if(topic = '', 'Sin Clasificación', topic) as topic,
        
        -- Metrics
        count() as count,
        avg(fwci) as fwci_avg,
        avg(percentile) as avg_percentile,
        
        -- High impact (NATIVOS)
        (sum(if(is_top_10, 1, 0)) / count()) * 100 as pct_top_10,
        (sum(if(is_top_1, 1, 0)) / count()) * 100 as pct_top_1,
        
        -- OA Status
        (sum(if(oa_status = 'gold', 1, 0)) / count()) * 100 as pct_oa_gold,
        (sum(if(oa_status = 'green', 1, 0)) / count()) * 100 as pct_oa_green,
        (sum(if(oa_status = 'hybrid', 1, 0)) / count()) * 100 as pct_oa_hybrid,
        (sum(if(oa_status = 'bronze', 1, 0)) / count()) * 100 as pct_oa_bronze,
        (sum(if(oa_status = 'closed', 1, 0)) / count()) * 100 as pct_oa_closed
        
    FROM (
        -- DEDUPLICACIÓN DE TRABAJOS (WORKS)
        SELECT 
            id,
            argMax(JSONExtractString(raw_data, 'primary_location', 'source', 'id'), updated_date) as journal_id,
            argMax(JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name'), updated_date) as domain,
            argMax(JSONExtractString(raw_data, 'primary_topic', 'field', 'display_name'), updated_date) as field,
            argMax(JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name'), updated_date) as subfield,
            argMax(JSONExtractString(raw_data, 'primary_topic', 'display_name'), updated_date) as topic,
            argMax(toFloat32OrZero(JSONExtractString(raw_data, 'fwci')), updated_date) as fwci,
            argMax(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')), updated_date) as percentile,
            argMax(
                coalesce(
                    JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_10_percent'),
                    JSONExtractBool(raw_data, 'is_in_top_10_percent')
                ), 
                updated_date
            ) as is_top_10,
            argMax(
                coalesce(
                    JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_1_percent'),
                    JSONExtractBool(raw_data, 'is_in_top_1_percent')
                ), 
                updated_date
            ) as is_top_1,
            argMax(JSONExtractString(raw_data, 'open_access', 'oa_status'), updated_date) as oa_status,
            argMax(toUInt16(JSONExtractInt(raw_data, 'publication_year')), updated_date) as pub_year
        FROM works
        GROUP BY id
    )
    WHERE 1=1 {where_clause.replace('toInt32OrZero(JSONExtractString(raw_data, \'publication_year\'))', 'pub_year')}
    GROUP BY {grouping} domain, field, subfield, topic WITH ROLLUP
    """
    return query

def compute_and_save_topic_metrics(client, group_col, filename):
    logger.info(f"Computing dual-period topic metrics (grouped by {group_col or 'Global'})...")
    
    # 1. Period: FULL
    logger.info("  → Processing Period: FULL...")
    query_full = _build_topic_hierarchy_query(group_col, min_year=None)
    res_full = client.execute(query_full, with_column_types=True)
    cols_full = [c[0] for c in res_full[1]]
    df_full = pd.DataFrame(res_full[0], columns=cols_full)
    
    # 2. Period: RECENT (2021-2025)
    logger.info("  → Processing Period: RECENT (2021+)...")
    query_recent = _build_topic_hierarchy_query(group_col, min_year=2021)
    res_recent = client.execute(query_recent, with_column_types=True)
    cols_recent = [c[0] for c in res_recent[1]]
    df_recent = pd.DataFrame(res_recent[0], columns=cols_recent)
    
    # Pre-process both
    def clean_and_level(df):
        df['domain'] = df['domain'].replace('', 'ALL').fillna('ALL')
        df['field'] = df['field'].replace('', 'ALL').fillna('ALL')
        df['subfield'] = df['subfield'].replace('', 'ALL').fillna('ALL')
        df['topic'] = df['topic'].replace('', 'ALL').fillna('ALL')
        
        def get_level(row):
            if row['topic'] != 'ALL': return 'topic'
            if row['subfield'] != 'ALL': return 'subfield'
            if row['field'] != 'ALL': return 'field'
            if row['domain'] != 'ALL': return 'domain'
            return 'total'
        df['level'] = df.apply(get_level, axis=1)
        return df

    df_full = clean_and_level(df_full)
    df_recent = clean_and_level(df_recent)
    
    # Add suffixes
    merge_keys = ([group_col] if group_col else []) + ['domain', 'field', 'subfield', 'topic', 'level']
    
    metric_cols = [c for c in df_full.columns if c not in merge_keys]
    df_full = df_full.rename(columns={c: f"{c}_full" for c in metric_cols})
    df_recent = df_recent.rename(columns={c: f"{c}_recent" for c in metric_cols})
    
    # Merge
    df_final = pd.merge(df_full, df_recent, on=merge_keys, how='outer').fillna(0)
    
    # Save to parquet
    data_dir = Path(__file__).parent.parent / 'data' / 'cache'
    data_dir.mkdir(parents=True, exist_ok=True)
    output_file = data_dir / filename
    
    df_final.to_parquet(output_file, index=False)
    logger.info(f"✓ Saved {len(df_final)} records to {filename}")

if __name__ == "__main__":
    client = get_client()
    
    # 1. Level Country
    compute_and_save_topic_metrics(client, 'country_code', 'sunburst_metrics_country.parquet')
    
    # 2. Level Global
    compute_and_save_topic_metrics(client, '', 'sunburst_metrics_latam.parquet')
    
    # 3. Level Journal
    compute_and_save_topic_metrics(client, 'journal_id', 'sunburst_metrics_journal.parquet')
    
    logger.info("¡Cálculo de métricas jerárquicas temáticas (Periodos Duales) completado en ClickHouse!")
