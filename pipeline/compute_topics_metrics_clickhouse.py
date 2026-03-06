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

def _build_topic_hierarchy_query(group_col):
    """
    Builds a query that performs hierarchical aggregation of metrics 
    at Domain, Field, and Subfield levels using ROLLUP.
    """
    # Note: raw_data in typical OpenAlex datasets in CH is often a JSON string.
    # We parse metrics from it.
    
    # Selection of grouping columns. group_col can be 'country_code', 'journal_id', or '' for LATAM.
    grouping = f"{group_col}, " if group_col else ""
    
    query = f"""
    SELECT
        {grouping}
        JSONExtractString(primary_topic, 'domain', 'display_name') as domain,
        JSONExtractString(primary_topic, 'field', 'display_name') as field,
        JSONExtractString(primary_topic, 'subfield', 'display_name') as subfield,
        
        -- Metrics
        count() as count,
        avg(toFloat32OrZero(JSONExtractString(raw_data, 'fwci'))) as fwci_avg,
        avg(toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile'))) as avg_percentile,
        
        -- High impact
        (sum(if(JSONExtractString(raw_data, 'is_in_top_10_percent') = 'true', 1, 0)) / count()) * 100 as pct_top_10,
        (sum(if(JSONExtractString(raw_data, 'is_in_top_1_percent') = 'true', 1, 0)) / count()) * 100 as pct_top_1,
        
        -- OA Status
        (sum(if(JSONExtractString(raw_data, 'oa_status') = 'gold', 1, 0)) / count()) * 100 as pct_oa_gold,
        (sum(if(JSONExtractString(raw_data, 'oa_status') = 'green', 1, 0)) / count()) * 100 as pct_oa_green,
        (sum(if(JSONExtractString(raw_data, 'oa_status') = 'hybrid', 1, 0)) / count()) * 100 as pct_oa_hybrid,
        (sum(if(JSONExtractString(raw_data, 'oa_status') = 'bronze', 1, 0)) / count()) * 100 as pct_oa_bronze,
        (sum(if(JSONExtractString(raw_data, 'oa_status') = 'closed', 1, 0)) / count()) * 100 as pct_oa_closed
        
    FROM works
    WHERE domain != ''
    GROUP BY {grouping} domain, field, subfield WITH ROLLUP
    """
    return query

def compute_and_save_topic_metrics(client, group_col, filename):
    logger.info(f"Computing topic metrics for hierarchical levels (grouped by {group_col or 'Global'})...")
    
    query = _build_topic_hierarchy_query(group_col)
    result = client.execute(query, with_column_types=True)
    
    # Convert to DataFrame
    columns = [c[0] for c in result[1]]
    df = pd.DataFrame(result[0], columns=columns)
    
    # Apply post-processing (ROLLUP leaves NULLs for aggregated levels)
    # We replace NULLs with 'ALL' for consistency with our presentation logic
    df['domain'] = df['domain'].replace('', 'ALL').fillna('ALL')
    df['field'] = df['field'].replace('', 'ALL').fillna('ALL')
    df['subfield'] = df['subfield'].replace('', 'ALL').fillna('ALL')
    
    # In ClickHouse, the top-most rollup record will have ALL in all hierarchy columns.
    # We determine the 'level' column based on where the NULLs start.
    def get_level(row):
        if row['subfield'] != 'ALL': return 'subfield'
        if row['field'] != 'ALL': return 'field'
        if row['domain'] != 'ALL': return 'domain'
        return 'total'

    df['level'] = df.apply(get_level, axis=1)
    
    # Save to parquet
    data_dir = Path(__file__).parent.parent / 'data' / 'cache'
    data_dir.mkdir(parents=True, exist_ok=True)
    output_file = data_dir / filename
    
    df.to_parquet(output_file, index=False)
    logger.info(f"✓ Saved {len(df)} records to {filename}")

if __name__ == "__main__":
    client = get_client()
    
    # 1. Level Country
    compute_and_save_topic_metrics(client, 'country_code', 'sunburst_metrics_country.parquet')
    
    # 2. Level Global
    compute_and_save_topic_metrics(client, '', 'sunburst_metrics_latam.parquet')
    
    # 3. Level Journal
    compute_and_save_topic_metrics(client, 'journal_id', 'sunburst_metrics_journal.parquet')
    
    logger.info("¡Cálculo de métricas jerárquicas temáticas completado en ClickHouse!")
