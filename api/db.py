"""
api/db.py - DuckDB and Parquet Data Access Layer for FastAPI
"""
import os
import json
import threading
from pathlib import Path
import numpy as np
import pandas as pd
import duckdb

from api.constants import COUNTRY_NAMES, ISO2_TO_ISO3, COUNTRY_COORDS

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
CACHE_DIR = DATA_DIR / 'cache'
UMAP_DIR = DATA_DIR / 'umap'
DUCKDB_PATH = DATA_DIR / 'revistaslatam.duckdb'

_con = None
_lock = threading.Lock()

def get_con():
    global _con
    if _con is None:
        with _lock:
            if _con is None:
                if DUCKDB_PATH.exists():
                    _con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
                else:
                    _con = duckdb.connect(':memory:')
                    _con.execute("SET preserve_insertion_order=false;")
                    _con.execute("SET threads TO 4;")
                    
                    views = {
                        'journals': DATA_DIR / 'latin_american_journals.parquet',
                        'works': DATA_DIR / 'latin_american_works.parquet',
                        'metrics_latam_annual': CACHE_DIR / 'metrics_latam_annual.parquet',
                        'metrics_latam_period': CACHE_DIR / 'metrics_latam_period.parquet',
                        'metrics_country_annual': CACHE_DIR / 'metrics_country_annual.parquet',
                        'metrics_country_period': CACHE_DIR / 'metrics_country_period.parquet',
                        'metrics_journal_annual': CACHE_DIR / 'metrics_journal_annual.parquet',
                        'metrics_journal_period': CACHE_DIR / 'metrics_journal_period.parquet',
                        'thematic_evolution': CACHE_DIR / 'thematic_evolution_latam.parquet',
                        'collaboration_network': CACHE_DIR / 'collaboration_network.parquet',
                        'umap_journals': UMAP_DIR / 'umap_journals_multimodal.parquet',
                        'articles_landscape': UMAP_DIR / 'umap_articles_landscape.parquet',
                        'sunburst_latam': CACHE_DIR / 'sunburst_metrics_latam.parquet',
                        'sunburst_country': CACHE_DIR / 'sunburst_metrics_country.parquet',
                        'sunburst_journal': CACHE_DIR / 'sunburst_metrics_journal.parquet',
                        'map_countries': CACHE_DIR / 'map_countries.parquet',
                        'map_journals': CACHE_DIR / 'map_journals.parquet',
                        'umap_countries': UMAP_DIR / 'umap_countries_recent.parquet',
                        'topics_mapping': DATA_DIR / 'works_topics_mapping.parquet',
                        'countries_topics': CACHE_DIR / 'countries_topics_metrics.parquet',
                        'journals_topics': DATA_DIR / 'journals_topics_sunburst.parquet'
                    }
                    for view_name, p_path in views.items():
                        if p_path.exists():
                            p_str = str(p_path).replace('\\', '/')
                            try:
                                _con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{p_str}')")
                            except Exception as e:
                                print(f"[API DB] Could not create view {view_name}: {e}")
    return _con

def query_df(sql, params=None):
    con = get_con()
    try:
        with _lock:
            cur = con.cursor()
            if params:
                res = cur.execute(sql, params).df()
            else:
                res = cur.execute(sql).df()
            cur.close()
            return res if res is not None else pd.DataFrame()
    except Exception as e:
        print(f"[API Query Error] {sql}: {e}")
        return pd.DataFrame()

def sanitize_records(df):
    """Converts DataFrame to list of dicts with NaN/None replaced by None/0 for JSON."""
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return []
    # Replace inf and nan
    df_clean = df.replace([np.inf, -np.inf], np.nan)
    return json.loads(df_clean.to_json(orient='records', date_format='iso'))
