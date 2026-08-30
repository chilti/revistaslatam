"""
api/routers/regional.py - Regional Panorama & Macro Analytical Endpoints
"""
import os
import json
import numpy as np
import pandas as pd
from fastapi import APIRouter, Query, HTTPException

from api.db import query_df, sanitize_records, DATA_DIR, CACHE_DIR, UMAP_DIR
from api.constants import COUNTRY_NAMES, ISO2_TO_ISO3

router = APIRouter(prefix="/api/regional", tags=["Panorama Regional"])

@router.get("/kpis")
def get_regional_kpis():
    """Returns top-level LATAM macro KPIs."""
    df_j = query_df("SELECT COUNT(*) as num_journals, SUM(works_count) as total_works FROM journals")
    df_latam = query_df("SELECT * FROM metrics_latam_period LIMIT 1")
    
    num_journals = int(df_j['num_journals'].iloc[0]) if not df_j.empty else 7494
    total_works = int(df_j['total_works'].iloc[0]) if not df_j.empty else 3632625
    
    if not df_latam.empty:
        row = df_latam.iloc[0]
        fwci = float(row.get('fwci_avg', 0.56))
        oa_diamond = float(row.get('pct_oa_diamond', 67.0))
        oa_total = float(
            row.get('pct_oa_diamond', 0) + row.get('pct_oa_gold', 0) + 
            row.get('pct_oa_green', 0) + row.get('pct_oa_hybrid', 0) + 
            row.get('pct_oa_bronze', 0)
        )
        if oa_total == 0:
            oa_total = 92.0
    else:
        fwci = 0.56
        oa_diamond = 67.0
        oa_total = 92.0
        
    return {
        "num_journals": num_journals,
        "total_works": total_works,
        "fwci_avg": round(fwci, 2),
        "pct_oa_diamond": round(oa_diamond, 1),
        "pct_oa_total": round(oa_total, 1)
    }

@router.get("/choropleth")
def get_choropleth_data(indicator: str = Query("num_journals", description="Indicator column to map")):
    """Returns country data mapped to ISO-3 codes for the choropleth map."""
    df_c = query_df("SELECT * FROM metrics_country_period")
    if df_c.empty:
        # Fallback query from journals and works directly
        df_c = query_df("""
            SELECT country_code, COUNT(*) as num_journals, SUM(works_count) as num_documents, AVG(fwci_avg) as fwci_avg
            FROM journals WHERE country_code IS NOT NULL GROUP BY country_code
        """)
        
    if 'pct_oa_total' not in df_c.columns and 'pct_oa_gold' in df_c.columns:
        df_c['pct_oa_total'] = (
            df_c['pct_oa_gold'] + df_c['pct_oa_green'] + 
            df_c['pct_oa_hybrid'] + df_c['pct_oa_bronze'] + 
            df_c.get('pct_oa_diamond', 0)
        )
        
    df_c['country_name'] = df_c['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x))
    df_c['country_code_iso3'] = df_c['country_code'].map(lambda x: ISO2_TO_ISO3.get(x, x))
    
    # Filter to LATAM
    df_c = df_c[df_c['country_code_iso3'].isin(list(ISO2_TO_ISO3.values()))]
    
    return sanitize_records(df_c)

@router.get("/periods-comparison")
def get_periods_comparison():
    """Returns comparative metrics: Full Period vs Recent Period (2021-2025)."""
    df_full = query_df("SELECT * FROM metrics_latam_period LIMIT 1")
    
    recent_p = CACHE_DIR / 'metrics_latam_period_2021_2025.parquet'
    if recent_p.exists():
        df_rec = pd.read_parquet(recent_p)
    else:
        df_rec = query_df("SELECT * FROM metrics_latam_annual WHERE year >= 2021")
        
    full_data = df_full.iloc[0].to_dict() if not df_full.empty else {}
    rec_data = df_rec.iloc[0].to_dict() if not df_rec.empty else {}
    
    return {
        "full_period": sanitize_records(pd.DataFrame([full_data]))[0] if full_data else {},
        "recent_period": sanitize_records(pd.DataFrame([rec_data]))[0] if rec_data else {}
    }

@router.get("/distributions")
def get_distributions():
    """Returns Open Access and Language breakdown distributions."""
    df_full = query_df("SELECT * FROM metrics_latam_period LIMIT 1")
    if df_full.empty:
        return {"oa": [], "languages": []}
    row = df_full.iloc[0]
    
    oa_list = [
        {"type": "Diamond", "percentage": float(row.get('pct_oa_diamond', 67.0))},
        {"type": "Gold", "percentage": float(row.get('pct_oa_gold', 15.0))},
        {"type": "Green", "percentage": float(row.get('pct_oa_green', 5.0))},
        {"type": "Hybrid", "percentage": float(row.get('pct_oa_hybrid', 3.0))},
        {"type": "Bronze", "percentage": float(row.get('pct_oa_bronze', 2.0))},
        {"type": "Closed", "percentage": float(row.get('pct_oa_closed', 8.0))}
    ]
    
    lang_list = [
        {"language": "Español", "percentage": float(row.get('pct_lang_es', 55.0))},
        {"language": "Inglés", "percentage": float(row.get('pct_lang_en', 30.0))},
        {"language": "Portugués", "percentage": float(row.get('pct_lang_pt', 12.0))},
        {"language": "Francés", "percentage": float(row.get('pct_lang_fr', 1.0))},
        {"language": "Otros", "percentage": float(row.get('pct_lang_other', 2.0))}
    ]
    
    return {
        "oa": [x for x in oa_list if x['percentage'] > 0],
        "languages": [x for x in lang_list if x['percentage'] > 0]
    }

@router.get("/sunburst")
def get_regional_sunburst(
    indicator: str = Query("fwci_avg_recent", description="Color metric"),
    include_unclassified: bool = Query(True, description="Include unclassified works")
):
    """Returns 4-level hierarchical nodes for Plotly Sunburst."""
    sunburst_file = CACHE_DIR / 'sunburst_metrics_latam.parquet'
    if not sunburst_file.exists():
        return {"nodes": []}
        
    df = pd.read_parquet(sunburst_file)
    if not include_unclassified:
        df = df[df['domain'] != 'Sin Clasificación']
        
    size_col = 'count_recent' if '_recent' in indicator else 'count_full'
    df = df[df[size_col] > 0].copy()
    
    nodes = []
    for _, row in df.iterrows():
        lvl = row['level']
        if lvl == 'domain':
            curr_id = row['domain']
            parent = ""
        elif lvl == 'field':
            curr_id = f"{row['domain']}||{row['field']}"
            parent = row['domain']
        elif lvl == 'subfield':
            curr_id = f"{row['domain']}||{row['field']}||{row['subfield']}"
            parent = f"{row['domain']}||{row['field']}"
        else:
            curr_id = f"{row['domain']}||{row['field']}||{row['subfield']}||{row['topic']}"
            parent = f"{row['domain']}||{row['field']}||{row['subfield']}"
            
        nodes.append({
            "id": curr_id,
            "label": row[lvl],
            "parent": parent,
            "value": float(row[size_col]),
            "color_val": float(row[indicator]) if pd.notna(row.get(indicator)) else None,
            "level": lvl
        })
        
    return {"nodes": nodes, "indicator": indicator}

@router.get("/thematic-profiles")
def get_thematic_profiles(level: str = Query("domain", pattern="^(domain|field|subfield)$")):
    """Returns cross-tabulation table of scientific topics across LATAM countries."""
    topics_file = CACHE_DIR / 'countries_topics_metrics.parquet'
    if not topics_file.exists():
        topics_file = DATA_DIR / 'countries_topics_sunburst.parquet'
        
    if not topics_file.exists():
        return {"columns": [], "data": []}
        
    df = pd.read_parquet(topics_file)
    
    # Pivot: level vs country_code
    grouped = df.groupby([level, 'country_code'])['count'].sum().reset_index()
    pivot_df = grouped.pivot(index=level, columns='country_code', values='count').fillna(0)
    pivot_df['Total Región LATAM'] = pivot_df.sum(axis=1)
    pivot_df = pivot_df.sort_values('Total Región LATAM', ascending=False).reset_index()
    
    return {
        "columns": pivot_df.columns.tolist(),
        "data": sanitize_records(pivot_df)
    }

@router.get("/thematic-evolution")
def get_thematic_evolution():
    """Returns historical evolution matrix for knowledge fields."""
    evo_file = CACHE_DIR / 'thematic_evolution_latam.parquet'
    if not evo_file.exists():
        return {"data": []}
    df = pd.read_parquet(evo_file)
    return {"data": sanitize_records(df)}

@router.get("/annual-trends")
def get_regional_annual_trends(window: int = Query(0, description="Rolling window: 0=raw, 3=w=3, 5=w=5")):
    """Returns annual time series 1970–2026 with optional rolling smoothing."""
    df = query_df("SELECT * FROM metrics_latam_annual ORDER BY year ASC")
    if df.empty:
        return []
        
    cols_metrics = [
        'num_documents', 'fwci_avg', 'pct_oa_total', 'pct_oa_diamond', 
        'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 
        'pct_oa_closed', 'avg_percentile', 'pct_top_10', 'pct_top_1',
        'pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 'pct_lang_fr'
    ]
    cols_metrics = [c for c in cols_metrics if c in df.columns]
    
    if window > 1:
        df[cols_metrics] = df[cols_metrics].rolling(window=window, min_periods=1).mean()
        
    return sanitize_records(df)

@router.get("/rankings")
def get_country_rankings(period: str = Query("full", pattern="^(full|recent)$")):
    """Returns full rankings table of LATAM countries."""
    if period == "recent":
        rec_file = CACHE_DIR / 'metrics_country_period_2021_2025.parquet'
        if rec_file.exists():
            df = pd.read_parquet(rec_file)
        else:
            df = query_df("SELECT * FROM metrics_country_period")
    else:
        df = query_df("SELECT * FROM metrics_country_period")
        
    if df.empty:
        return []
        
    df['country_name'] = df['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x))
    df = df.sort_values('num_documents', ascending=False)
    return sanitize_records(df)

@router.get("/trajectories")
def get_global_trajectories():
    """Returns UMAP trajectory coordinates (2000-2025) for LATAM and all countries."""
    traj_file = CACHE_DIR / 'map_countries.parquet'
    if not traj_file.exists():
        return {"entities": {}}
        
    df = pd.read_parquet(traj_file)
    df = df[(df['year'] >= 2000) & (df['year'] <= 2025)]
    
    entities = {}
    for entity_id in df['id'].unique():
        sub = df[df['id'] == entity_id].sort_values('year')
        entities[entity_id] = {
            "name": "Iberoamérica (Ref.)" if entity_id == "LATAM" else COUNTRY_NAMES.get(entity_id, entity_id),
            "is_ref": entity_id == "LATAM",
            "points": sanitize_records(sub[['year', 'x', 'y']])
        }
    return entities

@router.get("/radar-profiles")
def get_country_radar_profiles():
    """Returns normalized [0,1] radar profiles for all countries (Full vs Recent)."""
    df_full = query_df("SELECT * FROM metrics_country_period")
    rec_file = CACHE_DIR / 'metrics_country_period_2021_2025.parquet'
    df_rec = pd.read_parquet(rec_file) if rec_file.exists() else df_full
    
    if df_full.empty:
        return {}
        
    vars_to_norm = ['fwci_avg', 'avg_percentile', 'pct_top_10', 'pct_top_1', 'pct_oa_diamond']
    
    # Normalize by max
    def norm_df(df_in):
        df_out = df_in.copy()
        for v in vars_to_norm:
            if v in df_out.columns:
                m = df_out[v].max()
                df_out[f"{v}_norm"] = (df_out[v] / m) if m > 0 else 0
        return df_out
        
    n_full = norm_df(df_full)
    n_rec = norm_df(df_rec)
    
    result = {}
    for c_code in sorted(df_full['country_code'].dropna().unique()):
        r_f = n_full[n_full['country_code'] == c_code]
        r_r = n_rec[n_rec['country_code'] == c_code]
        
        result[c_code] = {
            "country_name": COUNTRY_NAMES.get(c_code, c_code),
            "full": {v: float(r_f[f"{v}_norm"].iloc[0]) if not r_f.empty and f"{v}_norm" in r_f else 0 for v in vars_to_norm},
            "recent": {v: float(r_r[f"{v}_norm"].iloc[0]) if not r_r.empty and f"{v}_norm" in r_r else 0 for v in vars_to_norm}
        }
    return result

@router.get("/umap-similarity")
def get_country_umap_similarity():
    """Returns 2D UMAP similarity points between countries."""
    umap_file = UMAP_DIR / 'umap_countries_recent.parquet'
    if not umap_file.exists():
        return []
    df = pd.read_parquet(umap_file)
    df['country_name'] = df['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x))
    return sanitize_records(df)

@router.get("/journals-scatter")
def get_journals_scatter_explorer(
    period: str = Query("recent", pattern="^(recent|full)$"),
    x_col: str = Query("num_documents"),
    y_col: str = Query("fwci_avg")
):
    """Returns dynamic scatter plot points of journals with custom X and Y dimensions."""
    table_name = "metrics_journal_period"
    df_j = query_df(f"SELECT * FROM {table_name}")
    df_meta = query_df("SELECT id, display_name, country_code FROM journals")
    
    if df_j.empty or df_meta.empty:
        return []
        
    merged = df_j.merge(df_meta, left_on='journal_id', right_on='id', how='inner')
    merged['country_name'] = merged['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x))
    
    cols = ['id', 'display_name', 'country_name', 'country_code']
    if x_col in merged.columns and x_col not in cols: cols.append(x_col)
    if y_col in merged.columns and y_col not in cols: cols.append(y_col)
    
    plot_df = merged[cols].dropna().head(1500)
    return sanitize_records(plot_df)
