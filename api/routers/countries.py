"""
api/routers/countries.py - Country-Level Analytical Endpoints
"""
import os
import json
import numpy as np
import pandas as pd
from fastapi import APIRouter, Query, HTTPException, Path

from api.db import query_df, sanitize_records, DATA_DIR, CACHE_DIR, UMAP_DIR
from api.constants import COUNTRY_NAMES, ISO2_TO_ISO3

router = APIRouter(prefix="/api/countries", tags=["Análisis por País"])

@router.get("")
def get_countries_list():
    """Returns the list of LATAM countries with their journal counts and metadata."""
    df_c = query_df("""
        SELECT country_code, COUNT(*) as num_journals, SUM(works_count) as total_works
        FROM journals
        WHERE country_code IS NOT NULL
        GROUP BY country_code
        ORDER BY total_works DESC
    """)
    if df_c.empty:
        # Fallback to predefined list
        return [{"country_code": k, "country_name": v} for k, v in COUNTRY_NAMES.items()]
        
    df_c['country_name'] = df_c['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x))
    df_c['country_code_iso3'] = df_c['country_code'].map(lambda x: ISO2_TO_ISO3.get(x, x))
    return sanitize_records(df_c)

@router.get("/{country_code}/summary")
def get_country_summary(country_code: str = Path(..., description="2-letter country code, e.g. MX, BR, AR")):
    """Returns comprehensive cienciometric summary for a specific country."""
    c_code = country_code.upper()
    df_period = query_df("SELECT * FROM metrics_country_period WHERE country_code = ?", [c_code])
    
    rec_file = CACHE_DIR / 'metrics_country_period_2021_2025.parquet'
    if rec_file.exists():
        df_rec_all = pd.read_parquet(rec_file)
        df_rec = df_rec_all[df_rec_all['country_code'] == c_code]
    else:
        df_rec = pd.DataFrame()
        
    df_j_count = query_df("SELECT COUNT(*) as num_journals, SUM(works_count) as total_works FROM journals WHERE country_code = ?", [c_code])
    
    full_data = df_period.iloc[0].to_dict() if not df_period.empty else {}
    rec_data = df_rec.iloc[0].to_dict() if not df_rec.empty else {}
    
    if full_data and not full_data.get('pct_doaj'):
        df_doaj = query_df("SELECT ROUND(100.0 * SUM(CASE WHEN is_in_doaj = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_doaj FROM journals WHERE country_code = ?", [c_code])
        if not df_doaj.empty and pd.notna(df_doaj['pct_doaj'].iloc[0]):
            full_data['pct_doaj'] = float(df_doaj['pct_doaj'].iloc[0])
            
    num_j = int(df_j_count['num_journals'].iloc[0]) if not df_j_count.empty and pd.notna(df_j_count['num_journals'].iloc[0]) else 0
    total_w = int(df_j_count['total_works'].iloc[0]) if not df_j_count.empty and pd.notna(df_j_count['total_works'].iloc[0]) else 0
    
    return {
        "country_code": c_code,
        "country_name": COUNTRY_NAMES.get(c_code, c_code),
        "num_journals": num_j,
        "total_works": total_w,
        "full_period": sanitize_records(pd.DataFrame([full_data]))[0] if full_data else {},
        "recent_period": sanitize_records(pd.DataFrame([rec_data]))[0] if rec_data else {}
    }

@router.get("/{country_code}/annual")
def get_country_annual_trends(
    country_code: str,
    window: int = Query(0, description="Rolling window: 0=raw, 3=w=3, 5=w=5"),
    min_year: int = Query(1970, description="Minimum year filter"),
    max_year: int = Query(2026, description="Maximum year filter")
):
    """Returns annual time series for a country."""
    c_code = country_code.upper()
    df = query_df(
        "SELECT * FROM metrics_country_annual WHERE country_code = ? AND year >= ? AND year <= ? ORDER BY year ASC",
        [c_code, min_year, max_year]
    )
    if df.empty:
        ann_file = CACHE_DIR / 'metrics_country_annual.parquet'
        if ann_file.exists():
            df = pd.read_parquet(ann_file)
            df = df[(df['country_code'] == c_code) & (df['year'] >= min_year) & (df['year'] <= max_year)].sort_values('year')
        else:
            return []
            
    df = df[(df['year'] >= min_year) & (df['year'] <= max_year)]
        
    cols_metrics = [
        'num_journals', 'num_documents', 'fwci_avg', 'pct_oa_total', 'pct_oa_diamond', 
        'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 
        'pct_oa_closed', 'avg_percentile', 'pct_top_10', 'pct_top_1',
        'pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 'pct_authors_domestic'
    ]
    cols_metrics = [c for c in cols_metrics if c in df.columns]
    
    if window > 1:
        df[cols_metrics] = df[cols_metrics].rolling(window=window, min_periods=1).mean()
        
    return sanitize_records(df)


@router.get("/{country_code}/sunburst")
def get_country_sunburst(
    country_code: str,
    indicator: str = Query("fwci_avg_recent"),
    include_unclassified: bool = Query(True)
):
    """Returns 4-level hierarchical nodes for a country."""
    c_code = country_code.upper()
    sunburst_file = CACHE_DIR / 'sunburst_metrics_country.parquet'
    if not sunburst_file.exists():
        return {"nodes": []}
        
    df = pd.read_parquet(sunburst_file)
    df = df[df['country_code'] == c_code]
    
    if df.empty:
        return {"nodes": []}
        
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

@router.get("/{country_code}/treemap")
def get_country_treemap(
    country_code: str,
    indicator: str = Query("fwci_avg_recent"),
    include_unclassified: bool = Query(False)
):
    """Returns nested Treemap format data for Domain -> Field -> Subfield for a country."""
    c_code = country_code.upper()
    sunburst_file = CACHE_DIR / 'sunburst_metrics_country.parquet'
    if not sunburst_file.exists():
        return {"nodes": []}
        
    df = pd.read_parquet(sunburst_file)
    df = df[df['country_code'] == c_code]
    
    if df.empty:
        return {"nodes": []}
        
    if not include_unclassified:
        df = df[(df['domain'] != 'Sin Clasificación') & (df['domain'] != 'Unknown')]
        
    size_col = 'count_recent' if '_recent' in indicator else 'count_full'
    df = df[df[size_col] > 0].copy()
    
    c_name = COUNTRY_NAMES.get(c_code, c_code)
    root_id = f"{c_code}_ROOT"
    
    nodes = []
    nodes.append({
        "id": root_id,
        "label": c_name,
        "parent": "",
        "value": float(df[df['level'] == 'domain'][size_col].sum()),
        "color_val": 1.0,
        "level": "root"
    })
    
    for _, row in df.iterrows():
        lvl = row['level']
        if lvl == 'domain':
            curr_id = row['domain']
            parent = root_id
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

@router.get("/{country_code}/journals")
def get_country_journals(country_code: str):
    """Returns the list of journals from a specific country."""
    c_code = country_code.upper()
    sql = """
        SELECT id, display_name, issn_l, publisher, works_count, cited_by_count, h_index, 
               fwci_avg, 2yr_mean_citedness, is_in_doaj, is_in_scielo, is_scopus, pct_oa_diamond
        FROM journals
        WHERE country_code = ?
        ORDER BY works_count DESC
    """
    df = query_df(sql, [c_code])
    return sanitize_records(df)

@router.get("/{country_code}/umap-journals")
def get_country_umap_journals(country_code: str):
    """Returns UMAP coordinates for journals of this country."""
    c_code = country_code.upper()
    umap_file = UMAP_DIR / 'umap_journals_recent.parquet'
    if not umap_file.exists():
        umap_file = UMAP_DIR / 'umap_journals_multimodal.parquet'
        
    if not umap_file.exists():
        return []
        
    df = pd.read_parquet(umap_file)
    df = df[df['country_code'] == c_code]
    return sanitize_records(df)

@router.get("/{country_code}/trajectory")
def get_country_trajectory(country_code: str):
    """Returns UMAP trajectory curves for the country vs LATAM enriched with performance metrics."""
    c_code = country_code.upper()
    traj_file = CACHE_DIR / 'trajectory_countries_coords.parquet'
    if not traj_file.exists():
        traj_file = CACHE_DIR / 'trajectory_coordinates.parquet'
    if not traj_file.exists():
        return {}
        
    df = pd.read_parquet(traj_file)
    df = df[(df['year'] >= 2000) & (df['year'] <= 2025) & (df['id'].isin([c_code, 'LATAM']))]
    
    # Enrich with metrics
    c_annual_file = CACHE_DIR / 'metrics_country_annual.parquet'
    l_annual_file = CACHE_DIR / 'metrics_latam_annual.parquet'
    
    all_metrics = []
    if c_annual_file.exists():
        c_df = pd.read_parquet(c_annual_file)
        all_metrics.append(c_df)
    if l_annual_file.exists():
        l_df = pd.read_parquet(l_annual_file)
        l_df['country_code'] = 'LATAM'
        all_metrics.append(l_df)
        
    if all_metrics:
        metrics_df = pd.concat(all_metrics, ignore_index=True)
        metric_cols = [c for c in metrics_df.columns if c not in ['id', 'name', 'type']]
        df = df.merge(metrics_df[metric_cols], left_on=['id', 'year'], right_on=['country_code', 'year'], how='left')
    
    cols_to_keep = [c for c in ['year', 'x', 'y', 'fwci_avg', 'pct_oa_diamond', 'pct_top_10', 'pct_top_1', 'pct_lang_en', 'num_documents', 'avg_percentile', 'pct_oa_gold', 'pct_authors_domestic'] if c in df.columns]
    
    result = {}
    for entity_id in df['id'].unique():
        sub = df[df['id'] == entity_id].sort_values('year')
        result[str(entity_id)] = {
            "name": "Iberoamérica (Ref.)" if entity_id == "LATAM" else COUNTRY_NAMES.get(str(entity_id), str(entity_id)),
            "is_ref": entity_id == "LATAM",
            "points": sanitize_records(sub[cols_to_keep])
        }
    return result


@router.get("/{country_code}/landscape")
def get_country_landscape_articles(country_code: str, limit: int = Query(2500)):
    """Returns sample of articles for this country with landscape coordinates and regional background sample."""
    c_code = country_code.upper()
    landscape_file = UMAP_DIR / 'umap_articles_landscape.parquet'
    if not landscape_file.exists():
        return {"country_articles": [], "bg_articles": []}
        
    df = pd.read_parquet(landscape_file)
    sub = df[df['country_code'] == c_code]
    bg = df[df['country_code'] != c_code]
    
    if len(sub) > limit:
        sub = sub.sample(limit, random_state=42)
    if len(bg) > 4000:
        bg = bg.sample(4000, random_state=42)
        
    cols = ['id', 'title', 'publication_year', 'journal_name', 'fwci', 'community_name', 'umap_x', 'umap_y']
    cols = [c for c in cols if c in df.columns]
    
    return {
        "country_articles": sanitize_records(sub[cols]),
        "bg_articles": sanitize_records(bg[['umap_x', 'umap_y']])
    }

@router.get("/{country_code}/journals-scatter")
def get_country_journals_scatter(
    country_code: str,
    period: str = Query("recent", pattern="^(recent|full)$")
):
    """Returns journal-level metrics for dynamic scatter plot exploration."""
    c_code = country_code.upper()
    file_name = 'metrics_journal_period_2021_2025.parquet' if period == 'recent' else 'metrics_journal_period.parquet'
    period_file = CACHE_DIR / file_name
    
    if not period_file.exists():
        return []
        
    df_metrics = pd.read_parquet(period_file)
    df_journals = query_df("SELECT id, display_name, country_code FROM journals WHERE country_code = ?", [c_code])
    
    if df_journals.empty:
        return []
        
    # Standardize IDs for merge
    df_journals['clean_id'] = df_journals['id'].astype(str).str.strip().str.rstrip('/').str.split('/').str[-1]
    df_metrics['clean_id'] = df_metrics['journal_id'].astype(str).str.strip().str.rstrip('/').str.split('/').str[-1]
    
    df_merged = df_journals.merge(df_metrics, on='clean_id', how='inner')
    
    # Calculate pct_oa_total if missing
    if 'pct_oa_total' not in df_merged.columns:
        df_merged['pct_oa_total'] = (
            df_merged['pct_oa_diamond'].fillna(0) + 
            df_merged['pct_oa_gold'].fillna(0) + 
            df_merged['pct_oa_green'].fillna(0) + 
            df_merged['pct_oa_hybrid'].fillna(0) + 
            df_merged['pct_oa_bronze'].fillna(0)
        )
        
    cols = [
        'id_x', 'display_name', 'num_documents', 'fwci_avg', 'pct_top_10', 'pct_top_1',
        'avg_percentile', 'pct_oa_total', 'pct_oa_diamond', 'pct_oa_gold', 'pct_oa_green',
        'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed', 'pct_authors_domestic',
        'pct_lang_es', 'pct_lang_en', 'pct_lang_pt'
    ]
    cols = [c for c in cols if c in df_merged.columns]
    res_df = df_merged[cols].rename(columns={'id_x': 'id'})
    return sanitize_records(res_df)

@router.get("/specialization-matrix")
def get_specialization_matrix(level: str = Query("domain", pattern="^(domain|field)$")):
    """Returns the Revealed Comparative Advantage (RCA) matrix (20 countries x disciplines)."""
    sunburst_file = CACHE_DIR / 'sunburst_metrics_country.parquet'
    if not sunburst_file.exists():
        return {"countries": [], "disciplines": [], "matrix": []}
        
    df = pd.read_parquet(sunburst_file)
    df = df[df['level'] == level]
    df = df[df[level].notna() & (df[level] != 'Sin Clasificación') & (df[level] != 'Unknown')]
    
    col_name = level
    grouped = df.groupby(['country_code', col_name])['count_full'].sum().reset_index()
    pivot = grouped.pivot(index='country_code', columns=col_name, values='count_full').fillna(0)
    
    # Calculate RCA: RCA_ij = (C_ij / sum_j C_ij) / (sum_i C_ij / sum_ij C_ij)
    row_sums = pivot.sum(axis=1)
    col_sums = pivot.sum(axis=0)
    total_sum = pivot.values.sum()
    
    # Avoid zero division
    row_sums = row_sums.replace(0, 1)
    col_sums = col_sums.replace(0, 1)
    
    rca_df = (pivot.div(row_sums, axis=0)).div(col_sums / total_sum, axis=1)
    
    countries_list = rca_df.index.tolist()
    disciplines_list = rca_df.columns.tolist()
    matrix_values = [[round(float(val), 2) for val in row] for row in rca_df.values]
    
    return {
        "countries": [{"code": c, "name": COUNTRY_NAMES.get(c, c)} for c in countries_list],
        "disciplines": disciplines_list,
        "matrix": matrix_values
    }

@router.get("/{country_code}/journals-distribution")
def get_country_journals_distribution(country_code: str):
    """Returns all journals for a country with metrics for Beeswarm/Strip plot."""
    c_code = country_code.upper()
    df = query_df("""
        SELECT id, display_name, issn_l, publisher, works_count, cited_by_count, fwci_avg, 
               h_index, pct_oa_diamond, pct_top_10, community_name, is_in_doaj, is_scopus
        FROM journals 
        WHERE country_code = ? 
        ORDER BY works_count DESC
    """, [c_code])
    return sanitize_records(df)

@router.get("/{country_code}/slope-data")
def get_country_slope_data(country_code: str):
    """Returns ranking changes across indicators between Full and Recent periods."""
    c_code = country_code.upper()
    df_full = query_df("SELECT country_code, fwci_avg, pct_oa_diamond, pct_top_10, num_documents FROM metrics_country_period")
    rec_file = CACHE_DIR / 'metrics_country_period_2021_2025.parquet'
    df_rec = pd.read_parquet(rec_file) if rec_file.exists() else df_full.copy()
    
    indicators = ['fwci_avg', 'pct_oa_diamond', 'pct_top_10', 'num_documents']
    results = []
    
    for ind in indicators:
        df_full_sorted = df_full.sort_values(ind, ascending=False).reset_index(drop=True)
        df_full_sorted['rank_full'] = df_full_sorted.index + 1
        
        df_rec_sorted = df_rec.sort_values(ind, ascending=False).reset_index(drop=True)
        df_rec_sorted['rank_recent'] = df_rec_sorted.index + 1
        
        r_f = df_full_sorted[df_full_sorted['country_code'] == c_code]
        r_r = df_rec_sorted[df_rec_sorted['country_code'] == c_code]
        
        rank_f = int(r_f['rank_full'].iloc[0]) if not r_f.empty else None
        rank_r = int(r_r['rank_recent'].iloc[0]) if not r_r.empty else None
        val_f = float(r_f[ind].iloc[0]) if not r_f.empty else None
        val_r = float(r_r[ind].iloc[0]) if not r_r.empty else None
        
        results.append({
            "indicator": ind,
            "rank_full": rank_f,
            "rank_recent": rank_r,
            "val_full": val_f,
            "val_recent": val_r,
            "rank_change": (rank_f - rank_r) if rank_f and rank_r else 0 # Positive means climbed rank
        })
        
    return results

@router.get("/{country_code}/thematic-evolution")
def get_country_thematic_evolution(
    country_code: str,
    level: str = Query("domain", pattern="^(domain|field|subfield|topic)$")
):
    """Returns aggregated yearly evolution matrix for knowledge fields of journals in a country."""
    c_code = country_code.upper()
    df_agg = query_df(f"""
        SELECT CAST(e.year AS INT) AS year, e.{level} AS name, SUM(e.num_documents) AS num_documents
        FROM thematic_evolution e
        JOIN journals j ON REGEXP_EXTRACT(e.journal_id, 'S[0-9]+') = REGEXP_EXTRACT(j.id, 'S[0-9]+')
        WHERE j.country_code = ? AND e.year >= 1985 AND e.{level} IS NOT NULL AND e.{level} != '' AND e.{level} != 'Sin Clasificación' AND e.{level} != 'Unknown'
        GROUP BY e.year, e.{level}
        ORDER BY year ASC, num_documents DESC
    """, [c_code])
    
    return sanitize_records(df_agg)

@router.get("/{country_code}/thematic-profiles")
def get_country_thematic_profiles(
    country_code: str,
    level: str = Query("domain", pattern="^(domain|field|subfield)$")
):
    """Returns journal-level thematic breakdown table (cross-tabulation of journals x thematic categories)."""
    c_code = country_code.upper()
    df = query_df(f"""
        SELECT j.display_name AS journal_name, e.{level} AS category, SUM(e.num_documents) AS count
        FROM thematic_evolution e
        JOIN journals j ON REGEXP_EXTRACT(e.journal_id, 'S[0-9]+') = REGEXP_EXTRACT(j.id, 'S[0-9]+')
        WHERE j.country_code = ? AND e.{level} IS NOT NULL AND e.{level} != '' AND e.{level} != 'Sin Clasificación' AND e.{level} != 'Unknown'
        GROUP BY j.display_name, e.{level}
    """, [c_code])
    
    if df.empty:
        return {"columns": ["Revista", "Total"], "data": []}
        
    pivot = df.pivot(index='journal_name', columns='category', values='count').fillna(0)
    pivot['Total'] = pivot.sum(axis=1)
    pivot = pivot.sort_values('Total', ascending=False)
    
    columns = ["Revista", "Total"] + [c for c in pivot.columns if c != 'Total']
    pivot_reset = pivot.reset_index().rename(columns={'journal_name': 'Revista'})
    
    for col in columns:
        if col != 'Revista':
            pivot_reset[col] = pivot_reset[col].astype(int)
            
    return {
        "columns": columns,
        "data": sanitize_records(pivot_reset[columns])
    }

