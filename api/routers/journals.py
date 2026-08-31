"""
api/routers/journals.py - Journal-Level Analytical Endpoints
"""
import os
import json
import numpy as np
import pandas as pd
from fastapi import APIRouter, Query, HTTPException, Path

from api.db import query_df, sanitize_records, DATA_DIR, CACHE_DIR, UMAP_DIR
from api.constants import COUNTRY_NAMES

router = APIRouter(prefix="/api/journals", tags=["Detalle de Revista"])

import unicodedata

def strip_accents(text: str) -> str:
    if not text: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

@router.get("/search")
def search_journals(
    q: str = Query("", description="Query string for journal name or ISSN"),
    country: str = Query("", description="Optional country code filter"),
    limit: int = Query(50, description="Max results")
):
    """Instant search and autocomplete for journals."""
    conditions = []
    params = []
    
    if country:
        conditions.append("country_code = ?")
        params.append(country.upper())
        
    if q.strip():
        tokens = [t.strip() for t in q.strip().split() if t.strip()]
        for t in tokens:
            conditions.append("(LOWER(display_name) LIKE ? OR LOWER(issn_l) LIKE ?)")
            params.extend([f"%{t.lower()}%", f"%{t.lower()}%"])
        
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT id, display_name, issn_l, country_code, publisher, works_count, cited_by_count, fwci_avg, community_name
        FROM journals
        {where_clause}
        ORDER BY works_count DESC
        LIMIT ?
    """
    params.append(limit)
    df = query_df(sql, params)
    return sanitize_records(df)

@router.get("/{journal_id:path}/details")
def get_journal_details(journal_id: str):
    """Returns technical profile and multi-tier indicators for a journal."""
    # Standardize OpenAlex ID
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    
    df = query_df("SELECT * FROM journals WHERE id = ?", [jid])
    if df.empty:
        # Try finding by partial ID
        df = query_df("SELECT * FROM journals WHERE id LIKE ?", [f"%{journal_id}%"])
        
    if df.empty:
        raise HTTPException(status_code=404, detail="Revista no encontrada")
        
    journal_data = df.iloc[0].to_dict()
    country_code = journal_data.get('country_code', '')
    journal_data['country_name'] = COUNTRY_NAMES.get(country_code, country_code)
    
    # Check period metrics
    df_p = query_df("SELECT * FROM metrics_journal_period WHERE journal_id = ?", [jid])
    period_data = df_p.iloc[0].to_dict() if not df_p.empty else {}
    
    rec_file = CACHE_DIR / 'metrics_journal_period_2021_2025.parquet'
    if rec_file.exists():
        df_rec_all = pd.read_parquet(rec_file)
        df_rec = df_rec_all[df_rec_all['journal_id'] == jid]
        recent_data = df_rec.iloc[0].to_dict() if not df_rec.empty else {}
    else:
        recent_data = {}
        
    return {
        "profile": sanitize_records(pd.DataFrame([journal_data]))[0],
        "full_period": sanitize_records(pd.DataFrame([period_data]))[0] if period_data else {},
        "recent_period": sanitize_records(pd.DataFrame([recent_data]))[0] if recent_data else {}
    }

@router.get("/{journal_id:path}/annual")
def get_journal_annual(
    journal_id: str,
    min_year: int = Query(1970, description="Minimum year filter"),
    max_year: int = Query(2026, description="Maximum year filter")
):
    """Returns longitudinal time series of a journal."""
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    df = query_df(
        "SELECT * FROM metrics_journal_annual WHERE journal_id = ? AND year >= ? AND year <= ? ORDER BY year ASC",
        [jid, min_year, max_year]
    )
    if df.empty:
        ann_file = CACHE_DIR / 'metrics_journal_annual.parquet'
        if ann_file.exists():
            df = pd.read_parquet(ann_file)
            df = df[(df['journal_id'] == jid) & (df['year'] >= min_year) & (df['year'] <= max_year)].sort_values('year')
        else:
            return []
            
    df = df[(df['year'] >= min_year) & (df['year'] <= max_year)]
    return sanitize_records(df)


@router.get("/{journal_id:path}/sunburst")
def get_journal_sunburst(
    journal_id: str,
    indicator: str = Query("fwci_avg_recent"),
    include_unclassified: bool = Query(True)
):
    """Returns 4-level sunburst nodes for a journal."""
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    sunburst_file = CACHE_DIR / 'sunburst_metrics_journal.parquet'
    if not sunburst_file.exists():
        return {"nodes": []}
        
    try:
        df = pd.read_parquet(sunburst_file, filters=[('journal_id', '==', jid)])
    except Exception:
        df = pd.DataFrame()
        
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

@router.get("/{journal_id:path}/articles")
def get_journal_articles(
    journal_id: str,
    year: int = Query(None, description="Optional year filter"),
    sort_by: str = Query("cited_by_count", pattern="^(cited_by_count|publication_year|fwci)$"),
    limit: int = Query(100, description="Page limit")
):
    """Ultra-fast paginated articles retrieval directly from DuckDB."""
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    
    order_clause = {
        'cited_by_count': 'cited_by_count DESC',
        'publication_year': 'publication_year DESC',
        'fwci': 'fwci DESC'
    }.get(sort_by, 'cited_by_count DESC')
    
    params = [jid]
    year_filter = ""
    if year:
        year_filter = "AND publication_year = ?"
        params.append(year)
        
    params.append(limit)
    
    sql = f"""
        SELECT id, title, publication_year, cited_by_count, fwci, doi
        FROM works
        WHERE journal_id = ? {year_filter}
        ORDER BY {order_clause}
        LIMIT ?
    """
    df = query_df(sql, params)
    return sanitize_records(df)

@router.get("/{journal_id:path}/landscape")
def get_journal_landscape(journal_id: str):
    """Returns articles of this journal in the landscape + spatial dispersion metric."""
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    landscape_file = UMAP_DIR / 'umap_articles_landscape.parquet'
    if not landscape_file.exists():
        return {"articles": [], "dispersion": 0.0}
        
    df = pd.read_parquet(landscape_file)
    sub = df[df['journal_id'] == jid]
    
    if sub.empty:
        return {"articles": [], "dispersion": 0.0}
        
    std_x = float(sub['umap_x'].std()) if len(sub) > 1 else 0.0
    std_y = float(sub['umap_y'].std()) if len(sub) > 1 else 0.0
    dispersion = float(np.sqrt(std_x**2 + std_y**2)) if not np.isnan(std_x) else 0.0
    
    return {
        "articles": sanitize_records(sub),
        "dispersion": round(dispersion, 3),
        "count": len(sub)
    }

@router.get("/{journal_id:path}/trajectory")
def get_journal_trajectory(journal_id: str):
    """Returns UMAP trajectory for the journal vs its country."""
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    
    # Get country code
    df_j = query_df("SELECT country_code FROM journals WHERE id = ?", [jid])
    c_code = df_j['country_code'].iloc[0] if not df_j.empty else ''
    
    traj_file = CACHE_DIR / 'trajectory_coordinates.parquet'
    if not traj_file.exists():
        return {}
        
    df = pd.read_parquet(traj_file)
    df = df[(df['year'] >= 2000) & (df['year'] <= 2025) & (df['id'].isin([jid, c_code]))]
    
    result = {}
    for entity_id in df['id'].unique():
        sub = df[df['id'] == entity_id].sort_values('year')
        result[str(entity_id)] = {
            "name": f"País: {c_code}" if entity_id == c_code else "Revista",
            "is_country": entity_id == c_code,
            "points": sanitize_records(sub[['year', 'x', 'y']])
        }
    return result


@router.get("/{journal_id:path}/radar-profile")
def get_journal_radar_profile(journal_id: str):
    """Returns normalized [0, 1] 6-dimension editorial maturity profile for radar chart."""
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    
    df_j = query_df("SELECT * FROM journals WHERE id = ?", [jid])
    if df_j.empty:
        raise HTTPException(status_code=404, detail="Revista no encontrada")
        
    row = df_j.iloc[0].to_dict()
    c_code = row.get('country_code', '')
    
    # Regional baseline
    df_latam = query_df("SELECT * FROM metrics_latam_period LIMIT 1")
    latam_row = df_latam.iloc[0].to_dict() if not df_latam.empty else {}
    
    # Country baseline
    df_c = query_df("SELECT * FROM metrics_country_period WHERE country_code = ?", [c_code])
    c_row = df_c.iloc[0].to_dict() if not df_c.empty else {}
    
    # 6 dimensions:
    # 1. FWCI (rel to 2.0 max)
    fwci_val = float(row.get('fwci_avg', 0) or 0)
    fwci_norm = min(1.0, fwci_val / 2.0)
    
    # 2. % Top 10% (rel to 20% max)
    top10_val = float(row.get('pct_top_10', 0) or 0)
    top10_norm = min(1.0, top10_val / 20.0)
    
    # 3. % OA Diamante
    diamond_val = float(row.get('pct_oa_diamond', 0) or 0)
    diamond_norm = min(1.0, diamond_val / 100.0)
    
    # 4. Multilingualism (Shannon entropy of ES, EN, PT normalized to 1)
    pes = float(row.get('pct_lang_es', 0) or 0) / 100.0
    pen = float(row.get('pct_lang_en', 0) or 0) / 100.0
    ppt = float(row.get('pct_lang_pt', 0) or 0) / 100.0
    p_sum = pes + pen + ppt
    if p_sum > 0:
        pes, pen, ppt = pes/p_sum, pen/p_sum, ppt/p_sum
        entropy = -sum(p * np.log(p + 1e-12) for p in [pes, pen, ppt] if p > 0)
        multi_norm = min(1.0, entropy / np.log(3))
    else:
        multi_norm = 0.2
        
    # 5. Internationality (100 - domestic author %)
    dom_val = float(row.get('pct_authors_domestic', 50) or 50)
    intl_norm = min(1.0, max(0.0, (100.0 - dom_val) / 100.0))
    
    # 6. Indexation Score (DOAJ, SciELO, Scopus, CORE)
    idx_score = 0.0
    if row.get('is_in_doaj') or row.get('is_doaj'): idx_score += 0.3
    if row.get('is_in_scielo'): idx_score += 0.3
    if row.get('is_scopus'): idx_score += 0.25
    if row.get('is_core_x') or row.get('is_core_y'): idx_score += 0.15
    idx_norm = min(1.0, idx_score)
    
    # Baselines
    def get_baseline_profile(b_row):
        b_fwci = min(1.0, float(b_row.get('fwci_avg', 0.5) or 0.5) / 2.0)
        b_top10 = min(1.0, float(b_row.get('pct_top_10', 5.0) or 5.0) / 20.0)
        b_diam = min(1.0, float(b_row.get('pct_oa_diamond', 60.0) or 60.0) / 100.0)
        b_dom = float(b_row.get('pct_authors_domestic', 65.0) or 65.0)
        b_intl = min(1.0, max(0.0, (100.0 - b_dom) / 100.0))
        return {
            "FWCI": round(b_fwci, 2),
            "Top 10%": round(b_top10, 2),
            "OA Diamante": round(b_diam, 2),
            "Multilingüismo": 0.5,
            "Internacionalización": round(b_intl, 2),
            "Indexación": 0.6
        }

    return {
        "axes": ["FWCI", "Top 10%", "OA Diamante", "Multilingüismo", "Internacionalización", "Indexación"],
        "journal": {
            "FWCI": round(fwci_norm, 2),
            "Top 10%": round(top10_norm, 2),
            "OA Diamante": round(diamond_norm, 2),
            "Multilingüismo": round(multi_norm, 2),
            "Internacionalización": round(intl_norm, 2),
            "Indexación": round(idx_norm, 2)
        },
        "country": get_baseline_profile(c_row),
        "latam": get_baseline_profile(latam_row)
    }

@router.get("/{journal_id:path}/citations-distribution")
def get_journal_citations_distribution(journal_id: str):
    """Returns article citation counts and FWCI from works table for Box Plot and Violin Plot."""
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    
    df = query_df("""
        SELECT cited_by_count, fwci, percentile, publication_year, oa_status
        FROM works 
        WHERE journal_id = ? 
        ORDER BY cited_by_count DESC
        LIMIT 2500
    """, [jid])
    
    if df.empty:
        return {"citations": [], "fwci": [], "percentiles": []}
        
    return {
        "citations": [int(x) for x in df['cited_by_count'].dropna().tolist()],
        "fwci": [round(float(x), 3) for x in df['fwci'].dropna().tolist() if x >= 0],
        "percentiles": [round(float(x), 1) for x in df['percentile'].dropna().tolist() if x >= 0],
        "years": [int(x) for x in df['publication_year'].dropna().tolist()]
    }

@router.get("/{journal_id:path}/connected-trajectory")
def get_journal_connected_trajectory(journal_id: str):
    """Returns longitudinal time series (works vs FWCI) for Connected Scatter Plot."""
    jid = journal_id if journal_id.startswith("http") else f"https://openalex.org/{journal_id}"
    df = query_df("""
        SELECT year, num_documents, fwci_avg, avg_percentile, pct_top_10, pct_oa_diamond
        FROM metrics_journal_annual 
        WHERE journal_id = ? 
        ORDER BY year ASC
    """, [jid])
    return sanitize_records(df)

