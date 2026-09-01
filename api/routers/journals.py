"""
api/routers/journals.py - Journal-Level Analytical Endpoints
"""
import os
import json
import numpy as np
import pandas as pd
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query, HTTPException, Path

from api.db import query_df, sanitize_records, DATA_DIR, CACHE_DIR, UMAP_DIR
from api.constants import COUNTRY_NAMES

router = APIRouter(prefix="/api/journals", tags=["Detalle de Revista"])

import unicodedata

def strip_accents(text: str) -> str:
    if not text: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_journal_id(raw_id: str) -> str:
    if not raw_id:
        return ""
    clean = raw_id.strip()
    clean = clean.replace("https:/openalex.org/", "").replace("https://openalex.org/", "").replace("http:/openalex.org/", "").replace("http://openalex.org/", "")
    if "/" in clean:
        clean = clean.split("/")[-1]
    return f"https://openalex.org/{clean}"

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
    jid = normalize_journal_id(journal_id)
    
    df = query_df("SELECT * FROM journals WHERE id = ?", [jid])
    if df.empty:
        # Try finding by partial ID
        df = query_df("SELECT * FROM journals WHERE id LIKE ?", [f"%{journal_id}%"])
        
    if df.empty:
        raise HTTPException(status_code=404, detail="Revista no encontrada")
        
    journal_data = df.iloc[0].to_dict()
    country_code = journal_data.get('country_code', '')
    journal_data['country_name'] = COUNTRY_NAMES.get(country_code, country_code)
    
    # Enrich with multimodal metrics (pagerank, eigenfactor, avg_percentile, i10_index)
    umap_file = UMAP_DIR / 'umap_journals_multimodal.parquet'
    if umap_file.exists():
        try:
            df_u = pd.read_parquet(umap_file, filters=[('id', '==', jid)])
            if not df_u.empty:
                u_row = df_u.iloc[0].to_dict()
                for field in ['pagerank', 'eigenfactor', 'avg_percentile', 'i10_index']:
                    if field in u_row and pd.notna(u_row[field]) and (field not in journal_data or pd.isna(journal_data.get(field))):
                        journal_data[field] = u_row[field]
        except Exception:
            pass
    
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

    # Calculate recent period citations and h/i10 index for 2021-2025 from works
    try:
        res_cites = query_df("""
            SELECT 
                COALESCE(SUM(cited_by_count), 0) as cited_by_count,
                LIST(cited_by_count ORDER BY cited_by_count DESC) as cites_list
            FROM works 
            WHERE journal_id = ? AND publication_year BETWEEN 2021 AND 2025
        """, [jid])
        if not res_cites.empty:
            recent_data['cited_by_count'] = int(res_cites['cited_by_count'].iloc[0])
            c_val = res_cites['cites_list'].iloc[0]
            c_list = list(c_val) if c_val is not None else []
            h_rec = 0
            for i, c in enumerate(c_list, 1):
                if c >= i:
                    h_rec = i
                else:
                    break
            recent_data['h_index'] = h_rec
            recent_data['i10_index'] = sum(1 for c in c_list if c >= 10)
    except Exception:
        pass
        
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
    jid = normalize_journal_id(journal_id)
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
    jid = normalize_journal_id(journal_id)
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

@router.get("/{journal_id:path}/treemap")
def get_journal_treemap(
    journal_id: str,
    indicator: str = Query("fwci_avg_recent"),
    include_unclassified: bool = Query(False)
):
    """Returns nested Treemap format data for Domain -> Field -> Subfield for a journal."""
    jid = normalize_journal_id(journal_id)
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
        df = df[(df['domain'] != 'Sin Clasificación') & (df['domain'] != 'Unknown')]
        
    size_col = 'count_recent' if '_recent' in indicator else 'count_full'
    df = df[df[size_col] > 0].copy()
    
    root_id = "JOURNAL_ROOT"
    
    nodes = []
    nodes.append({
        "id": root_id,
        "label": "Revista",
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


@router.get("/{journal_id:path}/articles")
def get_journal_articles(
    journal_id: str,
    year: int = Query(None, description="Optional year filter"),
    sort_by: str = Query("cited_by_count", pattern="^(cited_by_count|publication_year|fwci)$"),
    limit: int = Query(100, description="Page limit, 0 for all")
):
    """Ultra-fast paginated articles retrieval directly from DuckDB."""
    jid = normalize_journal_id(journal_id)
    
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
        
    limit_clause = ""
    if limit and limit > 0:
        limit_clause = f"LIMIT {int(limit)}"
        
    sql = f"""
        SELECT id, doi, title, publication_year, cited_by_count, fwci, percentile,
               is_in_top_10_percent, is_in_top_1_percent, is_domestic_author,
               oa_status, language, is_retracted, is_paratext
        FROM works
        WHERE journal_id = ? {year_filter}
        ORDER BY {order_clause}
        {limit_clause}
    """
    df = query_df(sql, params)
    return sanitize_records(df)


@router.get("/{journal_id:path}/landscape")
def get_journal_landscape(journal_id: str, limit: int = Query(2500)):
    """Returns articles of this journal in the landscape + spatial dispersion metric + regional background sample."""
    jid = normalize_journal_id(journal_id)
    clean_code = jid.split('/')[-1]
    landscape_file = UMAP_DIR / 'umap_articles_landscape.parquet'
    if not landscape_file.exists():
        return {"articles": [], "bg_articles": [], "dispersion": 0.0, "min_year": 1985, "max_year": 2026}
        
    df = pd.read_parquet(landscape_file)
    sub = df[(df['journal_id'] == jid) | (df['journal_id'].astype(str).str.contains(clean_code, na=False))]
    bg = df[~df['id'].isin(sub['id'])] if not sub.empty else df
    
    if len(bg) > 4000:
        bg = bg.sample(4000, random_state=42)
    if len(sub) > limit:
        sub = sub.sample(limit, random_state=42)
        
    min_year = int(df['publication_year'].min()) if 'publication_year' in df.columns else 1985
    max_year = int(df['publication_year'].max()) if 'publication_year' in df.columns else 2026
    
    if sub.empty:
        return {
            "articles": [], 
            "bg_articles": sanitize_records(bg[['umap_x', 'umap_y']]), 
            "dispersion": 0.0,
            "count": 0,
            "min_year": min_year,
            "max_year": max_year
        }
        
    std_x = float(sub['umap_x'].std()) if len(sub) > 1 else 0.0
    std_y = float(sub['umap_y'].std()) if len(sub) > 1 else 0.0
    dispersion = float(np.sqrt(std_x**2 + std_y**2)) if not np.isnan(std_x) else 0.0
    
    cols = ['id', 'title', 'publication_year', 'journal_name', 'fwci', 'community_name', 'umap_x', 'umap_y']
    cols = [c for c in cols if c in sub.columns]
    
    return {
        "articles": sanitize_records(sub[cols]),
        "bg_articles": sanitize_records(bg[['umap_x', 'umap_y']]),
        "dispersion": round(dispersion, 3),
        "count": len(sub),
        "min_year": min_year,
        "max_year": max_year
    }

@router.get("/{journal_id:path}/trajectory")
def get_journal_trajectory(journal_id: str):
    """Returns UMAP trajectory for the journal vs its country enriched with performance metrics."""
    jid = normalize_journal_id(journal_id)
    
    # Get journal name and country code
    df_j = query_df("SELECT display_name, country_code FROM journals WHERE id = ?", [jid])
    j_name = df_j['display_name'].iloc[0] if not df_j.empty else 'Revista'
    c_code = df_j['country_code'].iloc[0] if not df_j.empty else ''
    c_name = COUNTRY_NAMES.get(c_code, c_code)
    
    traj_file = CACHE_DIR / 'trajectory_journals_coords.parquet'
    if not traj_file.exists():
        traj_file = CACHE_DIR / 'trajectory_coordinates.parquet'
    if not traj_file.exists():
        return {}
        
    df = pd.read_parquet(traj_file)
    df = df[(df['year'] >= 2000) & (df['year'] <= 2025) & (df['id'].isin([jid, c_code]))]
    
    # Enrich with metrics
    j_annual_file = CACHE_DIR / 'metrics_journal_annual.parquet'
    c_annual_file = CACHE_DIR / 'metrics_country_annual.parquet'
    
    all_metrics = []
    if j_annual_file.exists():
        j_df = pd.read_parquet(j_annual_file)
        j_sub = j_df[j_df['journal_id'] == jid].copy()
        j_sub['id'] = jid
        all_metrics.append(j_sub)
    if c_annual_file.exists() and c_code:
        c_df = pd.read_parquet(c_annual_file)
        c_sub = c_df[c_df['country_code'] == c_code].copy()
        c_sub['id'] = c_code
        all_metrics.append(c_sub)
        
    if all_metrics:
        metrics_df = pd.concat(all_metrics, ignore_index=True)
        metric_cols = [c for c in metrics_df.columns if c not in ['name', 'type', 'journal_id', 'country_code']]
        df = df.merge(metrics_df[metric_cols], on=['id', 'year'], how='left')
        
    cols_to_keep = [c for c in ['year', 'x', 'y', 'fwci_avg', 'pct_oa_diamond', 'pct_top_10', 'pct_top_1', 'pct_lang_en', 'num_documents', 'avg_percentile', 'pct_oa_gold', 'pct_authors_domestic'] if c in df.columns]
    
    result = {}
    for entity_id in df['id'].unique():
        sub = df[df['id'] == entity_id].sort_values('year')
        is_country = entity_id == c_code
        result[str(entity_id)] = {
            "name": f"País: {c_name}" if is_country else j_name,
            "is_country": is_country,
            "points": sanitize_records(sub[cols_to_keep])
        }
    return result


@router.get("/{journal_id:path}/radar-profile")
def get_journal_radar_profile(journal_id: str):
    """Returns normalized [0, 1] 6-dimension editorial maturity profile for radar chart."""
    jid = normalize_journal_id(journal_id)
    
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
    jid = normalize_journal_id(journal_id)
    
    df = query_df("""
        SELECT cited_by_count, fwci, percentile, publication_year, oa_status
        FROM works 
        WHERE journal_id = ? AND publication_year >= 1970 AND publication_year <= 2026
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
    jid = normalize_journal_id(journal_id)
    df = query_df("""
        SELECT year, num_documents, fwci_avg, avg_percentile, pct_top_10, pct_oa_diamond
        FROM metrics_journal_annual 
        WHERE journal_id = ? AND year >= 1970 AND year <= 2026
        ORDER BY year ASC
    """, [jid])
@router.get("/{journal_id:path}/thematic-evolution")
def get_journal_thematic_evolution(
    journal_id: str,
    level: str = Query("domain", pattern="^(domain|field|subfield|topic)$")
):
    """Returns aggregated yearly evolution matrix for knowledge fields of a journal."""
    jid = normalize_journal_id(journal_id)
    clean_code = jid.split('/')[-1]
    
    df_agg = query_df(f"""
        SELECT CAST(e.year AS INT) AS year, e.{level} AS name, SUM(e.num_documents) AS num_documents
        FROM thematic_evolution e
        WHERE REGEXP_EXTRACT(e.journal_id, 'S[0-9]+') = ? AND e.year >= 1985 AND e.{level} IS NOT NULL AND e.{level} != '' AND e.{level} != 'Sin Clasificación' AND e.{level} != 'Unknown'
        GROUP BY e.year, e.{level}
        ORDER BY year ASC, num_documents DESC
    """, [clean_code])
    
    return sanitize_records(df_agg)


@router.get("/{journal_id:path}/export-openalex")
def export_journal_articles_openalex(
    journal_id: str,
    format: str = Query("json", pattern="^(json|jsonl|csv)$"),
    year_min: Optional[int] = Query(None),
    year_max: Optional[int] = Query(None),
    limit: Optional[int] = Query(None)
):
    """Exports complete OpenAlex work records (all 88 fields or full JSON/JSONL) for a journal."""
    from pipeline_revistaslatam.export_articles_openalex import (
        get_work_ids_from_db, fetch_works_batch, map_work_to_openalex_csv_row, OPENALEX_CSV_COLUMNS
    )
    import io
    import csv
    from fastapi.responses import Response

    jid = normalize_journal_id(journal_id)
    work_ids = get_work_ids_from_db(journal_id=jid, year_min=year_min, year_max=year_max, limit=limit)
    if not work_ids:
        raise HTTPException(status_code=404, detail="No se encontraron artículos para esta revista")

    works = fetch_works_batch(work_ids, max_workers=16)
    clean_jid = jid.split('/')[-1]

    if format == 'jsonl':
        content = "\n".join(json.dumps(w, ensure_ascii=False) for w in works)
        media_type = "application/x-ndjson; charset=utf-8"
        filename = f"openalex_{clean_jid}.jsonl"
    elif format == 'csv':
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=OPENALEX_CSV_COLUMNS)
        writer.writeheader()
        for w in works:
            writer.writerow(map_work_to_openalex_csv_row(w))
        content = output.getvalue()
        media_type = "text/csv; charset=utf-8"
        filename = f"openalex_{clean_jid}.csv"
    else:
        content = json.dumps(works, ensure_ascii=False, indent=2)
        media_type = "application/json; charset=utf-8"
        filename = f"openalex_{clean_jid}.json"

    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )



