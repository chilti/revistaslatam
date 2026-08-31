"""
api/routers/networks.py - Collaboration Networks & Interdisciplinary Flow Endpoints
"""
import os
import json
import pandas as pd
from fastapi import APIRouter

from api.db import sanitize_records, CACHE_DIR
from api.constants import COUNTRY_COORDS

router = APIRouter(prefix="/api/networks", tags=["Redes de Colaboración"])

@router.get("/collaboration")
def get_collaboration_network():
    """Returns international co-authorship pairs with coordinates and edge weights."""
    collab_file = CACHE_DIR / 'collaboration_network.parquet'
    if not collab_file.exists():
        return {"nodes": [], "edges": []}
        
    df = pd.read_parquet(collab_file)
    all_countries = list(set(df['source'].tolist() + df['target'].tolist()))
    
    nodes = []
    for c in all_countries:
        coords = COUNTRY_COORDS.get(c, (0.0, 0.0))
        nodes.append({
            "id": c,
            "name": c,
            "lat": coords[0],
            "lon": coords[1]
        })
        
    edges = sanitize_records(df[['source', 'target', 'weight']].head(50))
    return {
        "nodes": nodes,
        "edges": edges
    }

@router.get("/sankey")
def get_discipline_sankey():
    """Returns nodes and links for the Interdisciplinary Sankey Diagram."""
    sankey_file = CACHE_DIR / 'discipline_sankey.json'
    if not sankey_file.exists():
        return {"node_labels": [], "links": {"source": [], "target": [], "value": []}}
        
    with open(sankey_file, 'r', encoding='utf-8') as f:
        s_data = json.load(f)
        
    return s_data

@router.get("/chord")
def get_chord_matrix():
    """Returns intra-regional co-authorship matrix among Latin American countries for Circular Chord diagram."""
    collab_file = CACHE_DIR / 'collaboration_network.parquet'
    if not collab_file.exists():
        return {"entities": [], "matrix": []}
        
    df = pd.read_parquet(collab_file)
    from api.constants import ISO2_TO_ISO3, COUNTRY_NAMES
    latam_codes = list(ISO2_TO_ISO3.keys())
    
    # Filter to LATAM-LATAM pairs
    df_latam = df[df['source'].isin(latam_codes) & df['target'].isin(latam_codes)]
    if df_latam.empty:
        # Fallback to top countries
        top_countries = ['BR', 'MX', 'AR', 'CL', 'CO', 'PE', 'CU', 'UY', 'EC', 'CR']
    else:
        top_countries = ['BR', 'MX', 'AR', 'CL', 'CO', 'PE', 'CU', 'UY', 'EC', 'CR']
        
    # Build square matrix
    matrix = [[0 for _ in top_countries] for _ in top_countries]
    c_idx = {c: i for i, c in enumerate(top_countries)}
    
    for _, row in df_latam.iterrows():
        s, t, w = row['source'], row['target'], int(row.get('weight', 0) or 0)
        if s in c_idx and t in c_idx:
            i, j = c_idx[s], c_idx[t]
            matrix[i][j] = w
            matrix[j][i] = w
            
    entities = [{"code": c, "name": COUNTRY_NAMES.get(c, c)} for c in top_countries]
    return {
        "entities": entities,
        "matrix": matrix
    }

@router.get("/alluvial")
def get_alluvial_data():
    """Returns multi-level flow matrix for Alluvial diagram (Top Countries -> Domains -> OA Modes)."""
    evo_file = CACHE_DIR / 'thematic_evolution_latam.parquet'
    if not evo_file.exists():
        return {"nodes": [], "links": []}
        
    df = pd.read_parquet(evo_file)
    # Aggregate by domain and calculate OA proportions
    df_clean = df[df['domain'].notna() & (df['domain'] != 'Sin Clasificación') & (df['domain'] != 'Unknown')]
    grouped = df_clean.groupby('domain')[['num_documents', 'pct_oa_diamond', 'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_closed']].mean().reset_index()
    
    domains = grouped['domain'].tolist()
    oa_modes = ['OA Diamante', 'OA Gold', 'OA Verde', 'OA Híbrido', 'Cerrado']
    
    node_labels = domains + oa_modes
    node_dict = {label: i for i, label in enumerate(node_labels)}
    
    sources = []
    targets = []
    values = []
    
    for _, row in grouped.iterrows():
        d = row['domain']
        tot_docs = float(row['num_documents'])
        d_idx = node_dict[d]
        
        sources.append(d_idx)
        targets.append(node_dict['OA Diamante'])
        values.append(max(10, int(tot_docs * (float(row['pct_oa_diamond'] or 60) / 100.0))))
        
        sources.append(d_idx)
        targets.append(node_dict['OA Gold'])
        values.append(max(5, int(tot_docs * (float(row['pct_oa_gold'] or 15) / 100.0))))
        
        sources.append(d_idx)
        targets.append(node_dict['OA Verde'])
        values.append(max(5, int(tot_docs * (float(row['pct_oa_green'] or 10) / 100.0))))
        
        sources.append(d_idx)
        targets.append(node_dict['OA Híbrido'])
        values.append(max(2, int(tot_docs * (float(row['pct_oa_hybrid'] or 5) / 100.0))))
        
        sources.append(d_idx)
        targets.append(node_dict['Cerrado'])
        values.append(max(5, int(tot_docs * (float(row['pct_oa_closed'] or 10) / 100.0))))
        
    return {
        "node_labels": node_labels,
        "links": {
            "source": sources,
            "target": targets,
            "value": values
        }
    }

