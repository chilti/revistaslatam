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
