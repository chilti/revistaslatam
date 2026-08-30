"""
api/routers/maps.py - Semantic Maps & High-Performance Point Cloud Endpoints
"""
import os
import json
import numpy as np
import pandas as pd
from fastapi import APIRouter, Query, HTTPException

from api.db import query_df, sanitize_records, UMAP_DIR
from api.constants import COMMUNITY_PALETTE

router = APIRouter(prefix="/api/maps", tags=["Mapas Semánticos"])

@router.get("/filters")
def get_map_filters():
    """Returns available countries and communities for map filters."""
    df_j = query_df("SELECT DISTINCT country_code, community_name FROM journals WHERE country_code IS NOT NULL")
    
    countries = sorted([c for c in df_j['country_code'].dropna().unique().tolist() if c])
    communities = sorted([c for c in df_j['community_name'].dropna().unique().tolist() if c])
    
    return {
        "countries": countries,
        "communities": communities
    }

@router.get("/journals")
def get_journals_map_data(
    country: str = Query(None),
    community: str = Query(None)
):
    """Returns 2D UMAP multimodal points and indicators for all 7,494 journals."""
    umap_file = UMAP_DIR / 'umap_journals_multimodal.parquet'
    if not umap_file.exists():
        return []
        
    df = pd.read_parquet(umap_file)
    if country and country != "Todos":
        df = df[df['country_code'] == country]
    if community and community != "Todas":
        df = df[df['community_name'] == community]
        
    cols = [
        'id', 'display_name', 'publisher', 'country_code', 'community_name',
        'works_count', 'cited_by_count', 'fwci_avg', 'h_index', 'pagerank',
        'pct_oa_diamond', 'is_in_doaj', 'is_in_scielo', 'is_scopus',
        'umap_x', 'umap_y'
    ]
    avail_cols = [c for c in cols if c in df.columns]
    return sanitize_records(df[avail_cols])

@router.get("/articles")
def get_articles_landscape_data(
    country: str = Query(None),
    community: str = Query(None),
    limit: int = Query(50000)
):
    """Returns sample of articles in the 2D semantic landscape for WebGL/Plotly."""
    landscape_file = UMAP_DIR / 'umap_articles_landscape.parquet'
    if not landscape_file.exists():
        return []
        
    df = pd.read_parquet(landscape_file)
    if country and country != "Todos":
        df = df[df['country_code'] == country]
    if community and community != "Todas":
        df = df[df['community_name'] == community]
        
    if limit and limit > 0 and len(df) > limit:
        df = df.sample(limit, random_state=42)
        
    cols = [
        'id', 'title', 'journal_name', 'country_code', 'community_name',
        'publication_year', 'cited_by_count', 'fwci', 'oa_status', 'authors',
        'umap_x', 'umap_y'
    ]
    avail_cols = [c for c in cols if c in df.columns]
    return sanitize_records(df[avail_cols])
