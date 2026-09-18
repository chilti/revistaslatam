"""
api/services/collaboration_service.py
Service for extraction and structuring of Country-to-Country Co-authorship Networks
for Latin American Journals and Countries.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from functools import lru_cache

from api.constants import COUNTRY_NAMES, ISO2_TO_ISO3, COUNTRY_COORDS
from api.db import CACHE_DIR, DATA_DIR, query_df

logger = logging.getLogger("revistaslatam.collaboration")

COUNTRY_COLLAB_CACHE_FILE = CACHE_DIR / "country_collaborations.json"

# In-memory LRU cache for journals to avoid repeated DB calls
_JOURNAL_COLLAB_MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}


def get_ch_client():
    """Lazily import and initialize ClickHouse client."""
    try:
        from pipeline_revistaslatam.extract_clickhouse import get_ch_client as _get_client
        return _get_client()
    except Exception as e:
        logger.warning(f"Could not connect to ClickHouse: {e}")
        return None


def format_collaboration_network(
    raw_edges: List[tuple],
    raw_nodes: List[tuple],
    anchor_code: Optional[str] = None
) -> Dict[str, Any]:
    """
    Transforms raw edge and node tuples into a standardized geonetwork payload.
    """
    node_map = {}
    for row in raw_nodes:
        c_code = str(row[0]).strip().upper()
        if not c_code or len(c_code) != 2:
            continue
        cnt = int(row[1]) if len(row) > 1 else 0
        coords = COUNTRY_COORDS.get(c_code, (0.0, 0.0))
        node_map[c_code] = {
            "id": c_code,
            "name": COUNTRY_NAMES.get(c_code, c_code),
            "iso3": ISO2_TO_ISO3.get(c_code, c_code),
            "lat": coords[0],
            "lon": coords[1],
            "count": cnt,
            "is_anchor": (c_code == anchor_code) if anchor_code else False
        }

    edges = []
    total_coauthored = 0
    partner_counts = {}

    for row in raw_edges:
        c1 = str(row[0]).strip().upper()
        c2 = str(row[1]).strip().upper()
        if not c1 or not c2 or c1 == c2 or len(c1) != 2 or len(c2) != 2:
            continue
        weight = int(row[2]) if len(row) > 2 else 1
        total_coauthored += weight

        # Track partner if anchor is set
        if anchor_code:
            if c1 == anchor_code:
                partner_counts[c2] = partner_counts.get(c2, 0) + weight
            elif c2 == anchor_code:
                partner_counts[c1] = partner_counts.get(c1, 0) + weight
        else:
            partner_counts[c1] = partner_counts.get(c1, 0) + weight
            partner_counts[c2] = partner_counts.get(c2, 0) + weight

        # Ensure both endpoints exist in nodes
        for c in (c1, c2):
            if c not in node_map:
                coords = COUNTRY_COORDS.get(c, (0.0, 0.0))
                node_map[c] = {
                    "id": c,
                    "name": COUNTRY_NAMES.get(c, c),
                    "iso3": ISO2_TO_ISO3.get(c, c),
                    "lat": coords[0],
                    "lon": coords[1],
                    "count": weight,
                    "is_anchor": (c == anchor_code) if anchor_code else False
                }

        coords1 = node_map[c1]["lat"], node_map[c1]["lon"]
        coords2 = node_map[c2]["lat"], node_map[c2]["lon"]

        edges.append({
            "source": c1,
            "target": c2,
            "source_name": node_map[c1]["name"],
            "target_name": node_map[c2]["name"],
            "source_lat": coords1[0],
            "source_lon": coords1[1],
            "target_lat": coords2[0],
            "target_lon": coords2[1],
            "weight": weight
        })

    # Sort edges descending
    edges.sort(key=lambda x: x["weight"], reverse=True)
    nodes = sorted(list(node_map.values()), key=lambda x: x["count"], reverse=True)

    top_partner = None
    if partner_counts:
        best_p = max(partner_counts.items(), key=lambda x: x[1])
        top_partner = {
            "code": best_p[0],
            "name": COUNTRY_NAMES.get(best_p[0], best_p[0]),
            "collaborations": best_p[1]
        }

    return {
        "nodes": nodes,
        "edges": edges,
        "summary": {
            "total_countries": len(nodes),
            "total_edges": len(edges),
            "total_coauthored_articles": total_coauthored,
            "top_partner": top_partner
        }
    }


def get_journal_collaboration(journal_id: str) -> Dict[str, Any]:
    """
    Computes international co-authorship matrix for a specific journal.
    Uses memory cache first, then queries ClickHouse works table.
    """
    clean_id = journal_id.split("/")[-1].strip()
    if clean_id in _JOURNAL_COLLAB_MEMORY_CACHE:
        return _JOURNAL_COLLAB_MEMORY_CACHE[clean_id]

    client = get_ch_client()
    if not client:
        return {"nodes": [], "edges": [], "summary": {"total_countries": 0, "total_edges": 0, "total_coauthored_articles": 0, "top_partner": None}}

    try:
        # 1. Co-authorship edges (pairs of countries collaborating on articles in this journal)
        edge_query = f"""
        SELECT c1, c2, count() as weight
        FROM (
            SELECT arrayJoin(arrayDistinct(arrayFilter(x -> x != '', all_country_codes))) as c1,
                   arrayDistinct(arrayFilter(x -> x != '', all_country_codes)) as arr
            FROM works
            WHERE source_id LIKE '%{clean_id}%' AND length(arr) > 1
        )
        ARRAY JOIN arr as c2
        WHERE c1 < c2
        GROUP BY c1, c2
        ORDER BY weight DESC
        LIMIT 60
        """
        raw_edges = client.query(edge_query).result_rows

        # 2. Node representation counts (all countries authoring articles in this journal)
        node_query = f"""
        SELECT c, count() as doc_count
        FROM (
            SELECT arrayJoin(arrayDistinct(arrayFilter(x -> x != '', all_country_codes))) as c
            FROM works
            WHERE source_id LIKE '%{clean_id}%' AND c != ''
        )
        GROUP BY c
        ORDER BY doc_count DESC
        LIMIT 60
        """
        raw_nodes = client.query(node_query).result_rows

        # Find home country of journal if available
        anchor_country = None
        j_df = query_df("SELECT country_code FROM journals WHERE id LIKE ? LIMIT 1", [f"%{clean_id}%"])
        if not j_df.empty and j_df["country_code"].iloc[0]:
            anchor_country = str(j_df["country_code"].iloc[0]).upper()

        payload = format_collaboration_network(raw_edges, raw_nodes, anchor_code=anchor_country)
        payload["journal_id"] = clean_id
        payload["anchor_country"] = anchor_country

        # Cache in memory (max 256 items)
        if len(_JOURNAL_COLLAB_MEMORY_CACHE) > 256:
            _JOURNAL_COLLAB_MEMORY_CACHE.clear()
        _JOURNAL_COLLAB_MEMORY_CACHE[clean_id] = payload

        return payload

    except Exception as e:
        logger.error(f"Error computing journal collaboration for {clean_id}: {e}")
        return {"nodes": [], "edges": [], "summary": {"total_countries": 0, "total_edges": 0, "total_coauthored_articles": 0, "top_partner": None}}


def get_country_collaboration(country_code: str) -> Dict[str, Any]:
    """
    Computes/retrieves international co-authorship matrix for articles published
    in journals edited in that country (Scope requested: 'La colaboración observada
    dentro de los artículos de las revistas editadas en ese país').
    """
    c_code = country_code.strip().upper()

    # 1. Try to read from precomputed cache file first
    if COUNTRY_COLLAB_CACHE_FILE.exists():
        try:
            with open(COUNTRY_COLLAB_CACHE_FILE, "r", encoding="utf-8") as f:
                cache_data = json.load(f)
                if c_code in cache_data:
                    return cache_data[c_code]
        except Exception as e:
            logger.warning(f"Error reading country collaboration cache: {e}")

    # 2. Cache miss: Compute dynamically via ClickHouse
    client = get_ch_client()
    if not client:
        return {"nodes": [], "edges": [], "summary": {"total_countries": 0, "total_edges": 0, "total_coauthored_articles": 0, "top_partner": None}}

    try:
        # Retrieve journals for this country
        j_df = query_df("SELECT id FROM journals WHERE country_code = ?", [c_code])
        if j_df.empty:
            return {"nodes": [], "edges": [], "summary": {"total_countries": 0, "total_edges": 0, "total_coauthored_articles": 0, "top_partner": None}}

        jids = tuple(j_df["id"].tolist())

        # Edges query
        edge_query = f"""
        SELECT c1, c2, count() as weight
        FROM (
            SELECT arrayJoin(arrayDistinct(arrayFilter(x -> x != '', all_country_codes))) as c1,
                   arrayDistinct(arrayFilter(x -> x != '', all_country_codes)) as arr
            FROM works
            WHERE source_id IN {jids} AND length(arr) > 1
        )
        ARRAY JOIN arr as c2
        WHERE c1 < c2
        GROUP BY c1, c2
        ORDER BY weight DESC
        LIMIT 60
        """
        raw_edges = client.query(edge_query).result_rows

        # Nodes query
        node_query = f"""
        SELECT c, count() as doc_count
        FROM (
            SELECT arrayJoin(arrayDistinct(arrayFilter(x -> x != '', all_country_codes))) as c
            FROM works
            WHERE source_id IN {jids} AND c != ''
        )
        GROUP BY c
        ORDER BY doc_count DESC
        LIMIT 60
        """
        raw_nodes = client.query(node_query).result_rows

        payload = format_collaboration_network(raw_edges, raw_nodes, anchor_code=c_code)
        payload["country_code"] = c_code
        payload["anchor_country"] = c_code
        return payload

    except Exception as e:
        logger.error(f"Error computing country collaboration for {c_code}: {e}")
        return {"nodes": [], "edges": [], "summary": {"total_countries": 0, "total_edges": 0, "total_coauthored_articles": 0, "top_partner": None}}
