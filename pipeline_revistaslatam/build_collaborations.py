#!/usr/bin/env python3
"""
pipeline_revistaslatam/build_collaborations.py
Pipeline Step: Precomputes Country-level Co-authorship Networks
for all Latin American Countries based on journals edited in each country.
Output: data/cache/country_collaborations.json
"""
import sys
import os
import time
import json
from pathlib import Path
import pandas as pd

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from api.services.collaboration_service import (
    get_ch_client,
    format_collaboration_network,
    COUNTRY_COLLAB_CACHE_FILE
)
from api.constants import COUNTRY_NAMES


def build_all_country_collaborations():
    print("=" * 70)
    print("PRECOMPUTING COUNTRY-LEVEL CO-AUTHORSHIP NETWORKS (LATAM JOURNALS)")
    print("=" * 70)

    data_dir = BASE_DIR / 'data'
    cache_dir = data_dir / 'cache'
    cache_dir.mkdir(parents=True, exist_ok=True)

    journals_file = data_dir / 'latin_american_journals.parquet'
    if not journals_file.exists():
        print(f"❌ Error: {journals_file} does not exist.")
        return False

    j_df = pd.read_parquet(journals_file)
    client = get_ch_client()
    if not client:
        print("❌ Error: Could not connect to ClickHouse.")
        return False

    country_counts = j_df['country_code'].value_counts()
    countries = country_counts[country_counts > 0].index.tolist()

    print(f"📊 Found {len(countries)} countries with edited journals to process.\n")

    results = {}
    total_start = time.time()

    for idx, c_code in enumerate(countries, start=1):
        t0 = time.time()
        c_code_clean = str(c_code).strip().upper()
        country_name = COUNTRY_NAMES.get(c_code_clean, c_code_clean)
        
        country_jids = j_df[j_df['country_code'] == c_code]['id'].tolist()
        if not country_jids:
            continue

        jids_tuple = tuple(country_jids)
        print(f"[{idx}/{len(countries)}] Procesando {country_name} ({c_code_clean}) — {len(country_jids)} revistas...", end=" ", flush=True)

        try:
            # 1. Aristas de coautoría (pares de países en artículos de revistas de este país)
            edge_query = f"""
            SELECT c1, c2, count() as weight
            FROM (
                SELECT arrayJoin(arrayDistinct(arrayFilter(x -> x != '', all_country_codes))) as c1,
                       arrayDistinct(arrayFilter(x -> x != '', all_country_codes)) as arr
                FROM works
                WHERE source_id IN {jids_tuple} AND length(arr) > 1
            )
            ARRAY JOIN arr as c2
            WHERE c1 < c2
            GROUP BY c1, c2
            ORDER BY weight DESC
            LIMIT 75
            """
            raw_edges = client.query(edge_query).result_rows

            # 2. Conteo de países representados
            node_query = f"""
            SELECT c, count() as doc_count
            FROM (
                SELECT arrayJoin(arrayDistinct(arrayFilter(x -> x != '', all_country_codes))) as c
                FROM works
                WHERE source_id IN {jids_tuple} AND c != ''
            )
            GROUP BY c
            ORDER BY doc_count DESC
            LIMIT 75
            """
            raw_nodes = client.query(node_query).result_rows

            payload = format_collaboration_network(raw_edges, raw_nodes, anchor_code=c_code_clean)
            payload["country_code"] = c_code_clean
            payload["country_name"] = country_name
            payload["num_journals"] = len(country_jids)

            results[c_code_clean] = payload
            elapsed = time.time() - t0
            print(f"✓ ({len(raw_edges)} enlaces, {len(raw_nodes)} países en {elapsed:.2f}s)")

        except Exception as e:
            print(f"⚠️ Error: {e}")

    # Guardar en archivo JSON de caché
    with open(COUNTRY_COLLAB_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    total_time = time.time() - total_start
    print(f"\n✅ Matriz de coautoría guardada exitosamente en {COUNTRY_COLLAB_CACHE_FILE}")
    print(f"⏱️ Tiempo total: {total_time:.2f} segundos ({total_time/60:.2f} minutos)")
    print("=" * 70)
    return True


if __name__ == '__main__':
    success = build_all_country_collaborations()
    sys.exit(0 if success else 1)
