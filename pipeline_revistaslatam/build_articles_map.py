#!/usr/bin/env python3
"""
Pipeline Step: Build Article-Level Multilingual Semantic Landscape (Info Tlachia Methodology)
Extracts Title + Abstract (no topics, no concepts, no country metadata),
generates dense embeddings with trilingual stopwords (ES + PT + EN),
computes 2D UMAP projection and community labels,
and calculates journal centroids (Mean Pooling) for total topological alignment.
"""
import os
import sys
import argparse
import time
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# Force UTF-8 on Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Add src to path
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR / 'src'))

from article_manifold import (
    clean_pure_text,
    generate_article_embeddings,
    project_articles_umap,
    detect_article_communities,
    extract_cluster_keywords,
    compute_journal_centroids,
    TRILINGUAL_STOPWORDS
)

def main():
    parser = argparse.ArgumentParser(description="Build Article-Level Semantic Landscape")
    parser.add_argument("--sample", type=int, default=100000, help="Number of articles to sample for the map (default: 100,000)")
    parser.add_argument("--dim", type=int, default=128, help="Embedding dimension (default: 128)")
    parser.add_argument("--neighbors", type=int, default=15, help="UMAP n_neighbors (default: 15)")
    parser.add_argument("--min-dist", type=float, default=0.1, help="UMAP min_dist (default: 0.1)")
    args = parser.parse_args()

    print("=" * 75)
    print("🚀 BUILDING ARTICLE-LEVEL SEMANTIC LANDSCAPE (INFO TLACHIA METHODOLOGY)")
    print("=" * 75)
    print(f"Sample size: {args.sample:,} articles")
    print(f"Text source: PURE Title + Abstract (NO concepts/topics, NO country metadata)")
    print(f"Stopwords: Trilingual (Español + Português + English) [{len(TRILINGUAL_STOPWORDS)} terms]")
    print("=" * 75)

    data_dir = BASE_DIR / 'data'
    umap_dir = data_dir / 'umap'
    cache_dir = data_dir / 'cache'
    umap_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    works_file = data_dir / 'latin_american_works.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    model_path = str(BASE_DIR / 'nomic-embed')

    if not works_file.exists():
        print(f"❌ Error: Works file not found at {works_file}")
        sys.exit(1)

    # 1. Load sample of articles
    print("\n[Paso 1] Leyendo artículos desde Parquet...")
    start_t = time.time()
    pf = pq.ParquetFile(works_file)
    total_rows = pf.metadata.num_rows
    print(f"  → Total de artículos disponibles en dataset crudo: {total_rows:,}")

    cols_to_load = [
        'id', 'doi', 'title', 'abstract_inverted_index', 'publication_year',
        'journal_id', 'fwci', 'oa_status', 'is_domestic_author', 'language',
        'cited_by_count'
    ]

    # Sample batches evenly across the file
    target_sample = min(args.sample, total_rows)
    batch_size = 50000
    batches_to_take = max(1, target_sample // batch_size + 1)
    
    sample_dfs = []
    total_collected = 0

    for i, batch in enumerate(pf.iter_batches(batch_size=batch_size, columns=cols_to_load)):
        df_b = batch.to_pandas()
        # Drop entries without title
        df_b = df_b[df_b['title'].notna() & (df_b['title'].str.strip() != '')]
        sample_dfs.append(df_b)
        total_collected += len(df_b)
        if total_collected >= target_sample:
            break

    df_works = pd.concat(sample_dfs, ignore_index=True)
    if len(df_works) > target_sample:
        df_works = df_works.sample(target_sample, random_state=42).reset_index(drop=True)

    print(f"  ✅ Cargados {len(df_works):,} artículos en {time.time() - start_t:.1f}s")

    # 2. Enrich with journal metadata (name and country for visualization ONLY, not text embedding)
    if journals_file.exists():
        df_journals = pd.read_parquet(journals_file)
        df_works = df_works.merge(
            df_journals[['id', 'display_name', 'country_code']],
            left_on='journal_id',
            right_on='id',
            how='left',
            suffixes=('', '_journal')
        )
        if 'display_name' in df_works.columns:
            df_works['journal_name'] = df_works['display_name'].fillna('Revista Desconocida')
            df_works.drop(columns=['display_name'], inplace=True, errors='ignore')
        if 'id_journal' in df_works.columns:
            df_works.drop(columns=['id_journal'], inplace=True, errors='ignore')

    # 3. Clean pure text (Title + Abstract ONLY)
    print("\n[Paso 2] Reconstruyendo y limpiando texto puro (Título + Resumen)...")
    t0 = time.time()
    pure_texts = []
    for _, row in df_works.iterrows():
        t = row.get('title')
        a = row.get('abstract_inverted_index')
        cleaned = clean_pure_text(t, a)
        pure_texts.append(cleaned)
    df_works['clean_text'] = pure_texts
    print(f"  ✅ Textos procesados en {time.time() - t0:.1f}s")

    # 4. Generate dense multilingual embeddings
    print("\n[Paso 3] Generando embeddings vectoriales densos...")
    t1 = time.time()
    embeddings = generate_article_embeddings(df_works['clean_text'].tolist(), model_path=model_path, dim=args.dim)
    print(f"  ✅ Matriz de embeddings generada: {embeddings.shape} en {time.time() - t1:.1f}s")

    # 5. Compute UMAP 2D projection
    print("\n[Paso 4] Proyectando espacio semántico con UMAP 2D (métrica Coseno)...")
    t2 = time.time()
    coords = project_articles_umap(embeddings, n_components=2, n_neighbors=args.neighbors, min_dist=args.min_dist)
    df_works['umap_x'] = coords[:, 0].round(4)
    df_works['umap_y'] = coords[:, 1].round(4)
    print(f"  ✅ Proyección 2D calculada en {time.time() - t2:.1f}s")

    # 6. Detect thematic communities & extract keywords
    print("\n[Paso 5] Detectando macro-comunidades temáticas y extrayendo palabras clave...")
    t3 = time.time()
    cluster_labels = detect_article_communities(coords, n_clusters=12, min_cluster_size=80)
    df_works['cluster_id'] = cluster_labels
    cluster_names = extract_cluster_keywords(df_works['clean_text'].tolist(), cluster_labels, top_n=3)
    df_works['community_name'] = df_works['cluster_id'].map(cluster_names)
    print(f"  ✅ {len(cluster_names)} comunidades temáticas identificadas en {time.time() - t3:.1f}s")
    for cid, cname in sorted(cluster_names.items()):
        cnt = (df_works['cluster_id'] == cid).sum()
        print(f"     • [Clúster {cid:02d}] {cname} ({cnt:,} artículos)")

    # 7. Save Article Landscape Parquet
    out_articles_file = umap_dir / 'umap_articles_landscape.parquet'
    save_cols = [
        'id', 'doi', 'title', 'publication_year', 'journal_id', 'journal_name',
        'country_code', 'fwci', 'oa_status', 'cited_by_count', 'language',
        'umap_x', 'umap_y', 'cluster_id', 'community_name'
    ]
    avail_save_cols = [c for c in save_cols if c in df_works.columns]
    df_works[avail_save_cols].to_parquet(out_articles_file, index=False)
    mb = out_articles_file.stat().st_size / (1024 * 1024)
    print(f"\n  💾 Paisaje semántico guardado en: {out_articles_file} ({mb:.1f} MB)")

    # 8. Compute and update clean Journal Centroids (Mean Pooling)
    print("\n[Paso 6] Calculando baricentros semánticos puros de revistas (Mean Pooling)...")
    journal_centroids = compute_journal_centroids(df_works)
    print(f"  → Baricentros calculados para {len(journal_centroids):,} revistas")

    # Merge with existing journal metadata/metrics
    umap_journals_file = umap_dir / 'umap_journals_multimodal.parquet'
    if umap_journals_file.exists():
        df_journals_prev = pd.read_parquet(umap_journals_file)
        # Update umap_x and umap_y with clean centroids
        if 'id' in df_journals_prev.columns:
            df_journals_updated = df_journals_prev.drop(columns=['umap_x', 'umap_y', 'journal_id', 'journal_id_x', 'journal_id_y'], errors='ignore')
            df_journals_updated = df_journals_updated.merge(
                journal_centroids[['journal_id', 'umap_x', 'umap_y']],
                left_on='id',
                right_on='journal_id',
                how='left'
            )
            df_journals_updated.drop(columns=['journal_id'], inplace=True, errors='ignore')
            df_journals_updated['umap_x'] = df_journals_updated['umap_x'].fillna(0.0)
            df_journals_updated['umap_y'] = df_journals_updated['umap_y'].fillna(0.0)
            df_journals_updated.to_parquet(umap_journals_file, index=False)
            print(f"  ✅ Coordenadas de revistas actualizadas con baricentro puro en: {umap_journals_file}")

    print("\n" + "=" * 75)
    print("✅ PIPELINE DE PAISAJE SEMÁNTICO COMPLETADO CON ÉXITO")
    print(f"   Tiempo total: {time.time() - start_t:.1f}s")
    print("=" * 75)

if __name__ == '__main__':
    main()
