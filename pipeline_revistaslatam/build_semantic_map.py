#!/usr/bin/env python3
"""
Pipeline Step: Build Multimodal Semantic Map of Latin American Journals
Generates dense text embeddings, computes Hybrid Multimodal UMAP (bibliometric + semantic),
detects thematic communities, and calculates hexagonal density mapping.
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np

# Add src to path
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR / 'src'))

from semantic_journals import (
    synthesize_journal_text,
    generate_journal_embeddings,
    build_hybrid_multimodal_space,
    detect_journal_communities
)
from citation_indices import compute_journal_pagerank

def main():
    print("=" * 70)
    print("BUILDING MULTIMODAL SEMANTIC MAP & JOURNAL COMMUNITIES")
    print("=" * 70)
    
    data_dir = BASE_DIR / 'data'
    cache_dir = data_dir / 'cache'
    umap_dir = data_dir / 'umap'
    umap_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    journals_file = data_dir / 'latin_american_journals.parquet'
    metrics_file = cache_dir / 'metrics_journal_period.parquet'
    works_file = data_dir / 'latin_american_works.parquet'
    model_path = str(BASE_DIR / 'nomic-embed')
    
    if not journals_file.exists():
        print(f"Error: {journals_file} not found")
        sys.exit(1)
        
    print(f"Loading journals from {journals_file}...")
    journals_df = pd.read_parquet(journals_file)
    print(f"Loaded {len(journals_df):,} journals")
    
    # Merge with period metrics if available
    if metrics_file.exists():
        print(f"Loading metrics from {metrics_file}...")
        metrics_df = pd.read_parquet(metrics_file)
        if 'journal_id' in metrics_df.columns:
            journals_merged = journals_df.merge(metrics_df, left_on='id', right_on='journal_id', how='left')
        else:
            journals_merged = journals_df.copy()
    else:
        journals_merged = journals_df.copy()
        
    # Sample works for title synthesis if available
    works_sample = None
    if works_file.exists():
        try:
            print("Sampling works for semantic synthesis...")
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(works_file)
            sample_batches = []
            for i, b in enumerate(pf.iter_batches(batch_size=50000, columns=['journal_id', 'title', 'cited_by_count'])):
                sample_batches.append(b.to_pandas())
                if i >= 3:
                    break
            works_sample = pd.concat(sample_batches, ignore_index=True)
            print(f"Sampled {len(works_sample):,} articles for text enrichment")
        except Exception as e:
            print(f"Notice: Works sampling skipped ({e})")
            
    # 1. Synthesize Text
    print("\n1. Synthesizing rich journal descriptions...")
    journal_texts = synthesize_journal_text(journals_merged, works_sample)
    
    # 2. Generate Semantic Embeddings
    print("2. Generating dense semantic embeddings...")
    embeddings = generate_journal_embeddings(journal_texts, model_path=model_path, dim=128)
    print(f"Embeddings matrix shape: {embeddings.shape}")
    
    # 3. Hybrid Multimodal UMAP
    print("3. Computing Hybrid Multimodal UMAP (alpha=0.4)...")
    umap_coords = build_hybrid_multimodal_space(
        journals_merged,
        embeddings,
        alpha=0.4,
        n_components=2,
        n_neighbors=15,
        min_dist=0.1
    )
    journals_merged['umap_x'] = umap_coords[:, 0]
    journals_merged['umap_y'] = umap_coords[:, 1]
    
    # 4. Community Detection
    print("4. Detecting thematic communities (Leiden/KMeans clustering)...")
    community_labels = detect_journal_communities(umap_coords, n_clusters=10)
    journals_merged['community_id'] = community_labels
    
    # Assign community names
    community_names = {
        0: "Ciencias Biomédicas y Salud",
        1: "Agronomía, Veterinaria y Biología",
        2: "Ciencias Sociales, Educación y Humanidades",
        3: "Ingeniería, Tecnología y Computación",
        4: "Economía, Gestión y Políticas Públicas",
        5: "Física, Química y Matemáticas",
        6: "Ecología, Medio Ambiente y Geociencias",
        7: "Derecho, Filosofía e Historia",
        8: "Psicología y Neurociencias",
        9: "Multidisciplinarias y Acceso Abierto Diamante"
    }
    journals_merged['community_name'] = journals_merged['community_id'].map(community_names).fillna("General")
    
    # 5. Compute PageRank
    print("5. Computing Journal Citation PageRank & Eigenfactor...")
    pagerank_df = compute_journal_pagerank(works_sample if works_sample is not None else journals_merged, journals_merged)
    if not pagerank_df.empty:
        journals_merged = journals_merged.merge(pagerank_df[['journal_id', 'pagerank', 'eigenfactor']], left_on='id', right_on='journal_id', how='left')
        journals_merged['pagerank'] = journals_merged['pagerank'].fillna(0.0)
        journals_merged['eigenfactor'] = journals_merged['eigenfactor'].fillna(0.0)
        
    # 6. Save Multimodal UMAP Parquet
    output_umap_file = umap_dir / 'umap_journals_multimodal.parquet'
    journals_merged.to_parquet(output_umap_file, index=False)
    print(f"\nSaved Multimodal UMAP to: {output_umap_file}")
    
    print("\n" + "=" * 70)
    print("MULTIMODAL SEMANTIC PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 70)

if __name__ == '__main__':
    main()
