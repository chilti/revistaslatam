"""
semantic_journals.py - Multimodal Semantic Mapping, Hybrid UMAP & Community Detection for Journals
Combines text embeddings (Nomic/SentenceTransformers/TF-IDF SVD) with bibliometric indicators.
"""
import os
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import KMeans
from umap import UMAP

# Try importing sentence_transformers if available
try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False

# Import trilingual stopwords from article_manifold
try:
    from article_manifold import TRILINGUAL_STOPWORDS
except ImportError:
    from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
    TRILINGUAL_STOPWORDS = list(ENGLISH_STOP_WORDS)

def synthesize_journal_text(journals_df, works_df=None, max_works_per_journal=50):
    """
    Creates pure textual descriptors for each journal combining title and sample article titles.
    NO publisher, NO country metadata to avoid geopolitical and lexical segregation.
    """
    if journals_df is None or len(journals_df) == 0:
        return pd.Series(dtype=str)
    
    # 1. Base title text (pure name)
    texts = {}
    for idx, row in journals_df.iterrows():
        jid = row.get('id') or row.get('journal_id')
        name = str(row.get('display_name', '')).strip()
        texts[jid] = name
        
    # 2. Add sample article titles if works_df is provided
    if works_df is not None and 'journal_id' in works_df.columns and 'title' in works_df.columns:
        works_sample = works_df.dropna(subset=['journal_id', 'title'])
        # Group top titles
        grouped = works_sample.groupby('journal_id')['title'].apply(lambda s: " | ".join(s.head(max_works_per_journal)))
        for jid, titles_str in grouped.items():
            if jid in texts:
                texts[jid] = f"{texts[jid]} | {titles_str}"
                
    return pd.Series(texts)

def generate_journal_embeddings(journal_texts, model_path=None, dim=128):
    """
    Generates dense semantic embeddings for journals.
    Uses local nomic-embed model if available, else fast TF-IDF with trilingual stopwords + TruncatedSVD.
    """
    if len(journal_texts) == 0:
        return np.empty((0, dim))
        
    texts_list = [str(t) if pd.notna(t) else "" for t in journal_texts]
    
    # Check if local nomic-embed or sentence_transformers can be loaded
    loaded = False
    embeddings = None
    
    if HAS_SENTENCE_TRANSFORMERS and model_path and os.path.exists(model_path):
        try:
            print(f"Loading local embedding model from {model_path}...")
            model = SentenceTransformer(model_path)
            embeddings = model.encode(texts_list, batch_size=64, show_progress_bar=False, normalize_embeddings=True)
            loaded = True
        except Exception as e:
            print(f"Notice: Falling back to TF-IDF SVD embeddings ({e})")
            
    if not loaded:
        # High-performance TF-IDF with trilingual stopwords + sublinear term frequency + SVD
        tfidf = TfidfVectorizer(
            max_features=15000,
            stop_words=TRILINGUAL_STOPWORDS,
            ngram_range=(1, 2),
            sublinear_tf=True
        )
        X_tfidf = tfidf.fit_transform(texts_list)
        n_comp = min(dim, X_tfidf.shape[1] - 1, len(texts_list) - 1)
        if n_comp < 2:
            n_comp = 2
        svd = TruncatedSVD(n_components=n_comp, random_state=42)
        embeddings = svd.fit_transform(X_tfidf)
        # Normalize
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        embeddings = embeddings / norms
        
    return embeddings

def build_hybrid_multimodal_space(biblio_df, semantic_embeddings, alpha=0.4, n_components=2, n_neighbors=15, min_dist=0.1):
    """
    Constructs the Hybrid Multimodal Space:
    X_multimodal = [ alpha * X_biblio_norm | (1 - alpha) * X_semantic_reduced ]
    """
    n_samples = len(biblio_df)
    if n_samples < 3:
        return np.zeros((n_samples, n_components))
        
    # 1. Prepare bibliometric features
    biblio_cols = [
        'fwci_avg', 'pct_oa_diamond', 'pct_top_10', 'pct_top_1',
        'avg_percentile', 'h_index', 'price_index', 'shannon_diversity'
    ]
    available_cols = [c for c in biblio_cols if c in biblio_df.columns]
    
    if available_cols:
        X_biblio = biblio_df[available_cols].fillna(0.0).values
        scaler = RobustScaler()
        X_biblio_norm = scaler.fit_transform(X_biblio)
        # Scale to unit norm
        b_norms = np.linalg.norm(X_biblio_norm, axis=1, keepdims=True)
        b_norms[b_norms == 0] = 1.0
        X_biblio_norm = X_biblio_norm / b_norms
    else:
        X_biblio_norm = np.zeros((n_samples, 1))
        
    # 2. Prepare semantic features
    if semantic_embeddings is not None and len(semantic_embeddings) == n_samples:
        if semantic_embeddings.shape[1] > 16:
            # Intermediate SVD reduction to balance dimensions
            svd = TruncatedSVD(n_components=min(16, semantic_embeddings.shape[1]), random_state=42)
            X_sem = svd.fit_transform(semantic_embeddings)
        else:
            X_sem = semantic_embeddings.copy()
        s_norms = np.linalg.norm(X_sem, axis=1, keepdims=True)
        s_norms[s_norms == 0] = 1.0
        X_sem_norm = X_sem / s_norms
    else:
        X_sem_norm = np.zeros((n_samples, 1))
        
    # 3. Multimodal Concatenation
    X_multimodal = np.hstack([alpha * X_biblio_norm, (1.0 - alpha) * X_sem_norm])
    
    # 4. 2D UMAP Projection
    actual_neighbors = min(n_neighbors, max(2, n_samples - 1))
    umap_model = UMAP(
        n_components=n_components,
        n_neighbors=actual_neighbors,
        min_dist=min_dist,
        metric='cosine',
        random_state=42
    )
    coords = umap_model.fit_transform(X_multimodal)
    return coords

def detect_journal_communities(umap_coords_or_embeddings, n_clusters=8):
    """
    Detects thematic clusters and communities among journals.
    """
    n_samples = len(umap_coords_or_embeddings)
    if n_samples == 0:
        return np.array([])
    k = min(n_clusters, max(1, n_samples // 3))
    if k <= 1:
        return np.zeros(n_samples, dtype=int)
        
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(umap_coords_or_embeddings)
    return labels

