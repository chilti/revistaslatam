"""
article_manifold.py - Multilingual Semantic Landscape & Manifold Learning for Articles
Processes pure Title + Abstract text (excluding concepts/topics and geopolitical metadata),
applies trilingual stopwords (ES + PT + EN), dimensionality reduction (UMAP),
and calculates journal centroids (mean pooling).
"""
import os
import json
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import KMeans
from umap import UMAP

# Optional HDBSCAN
try:
    import hdbscan
    HAS_HDBSCAN = True
except ImportError:
    HAS_HDBSCAN = False

# Optional SentenceTransformer
try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False


# ============================================================================
# Trilingual Stopwords (Spanish + Portuguese + English + Academic Noise)
# ============================================================================
SPANISH_STOPWORDS = [
    "el", "la", "los", "las", "un", "una", "unos", "unas", "y", "o", "en", "de", "del", "al", "a",
    "ante", "bajo", "cabe", "con", "contra", "desde", "durante", "entre", "hacia", "hasta",
    "mediante", "para", "por", "según", "sin", "so", "sobre", "tras", "que", "es", "se", "su",
    "sus", "como", "más", "pero", "este", "esta", "estos", "estas", "son", "fue", "lo", "ya",
    "muy", "también", "nos", "sí", "qué", "cuando", "donde", "quien", "porque", "estudio",
    "análisis", "efecto", "uso", "artículo", "revista", "investigación", "resultados", "conclusiones",
    "objetivo", "método", "metodología", "desarrollo", "evaluación", "caso", "modelo", "datos",
    "perspectiva", "evidencia", "revisión", "enfoque", "proceso", "sistema", "presente", "trabajo"
]

PORTUGUESE_STOPWORDS = [
    "de", "a", "o", "que", "e", "do", "da", "em", "um", "para", "é", "com", "não", "uma", "os", "no",
    "se", "na", "por", "mais", "as", "dos", "como", "mas", "foi", "ao", "ele", "das", "tem", "à",
    "seu", "sua", "ou", "ser", "quando", "muito", "há", "nos", "já", "está", "eu", "também", "só",
    "pelo", "pela", "até", "isso", "ela", "entre", "era", "depois", "sem", "mesmo", "aos", "ter",
    "seus", "quem", "nas", "me", "esse", "eles", "estão", "você", "tinha", "foram", "essa", "num",
    "nem", "suas", "meu", "às", "minha", "têm", "numa", "pelos", "elas", "havia", "seja", "qual",
    "será", "nós", "tenho", "lhe", "deles", "essas", "esses", "pelas", "este", "fosse", "dele",
    "tu", "te", "vocês", "vos", "lhes", "meus", "minhas", "teu", "tua", "teus", "tuas", "nosso",
    "nossa", "nossos", "nossas", "dela", "delas", "esta", "estes", "estas", "isto", "aquilo",
    "estudo", "análise", "efeito", "uso", "artigo", "revista", "pesquisa", "resultados",
    "conclusões", "objetivo", "método", "metodologia", "desenvolvimento", "avaliação", "caso",
    "modelo", "dados", "perspectiva", "evidência", "revisão", "processo", "sistema", "trabalho"
]

import html
import re

ACADEMIC_STOPWORDS = [
    "study", "analysis", "effect", "effects", "use", "based", "using", "results", "methods",
    "conclusions", "objective", "paper", "journal", "article", "research", "evaluation",
    "development", "approach", "review", "systematic", "model", "data", "case", "impact",
    "evidence", "perspective", "overview", "process", "system", "present", "studies", "analyzed"
]

GEOGRAPHIC_NOISE_STOPWORDS = [
    "mexico", "méxico", "brasil", "brazil", "colombia", "argentina", "chile", "peru", "perú",
    "cuba", "ecuador", "venezuela", "uruguay", "bolivia", "paraguay", "espana", "españa", "portugal",
    "latinoamerica", "latinoamérica", "latinoamericana", "latinoamericano", "brasileño", "brasileña",
    "mexicano", "mexicana", "argentino", "argentina", "chileno", "chilena", "colombiano", "colombiana",
    "brazilian", "mexican", "argentinian", "chilean", "colombian", "latin", "america", "américa",
    "quot", "amp", "nbsp", "http", "https", "doi", "org", "www", "url", "available", "disponible",
    "disponível", "com", "edu", "gov", "pdf", "html", "issn", "vol", "num", "pp", "pag", "pág"
]

TRILINGUAL_STOPWORDS = list(set(list(ENGLISH_STOP_WORDS) + SPANISH_STOPWORDS + PORTUGUESE_STOPWORDS + ACADEMIC_STOPWORDS + GEOGRAPHIC_NOISE_STOPWORDS))


# ============================================================================
# Text Cleaning and Abstract Inverted Index Inversion
# ============================================================================
def reconstruct_abstract_inverted_index(inverted_index):
    """
    Reconstructs standard continuous abstract string from OpenAlex inverted index.
    """
    if not inverted_index or pd.isna(inverted_index):
        return ""
    if isinstance(inverted_index, str):
        if not (inverted_index.startswith('{') or inverted_index.startswith('[')):
            return html.unescape(inverted_index).strip()
        try:
            inverted_index = json.loads(inverted_index)
        except Exception:
            return html.unescape(inverted_index).strip()
            
    if isinstance(inverted_index, dict):
        word_positions = []
        for word, positions in inverted_index.items():
            if isinstance(positions, (list, tuple)):
                clean_word = html.unescape(str(word))
                for pos in positions:
                    word_positions.append((pos, clean_word))
        if word_positions:
            word_positions.sort(key=lambda x: x[0])
            return " ".join(w for _, w in word_positions)
    return ""

def clean_pure_text(title, abstract=None):
    """
    Combines ONLY title and abstract into clean academic text.
    Unescapes HTML entities, removes URLs and numbers, NO concepts, NO topics, NO publishers, NO country codes.
    """
    t_str = html.unescape(str(title)).strip() if pd.notna(title) else ""
    a_str = ""
    if abstract is not None and pd.notna(abstract):
        if isinstance(abstract, (dict, str)):
            a_str = reconstruct_abstract_inverted_index(abstract)
        else:
            a_str = html.unescape(str(abstract)).strip()
            
    parts = []
    if t_str and t_str.lower() != "none":
        parts.append(t_str)
    if a_str and a_str.lower() != "none":
        parts.append(a_str)
        
    full_text = ". ".join(parts)
    # Remove HTML tags & residual entities
    full_text = re.sub(r'<[^>]+>', ' ', full_text)
    full_text = re.sub(r'&[a-zA-Z]+;', ' ', full_text)
    full_text = re.sub(r'\s+', ' ', full_text).strip()
    return full_text


# ============================================================================
# Dense & Multilingual Vector Embeddings
# ============================================================================
def generate_article_embeddings(texts, model_path=None, dim=128):
    """
    Generates dense embeddings for pure article texts.
    Prefers local Nomic / SentenceTransformers, falls back to sublinear TF-IDF + TruncatedSVD with trilingual stopwords.
    """
    if len(texts) == 0:
        return np.empty((0, dim))
        
    texts_clean = [str(t) if pd.notna(t) and str(t).strip() != "" else "artigo cientifico" for t in texts]
    loaded = False
    embeddings = None
    
    if HAS_SENTENCE_TRANSFORMERS and model_path and os.path.exists(model_path):
        try:
            print(f"Loading local SentenceTransformer from {model_path}...")
            model = SentenceTransformer(model_path)
            embeddings = model.encode(texts_clean, batch_size=64, show_progress_bar=True, normalize_embeddings=True)
            loaded = True
        except Exception as e:
            print(f"Notice: SentenceTransformer failed ({e}). Using optimized Trilingual TF-IDF SVD.")
            
    if not loaded:
        print("Computing High-Resolution Trilingual TF-IDF with TruncatedSVD...")
        tfidf = TfidfVectorizer(
            max_features=25000,
            stop_words=TRILINGUAL_STOPWORDS,
            ngram_range=(1, 2),
            sublinear_tf=True,
            min_df=2
        )
        X_tfidf = tfidf.fit_transform(texts_clean)
        n_comp = min(dim, X_tfidf.shape[1] - 1, len(texts_clean) - 1)
        if n_comp < 2:
            n_comp = 2
        svd = TruncatedSVD(n_components=n_comp, random_state=42)
        embeddings = svd.fit_transform(X_tfidf)
        # Unit-norm normalization
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        embeddings = embeddings / norms
        
    return embeddings


# ============================================================================
# UMAP 2D Manifold Projection & Community Detection
# ============================================================================
def project_articles_umap(embeddings, n_components=2, n_neighbors=15, min_dist=0.1):
    """
    Computes 2D UMAP projection from dense embeddings using cosine metric.
    """
    n_samples = len(embeddings)
    if n_samples < 3:
        return np.zeros((n_samples, n_components))
        
    actual_neighbors = min(n_neighbors, max(2, n_samples - 1))
    reducer = UMAP(
        n_components=n_components,
        n_neighbors=actual_neighbors,
        min_dist=min_dist,
        metric='cosine',
        random_state=42
    )
    coords = reducer.fit_transform(embeddings)
    return coords

def detect_article_communities(embeddings_or_coords, n_clusters=12, min_cluster_size=60):
    """
    Detects thematic macro-clusters using HDBSCAN if available, or KMeans.
    """
    n_samples = len(embeddings_or_coords)
    if n_samples < 5:
        return np.zeros(n_samples, dtype=int)
        
    if HAS_HDBSCAN and len(embeddings_or_coords) > 200:
        try:
            clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, metric='euclidean', cluster_selection_epsilon=0.3)
            labels = clusterer.fit_predict(embeddings_or_coords)
            # Reassign noise (-1) to closest cluster
            if (labels == -1).any() and (labels >= 0).any():
                from sklearn.neighbors import NearestNeighbors
                core_mask = labels >= 0
                core_coords = embeddings_or_coords[core_mask]
                core_labels = labels[core_mask]
                nn = NearestNeighbors(n_neighbors=1).fit(core_coords)
                noise_indices = np.where(labels == -1)[0]
                _, nearest = nn.kneighbors(embeddings_or_coords[noise_indices])
                labels[noise_indices] = core_labels[nearest.flatten()]
            return labels
        except Exception:
            pass
            
    # Fallback to KMeans
    k = min(n_clusters, max(2, n_samples // 50))
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    return km.fit_predict(embeddings_or_coords)

def extract_cluster_keywords(texts, cluster_labels, top_n=3):
    """
    Extracts representative cluster labels using TF-IDF over clustered article texts.
    """
    df_temp = pd.DataFrame({'text': texts, 'cluster': cluster_labels})
    cluster_names = {}
    
    for cl_id in df_temp['cluster'].unique():
        cl_texts = df_temp[df_temp['cluster'] == cl_id]['text'].tolist()
        if not cl_texts:
            cluster_names[cl_id] = f"Comunidad {cl_id}"
            continue
        try:
            vec = TfidfVectorizer(stop_words=TRILINGUAL_STOPWORDS, max_features=1000, ngram_range=(1, 2))
            X = vec.fit_transform(cl_texts)
            sums = np.array(X.sum(axis=0)).flatten()
            words = vec.get_feature_names_out()
            sorted_idx = sums.argsort()[::-1]
            top_words = [words[i].title() for i in sorted_idx[:top_n] if len(words[i]) > 3]
            cluster_names[cl_id] = " / ".join(top_words) if top_words else f"Comunidad {cl_id}"
        except Exception:
            cluster_names[cl_id] = f"Comunidad {cl_id}"
            
    return cluster_names

def compute_journal_centroids(df_articles):
    """
    Computes journal centroids (Mean Pooling) directly from article coordinates.
    """
    if df_articles is None or len(df_articles) == 0:
        return pd.DataFrame(columns=['journal_id', 'umap_x', 'umap_y', 'article_count'])
        
    grouped = df_articles.groupby('journal_id').agg(
        umap_x=('umap_x', 'mean'),
        umap_y=('umap_y', 'mean'),
        article_count=('id', 'count')
    ).reset_index()
    
    return grouped
