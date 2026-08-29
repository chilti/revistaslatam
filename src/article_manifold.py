"""
article_manifold.py - Multilingual Semantic Landscape & Manifold Learning for Articles
Processes pure Title + Abstract text (excluding concepts/topics and geopolitical metadata),
applies trilingual stopwords (ES + PT + EN), dimensionality reduction (UMAP),
and calculates journal centroids (mean pooling).
Fully supports GPU acceleration via RAPIDS cuML and PyTorch CUDA with automatic CPU multicore fallback.
"""
import os
import json
import re
import html
import time
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import MiniBatchKMeans, KMeans
from umap import UMAP

# GPU Acceleration Detection (RAPIDS cuML & PyTorch CUDA)
try:
    import cuml
    from cuml.manifold import UMAP as GPU_UMAP
    from cuml.decomposition import TruncatedSVD as GPU_TruncatedSVD
    from cuml.cluster import HDBSCAN as GPU_HDBSCAN, KMeans as GPU_KMeans
    HAS_CUML = True
except ImportError:
    HAS_CUML = False

try:
    import torch
    HAS_TORCH_CUDA = torch.cuda.is_available()
    CUDA_DEVICE_NAME = torch.cuda.get_device_name(0) if HAS_TORCH_CUDA else ""
except ImportError:
    HAS_TORCH_CUDA = False
    CUDA_DEVICE_NAME = ""

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
# Hardware Status Check
# ============================================================================
def get_hardware_info():
    info = {
        "has_cuml": HAS_CUML,
        "has_torch_cuda": HAS_TORCH_CUDA,
        "cuda_device": CUDA_DEVICE_NAME,
        "cpu_threads": os.cpu_count() or 4
    }
    return info


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
    full_text = re.sub(r'<[^>]+>', ' ', full_text)
    full_text = re.sub(r'&[a-zA-Z]+;', ' ', full_text)
    full_text = re.sub(r'\s+', ' ', full_text).strip()
    return full_text


# ============================================================================
# Dense & Multilingual Vector Embeddings (GPU Accelerated / Multicore CPU)
# ============================================================================
def generate_article_embeddings(texts, model_path=None, dim=128, use_gpu=True, batch_size=64):
    """
    Generates dense embeddings for pure article texts.
    1. Attempts to use Nomic MoE (text-embedding-nomic-ai-nomic-embed-text-v2-moe) via local/remote API.
    2. Supports local SentenceTransformers on GPU/CPU.
    3. Seamless fallback to GPU TruncatedSVD (cuML) / CPU Multicore TF-IDF SVD.
    """
    if len(texts) == 0:
        return np.empty((0, dim), dtype=np.float32)
        
    texts_clean = [str(t) if pd.notna(t) and str(t).strip() != "" else "artigo cientifico" for t in texts]
    loaded = False
    embeddings = None
    
    # 1. Check local / remote embedding endpoint (e.g. Nomic MoE in LM Studio / Ollama)
    emb_model = os.getenv("EMBEDDING_MODEL", "text-embedding-nomic-ai-nomic-embed-text-v2-moe")
    base_url = os.getenv("LLM_BASE_URL", "http://127.0.0.1:1234/v1/")
    api_key = os.getenv("LLM_API_KEY", "lm-studio")
    
    try:
        from openai import OpenAI
        print(f"Intentando generar embeddings con {emb_model} en {base_url} ({len(texts_clean):,} textos)...")
        client = OpenAI(base_url=base_url, api_key=api_key, timeout=20.0)
        
        # Test first batch
        test_resp = client.embeddings.create(model=emb_model, input=texts_clean[:min(2, len(texts_clean))])
        if test_resp and test_resp.data and len(test_resp.data) > 0:
            actual_dim = len(test_resp.data[0].embedding)
            print(f"  -> 🚀 Conexión exitosa a {emb_model} ({actual_dim}d). Procesando en lotes de {batch_size}...")
            
            all_vecs = []
            for i in range(0, len(texts_clean), batch_size):
                chunk = texts_clean[i:i + batch_size]
                r = client.embeddings.create(model=emb_model, input=chunk)
                all_vecs.extend([item.embedding for item in r.data])
                if (i // batch_size) % 20 == 0 and i > 0:
                    print(f"     Progreso Nomic MoE: {min(i + batch_size, len(texts_clean)):,} / {len(texts_clean):,} textos")
                    
            embeddings = np.asarray(all_vecs, dtype=np.float32)
            loaded = True
            print(f"  ✅ Embeddings Nomic MoE generados: {embeddings.shape}")
    except Exception as e:
        print(f"  -> ℹ️ Endpoint de {emb_model} no disponible ({e}). Usando motor de respaldo.")
    
    # 2. Neural model on GPU via SentenceTransformers
    if not loaded and HAS_SENTENCE_TRANSFORMERS and model_path and os.path.exists(model_path):
        try:
            device = "cuda" if (HAS_TORCH_CUDA and use_gpu) else "cpu"
            print(f"Loading SentenceTransformer on {device} ({CUDA_DEVICE_NAME if device == 'cuda' else 'CPU'})...")
            model = SentenceTransformer(model_path, device=device)
            embeddings = model.encode(texts_clean, batch_size=128 if device == 'cuda' else 32, show_progress_bar=True, normalize_embeddings=True)
            loaded = True
        except Exception as e:
            print(f"Notice: SentenceTransformer failed ({e}). Falling back to TF-IDF SVD.")
            
    # 3. High-Resolution Trilingual TF-IDF + SVD (GPU via cuML or CPU Multicore)
    if not loaded:
        print(f"Computing High-Resolution Trilingual TF-IDF (25k features, sublinear TF)...")
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
            
        if HAS_CUML and use_gpu:
            try:
                print(f"  -> 🚀 [GPU cuML] Ejecutando TruncatedSVD ({n_comp}d) en {CUDA_DEVICE_NAME}...")
                svd = GPU_TruncatedSVD(n_components=n_comp, random_state=42)
                embeddings = svd.fit_transform(X_tfidf.astype(np.float32))
                if hasattr(embeddings, 'to_numpy'):
                    embeddings = embeddings.to_numpy()
                elif hasattr(embeddings, 'values'):
                    embeddings = embeddings.values
            except Exception as e:
                print(f"  -> ⚠️ GPU TruncatedSVD notice ({e}). Cayendo en CPU SVD...")
                svd = TruncatedSVD(n_components=n_comp, algorithm='randomized', random_state=42)
                embeddings = svd.fit_transform(X_tfidf)
        else:
            print(f"  -> 💻 [CPU] Ejecutando TruncatedSVD ({n_comp}d, multicore randomized)...")
            svd = TruncatedSVD(n_components=n_comp, algorithm='randomized', random_state=42)
            embeddings = svd.fit_transform(X_tfidf)
            
        # Unit-norm normalization
        embeddings = np.asarray(embeddings, dtype=np.float32)
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        embeddings = embeddings / norms
        
    return embeddings


# ============================================================================
# UMAP 2D Manifold Projection (GPU Accelerated / Multicore CPU)
# ============================================================================
def project_articles_umap(
    embeddings,
    n_components=2,
    n_neighbors=30,
    min_dist=0.35,
    spread=1.8,
    repulsion_strength=1.5,
    negative_sample_rate=10,
    metric='cosine',
    random_state=42,
    use_gpu=True
):
    """
    Computes 2D UMAP projection from dense embeddings with enhanced cluster dispersion.
    Automatically uses GPU-accelerated cuML if available for processing millions of articles.
    """
    n_samples = len(embeddings)
    if n_samples < 3:
        return np.zeros((n_samples, n_components), dtype=np.float32)
        
    actual_neighbors = min(n_neighbors, max(2, n_samples - 1))
    embeddings = np.asarray(embeddings, dtype=np.float32)
    
    if HAS_CUML and use_gpu:
        try:
            print(f"  -> 🚀 [GPU cuML] Ejecutando UMAP 2D acelerado en {CUDA_DEVICE_NAME} ({n_samples:,} artículos)...")
            reducer = GPU_UMAP(
                n_components=n_components,
                n_neighbors=actual_neighbors,
                min_dist=min_dist,
                spread=spread,
                repulsion_strength=repulsion_strength,
                negative_sample_rate=negative_sample_rate,
                metric=metric,
                random_state=random_state
            )
            coords = reducer.fit_transform(embeddings)
            if hasattr(coords, 'to_numpy'):
                coords = coords.to_numpy()
            elif hasattr(coords, 'values'):
                coords = coords.values
            return np.asarray(coords, dtype=np.float32)
        except Exception as e:
            print(f"  -> ⚠️ GPU UMAP notice ({e}). Cayendo en CPU UMAP...")
            
    print(f"  -> 💻 [CPU] Ejecutando UMAP 2D (umap-learn, n_jobs=-1)...")
    reducer = UMAP(
        n_components=n_components,
        n_neighbors=actual_neighbors,
        min_dist=min_dist,
        spread=spread,
        repulsion_strength=repulsion_strength,
        negative_sample_rate=negative_sample_rate,
        metric=metric,
        random_state=random_state,
        n_jobs=-1
    )
    coords = reducer.fit_transform(embeddings)
    return np.asarray(coords, dtype=np.float32)


# ============================================================================
# Community Detection (GPU HDBSCAN/KMeans or CPU)
# ============================================================================
def detect_article_communities(embeddings_or_coords, n_clusters=12, min_cluster_size=100, use_gpu=True):
    """
    Detects thematic macro-clusters using HDBSCAN / MiniBatchKMeans (GPU or CPU).
    """
    n_samples = len(embeddings_or_coords)
    if n_samples < 5:
        return np.zeros(n_samples, dtype=int)
        
    data = np.asarray(embeddings_or_coords, dtype=np.float32)
    
    # Try GPU HDBSCAN via cuML
    if HAS_CUML and use_gpu:
        try:
            print(f"  -> 🚀 [GPU cuML] Ejecutando HDBSCAN en {CUDA_DEVICE_NAME}...")
            clusterer = GPU_HDBSCAN(min_cluster_size=min_cluster_size)
            labels = clusterer.fit_predict(data)
            if hasattr(labels, 'to_numpy'):
                labels = labels.to_numpy()
            return np.asarray(labels, dtype=int)
        except Exception:
            pass

    # CPU HDBSCAN
    if HAS_HDBSCAN and n_samples > 200:
        try:
            clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, metric='euclidean', cluster_selection_epsilon=0.3)
            labels = clusterer.fit_predict(data)
            if (labels == -1).any() and (labels >= 0).any():
                from sklearn.neighbors import NearestNeighbors
                core_mask = labels >= 0
                core_coords = data[core_mask]
                core_labels = labels[core_mask]
                nn = NearestNeighbors(n_neighbors=1, n_jobs=-1).fit(core_coords)
                noise_indices = np.where(labels == -1)[0]
                _, nearest = nn.kneighbors(data[noise_indices])
                labels[noise_indices] = core_labels[nearest.flatten()]
            return labels
        except Exception:
            pass
            
    # Scalable MiniBatchKMeans for massive datasets (millions of rows)
    if n_samples > 50000:
        mbk = MiniBatchKMeans(n_clusters=n_clusters, batch_size=4096, random_state=42, n_init='auto')
        return mbk.fit_predict(data)
        
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
    return km.fit_predict(data)


# ============================================================================
# Journal Centroid Calculation (Mean Pooling)
# ============================================================================
def compute_journal_centroids(df_works, df_journals=None):
    """
    Computes journal centroids by mean pooling of their articles' 2D UMAP coordinates.
    """
    if 'umap_x' not in df_works.columns or 'umap_y' not in df_works.columns or 'journal_id' not in df_works.columns:
        return pd.DataFrame()
        
    centroids = df_works.groupby('journal_id').agg(
        umap_x=('umap_x', 'mean'),
        umap_y=('umap_y', 'mean'),
        article_sample_count=('id', 'count')
    ).reset_index()
    
    centroids.rename(columns={'journal_id': 'id'}, inplace=True)
    
    if df_journals is not None and not df_journals.empty and 'id' in df_journals.columns:
        cols_to_keep = [c for c in df_journals.columns if c not in ['umap_x', 'umap_y']]
        merged = df_journals[cols_to_keep].merge(centroids, on='id', how='left')
        return merged
        
    return centroids
