"""
llm_cluster_labeler.py - Semantic Clustering and LLM Labeling for Latin American Scholarly Articles
Based on the methodology from Sinapsis AI / spatial_metrics (RAGs).
Calculates embedding centroids, extracts top representative documents, and queries an LLM
(local LM Studio / Ollama / OpenAI / Gemini compatible) to generate concise, academic labels in Spanish.
"""
import os
import sys
import json
import time
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS

try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

# Trilingual and Academic Stopwords
SPANISH_STOPWORDS = [
    "el", "la", "los", "las", "un", "una", "unos", "unas", "y", "o", "en", "de", "del", "al", "a", 
    "ante", "bajo", "cabe", "con", "contra", "desde", "durante", "entre", "hacia", "hasta", 
    "mediante", "para", "por", "según", "sin", "so", "sobre", "tras", "que", "es", "se", "su", 
    "sus", "como", "más", "pero", "este", "esta", "estos", "estas", "son", "fue", "lo", "ya", 
    "muy", "también", "nos", "sí", "qué", "cuando", "donde", "quien", "porque", "estudio", 
    "análisis", "efecto", "uso", "study", "analysis", "effect", "use", "based", "artigo",
    "pesquisa", "objetivo", "resultados", "metodologia", "conclusao", "sobre", "entre"
]
PORTUGUESE_STOPWORDS = [
    "de", "a", "o", "que", "e", "do", "da", "em", "um", "para", "com", "nao", "uma", "os", "no",
    "se", "na", "por", "mais", "as", "dos", "como", "mas", "foi", "ao", "ele", "das", "tem", "a",
    "seu", "sua", "ou", "ser", "quando", "muito", "ha", "nos", "ja", "estao", "eu", "tambem"
]
ALL_STOPWORDS = list(set(list(ENGLISH_STOP_WORDS) + SPANISH_STOPWORDS + PORTUGUESE_STOPWORDS))

class LLMConfig:
    @staticmethod
    def get_base_url():
        return os.getenv("LLM_BASE_URL", "http://127.0.0.1:1234/v1")

    @staticmethod
    def get_model_name():
        return os.getenv("LLM_MODEL", "default")

    @staticmethod
    def get_api_key():
        return os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY") or "sk-lm-VAulEVEi:bZFVZZK3oGOxDI4gJcV1"

def get_llm_client():
    if not HAS_OPENAI:
        return None, None
    base_url = LLMConfig.get_base_url()
    api_key = LLMConfig.get_api_key()
    model_name = LLMConfig.get_model_name()
    for candidate_url in [base_url, "http://127.0.0.1:1234/v1", "http://localhost:1234/v1", "http://172.17.0.1:1234/v1"]:
        try:
            client = OpenAI(base_url=candidate_url, api_key=api_key, timeout=12.0)
            # Test ping
            client.models.list()
            return client, model_name
        except Exception:
            continue
    return None, None

def get_top_keywords_cluster(texts, n=5):
    if not texts or len(texts) == 0:
        return ""
    try:
        vec = TfidfVectorizer(stop_words=ALL_STOPWORDS, max_features=2500, ngram_range=(1, 2))
        X = vec.fit_transform(texts)
        features = vec.get_feature_names_out()
        sums = X.sum(axis=0)
        words = [(features[col], sums[0, col]) for col in range(sums.shape[1])]
        words = [w for w in words if len(w[0]) > 3]
        words = sorted(words, key=lambda x: x[1], reverse=True)
        
        final_words = []
        for w, score in words:
            if len(final_words) >= n:
                break
            is_redundant = any(w in fw or fw in w for fw in final_words)
            if not is_redundant:
                final_words.append(w)
        return " / ".join([w.title() for w in final_words])
    except Exception:
        return ""

def query_llm_label(client, model_name, keywords_str, representative_titles):
    bullet_titles = "\n".join([f"- {t}" for t in representative_titles if t])
    prompt = f"""Analiza los siguientes títulos de artículos de investigación y palabras clave que pertenecen al mismo grupo temático (clúster).
Genera un título o etiqueta sumamente descriptivo, corto y conciso para este grupo.

Reglas obligatorias:
1. La etiqueta debe resumir el área temática común de forma clara y profesional.
2. Debe ser muy corta (máximo de 2 a 4 palabras).
3. Debe estar en ESPAÑOL (traduce los términos si es necesario para mantener coherencia).
4. Responde ÚNICAMENTE con la etiqueta generada, sin explicaciones, sin comillas, sin introducciones.

Palabras clave (TF-IDF):
{keywords_str}

Títulos representativos:
{bullet_titles}

Etiqueta del grupo:"""

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "Eres un asistente científico experto en organizar y etiquetar literatura académica latinoamericana."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=128,
            timeout=15.0
        )
        content = response.choices[0].message.content or ""
        if "{" in content and "}" in content:
            try:
                import json
                data = json.loads(content)
                if isinstance(data, dict):
                    content = data.get("response") or data.get("label") or list(data.values())[0] or content
            except Exception:
                pass
        llm_label = str(content).strip().replace('"', '').replace("'", "").replace("\n", " ").strip()
        if llm_label.endswith('.'):
            llm_label = llm_label[:-1]
        return llm_label if len(llm_label) >= 2 else None
    except Exception as e:
        return None

def label_article_clusters(df_works, embeddings, cluster_col='cluster_id', title_col='title', text_col='clean_text'):
    """
    Labels each thematic cluster using Centroid-Proximity + TF-IDF + LLM (with fallback).
    """
    print("\n[Etiquetado Semántico LLM] Iniciando metodología Sinapsis AI (Centroides + TF-IDF + LLM)...")
    client, model_name = get_llm_client()
    if client:
        print(f"  -> Conectado a LLM ({LLMConfig.get_base_url()} - Modelo: {model_name})")
    else:
        print("  -> LLM no disponible localmente. Se usará el extractor TF-IDF como fallback.")

    clusters = sorted(df_works[cluster_col].unique())
    cluster_labels_map = {}
    
    for c in clusters:
        if c == -1:
            cluster_labels_map[c] = "Ruido / Misceláneo"
            continue
            
        cluster_mask = (df_works[cluster_col] == c)
        cluster_df = df_works[cluster_mask]
        cluster_indices = np.where(cluster_mask)[0]
        
        titles = cluster_df[title_col].dropna().astype(str).tolist() if title_col in cluster_df.columns else []
        texts = cluster_df[text_col].dropna().astype(str).tolist() if text_col in cluster_df.columns else titles
        
        # 1. TF-IDF Keywords
        keywords_str = get_top_keywords_cluster(texts, n=4)
        fallback_label = " / ".join(keywords_str.split(" / ")[:3]) if keywords_str else f"Comunidad {c}"
        
        # 2. Geometric Centroid & Representative Titles
        assigned_label = fallback_label
        if client is not None and len(cluster_indices) > 0 and embeddings is not None:
            try:
                vectors_c = embeddings[cluster_indices]
                centroid = np.mean(vectors_c, axis=0)
                norms = np.linalg.norm(vectors_c, axis=1)
                c_norm = np.linalg.norm(centroid)
                if c_norm > 0:
                    sims = np.dot(vectors_c, centroid) / (norms * c_norm + 1e-9)
                    closest_local = np.argsort(1.0 - sims)[:10]
                    closest_global = cluster_indices[closest_local]
                    representative_titles = df_works.iloc[closest_global][title_col].dropna().tolist()
                    
                    llm_result = query_llm_label(client, model_name, keywords_str, representative_titles)
                    if llm_result and len(llm_result) > 2:
                        assigned_label = llm_result
            except Exception as e:
                pass
                
        cluster_labels_map[c] = assigned_label
        print(f"  • [Comunidad {c:02d}] {assigned_label} ({len(cluster_df):,} artículos)")

    return cluster_labels_map
