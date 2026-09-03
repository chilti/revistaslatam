"""
build_articles_map.py - High-Performance Semantic Article Landscape Generator
Builds the pure semantic article master manifold and projects journals on it.
Supports processing ALL 3.6+ million papers from Latin America.
Supports GPU acceleration (RAPIDS cuML / PyTorch CUDA) and multicore CPU streaming.
"""
import os
import sys
import time
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# Force UTF-8 and Line Buffering for real-time nohup tail -f logging
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True, encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(line_buffering=True, encoding='utf-8')
except Exception:
    pass

BASE_DIR = Path.cwd()
sys.path.append(str(BASE_DIR / 'src'))

# Load .env from project root (before any module imports that read env vars)
_env_file = BASE_DIR / '.env'
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=False)
        print(f'  -> ✅ Variables de entorno cargadas desde {_env_file}')
    except ImportError:
        # Manual fallback if python-dotenv not installed
        with open(_env_file) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith('#') and '=' in _line:
                    _k, _, _v = _line.partition('=')
                    os.environ.setdefault(_k.strip(), _v.strip())
        print(f'  -> ✅ Variables de entorno cargadas (manual) desde {_env_file}')

from article_manifold import (
    clean_pure_text,
    generate_article_embeddings,
    project_articles_umap,
    detect_article_communities,
    compute_journal_centroids,
    get_hardware_info,
    TRILINGUAL_STOPWORDS
)
from llm_cluster_labeler import label_article_clusters
from clickhouse_db import ch_client


def load_or_compute_embeddings(df_works, dim=128, use_gpu=True, cache_dir=None):
    """
    Obtiene los embeddings densos para los artículos de df_works:
    1. Verifica si existe caché local en .npy / .parquet en data/umap/.
    2. Si no hay caché local, consulta ClickHouse (tabla latin_american_works_embeddings).
    3. Si existen embeddings en ClickHouse, los recupera y los alinea con df_works.
    4. Si faltan embeddings para algunos artículos, calcula solo los faltantes.
    5. Guarda en caché local para acelerar ejecuciones posteriores.
    6. Si ClickHouse no está disponible, cae en generate_article_embeddings como fallback.
    """
    work_ids = df_works['id'].tolist()
    n_works = len(work_ids)

    # 1. Verificar caché local en disco
    if cache_dir is not None:
        npy_path = cache_dir / 'articles_embeddings_cache.npy'
        ids_path = cache_dir / 'articles_ids_cache.parquet'
        if npy_path.exists() and ids_path.exists():
            try:
                print(f'  -> 📂 Verificando caché local en disco ({npy_path})...')
                cached_df = pd.read_parquet(ids_path)
                if len(cached_df) == n_works and cached_df['id'].tolist() == work_ids:
                    embeddings = np.load(npy_path)
                    print(f'  ✅ Matriz de embeddings cargada instantáneamente desde caché local: {embeddings.shape}')
                    return embeddings
                else:
                    print('  -> ℹ️ El caché local no coincide exactamente con el conjunto actual de artículos. Verificando ClickHouse...')
            except Exception as e:
                print(f'  -> ⚠️ Error leyendo caché local ({e}). Verificando ClickHouse...')

    # 2. Verificar ClickHouse
    embeddings_matrix = None
    loaded_from_ch = False

    try:
        if ch_client.is_connected():
            ch_table = "latin_american_works_embeddings"
            count_res = ch_client.query(f"SELECT count() FROM {ch_table}").result_rows
            total_ch = count_res[0][0] if count_res else 0

            if total_ch > 0:
                print(f'  -> 🔍 Detectados {total_ch:,} embeddings guardados en ClickHouse ({ch_table}).')
                print(f'  -> 📥 Recuperando embeddings de ClickHouse en streaming y alineando con {n_works:,} artículos...')
                
                # Detect dimension from first row
                sample_dim_res = ch_client.query(f"SELECT length(embedding) FROM {ch_table} LIMIT 1").result_rows
                emb_dim = sample_dim_res[0][0] if sample_dim_res and sample_dim_res[0][0] else dim

                id_to_idx = {id_: idx for idx, id_ in enumerate(work_ids)}
                embeddings_matrix = np.zeros((n_works, emb_dim), dtype=np.float32)
                matched_mask = np.zeros(n_works, dtype=bool)

                t_ch0 = time.time()
                cl = ch_client.get_client()

                if n_works < 50000:
                    # Modo rápido para muestras/subconjuntos: consultar por bloques de IDs con WHERE id IN
                    chunk_size = 10000
                    for c_start in range(0, n_works, chunk_size):
                        sub_ids = work_ids[c_start:c_start + chunk_size]
                        df_res = cl.query_df(f"SELECT id, embedding FROM {ch_table} WHERE id IN %(ids)s", parameters={'ids': sub_ids})
                        for _, row in df_res.iterrows():
                            idx = id_to_idx.get(row['id'])
                            if idx is not None:
                                embeddings_matrix[idx] = row['embedding']
                                matched_mask[idx] = True
                else:
                    # Modo streaming completo para corpus masivo (3.4M+ artículos)
                    count_stream = 0
                    with cl.query_df_stream(f"SELECT id, embedding FROM {ch_table}", settings={'max_block_size': 50000}) as stream:
                        for chunk in stream:
                            c_ids = chunk['id'].values
                            c_embs = chunk['embedding'].values
                            for _id, _emb in zip(c_ids, c_embs):
                                idx = id_to_idx.get(_id)
                                if idx is not None:
                                    embeddings_matrix[idx] = _emb
                                    matched_mask[idx] = True
                            count_stream += len(chunk)
                            if count_stream % 250000 < len(chunk) or count_stream >= total_ch:
                                pct = (count_stream / total_ch) * 100
                                print(f'     Progreso descarga ClickHouse: {count_stream:,} / {total_ch:,} ({pct:.1f}%) | Alineados: {matched_mask.sum():,}')

                matched_count = int(matched_mask.sum())
                print(f'  ✅ {matched_count:,} / {n_works:,} embeddings alineados exitosamente desde ClickHouse en {time.time() - t_ch0:.1f}s.')

                # Si faltaran algunos IDs, calcular solo los faltantes
                missing_indices = np.where(~matched_mask)[0]
                if len(missing_indices) > 0:
                    print(f'  ⚠️ Faltan {len(missing_indices):,} embeddings en ClickHouse. Calculando únicamente los faltantes...')
                    missing_texts = [df_works['clean_text'].iloc[i] for i in missing_indices]
                    missing_embs = generate_article_embeddings(missing_texts, dim=emb_dim, use_gpu=use_gpu)
                    for idx_pos, orig_idx in enumerate(missing_indices):
                        embeddings_matrix[orig_idx] = missing_embs[idx_pos]
                    print(f'  ✅ {len(missing_indices):,} embeddings faltantes generados e integrados.')

                loaded_from_ch = True

                # Guardar en caché local para acelerar futuras ejecuciones
                if cache_dir is not None:
                    try:
                        print(f'  💾 Guardando matriz de embeddings en caché local para acelerar re-ejecuciones...')
                        np.save(cache_dir / 'articles_embeddings_cache.npy', embeddings_matrix)
                        pd.DataFrame({'id': work_ids}).to_parquet(cache_dir / 'articles_ids_cache.parquet', index=False)
                        print(f'  ✅ Caché local guardado en {cache_dir}.')
                    except Exception as ce:
                        print(f'  -> ℹ️ No se pudo guardar caché local ({ce}).')

                return embeddings_matrix
    except Exception as e:
        print(f'  -> ⚠️ Error al consultar embeddings desde ClickHouse: {e}')
        print('  -> ℹ️ Cayendo al generador de embeddings estándar...')

    # 3. Fallback: Generar todos los embeddings desde cero si ClickHouse no está disponible
    print(f'  -> 🚀 Generando embeddings desde cero ({dim}d)...')
    return generate_article_embeddings(df_works['clean_text'].tolist(), dim=dim, use_gpu=use_gpu)


def build_map(sample_limit=None, per_journal_max=None, batch_size=100000, use_gpu=True, dim=128):
    hw = get_hardware_info()
    
    print('=' * 80)
    print('🚀 GENERADOR DEL PAISAJE CIENTÍFICO DE ARTÍCULOS (SEMÁNTICA PURA: TÍTULO + RESUMEN)')
    print('=' * 80)
    
    if hw['has_cuml'] and use_gpu:
        print(f"⚡ ACELERACIÓN: 🚀 GPU NVIDIA con RAPIDS cuML ({hw['cuda_device']})")
    elif hw['has_torch_cuda'] and use_gpu:
        print(f"⚡ ACELERACIÓN: 🚀 GPU PyTorch CUDA ({hw['cuda_device']})")
    else:
        print(f"⚡ MODO: 💻 CPU Multicore ({hw['cpu_threads']} hilos de procesamiento)")
        
    if sample_limit is None or sample_limit <= 0:
        print(f"📊 MUESTRA: 🌟 TODOS LOS ARTÍCULOS DISPONIBLES (Sin límite artificial)")
    else:
        print(f"📊 MUESTRA: Máximo {sample_limit:,} artículos")
        
    if per_journal_max is None or per_journal_max <= 0:
        print(f"📖 POR REVISTA: Todos los artículos disponibles por revista")
    else:
        print(f"📖 POR REVISTA: Máximo {per_journal_max} artículos por revista")
        
    print(f"📝 TEXTO: Título + Resumen Invertido (SIN tópicos de citas, SIN metadatos de país)")
    print('=' * 80)

    data_dir = BASE_DIR / 'data'
    umap_dir = data_dir / 'umap'
    umap_dir.mkdir(parents=True, exist_ok=True)

    works_file = data_dir / 'latin_american_works.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'

    if not works_file.exists():
        print(f'❌ Error: No se encontró {works_file}')
        return

    # 1. Load journals metadata for fast display mapping
    df_journals = pd.DataFrame()
    if journals_file.exists():
        df_journals = pd.read_parquet(journals_file)
        print(f'✅ Revistas base cargadas: {len(df_journals):,}')

    # 2. Iterate and collect works
    print('\n[Paso 1] Leyendo artículos desde Parquet con procesamiento en streaming...')
    t0 = time.time()
    pf = pq.ParquetFile(works_file)
    total_in_parquet = pf.metadata.num_rows
    print(f'  -> Total de artículos en archivo: {total_in_parquet:,}')

    cols_to_load = [
        'id', 'doi', 'title', 'abstract_inverted_index', 'publication_year',
        'journal_id', 'fwci', 'oa_status', 'is_domestic_author', 'language',
        'cited_by_count'
    ]

    journal_counts = {}
    collected_rows = []
    total_scanned = 0

    for batch in pf.iter_batches(batch_size=batch_size, columns=cols_to_load):
        df_b = batch.to_pandas()
        total_scanned += len(df_b)
        
        # Keep only records with non-empty title
        df_b = df_b[df_b['title'].notna() & (df_b['title'].str.strip() != '')]
        
        if 'publication_year' in df_b.columns:
            df_b = df_b.sort_values('publication_year', ascending=False)

        # Apply per-journal filter if requested
        if per_journal_max is not None and per_journal_max > 0:
            for row in df_b.itertuples(index=False):
                j_id = getattr(row, 'journal_id', None)
                c = journal_counts.get(j_id, 0)
                if c < per_journal_max:
                    journal_counts[j_id] = c + 1
                    collected_rows.append(row._asdict())
                    if sample_limit and sample_limit > 0 and len(collected_rows) >= sample_limit:
                        break
        else:
            # All papers mode
            collected_rows.extend(df_b.to_dict(orient='records'))
            if sample_limit and sample_limit > 0 and len(collected_rows) >= sample_limit:
                collected_rows = collected_rows[:sample_limit]
                break

        if sample_limit and sample_limit > 0 and len(collected_rows) >= sample_limit:
            break

    df_works = pd.DataFrame(collected_rows)
    print(f'  ✅ Seleccionados {len(df_works):,} artículos en {time.time() - t0:.1f}s (Escaneados: {total_scanned:,})')

    # 3. Merge journal display name and country
    if not df_journals.empty:
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

    # 4. Clean Pure Text (Title + Abstract ONLY) — Vectorized for 3.5M scale
    print('\n[Paso 2] Limpiando texto puro (Título + Resumen invertido)...')
    t1 = time.time()
    from multiprocessing.pool import ThreadPool
    titles = df_works['title'].tolist()
    abstracts = df_works['abstract_inverted_index'].tolist() if 'abstract_inverted_index' in df_works.columns else [None] * len(df_works)

    def _clean_row(args):
        t, a = args
        return clean_pure_text(t, a)

    n_threads = min(32, (len(titles) // 50000) + 4)
    with ThreadPool(n_threads) as pool:
        pure_texts = pool.map(_clean_row, zip(titles, abstracts), chunksize=2000)

    df_works['clean_text'] = pure_texts
    print(f'  ✅ {len(pure_texts):,} textos limpios procesados en {time.time() - t1:.1f}s ({n_threads} hilos)')

    # 5. Load Precomputed Embeddings (ClickHouse / Local Cache) or Generate
    print(f'\n[Paso 3] Obteniendo embeddings densos ({dim}d)...')
    t2 = time.time()
    embeddings = load_or_compute_embeddings(df_works, dim=dim, use_gpu=use_gpu, cache_dir=umap_dir)
    print(f'  ✅ Matriz de embeddings lista: {embeddings.shape} en {time.time() - t2:.1f}s')

    # 6. UMAP 2D Projection (GPU cuML / Multicore CPU)
    print('\n[Paso 4] Calculando proyección UMAP 2D con alta dispersión...')
    t3 = time.time()
    coords = project_articles_umap(
        embeddings,
        n_components=2,
        n_neighbors=30,
        min_dist=0.35,
        spread=1.8,
        repulsion_strength=1.5,
        negative_sample_rate=10,
        use_gpu=use_gpu
    )
    df_works['umap_x'] = np.round(coords[:, 0], 4)
    df_works['umap_y'] = np.round(coords[:, 1], 4)
    print(f'  ✅ Coordenadas UMAP 2D generadas en {time.time() - t3:.1f}s')

    # 7. Detect Communities in Intrinsic Dimension (HDBSCAN) & LLM Labeling (Sinapsis AI Methodology)
    print('\n[Paso 5] Clustering en dimensión intrínseca (HDBSCAN) y etiquetado con Centroides + TF-IDF + LLM...')
    t4 = time.time()
    n_comm = min(25, max(10, len(df_works) // 4000))
    cluster_labels = detect_article_communities(embeddings, n_clusters=n_comm, min_cluster_size=150, use_gpu=use_gpu)
    df_works['cluster_id'] = cluster_labels
    cluster_names = label_article_clusters(
        df_works,
        embeddings=embeddings,
        cluster_col='cluster_id',
        title_col='title',
        text_col='clean_text'
    )
    df_works['community_name'] = df_works['cluster_id'].map(cluster_names)
    print(f'  ✅ {len(cluster_names)} comunidades etiquetadas en {time.time() - t4:.1f}s')

    # 8. Save Article Master Landscape Parquet
    out_articles = umap_dir / 'umap_articles_landscape.parquet'
    save_cols = [
        'id', 'doi', 'title', 'publication_year', 'journal_id', 'journal_name',
        'country_code', 'fwci', 'oa_status', 'cited_by_count', 'language',
        'umap_x', 'umap_y', 'cluster_id', 'community_name'
    ]
    avail_cols = [c for c in save_cols if c in df_works.columns]
    df_works[avail_cols].to_parquet(out_articles, index=False)
    mb = out_articles.stat().st_size / (1024 * 1024)
    print(f'\n  💾 Paisaje Maestro de Artículos guardado: {out_articles} ({mb:.1f} MB)')

    # 9. Compute Journal Centroids (Mean Pooling)
    print('\n[Paso 6] Calculando baricentros semánticos de revistas...')
    df_journals_updated = compute_journal_centroids(df_works, df_journals)
    if not df_journals_updated.empty:
        out_journals = umap_dir / 'umap_journals_multimodal.parquet'
        df_journals_updated.to_parquet(out_journals, index=False)
        print(f'  ✅ Baricentros calculados para {df_journals_updated["umap_x"].notna().sum():,} revistas')
        print(f'  💾 Mapa de revistas actualizado: {out_journals}')

    print('\n' + '=' * 80)
    print('✅ MAPA MAESTRO Y PROYECCIÓN DE REVISTAS FINALIZADO CON ÉXITO')
    print('=' * 80)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generar Mapa Semántico Maestro de Artículos LATAM.')
    parser.add_argument('--all', action='store_true', help='Procesar TODOS los 3.6+ millones de artículos sin límite.')
    parser.add_argument('--sample-limit', type=int, default=90000, help='Límite máximo de artículos a procesar (0 o omitido con --all para ilimitado).')
    parser.add_argument('--per-journal-max', type=int, default=100, help='Máximo de artículos por revista (0 para ilimitado).')
    parser.add_argument('--batch-size', type=int, default=100000, help='Tamaño del lote de lectura de Parquet.')
    parser.add_argument('--no-gpu', action='store_true', help='Forzar ejecución en CPU multicore.')
    parser.add_argument('--dim', type=int, default=128, help='Dimensión del vector denso de embedding.')

    args = parser.parse_args()

    s_limit = None if args.all or args.sample_limit <= 0 else args.sample_limit
    j_limit = None if args.all or args.per_journal_max <= 0 else args.per_journal_max
    use_gpu = not args.no_gpu

    build_map(
        sample_limit=s_limit,
        per_journal_max=j_limit,
        batch_size=args.batch_size,
        use_gpu=use_gpu,
        dim=args.dim
    )
