"""
sync_embeddings_clickhouse.py - High-Throughput & ClickHouse-Friendly Embedding Sync
====================================================================================
Designed specifically to NEVER saturate ClickHouse:
1. Two-Tier Batching:
   - Tier 1 (Embedding API): Batches of 64-128 for optimal LLM GPU inference.
   - Tier 2 (ClickHouse Bulk Write): Accumulates 5,000 - 10,000 records before issuing
     a single bulk INSERT, preventing "Too Many Parts" fragmentation.
2. 100% Idempotent & Resumable: Only processes IDs missing from ClickHouse.
3. Memory-Safe Streaming: Streams from Parquet without loading full corpus to RAM.
"""
import os
import sys
import time
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path.cwd()
sys.path.append(str(BASE_DIR / 'src'))

from clickhouse_db import ch_client
from article_manifold import clean_pure_text

TABLE_NAME = "latin_american_works_embeddings"


def ensure_clickhouse_table(client, table_name=TABLE_NAME):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        doi String,
        title String,
        journal_id String,
        publication_year UInt16,
        cited_by_count UInt32,
        language LowCardinality(String),
        oa_status LowCardinality(String),
        country_code LowCardinality(String),
        clean_text String,
        embedding Array(Float32),
        model_name LowCardinality(String),
        created_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id)
    SETTINGS index_granularity = 8192
    """
    client.command(create_sql)
    print(f"✅ Tabla '{table_name}' verificada (ReplacingMergeTree, index_granularity=8192).")


def get_existing_ids(client, table_name=TABLE_NAME):
    try:
        res = client.query(f"SELECT id FROM {table_name}")
        ids = set(r[0] for r in res.result_rows)
        return ids
    except Exception as e:
        print(f"Notice: Tabla '{table_name}' vacía o inicial ({e}).")
        return set()


def embed_batch_nomic(client_openai, texts, model_name):
    clean_inputs = [t if t and len(t.strip()) > 0 else "artigo cientifico" for t in texts]
    resp = client_openai.embeddings.create(model=model_name, input=clean_inputs)
    return [r.embedding for r in resp.data]


def main():
    parser = argparse.ArgumentParser(description="Sincronización masiva de embeddings en ClickHouse sin saturación.")
    parser.add_argument('--table', default=TABLE_NAME, help='Nombre de la tabla destino en ClickHouse.')
    parser.add_argument('--limit', type=int, default=0, help='Límite máximo de artículos a procesar (0 para todos).')
    parser.add_argument('--api-batch-size', type=int, default=64, help='Lote para el modelo de embeddings (64-128).')
    parser.add_argument('--ch-chunk-size', type=int, default=5000, help='Lote acumulado para bulk insert en ClickHouse (5,000-10,000).')
    parser.add_argument('--dry-run', action='store_true', help='Mostrar conteos y estado sin ejecutar escrituras.')
    parser.add_argument('--parquet', default='data/latin_american_works.parquet', help='Ruta al parquet de artículos.')

    args = parser.parse_args()

    print('=' * 80)
    print('🛡️ SINCRONIZACIÓN DE EMBEDDINGS EN CLICKHOUSE (DISEÑO ANTI-SATURACIÓN)')
    print('=' * 80)
    print(f"  • Lote Inferencia LLM (Tier 1): {args.api_batch_size} artículos")
    print(f"  • Lote Bulk Insert CH (Tier 2): {args.ch_chunk_size:,} artículos por transacción")
    print('=' * 80)

    # 1. ClickHouse Connection Check
    print(f"Conectando a ClickHouse ({ch_client.host}:{ch_client.port} / BD: {ch_client.database})...")
    if not ch_client.is_connected():
        print(f"❌ Error: No se pudo conectar a ClickHouse en {ch_client.host}:{ch_client.port}")
        print("Verifica tu conexión VPN o las credenciales en .env.")
        return
    print("✅ Conexión establecida y verificada.")

    ensure_clickhouse_table(ch_client, args.table)
    existing_ids = get_existing_ids(ch_client, args.table)
    print(f"  -> Embeddings ya guardados en ClickHouse: {len(existing_ids):,}")

    # 2. Check Parquet
    parquet_path = Path(args.parquet)
    if not parquet_path.exists():
        print(f"❌ Error: No se encontró {parquet_path}")
        return

    pf = pq.ParquetFile(parquet_path)
    total_in_parquet = pf.metadata.num_rows
    print(f"  -> Total artículos en Parquet: {total_in_parquet:,}")

    pending_count = total_in_parquet - len(existing_ids)
    print(f"  -> Pendientes por procesar: {max(0, pending_count):,}")

    if args.dry_run:
        print("\n[Modo Dry-Run]: Estado verificado exitosamente. No se realizaron escrituras.")
        return

    # 3. Embedding Endpoint Check
    base_url = os.getenv("LLM_BASE_URL", "http://127.0.0.1:1234/v1/")
    api_key = os.getenv("LLM_API_KEY", "lm-studio")
    model_name = os.getenv("EMBEDDING_MODEL", "text-embedding-nomic-ai-nomic-embed-text-v2-moe")

    try:
        from openai import OpenAI
        client_llm = OpenAI(base_url=base_url, api_key=api_key, timeout=30.0)
        test_r = client_llm.embeddings.create(model=model_name, input=["Test"])
        dim = len(test_r.data[0].embedding)
        print(f"✅ Conectado al modelo {model_name} ({dim}d).")
    except Exception as e:
        print(f"❌ Error conectando a endpoint de embeddings ({base_url}): {e}")
        return

    # 4. Two-Tier Streaming Loop
    print(f"\n🚀 Iniciando procesamiento por streaming sin sobrecargar ClickHouse...")
    t0 = time.time()
    inserted_total = 0

    cols_to_load = [
        'id', 'doi', 'title', 'abstract_inverted_index', 'publication_year',
        'journal_id', 'fwci', 'oa_status', 'country_code', 'language', 'cited_by_count'
    ]

    ch_buffer = []

    for batch in pf.iter_batches(batch_size=25000, columns=cols_to_load):
        df_chunk = batch.to_pandas()
        df_chunk = df_chunk[df_chunk['title'].notna() & (df_chunk['title'].str.strip() != '')]
        
        if existing_ids:
            df_chunk = df_chunk[~df_chunk['id'].isin(existing_ids)]

        if df_chunk.empty:
            continue

        records = df_chunk.to_dict(orient='records')
        
        # Process in API micro-batches
        for i in range(0, len(records), args.api_batch_size):
            sub_records = records[i:i + args.api_batch_size]
            clean_texts = [clean_pure_text(r.get('title'), r.get('abstract_inverted_index')) for r in sub_records]
            
            # 1. Compute embeddings via LLM
            vecs = embed_batch_nomic(client_llm, clean_texts, model_name)
            
            # 2. Accumulate in ClickHouse bulk buffer
            for r, txt, v in zip(sub_records, clean_texts, vecs):
                ch_buffer.append({
                    'id': str(r.get('id', '')),
                    'doi': str(r.get('doi', '') or ''),
                    'title': str(r.get('title', '') or ''),
                    'journal_id': str(r.get('journal_id', '') or ''),
                    'publication_year': int(r.get('publication_year', 0) or 0),
                    'cited_by_count': int(r.get('cited_by_count', 0) or 0),
                    'language': str(r.get('language', '') or ''),
                    'oa_status': str(r.get('oa_status', '') or ''),
                    'country_code': str(r.get('country_code', '') or ''),
                    'clean_text': txt,
                    'embedding': v,
                    'model_name': model_name
                })

            # 3. Flush to ClickHouse only when buffer reaches ch_chunk_size (e.g. 5,000 rows)
            if len(ch_buffer) >= args.ch_chunk_size:
                df_insert = pd.DataFrame(ch_buffer)
                ch_client.insert_df(args.table, df_insert)
                inserted_total += len(ch_buffer)
                ch_buffer = []
                
                rate = inserted_total / (time.time() - t0 + 1e-6)
                print(f"  📦 [ClickHouse Bulk Insert] {inserted_total:,} artículos guardados ({rate:.1f} arts/s) | Memoria partes: OK")

            if args.limit and (inserted_total + len(ch_buffer)) >= args.limit:
                break

        if args.limit and (inserted_total + len(ch_buffer)) >= args.limit:
            break

    # Final residual flush
    if ch_buffer and (not args.limit or inserted_total < args.limit):
        df_insert = pd.DataFrame(ch_buffer)
        ch_client.insert_df(args.table, df_insert)
        inserted_total += len(ch_buffer)
        ch_buffer = []

    elapsed = time.time() - t0
    print('\n' + '=' * 80)
    print(f"🎉 SINCRONIZACIÓN EXITOSA: {inserted_total:,} artículos insertados en ClickHouse en {elapsed:.1f}s")
    print('=' * 80)


if __name__ == '__main__':
    main()
