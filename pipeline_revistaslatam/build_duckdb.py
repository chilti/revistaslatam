#!/usr/bin/env python3
"""
Pipeline Step: Build Unified DuckDB Database (revistaslatam.duckdb)
Consolidates Parquet files into a native, high-performance DuckDB database with ART indexes.
"""
import sys
import time
from pathlib import Path
import duckdb

# Determine project directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
CACHE_DIR = DATA_DIR / 'cache'
UMAP_DIR = DATA_DIR / 'umap'
DUCKDB_PATH = DATA_DIR / 'revistaslatam.duckdb'

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

def build_duckdb():
    print("=" * 80)
    print("🦆 INICIANDO CONSTRUCCIÓN DE BASE DE DATOS DUCKDB (revistaslatam.duckdb)")
    print("=" * 80)
    
    t_start = time.time()
    
    # If database file already exists, remove it for a clean build
    if DUCKDB_PATH.exists():
        print(f"🗑️ Eliminando base de datos previa: {DUCKDB_PATH}")
        try:
            DUCKDB_PATH.unlink()
        except Exception as e:
            print(f"⚠️ No se pudo eliminar ({e}). Se sobreescribirán las tablas.")
            
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA memory_limit='8GB';")
    
    # 1. Journals Table (with enriched multimodal metadata)
    umap_path = UMAP_DIR / 'umap_journals_multimodal.parquet'
    journals_path = DATA_DIR / 'latin_american_journals.parquet'
    if umap_path.exists():
        print("📖 1. Consolidando tabla 'journals' (desde multimodal UMAP)...")
        u_str = str(umap_path).replace('\\', '/')
        con.execute(f"CREATE TABLE journals AS SELECT * FROM read_parquet('{u_str}')")
    elif journals_path.exists():
        print("📖 1. Consolidando tabla 'journals' (desde base)...")
        p_str = str(journals_path).replace('\\', '/')
        con.execute(f"CREATE TABLE journals AS SELECT * FROM read_parquet('{p_str}')")
        
    con.execute("CREATE INDEX IF NOT EXISTS idx_journals_id ON journals (id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_journals_country ON journals (country_code);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_journals_issn ON journals (issn_l);")
    print(f"   ✓ Journals indexados: {con.execute('SELECT count(*) FROM journals').fetchone()[0]:,} revistas.")
        
    # 2. Works Table (Articles - 2.48GB)
    works_path = DATA_DIR / 'latin_american_works.parquet'
    if works_path.exists():
        print("\n📄 2. Consolidando tabla 'works' (Artículos)...")
        t_w = time.time()
        w_str = str(works_path).replace('\\', '/')
        con.execute(f"""
        CREATE TABLE works AS 
        SELECT * FROM read_parquet('{w_str}')
        """)
        print(f"   ✓ Trabajos cargados en {time.time() - t_w:.2f}s. Creando índices ART...")
        con.execute("CREATE INDEX IF NOT EXISTS idx_works_journal_id ON works (journal_id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_works_id ON works (id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_works_year ON works (publication_year);")
        print(f"   ✓ Works indexados: {con.execute('SELECT count(*) FROM works').fetchone()[0]:,} artículos.")

    # 3. Metrics Tables
    metrics_map = {
        'metrics_country_annual': CACHE_DIR / 'metrics_country_annual.parquet',
        'metrics_country_period': CACHE_DIR / 'metrics_country_period.parquet',
        'metrics_journal_annual': CACHE_DIR / 'metrics_journal_annual.parquet',
        'metrics_journal_period': CACHE_DIR / 'metrics_journal_period.parquet',
        'metrics_latam_annual': CACHE_DIR / 'metrics_latam_annual.parquet',
        'metrics_latam_period': CACHE_DIR / 'metrics_latam_period.parquet',
        'thematic_evolution': CACHE_DIR / 'thematic_evolution_latam.parquet',
        'collaboration_network': CACHE_DIR / 'collaboration_network.parquet'
    }
    
    print("\n📊 3. Consolidando tablas de métricas y redes...")
    for table_name, parquet_file in metrics_map.items():
        if parquet_file.exists():
            f_str = str(parquet_file).replace('\\', '/')
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{f_str}')")
            print(f"   ✓ Tabla '{table_name}' consolidada ({con.execute(f'SELECT count(*) FROM {table_name}').fetchone()[0]:,} filas)")

    # 4. UMAP Tables
    umap_map = {
        'umap_journals': UMAP_DIR / 'umap_journals_multimodal.parquet',
        'articles_landscape': UMAP_DIR / 'umap_articles_landscape.parquet'
    }
    
    print("\n🗺️ 4. Consolidando variedades topológicas (UMAP)...")
    for table_name, parquet_file in umap_map.items():
        if parquet_file.exists():
            f_str = str(parquet_file).replace('\\', '/')
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{f_str}')")
            print(f"   ✓ Tabla '{table_name}' consolidada ({con.execute(f'SELECT count(*) FROM {table_name}').fetchone()[0]:,} filas)")

    # Checkpoint and optimize
    print("\n⚙️ 5. Optimizando almacenamiento y estadísticas...")
    con.execute("CHECKPOINT;")
    con.close()
    
    elapsed = time.time() - t_start
    size_mb = DUCKDB_PATH.stat().st_size / (1024 * 1024)
    print("\n" + "=" * 80)
    print(f"🎉 BASE DE DATOS DUCKDB CONSTRUIDA EXITOSAMENTE")
    print(f"   Ruta: {DUCKDB_PATH}")
    print(f"   Tamaño: {size_mb:.2f} MB")
    print(f"   Tiempo total: {elapsed:.2f} segundos")
    print("=" * 80)

if __name__ == '__main__':
    build_duckdb()
