#!/usr/bin/env python3
"""
Topic-level hierarchical metrics calculation (ClickHouse Native Vectorized Aggregation).
Aggregates dual-period (Full and Recent 2021-2025) 4-level metrics
(Domain, Field, Subfield, Topic) for Journals, Countries, and Region (LATAM).
Uses native physical columns in ClickHouse to eliminate 'Sin Clasificación' gaps
and avoid external API rate limits.
"""
import sys
import os
import time
from pathlib import Path
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Cargar variables de entorno (.env)
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
CACHE_DIR = DATA_DIR / 'cache'
JOURNALS_FILE = DATA_DIR / 'latin_american_journals.parquet'

# ClickHouse connection params
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def get_client():
    """Establece conexión nativa con ClickHouse con soporte para consultas grandes."""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
        settings={'max_query_size': 10485760}
    )

def format_in_clause(id_list):
    """Devuelve un formato seguro para la cláusula IN de ClickHouse."""
    if not id_list:
        return "('')"
    if len(id_list) == 1:
        return f"('{id_list[0]}')"
    return str(tuple(id_list))

def query_rollup_hierarchy(client, group_col, group_ids, where_extra="", chunk_size=500):
    """
    Ejecuta agregación jerárquica con ROLLUP en ClickHouse.
    Soporta chunks para evitar saturar memoria en lotes muy grandes.
    """
    dfs = []
    ids_list = list(group_ids)
    
    id_select = f"{group_col}," if group_col else ""
    group_by = f"{group_col}, " if group_col else ""
    
    total_chunks = max(1, (len(ids_list) + chunk_size - 1) // chunk_size) if group_col else 1
    
    for i in range(0, max(1, len(ids_list)), chunk_size):
        chunk = ids_list[i:i+chunk_size] if group_col else []
        chunk_filter = f"AND source_id IN {format_in_clause(chunk)}" if chunk else ""
        
        query = f"""
        SELECT 
            {id_select}
            if(domain = '', 'Sin Clasificación', domain) as domain,
            if(field = '', 'Sin Clasificación', field) as field,
            if(subfield = '', 'Sin Clasificación', subfield) as subfield,
            if(topic = '', 'Sin Clasificación', topic) as topic,
            count() as count,
            avg(fwci) as fwci_avg,
            avg(percentile) as avg_percentile,
            (sum(is_top_10) / count()) * 100 as pct_top_10,
            (sum(is_top_1) / count()) * 100 as pct_top_1,
            (countIf(oa_status = 'diamond') / count()) * 100 as pct_oa_diamond,
            (countIf(oa_status = 'gold') / count()) * 100 as pct_oa_gold,
            (countIf(oa_status = 'green') / count()) * 100 as pct_oa_green,
            (countIf(oa_status = 'hybrid') / count()) * 100 as pct_oa_hybrid,
            (countIf(oa_status = 'bronze') / count()) * 100 as pct_oa_bronze,
            (countIf(oa_status = 'closed') / count()) * 100 as pct_oa_closed
        FROM works
        WHERE 1=1 {chunk_filter} {where_extra}
        GROUP BY {group_by}domain, field, subfield, topic WITH ROLLUP
        """
        
        chunk_df = client.query_df(query)
        if not chunk_df.empty:
            dfs.append(chunk_df)
            
        if total_chunks > 1 and ((i // chunk_size) + 1) % 5 == 0:
            print(f"    ... procesado chunk {(i // chunk_size) + 1}/{total_chunks}")
            
    if not dfs:
        return pd.DataFrame()
        
    full_df = pd.concat(dfs, ignore_index=True)
    return full_df

def process_rollup_dataframe(df_raw, id_col='journal_id'):
    """
    Asigna niveles ('domain', 'field', 'subfield', 'topic') y estandariza
    las columnas jerárquicas según el contrato esperado por la API/Frontend.
    """
    if df_raw.empty:
        return pd.DataFrame()
        
    # Eliminar filas totales del ROLLUP (donde domain queda vacío)
    mask_valid = (df_raw['domain'] != '')
    if id_col and id_col in df_raw.columns:
        mask_valid = mask_valid & (df_raw[id_col] != '')
    df = df_raw[mask_valid].copy()
    
    # Determinar nivel jerárquico según las columnas vacías generadas por ROLLUP
    def get_level(r):
        if r['topic'] != '': return 'topic'
        if r['subfield'] != '': return 'subfield'
        if r['field'] != '': return 'field'
        return 'domain'
        
    df['level'] = df.apply(get_level, axis=1)
    
    # Formatear la jerarquía según el contrato de la API:
    # Cuando level == 'domain', field='ALL', subfield='ALL', topic=domain
    # Cuando level == 'field', subfield='ALL', topic=field
    # Cuando level == 'subfield', topic=subfield
    # Cuando level == 'topic', topic=topic_name
    df['topic'] = df.apply(lambda r: r[r['level']], axis=1)
    df['field'] = df.apply(lambda r: 'ALL' if r['level'] == 'domain' else r['field'], axis=1)
    df['subfield'] = df.apply(lambda r: 'ALL' if r['level'] in ('domain', 'field') else r['subfield'], axis=1)
    
    # Redondear métricas
    df['fwci_avg'] = df['fwci_avg'].round(3)
    df['avg_percentile'] = df['avg_percentile'].round(1)
    for col in ['pct_top_10', 'pct_top_1', 'pct_oa_diamond', 'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed']:
        df[col] = df[col].round(2)
        
    return df

def build_dual_period_sunburst(client, group_col, group_ids, id_rename=None, chunk_size=500):
    """Calcula periodos Full y Recent (2021-2025) y los combina con sufijos."""
    print(f"  → Calculando Periodo FULL (Historico completo)...")
    df_full_raw = query_rollup_hierarchy(client, group_col, group_ids, where_extra="", chunk_size=chunk_size)
    if id_rename and group_col in df_full_raw.columns:
        df_full_raw = df_full_raw.rename(columns={group_col: id_rename})
    target_id_col = id_rename or group_col
    df_full = process_rollup_dataframe(df_full_raw, id_col=target_id_col)
    
    print(f"  → Calculando Periodo RECENT (2021-2025)...")
    df_rec_raw = query_rollup_hierarchy(client, group_col, group_ids, where_extra="AND publication_year >= 2021", chunk_size=chunk_size)
    if id_rename and group_col in df_rec_raw.columns:
        df_rec_raw = df_rec_raw.rename(columns={group_col: id_rename})
    df_recent = process_rollup_dataframe(df_rec_raw, id_col=target_id_col)
    
    merge_keys = ([target_id_col] if target_id_col else []) + ['domain', 'field', 'subfield', 'topic', 'level']
    metric_cols = [c for c in df_full.columns if c not in merge_keys]
    
    df_full_renamed = df_full.rename(columns={c: f"{c}_full" for c in metric_cols})
    df_recent_renamed = df_recent.rename(columns={c: f"{c}_recent" for c in metric_cols})
    
    final_df = pd.merge(df_full_renamed, df_recent_renamed, on=merge_keys, how='outer').fillna(0)
    return final_df

def compute_thematic_evolution(client, all_jids, output_path, chunk_size=500):
    """Calcula la evolución anual de producción y desempeño por tema (1970-2026)."""
    print("\n📈 Calculando Evolución Temática Anual (thematic_evolution_latam)...")
    t0 = time.time()
    
    dfs = []
    ids_list = list(all_jids)
    total_chunks = max(1, (len(ids_list) + chunk_size - 1) // chunk_size)
    
    for i in range(0, len(ids_list), chunk_size):
        chunk = ids_list[i:i+chunk_size]
        chunk_filter = f"source_id IN {format_in_clause(chunk)}"
        
        q = f"""
        SELECT 
            source_id as journal_id,
            publication_year as year,
            if(domain = '', 'Sin Clasificación', domain) as domain,
            if(field = '', 'Sin Clasificación', field) as field,
            if(subfield = '', 'Sin Clasificación', subfield) as subfield,
            if(topic = '', 'Sin Clasificación', topic) as topic,
            count() as num_documents,
            round(avg(fwci), 3) as fwci_avg,
            round(avg(percentile), 1) as avg_percentile,
            round((sum(is_top_10) / count()) * 100, 2) as pct_top_10,
            round((sum(is_top_1) / count()) * 100, 2) as pct_top_1,
            round((countIf(oa_status = 'diamond') / count()) * 100, 2) as pct_oa_diamond,
            round((countIf(oa_status = 'gold') / count()) * 100, 2) as pct_oa_gold,
            round((countIf(oa_status = 'green') / count()) * 100, 2) as pct_oa_green,
            round((countIf(oa_status = 'hybrid') / count()) * 100, 2) as pct_oa_hybrid,
            round((countIf(oa_status = 'bronze') / count()) * 100, 2) as pct_oa_bronze,
            round((countIf(oa_status = 'closed') / count()) * 100, 2) as pct_oa_closed
        FROM works
        WHERE {chunk_filter} AND publication_year >= 1970
        GROUP BY source_id, publication_year, domain, field, subfield, topic
        """
        
        c_df = client.query_df(q)
        if not c_df.empty:
            dfs.append(c_df)
            
        if ((i // chunk_size) + 1) % 5 == 0:
            print(f"    ... procesado chunk {(i // chunk_size) + 1}/{total_chunks}")
            
    if dfs:
        evo_df = pd.concat(dfs, ignore_index=True)
        oa_cols = ['pct_oa_diamond', 'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze']
        evo_df['pct_oa_total'] = evo_df[oa_cols].sum(axis=1).clip(0, 100).round(2)
        evo_df.to_parquet(output_path, index=False)
        print(f"  ✓ Guardada evolución temática ({len(evo_df):,} registros) en {output_path.name} ({time.time() - t0:.1f}s)")
    else:
        print("  ⚠️ No se pudieron generar datos de evolución temática.")

def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("🚀 MOTOR CIENCIOMÉTRICO TEMÁTICO DE ALTO RENDIMIENTO (CLICKHOUSE NATIVO)")
    print("=" * 80)
    
    t_start = time.time()
    
    # 1. Cargar catálogo maestro de revistas
    if not JOURNALS_FILE.exists():
        print(f"❌ ERROR: No se encontró el catálogo de revistas: {JOURNALS_FILE}")
        sys.exit(1)
        
    journals_df = pd.read_parquet(JOURNALS_FILE)
    all_jids = journals_df['id'].tolist()
    print(f"📖 Catálogo cargado: {len(all_jids):,} revistas activas.")
    
    client = get_client()
    
    # =========================================================================
    # 2. SUNBURST REVISTAS (sunburst_metrics_journal.parquet)
    # =========================================================================
    print("\n📦 [1/4] Generando Sunburst Jerárquico por Revista...")
    t0 = time.time()
    df_journals = build_dual_period_sunburst(
        client, 
        group_col="source_id", 
        group_ids=all_jids, 
        id_rename="journal_id", 
        chunk_size=1000
    )
    j_out = CACHE_DIR / 'sunburst_metrics_journal.parquet'
    df_journals.to_parquet(j_out, index=False)
    print(f"  ✓ Sunburst de revistas guardado: {j_out.name} ({len(df_journals):,} filas, {time.time() - t0:.1f}s)")
    
    # =========================================================================
    # 3. SUNBURST PAÍSES (sunburst_metrics_country.parquet)
    # =========================================================================
    print("\n🌎 [2/4] Generando Sunburst Jerárquico por País...")
    t0 = time.time()
    
    # Agrupar revistas por país para consultar cada país en bloque
    countries = [c for c in journals_df['country_code'].dropna().unique() if c]
    country_dfs_full = []
    country_dfs_rec = []
    
    for c_code in countries:
        c_jids = journals_df[journals_df['country_code'] == c_code]['id'].tolist()
        if not c_jids:
            continue
            
        in_str = format_in_clause(c_jids)
        # Full
        df_cf = query_rollup_hierarchy(client, group_col="", group_ids=[], where_extra=f"AND source_id IN {in_str}")
        if not df_cf.empty:
            df_cf['country_code'] = c_code
            df_cf_proc = process_rollup_dataframe(df_cf, id_col='country_code')
            country_dfs_full.append(df_cf_proc)
            
        # Recent
        df_cr = query_rollup_hierarchy(client, group_col="", group_ids=[], where_extra=f"AND source_id IN {in_str} AND publication_year >= 2021")
        if not df_cr.empty:
            df_cr['country_code'] = c_code
            df_cr_proc = process_rollup_dataframe(df_cr, id_col='country_code')
            country_dfs_rec.append(df_cr_proc)
            
    if country_dfs_full and country_dfs_rec:
        c_full = pd.concat(country_dfs_full, ignore_index=True)
        c_rec = pd.concat(country_dfs_rec, ignore_index=True)
        
        merge_keys = ['country_code', 'domain', 'field', 'subfield', 'topic', 'level']
        metric_cols = [c for c in c_full.columns if c not in merge_keys]
        
        c_full = c_full.rename(columns={c: f"{c}_full" for c in metric_cols})
        c_rec = c_rec.rename(columns={c: f"{c}_recent" for c in metric_cols})
        
        df_country = pd.merge(c_full, c_rec, on=merge_keys, how='outer').fillna(0)
        c_out = CACHE_DIR / 'sunburst_metrics_country.parquet'
        df_country.to_parquet(c_out, index=False)
        print(f"  ✓ Sunburst de países guardado: {c_out.name} ({len(df_country):,} filas, {time.time() - t0:.1f}s)")
    
    # =========================================================================
    # 4. SUNBURST REGIONAL LATAM (sunburst_metrics_latam.parquet)
    # =========================================================================
    print("\n🌐 [3/4] Generando Sunburst Regional LATAM...")
    t0 = time.time()
    
    latam_in_str = format_in_clause(all_jids)
    
    # Full LATAM
    df_lf = query_rollup_hierarchy(client, group_col="", group_ids=[], where_extra=f"AND source_id IN {latam_in_str}")
    df_lf['country_code'] = 'LATAM'
    df_lf_proc = process_rollup_dataframe(df_lf, id_col='country_code')
    
    # Recent LATAM
    df_lr = query_rollup_hierarchy(client, group_col="", group_ids=[], where_extra=f"AND source_id IN {latam_in_str} AND publication_year >= 2021")
    df_lr['country_code'] = 'LATAM'
    df_lr_proc = process_rollup_dataframe(df_lr, id_col='country_code')
    
    merge_keys = ['country_code', 'domain', 'field', 'subfield', 'topic', 'level']
    metric_cols = [c for c in df_lf_proc.columns if c not in merge_keys]
    
    df_lf_proc = df_lf_proc.rename(columns={c: f"{c}_full" for c in metric_cols})
    df_lr_proc = df_lr_proc.rename(columns={c: f"{c}_recent" for c in metric_cols})
    
    df_latam = pd.merge(df_lf_proc, df_lr_proc, on=merge_keys, how='outer').fillna(0)
    l_out = CACHE_DIR / 'sunburst_metrics_latam.parquet'
    df_latam.to_parquet(l_out, index=False)
    print(f"  ✓ Sunburst regional LATAM guardado: {l_out.name} ({len(df_latam):,} filas, {time.time() - t0:.1f}s)")
    
    # =========================================================================
    # 5. EVOLUCIÓN TEMÁTICA ANUAL (thematic_evolution_latam.parquet)
    # =========================================================================
    print("\n📊 [4/4] Generando Evolución Temática Anual...")
    evo_out = CACHE_DIR / 'thematic_evolution_latam.parquet'
    compute_thematic_evolution(client, all_jids, evo_out, chunk_size=1000)
    
    # =========================================================================
    # 6. TABLA CROSSTAB PAÍSES-TÓPICOS (countries_topics_sunburst.parquet)
    # =========================================================================
    print("\n🗺️ Generando Tabla Temática País-Tópicos (Thematic Profiles)...")
    if 'df_country' in locals() and not df_country.empty:
        ct_df = df_country[df_country['level'] == 'topic'][['country_code', 'domain', 'field', 'subfield', 'topic', 'count_full']].copy()
        ct_df = ct_df.rename(columns={'count_full': 'count'})
        
        c_totals = ct_df.groupby('country_code')['count'].transform('sum')
        ct_df['share'] = np.where(c_totals > 0, ct_df['count'] / c_totals, 0.0)
        
        ct_out1 = DATA_DIR / 'countries_topics_sunburst.parquet'
        ct_out2 = CACHE_DIR / 'countries_topics_metrics.parquet'
        ct_df.to_parquet(ct_out1, index=False)
        ct_df.to_parquet(ct_out2, index=False)
        print(f"  ✓ Guardado perfil de temas por país ({len(ct_df):,} registros) en {ct_out1.name} y {ct_out2.name}")
        
    elapsed = time.time() - t_start
    print("\n" + "=" * 80)
    print(f"🎉 ¡CÁLCULO JERÁRQUICO TEMÁTICO FINALIZADO EXITOSAMENTE EN {elapsed:.1f}s ({elapsed/60:.2f} min)!")
    print("=" * 80)

if __name__ == '__main__':
    main()
