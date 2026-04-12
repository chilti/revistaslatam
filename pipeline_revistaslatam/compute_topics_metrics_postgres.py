#!/usr/bin/env python3
"""
Topic-level metrics calculation for Postgres pipeline.
Aggregates performance metrics (FWCI, Percentile, OA) at each level 
of the topic hierarchy (Domain, Field, Subfield).
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
import time

# Add src to path if needed (assuming structure)
# sys.path.append(str(Path(__file__).parent / 'src'))

def calculate_metrics_for_group(group_df):
    """
    Calculates the standard suite of metrics for a given subset of works.
    Matches the logic used in other pipeline scripts.
    """
    total = len(group_df)
    if total == 0:
        return pd.Series({
            'count': 0,
            'fwci_avg': 0.0,
            'avg_percentile': 0.0,
            'pct_top_10': 0.0,
            'pct_top_1': 0.0,
            'pct_oa_diamond': 0.0,
            'pct_oa_gold': 0.0,
            'pct_oa_green': 0.0,
            'pct_oa_hybrid': 0.0,
            'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0
        })

    # Numeric metrics
    fwci = pd.to_numeric(group_df['fwci'], errors='coerce').mean()
    percentile = pd.to_numeric(group_df['citation_normalized_percentile'], errors='coerce').mean()
    
    # Booleans / Binary counts
    top10 = (pd.to_numeric(group_df['is_in_top_10_percent'], errors='coerce').fillna(0).astype(bool).sum() / total) * 100
    top1 = (pd.to_numeric(group_df['is_in_top_1_percent'], errors='coerce').fillna(0).astype(bool).sum() / total) * 100
    
    # OA Status
    oa_counts = group_df['oa_status'].value_counts()
    
    return pd.Series({
        'count': total,
        'fwci_avg': round(fwci, 3) if pd.notna(fwci) else 0.0,
        'avg_percentile': round(percentile, 1) if pd.notna(percentile) else 0.0,
        'pct_top_10': round(top10, 2),
        'pct_top_1': round(top1, 2),
        'pct_oa_diamond': round((oa_counts.get('diamond', 0) / total) * 100, 2),
        'pct_oa_gold': round((oa_counts.get('gold', 0) / total) * 100, 2),
        'pct_oa_green': round((oa_counts.get('green', 0) / total) * 100, 2),
        'pct_oa_hybrid': round((oa_counts.get('hybrid', 0) / total) * 100, 2),
        'pct_oa_bronze': round((oa_counts.get('bronze', 0) / total) * 100, 2),
        'pct_oa_closed': round((oa_counts.get('closed', 0) / total) * 100, 2)
    })

    
def calculate_from_agg(df):
    total_docs = df['count'].sum()
    if total_docs == 0:
        return pd.Series({
            'count': 0, 'fwci_avg': 0.0, 'avg_percentile': 0.0,
            'pct_top_10': 0.0, 'pct_top_1': 0.0, 'pct_oa_gold': 0.0,
            'pct_oa_green': 0.0, 'pct_oa_hybrid': 0.0, 'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0
        })
    
    # Weighted averages for FWCI and Percentile
    fwci = (df['fwci_avg'] * df['count']).sum() / total_docs
    perc = (df['avg_percentile'] * df['count']).sum() / total_docs
    
    # Weighted averages for percentages
    res = {
        'count': total_docs,
        'fwci_avg': round(fwci, 3),
        'avg_percentile': round(perc, 1)
    }
    for col in ['pct_top_10', 'pct_top_1', 'pct_oa_diamond', 'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed']:
        res[col] = round((df[col] * df['count']).sum() / total_docs, 2)
        
    return pd.Series(res)

def compute_thematic_evolution_legacy(works_df, topics_df, output_path):
    """
    Calcula la evolución anual de indicadores a nivel de LATAM y por tema.
    Asegura que los totales no se inflen y que, si hay mapeo granular, se use.
    """
    print("\n📈 Computing LATAM Thematic Evolution...")
    
    data_dir = Path(output_path).parent.parent
    mapping_file = data_dir / 'works_topics_mapping.parquet'
    
    if mapping_file.exists():
        print("  → Usando mapeo granular para la evolución histórica (Precisión Máxima)")
        mapping_df = pd.read_parquet(mapping_file)
        mapping_df['work_id'] = mapping_df['work_id'].str.replace('https://openalex.org/', '', regex=False)
        
        # Merge works with their specific topics
        # Eliminar journal_id del mapeo para evitar colisión
        m_cols = [c for c in mapping_df.columns if c != 'journal_id']
        merged_evo = pd.merge(works_df, mapping_df[m_cols], left_on='id', right_on='work_id')
        
        # Agrupar por revista, año y jerarquía
        group_cols = ['journal_id', 'publication_year', 'domain', 'field', 'subfield', 'topic_name']
        try:
            df_evo = merged_evo.groupby(group_cols).apply(calculate_metrics_for_group, include_groups=False).reset_index()
        except TypeError:
            df_evo = merged_evo.groupby(group_cols).apply(calculate_metrics_for_group).reset_index()
        
        df_evo = df_evo.rename(columns={'topic_name': 'topic', 'publication_year': 'year', 'count': 'num_documents'})
    else:
        print("  ⚠️ Usando método uniforme con 'share' para evitar inflación (Legacy Mode)")
        # 1. Agrupar por revista y año
        try:
            j_year_metrics = works_df.groupby(['journal_id', 'publication_year']).apply(calculate_metrics_for_group, include_groups=False).reset_index()
        except TypeError:
            j_year_metrics = works_df.groupby(['journal_id', 'publication_year']).apply(calculate_metrics_for_group).reset_index()
        
        j_year_metrics = j_year_metrics.rename(columns={'count': 'num_documents', 'publication_year': 'year'})
        
        # 2. Unir con los tópicos de las revistas pero aplicando el share
        df_evo = pd.merge(j_year_metrics, topics_df[['journal_id', 'topic_name', 'subfield', 'field', 'domain', 'share']], on='journal_id')
        
        # CORRECCIÓN DE INFLACIÓN: Multiplicar conteos por el share del tema
        df_evo['num_documents'] = df_evo['num_documents'] * df_evo['share']
        df_evo = df_evo.rename(columns={'topic_name': 'topic'})

    # 3. Calcular pct_oa_total (Suma de todas las vías OA) para compatibilidad
    oa_cols = ['pct_oa_diamond', 'pct_oa_gold', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze']
    for col in oa_cols:
        if col not in df_evo.columns: df_evo[col] = 0
    df_evo['pct_oa_total'] = df_evo[oa_cols].sum(axis=1).clip(0, 100)
    
    # Save base evolution
    df_evo.to_parquet(output_path, index=False)
    print(f"  ✓ Saved evolution with {len(df_evo)} records.")

def aggregate_granular(works_df, mapping_df, group_cols, suffix=""):
    """
    Agregación granular usando el mapeo de tópicos por artículo.
    Permite que cada tema tenga sus propios indicadores.
    """
    if len(works_df) == 0:
        return pd.DataFrame()

    print(f"  → Agregando datos granulares para {len(works_df)} artículos ({suffix})...")
    
    # Unir trabajos con sus tópicos
    # Eliminar journal_id del mapeo si existe para evitar colisión con el de works_df
    mapping_cols = mapping_df.columns.tolist()
    if 'journal_id' in mapping_cols:
        mapping_cols.remove('journal_id')
    
    merged = pd.merge(
        works_df, 
        mapping_df[mapping_cols], 
        left_on='id', 
        right_on='work_id', 
        how='left'
    )
    
    # Manejar artículos sin tópicos (Data Integrity)
    hierarchy_cols = ['domain', 'field', 'subfield', 'topic_name']
    for col in hierarchy_cols:
        merged[col] = merged[col].fillna('Sin Clasificación')
    
    if merged.empty:
        return pd.DataFrame()

    results_list = []
    levels = ['domain', 'field', 'subfield', 'topic_name']
    
    for i, level_col in enumerate(levels):
        current_hierarchy = levels[:i+1]
        grouping = group_cols + current_hierarchy
        
        # Agrupar y calcular
        try:
            agg = merged.groupby(grouping).apply(calculate_metrics_for_group, include_groups=False).reset_index()
        except TypeError:
            agg = merged.groupby(grouping).apply(calculate_metrics_for_group).reset_index()
            
        # Renombrar columna actual a 'topic'
        agg['level'] = level_col.replace('_name', '')
        agg['topic'] = agg[level_col]
        
        # Rellenar jerarquía faltante con 'ALL' para compatibilidad
        for l_name in ['domain', 'field', 'subfield', 'topic']:
            if l_name not in agg.columns:
                agg[l_name] = 'ALL'
        
        results_list.append(agg)
        
    final_df = pd.concat(results_list, ignore_index=True)
    
    # Limpiar columnas de entrada que ya no necesitamos (usamos 'topic')
    for col_to_drop in ['topic_name', 'domain_name', 'field_name', 'subfield_name']:
        if col_to_drop in final_df.columns:
            final_df = final_df.drop(columns=[col_to_drop])
    
    # Renombrar columnas para el dashboard
    # Suffix ya incluye el guion bajo si es necesario (ej: _recent)
    metric_cols = [c for c in final_df.columns if c not in (group_cols + levels + ['topic', 'level'])]
    rename_cols = {col: f"{col}{suffix}" for col in metric_cols}
    final_df = final_df.rename(columns=rename_cols)
    
    # Normalizar nombres de columnas de jerarquía para el merge final
    # (Ya eliminados arriba)
        
    return final_df

def aggregate_hierarchy_from_agg(df, group_cols, suffix=""):
    """Fallback: Agregación basada en perfiles de revista (uniforme)"""
    levels = ['domain', 'field', 'subfield', 'topic']
    all_results = []
    
    # Helper for modern pandas compatibility
    def apply_with_groups_fix(obj, func):
        try:
            return obj.apply(func, include_groups=False)
        except TypeError:
            return obj.apply(func)

    print(f"  → Aggregating level: Topic ({suffix})...")
    res_topic = apply_with_groups_fix(df.groupby(group_cols + levels), calculate_from_agg).reset_index()
    res_topic['level'] = 'topic'
    all_results.append(res_topic)

    print(f"  → Aggregating level: Subfield ({suffix})...")
    res_sub = apply_with_groups_fix(df.groupby(group_cols + ['domain', 'field', 'subfield']), calculate_from_agg).reset_index()
    res_sub['topic'] = 'ALL'
    res_sub['level'] = 'subfield'
    all_results.append(res_sub)
    
    print(f"  → Aggregating level: Field ({suffix})...")
    res_field = apply_with_groups_fix(df.groupby(group_cols + ['domain', 'field']), calculate_from_agg).reset_index()
    res_field['subfield'] = 'ALL'
    res_field['topic'] = 'ALL'
    res_field['level'] = 'field'
    all_results.append(res_field)
    
    print(f"  → Aggregating level: Domain ({suffix})...")
    res_domain = apply_with_groups_fix(df.groupby(group_cols + ['domain']), calculate_from_agg).reset_index()
    res_domain['field'] = 'ALL'
    res_domain['subfield'] = 'ALL'
    res_domain['topic'] = 'ALL'
    res_domain['level'] = 'domain'
    all_results.append(res_domain)

    final_df = pd.concat(all_results, ignore_index=True)
    
    # Apply suffix to metric columns
    if suffix:
        metric_cols = [c for c in final_df.columns if c not in (group_cols + levels + ['level'])]
        rename_cols = {col: f"{col}{suffix}" for col in metric_cols}
        final_df = final_df.rename(columns=rename_cols)
    
    return final_df

def main():
    data_dir = Path(__file__).parent.parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    topics_file = data_dir / 'journals_topics_sunburst.parquet'
    journals_file = data_dir / 'latin_american_journals.parquet'
    
    output_dir = data_dir / 'cache'
    output_dir.mkdir(exist_ok=True)

    print("=" * 70)
    print("POSTGRES TOPIC METRICS ENGINE (PANDAS) - DUAL PERIOD")
    print("=" * 70)

    # 1. Load Data
    print("\n⚙️  Loading data...")
    works_df = pd.read_parquet(works_file, columns=[
        'id', 'journal_id', 'fwci', 'citation_normalized_percentile', 
        'is_in_top_10_percent', 'is_in_top_1_percent', 'oa_status', 'publication_year'
    ])
    
    # Normalizar IDs (quitar prefijo https://openalex.org/)
    works_df['id'] = works_df['id'].str.replace('https://openalex.org/', '', regex=False)
    
    topics_df = pd.read_parquet(topics_file)
    journals_df = pd.read_parquet(journals_file, columns=['id', 'country_code'])
    
    print(f"  ✓ {len(works_df):,} works loaded")
    
    # Merge with journals to get country_code
    print("  → Merging works with journals...")
    # Renombrar id de revista para evitar colisión con id de artículo (KeyError: 'id')
    journals_df = journals_df.rename(columns={'id': 'journal_id_check'})
    works_df = pd.merge(works_df, journals_df, left_on='journal_id', right_on='journal_id_check')
    # Eliminar columna auxiliar
    if 'journal_id_check' in works_df.columns:
        works_df = works_df.drop(columns=['journal_id_check'])
    
    # Cargar mapeo de tópicos si existe
    mapping_file = data_dir / 'works_topics_mapping.parquet'
    mapping_df = None
    if mapping_file.exists():
        print(f"📖 Cargando mapeo de tópicos granular: {mapping_file}")
        mapping_df = pd.read_parquet(mapping_file)
        # Normalizar IDs en el mapeo también
        mapping_df['work_id'] = mapping_df['work_id'].str.replace('https://openalex.org/', '', regex=False)

    # Function to process a specific dataframe subset
    def process_period(df_subset, period_suffix):
        print(f"\n📑 Processing Period: {period_suffix.upper()}...")
        
        if mapping_df is not None:
            # MÉTODO A: Granular (Variación real por tema)
            c_m = aggregate_granular(df_subset, mapping_df, ['country_code'], period_suffix)
            l_m = aggregate_granular(df_subset, mapping_df, [], period_suffix)
            if not l_m.empty: l_m['country_code'] = 'LATAM'
            j_m = aggregate_granular(df_subset, mapping_df, ['journal_id'], period_suffix)
        else:
            # MÉTODO B: Fallback (Uniforme por revista)
            print("⚠️ No hay mapeo granular. Los indicadores por tema serán uniformes a la revista.")
            # Aggregation at journal level
            try:
                j_agg = df_subset.groupby(['journal_id', 'country_code']).apply(calculate_metrics_for_group, include_groups=False).reset_index()
            except TypeError:
                j_agg = df_subset.groupby(['journal_id', 'country_code']).apply(calculate_metrics_for_group).reset_index()
                
            # Topic hierarchy metadata
            j_h = topics_df[['journal_id', 'domain', 'field', 'subfield', 'topic_name', 'share']].copy()
            j_h = j_h.rename(columns={'topic_name': 'topic'})
            j_h['share'] = pd.to_numeric(j_h['share'], errors='coerce').fillna(0.0).astype(float)
            
            # Share normalization
            s_sum = j_h.groupby('journal_id')['share'].transform('sum')
            mask_z = (s_sum <= 0)
            if mask_z.any():
                t_counts = j_h.groupby('journal_id')['journal_id'].transform('count')
                j_h.loc[mask_z, 'share'] = 1.0 / t_counts[mask_z]
            s_sum = j_h.groupby('journal_id')['share'].transform('sum')
            j_h['share'] = j_h['share'] / s_sum
            
            # Merge metrics + topic hierarchy
            enr = pd.merge(j_agg, j_h, on='journal_id')
            enr['count'] = enr['count'] * enr['share']
            
            # Aggregate at hierarchy levels
            c_m = aggregate_hierarchy_from_agg(enr, ['country_code'], period_suffix)
            l_m = aggregate_hierarchy_from_agg(enr, [], period_suffix)
            l_m['country_code'] = 'LATAM'
            j_m = aggregate_hierarchy_from_agg(enr, ['journal_id'], period_suffix)
        
        return c_m, l_m, j_m

    # PERIOD 1: Full
    c_full, l_full, j_full = process_period(works_df, "_full")
    
    # PERIOD 2: Recent (2021-2025)
    recent_mask = (works_df['publication_year'] >= 2021)
    c_recent, l_recent, j_recent = process_period(works_df[recent_mask], "_recent")

    # Combine results
    merge_cols = ['country_code', 'domain', 'field', 'subfield', 'topic', 'level']
    final_country = pd.merge(c_full, c_recent, on=merge_cols, how='outer').fillna(0)
    final_latam = pd.merge(l_full, l_recent, on=merge_cols, how='outer').fillna(0)
    
    merge_cols_j = ['journal_id', 'domain', 'field', 'subfield', 'topic', 'level']
    final_journal = pd.merge(j_full, j_recent, on=merge_cols_j, how='outer').fillna(0)

    # Save
    final_country.to_parquet(output_dir / 'sunburst_metrics_country.parquet', index=False)
    final_latam.to_parquet(output_dir / 'sunburst_metrics_latam.parquet', index=False)
    final_journal.to_parquet(output_dir / 'sunburst_metrics_journal.parquet', index=False)

    # NEW: Evolutionary thematic data
    compute_thematic_evolution_legacy(works_df, topics_df, output_dir / 'thematic_evolution_latam.parquet')

    print(f"\n✅ Topic Metrics (Dual Period) Saved to {output_dir}")

if __name__ == "__main__":
    main()
