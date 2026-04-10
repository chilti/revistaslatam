import pandas as pd
from pathlib import Path
import os
import time

def generate_country_sunburst():
    """
    Genera el archivo countries_topics_sunburst.parquet agregando
    los tópicos de journals_topics_sunburst.parquet por país.
    """
    print("=" * 70)
    print("Generando Sunburst Temático a nivel País")
    print("=" * 70)
    
    data_dir = Path(__file__).parent.parent / 'data'
    journals_file = data_dir / 'latin_american_journals.parquet'
    topics_file = data_dir / 'journals_topics_sunburst.parquet'
    output_file = data_dir / 'countries_topics_sunburst.parquet'
    
    if not journals_file.exists():
        print(f"❌ Error: No se encontró el archivo de revistas: {journals_file}")
        return
        
    if not topics_file.exists():
        print(f"❌ Error: No se encontró el archivo de tópicos: {topics_file}")
        return
        
    print(f"  → Cargando {journals_file.name}...")
    journals_df = pd.read_parquet(journals_file)
    print(f"    ✓ {len(journals_df):,} revistas cargadas")
    
    print(f"  → Cargando {topics_file.name}...")
    topics_df = pd.read_parquet(topics_file)
    print(f"    ✓ {len(topics_df):,} registros de tópicos cargados")
    
    print("  → Combinando datos y agregando por país...")
    start_time = time.time()
    
    # Solo necesitamos el mapeo de journal_id a country_code
    journal_country_map = journals_df[['id', 'country_code']].rename(columns={'id': 'journal_id'})
    
    # Unir para obtener el país de cada revista en los tópicos
    merged_df = pd.merge(topics_df, journal_country_map, on='journal_id', how='inner')
    
    # Mapeo de columnas para asegurar consistencia con el nivel 4 (topic)
    if 'topic' not in merged_df.columns and 'topic_name' in merged_df.columns:
        merged_df = merged_df.rename(columns={'topic_name': 'topic'})

    # Agrupar por país y la jerarquía temática completa (4 niveles)
    group_cols = ['country_code', 'domain', 'field', 'subfield', 'topic']
    
    # Verificar disponibilidad de cada nivel
    available_group_cols = [col for col in group_cols if col in merged_df.columns]
    
    missing = set(group_cols) - set(available_group_cols)
    if missing:
        print(f"⚠️ Advertencia: Faltan niveles en la jerarquía: {missing}")
        
    country_topics = merged_df.groupby(available_group_cols, as_index=False)['count'].sum()
    
    # Opcional: Calcular el nuevo 'share' a nivel país
    # Guardamos el total por país para calcular los porcentajes
    country_totals = country_topics.groupby('country_code')['count'].sum().reset_index()
    country_totals.rename(columns={'count': 'country_total_count'}, inplace=True)
    
    country_topics = pd.merge(country_topics, country_totals, on='country_code')
    country_topics['share'] = country_topics['count'] / country_topics['country_total_count']
    
    # Limpiar
    country_topics.drop(columns=['country_total_count'], inplace=True)
    
    # Guardar
    print(f"  → Guardando {len(country_topics):,} registros agregados en {output_file.name}...")
    country_topics.to_parquet(output_file, index=False)
    
    elapsed = time.time() - start_time
    print(f"✅ ¡Agregación por País completada exitosamente en {elapsed:.1f}s!")

if __name__ == "__main__":
    generate_country_sunburst()
