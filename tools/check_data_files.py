"""
Script de diagnóstico para verificar el estado de la descarga de datos.
"""
import pandas as pd
import os
from pathlib import Path

# Rutas
data_dir = Path(__file__).parent / 'data'
journals_file = data_dir / 'latin_american_journals.parquet'
works_file = data_dir / 'latin_american_works.parquet'

print("="*70)
print("DIAGNÓSTICO DE DESCARGA DE DATOS")
print("="*70)

# 1. Verificar journals
print("\n📚 JOURNALS (Revistas)")
print("-"*70)
if journals_file.exists():
    journals_df = pd.read_parquet(journals_file)
    print(f"✅ Archivo existe: {journals_file}")
    print(f"   Total de revistas: {len(journals_df):,}")
    
    # Por país
    if 'country_code' in journals_df.columns:
        print(f"\n   Revistas por país:")
        country_counts = journals_df['country_code'].value_counts().sort_index()
        for country, count in country_counts.items():
            print(f"      {country}: {count:,}")
    
    # Fecha de descarga
    if 'download_date' in journals_df.columns:
        dates = pd.to_datetime(journals_df['download_date'])
        print(f"\n   Fecha más antigua: {dates.min()}")
        print(f"   Fecha más reciente: {dates.max()}")
else:
    print(f"❌ Archivo NO existe: {journals_file}")

# 2. Verificar works
print("\n\n📄 WORKS (Trabajos/Artículos)")
print("-"*70)
if works_file.exists():
    # Leer solo columnas necesarias para diagnóstico
    works_df = pd.read_parquet(works_file, columns=['journal_id', 'journal_name', 'download_date'])
    print(f"✅ Archivo existe: {works_file}")
    print(f"   Total de trabajos: {len(works_df):,}")
    
    # Revistas con trabajos descargados
    journals_with_works = works_df['journal_id'].nunique()
    print(f"   Revistas con trabajos descargados: {journals_with_works:,}")
    
    # Trabajos por revista (top 10)
    print(f"\n   Top 10 revistas por número de trabajos:")
    top_journals = works_df.groupby('journal_name').size().sort_values(ascending=False).head(10)
    for journal, count in top_journals.items():
        print(f"      {journal}: {count:,}")
    
    # Fecha de descarga
    if 'download_date' in works_df.columns:
        dates = pd.to_datetime(works_df['download_date'])
        print(f"\n   Fecha más antigua: {dates.min()}")
        print(f"   Fecha más reciente: {dates.max()}")
    
    # Verificar si hay journals sin works
    if journals_file.exists():
        all_journal_ids = set(journals_df['id'].unique())
        downloaded_journal_ids = set(works_df['journal_id'].unique())
        missing = all_journal_ids - downloaded_journal_ids
        
        print(f"\n   Revistas SIN trabajos descargados: {len(missing):,}")
        
        if len(missing) > 0 and len(missing) <= 20:
            print(f"\n   Revistas faltantes:")
            missing_journals = journals_df[journals_df['id'].isin(missing)]
            for _, journal in missing_journals.iterrows():
                print(f"      - {journal.get('display_name', 'N/A')} ({journal.get('country_code', 'N/A')})")
        elif len(missing) > 20:
            print(f"\n   Revistas faltantes por país:")
            missing_journals = journals_df[journals_df['id'].isin(missing)]
            if 'country_code' in missing_journals.columns:
                missing_by_country = missing_journals['country_code'].value_counts().sort_index()
                for country, count in missing_by_country.items():
                    print(f"      {country}: {count:,}")
else:
    print(f"❌ Archivo NO existe: {works_file}")

# 3. Verificar duplicados
print("\n\n🔍 VERIFICACIÓN DE DUPLICADOS")
print("-"*70)
if works_file.exists():
    # Leer columna 'id' de works
    works_ids = pd.read_parquet(works_file, columns=['id'])
    total_works = len(works_ids)
    unique_works = works_ids['id'].nunique()
    duplicates = total_works - unique_works
    
    if duplicates > 0:
        print(f"⚠️  DUPLICADOS ENCONTRADOS!")
        print(f"   Total de trabajos: {total_works:,}")
        print(f"   Trabajos únicos: {unique_works:,}")
        print(f"   Duplicados: {duplicates:,}")
        
        # Encontrar los duplicados
        duplicate_ids = works_ids[works_ids.duplicated(subset=['id'], keep=False)]
        if len(duplicate_ids) > 0:
            print(f"\n   Primeros 10 IDs duplicados:")
            for work_id in duplicate_ids['id'].unique()[:10]:
                count = (works_ids['id'] == work_id).sum()
                print(f"      {work_id}: {count} veces")
    else:
        print(f"✅ No hay duplicados")
        print(f"   Total de trabajos únicos: {unique_works:,}")

# 4. Tamaño de archivos
print("\n\n💾 TAMAÑO DE ARCHIVOS")
print("-"*70)
if journals_file.exists():
    size_mb = journals_file.stat().st_size / (1024 * 1024)
    print(f"   Journals: {size_mb:.2f} MB")

if works_file.exists():
    size_mb = works_file.stat().st_size / (1024 * 1024)
    print(f"   Works: {size_mb:.2f} MB")

print("\n" + "="*70)
print("DIAGNÓSTICO COMPLETO")
print("="*70)
