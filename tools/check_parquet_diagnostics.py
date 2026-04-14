import pandas as pd
import os
from pathlib import Path

# Configuración
DATA_DIR = Path('data')
WORKS_FILE = DATA_DIR / 'latin_american_works.parquet'

def check_diagnostics():
    if not WORKS_FILE.exists():
        print(f"❌ No se encontró el archivo: {WORKS_FILE}")
        return

    print("="*60)
    print("DIAGNÓSTICO DE DATOS PARQUET")
    print("="*60)
    
    print(f"📖 Cargando {WORKS_FILE}...")
    df = pd.read_parquet(WORKS_FILE)
    
    # 1. Conteo General
    print(f"\n📊 RESUMEN GENERAL:")
    print(f"  Filas totales: {len(df):,}")
    print(f"  Columnas: {list(df.columns)}")
    
    # 2. Análisis de Tipos
    print(f"\n🧬 TIPOS DE DATOS:")
    print(df.dtypes)
    
    # 3. Análisis de Excelencia (Top 10%)
    if 'is_in_top_10_percent' in df.columns:
        col = df['is_in_top_10_percent']
        print(f"\n🏆 INDICADOR TOP 10%:")
        print(f"  Valores únicos: {col.unique()}")
        print(f"  Conteos:\n{col.value_counts(dropna=False)}")
        
        # Simular lógica del pipeline
        as_bool = pd.to_numeric(col, errors='coerce').fillna(0).astype(bool)
        print(f"  Resultados como Boolean (sum): {as_bool.sum():,} ({ (as_bool.sum()/len(df))*100:.3f}%)")
    else:
        print("\n❌ Columna 'is_in_top_10_percent' NO encontrada.")

    # 4. Análisis de Años
    if 'publication_year' in df.columns:
        print(f"\n📅 RANGO DE AÑOS: {df['publication_year'].min()} - {df['publication_year'].max()}")
        print(f"  Conteos por año (muesra últimos 5):\n{df['publication_year'].value_counts().sort_index().tail(5)}")

    # 5. Muestra de JSON Crudo (si existiera o campos relacionados)
    print("\n🔍 MUESTRA DE DATOS (5 filas):")
    print(df[['id', 'publication_year', 'is_in_top_10_percent']].head())

if __name__ == "__main__":
    check_diagnostics()
