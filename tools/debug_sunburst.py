import pandas as pd
import os
from pathlib import Path

# Rutas
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
CACHE_DIR = DATA_DIR / 'cache'

SUNBURST_FILE = DATA_DIR / 'journals_topics_sunburst.parquet'
JOURNALS_FILE = DATA_DIR / 'latin_american_journals.parquet'

print("="*60)
print("DIAGNÓSTICO DE SUNBURST PARQUET")
print("="*60)
print(f"Directorio Base: {BASE_DIR}")
print(f"Directorio Data: {DATA_DIR}")

# 1. Verificar existencia
print("\n1. Verificando archivos...")
if SUNBURST_FILE.exists():
    print(f"✅ FOUND: {SUNBURST_FILE}")
    print(f"   Tamaño: {SUNBURST_FILE.stat().st_size / 1024:.2f} KB")
else:
    print(f"❌ MISSING: {SUNBURST_FILE}")

# Verificar si está en cache por error
CACHE_FILE = CACHE_DIR / 'journals_topics_sunburst.parquet'
if CACHE_FILE.exists():
    print(f"⚠️  FOUND IN CACHE: {CACHE_FILE}")
    print("   (Debería estar en 'data/', no en 'data/cache/')")

# 2. Intentar leer
if SUNBURST_FILE.exists():
    print("\n2. Leyendo archivo Sunburst...")
    try:
        df = pd.read_parquet(SUNBURST_FILE)
        print("✅ Lectura exitosa")
        print(f"   Filas: {len(df)}")
        print(f"   Columnas: {list(df.columns)}")
        
        # Verificar columnas críticas
        required = ['journal_id', 'topic_name', 'field', 'domain', 'count']
        missing = [c for c in required if c not in df.columns]
        
        if missing:
            print(f"❌ FALTAN COLUMNAS: {missing}")
        else:
            print("✅ Todas las columnas requeridas están presentes")
            
        # Muestra
        print("\n   Ejemplo de datos:")
        print(df.head(2).to_string())
        
        # 3. Cruce con Journals
        if JOURNALS_FILE.exists():
            print("\n3. Verificando cruce con Revistas...")
            j_df = pd.read_parquet(JOURNALS_FILE)
            j_ids = set(j_df['id'].unique())
            s_ids = set(df['journal_id'].unique())
            
            common = j_ids.intersection(s_ids)
            print(f"   Revistas en Journals: {len(j_ids)}")
            print(f"   Revistas en Sunburst: {len(s_ids)}")
            print(f"   Coincidencias: {len(common)}")
            
            if len(common) == 0:
                print("❌ NO HAY COINCIDENCIAS DE ID! (Verifica formato de IDs)")
                print(f"   Ejemplo ID Journal: {list(j_ids)[0]}")
                print(f"   Ejemplo ID Sunburst: {list(s_ids)[0]}")
            else:
                print("✅ Los IDs coinciden correctamente")
                
    except Exception as e:
        print(f"❌ ERROR LEYENDO PARQUET: {e}")

print("\n" + "="*60)
