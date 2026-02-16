"""
Script simplificado para diagnosticar trabajos faltantes.
Usa la misma lógica que extract_postgres.py
"""
import pandas as pd
import psycopg2

JOURNAL_ID = 'https://openalex.org/S2737081250'
JOURNAL_NAME = 'Estudios Demográficos y Urbanos'

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

print("="*70)
print(f"DIAGNÓSTICO SIMPLIFICADO: {JOURNAL_NAME}")
print("="*70)

# 1. Contar en parquet
print("\n1. Trabajos en parquet consolidado:")
df_works = pd.read_parquet('data/latin_american_works.parquet', columns=['journal_id'])
count_parquet = (df_works['journal_id'] == JOURNAL_ID).sum()
print(f"   {count_parquet} trabajos")

# 2. Contar en PostgreSQL (usando la misma query que extract_postgres.py)
print("\n2. Trabajos en PostgreSQL:")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Misma query que usa extract_postgres.py
    query = """
        SELECT COUNT(*)
        FROM openalex.works w
        INNER JOIN openalex.works_primary_location wpl ON wpl.work_id = w.id
        WHERE wpl.source_id = %s
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (JOURNAL_ID,))
    count_postgres = cursor.fetchone()[0]
    print(f"   {count_postgres} trabajos")
    
    # 3. Comparación
    print("\n3. Comparación:")
    print(f"   PostgreSQL: {count_postgres}")
    print(f"   Parquet:    {count_parquet}")
    print(f"   Diferencia: {count_postgres - count_parquet}")
    
    if count_postgres > count_parquet:
        print(f"\n   ⚠️ Faltan {count_postgres - count_parquet} trabajos en el parquet")
        print(f"   Esto significa que NO se descargaron todos los trabajos de PostgreSQL")
        
        # Verificar archivos parciales
        from pathlib import Path
        parts_dir = Path('data/works_parts')
        if parts_dir.exists():
            partial_files = list(parts_dir.glob('*S2737081250*.parquet'))
            if partial_files:
                print(f"\n   📁 Archivos parciales encontrados:")
                total_in_parts = 0
                for pf in partial_files:
                    df_part = pd.read_parquet(pf, columns=['journal_id'])
                    count_in_part = (df_part['journal_id'] == JOURNAL_ID).sum()
                    total_in_parts += count_in_part
                    print(f"      {pf.name}: {count_in_part} trabajos")
                
                print(f"\n   Total en parciales: {total_in_parts}")
                
                if total_in_parts >= count_postgres:
                    print(f"   ✅ Los trabajos SÍ están en archivos parciales")
                    print(f"   Solución: Ejecutar consolidate_files_stream.py")
                else:
                    print(f"   ⚠️ Aún faltan {count_postgres - total_in_parts} trabajos")
                    print(f"   Solución: Re-ejecutar extract_postgres.py (sin --journals-only)")
    elif count_postgres == count_parquet:
        print(f"\n   ✅ Todos los trabajos de PostgreSQL están en el parquet")
    else:
        print(f"\n   ⚠️ El parquet tiene MÁS trabajos que PostgreSQL")
        print(f"   Esto es extraño y requiere investigación")
    
    conn.close()
    
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "="*70)
