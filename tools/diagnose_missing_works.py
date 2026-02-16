"""
Script para diagnosticar por qué faltan 96 trabajos de 'Estudios Demográficos y Urbanos'
en latin_american_works.parquet (1889) vs el total en OpenAlex (1985).

Investigará:
1. ¿Están en PostgreSQL pero no se descargaron?
2. ¿Están en archivos parciales no consolidados?
3. ¿Qué años/características tienen los trabajos faltantes?
"""
import pandas as pd
import psycopg2
from pathlib import Path
import json

JOURNAL_ID = 'https://openalex.org/S2737081250'
JOURNAL_NAME = 'Estudios Demográficos y Urbanos'
WORKS_FILE = Path('data/latin_american_works.parquet')
WORKS_PARTS_DIR = Path('data/works_parts')

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

print("="*70)
print(f"DIAGNÓSTICO: Trabajos Faltantes de {JOURNAL_NAME}")
print("="*70)

# 1. Contar en el parquet consolidado
print("\n1️⃣ TRABAJOS EN PARQUET CONSOLIDADO")
print("-"*70)
if WORKS_FILE.exists():
    df_works = pd.read_parquet(WORKS_FILE, columns=['journal_id', 'id', 'publication_year'])
    works_in_parquet = df_works[df_works['journal_id'] == JOURNAL_ID]
    count_parquet = len(works_in_parquet)
    print(f"Trabajos en {WORKS_FILE.name}: {count_parquet}")
    
    # Distribución por año
    year_dist = works_in_parquet['publication_year'].value_counts().sort_index()
    print(f"\nDistribución por año (primeros 10):")
    for year, count in year_dist.head(10).items():
        print(f"  {year}: {count}")
    print(f"  ...")
    for year, count in year_dist.tail(5).items():
        print(f"  {year}: {count}")
else:
    print(f"❌ {WORKS_FILE} no existe")
    count_parquet = 0

# 2. Buscar en archivos parciales
print("\n2️⃣ TRABAJOS EN ARCHIVOS PARCIALES (works_parts/)")
print("-"*70)
if WORKS_PARTS_DIR.exists():
    partial_files = list(WORKS_PARTS_DIR.glob('*.parquet'))
    print(f"Encontrados {len(partial_files)} archivos parciales")
    
    total_in_parts = 0
    for pf in partial_files:
        try:
            df_part = pd.read_parquet(pf, columns=['journal_id'])
            count_in_part = (df_part['journal_id'] == JOURNAL_ID).sum()
            if count_in_part > 0:
                total_in_parts += count_in_part
                print(f"  {pf.name}: {count_in_part} trabajos")
        except:
            pass
    
    print(f"\nTotal en archivos parciales: {total_in_parts}")
    
    if total_in_parts > count_parquet:
        print(f"⚠️ HAY {total_in_parts - count_parquet} TRABAJOS EN PARCIALES NO CONSOLIDADOS")
        print(f"   Solución: Ejecutar python pipeline/consolidate_files_stream.py")
else:
    print(f"❌ Directorio {WORKS_PARTS_DIR} no existe")
    total_in_parts = 0

# 3. Verificar en PostgreSQL
print("\n3️⃣ TRABAJOS EN POSTGRESQL")
print("-"*70)
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Primero, verificar qué columnas tiene la tabla works
    print("Verificando esquema de openalex.works...")
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'openalex' 
        AND table_name = 'works'
        AND column_name LIKE '%location%' OR column_name LIKE '%source%' OR column_name = 'journal_id'
        ORDER BY ordinal_position;
    """)
    
    columns = cursor.fetchall()
    print(f"Columnas relacionadas con journal/source:")
    for col_name, col_type in columns:
        print(f"  - {col_name} ({col_type})")
    
    # Intentar diferentes formas de filtrar por journal
    count_postgres = None
    filter_column = None
    
    # Opción 1: journal_id directo
    try:
        query_count = """
            SELECT COUNT(*) 
            FROM openalex.works 
            WHERE journal_id = %s
        """
        cursor.execute(query_count, (JOURNAL_ID,))
        count_postgres = cursor.fetchone()[0]
        print(f"\n✅ Usando journal_id: {count_postgres} trabajos")
        filter_column = 'journal_id'
    except Exception as e1:
        conn.rollback()  # Importante: resetear transacción
        print(f"⚠️ journal_id no funciona: {e1}")
        
        # Opción 2: host_venue_id
        try:
            query_count = """
                SELECT COUNT(*) 
                FROM openalex.works 
                WHERE host_venue_id = %s
            """
            cursor.execute(query_count, (JOURNAL_ID,))
            count_postgres = cursor.fetchone()[0]
            print(f"\n✅ Usando host_venue_id: {count_postgres} trabajos")
            filter_column = 'host_venue_id'
        except Exception as e2:
            conn.rollback()  # Importante: resetear transacción
            print(f"⚠️ host_venue_id no funciona: {e2}")
            
            # Opción 3: Buscar en el parquet cómo se descargaron
            print(f"\n⚠️ No se encontró columna estándar. Verificando cómo se descargaron los datos...")
            
            # Ver qué columnas tiene realmente la tabla works
            try:
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'openalex' 
                    AND table_name = 'works'
                    ORDER BY ordinal_position
                    LIMIT 50;
                """)
                all_cols = cursor.fetchall()
                print("Columnas de openalex.works:")
                for col_name, col_type in all_cols:
                    print(f"  - {col_name} ({col_type})")
            except:
                conn.rollback()
                pass
            
            filter_column = None
    
    if count_postgres and filter_column:
        # Distribución por año en PostgreSQL
        query_years = f"""
            SELECT publication_year, COUNT(*) as count
            FROM openalex.works 
            WHERE {filter_column} = %s
            GROUP BY publication_year
            ORDER BY publication_year
        """
        cursor.execute(query_years, (JOURNAL_ID,))
        years_pg = cursor.fetchall()
        
        print(f"\nDistribución por año en PostgreSQL (primeros 10):")
        for year, count in years_pg[:10]:
            print(f"  {year}: {count}")
        print(f"  ...")
        for year, count in years_pg[-5:]:
            print(f"  {year}: {count}")
        
        # Comparar con parquet
        print(f"\n4️⃣ COMPARACIÓN POSTGRESQL vs PARQUET")
        print("-"*70)
        
        if count_parquet > 0:
            years_parquet = works_in_parquet['publication_year'].value_counts().to_dict()
            
            missing_by_year = {}
            for year, count_pg in years_pg:
                count_pq = years_parquet.get(year, 0)
                diff = count_pg - count_pq
                if diff > 0:
                    missing_by_year[year] = diff
            
            if missing_by_year:
                print(f"Años con trabajos faltantes:")
                for year in sorted(missing_by_year.keys()):
                    diff = missing_by_year[year]
                    print(f"  {year}: {diff} trabajos faltantes")
                
                print(f"\nTotal faltante: {sum(missing_by_year.values())} trabajos")
            else:
                print(f"✅ No hay discrepancias por año")
        
        # Buscar IDs de trabajos faltantes (primeros 10)
        print(f"\n5️⃣ MUESTRA DE TRABAJOS FALTANTES")
        print("-"*70)
        
        if count_parquet > 0:
            # Obtener IDs en parquet
            ids_parquet = set(works_in_parquet['id'].tolist())
            
            # Obtener IDs en PostgreSQL (limitado a 2000 para no saturar)
            query_ids = f"""
                SELECT id, publication_year, display_name
                FROM openalex.works 
                WHERE {filter_column} = %s
                LIMIT 2000
            """
            cursor.execute(query_ids, (JOURNAL_ID,))
            works_pg = cursor.fetchall()
            
            missing_works = []
            for work_id, year, title in works_pg:
                if work_id not in ids_parquet:
                    missing_works.append((work_id, year, title))
            
            if missing_works:
                print(f"Primeros 10 trabajos faltantes:")
                for work_id, year, title in missing_works[:10]:
                    print(f"  {year} | {work_id}")
                    print(f"       {title[:80] if title else 'Sin título'}...")
            else:
                print(f"✅ Todos los trabajos de la muestra están en el parquet")
    else:
        print("\n⚠️ No se pudo verificar PostgreSQL (columna de filtro no encontrada)")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Error conectando a PostgreSQL: {e}")
    count_postgres = None

# 6. Resumen
print("\n" + "="*70)
print("RESUMEN")
print("="*70)
print(f"OpenAlex metadata (journals): 1985")
print(f"PostgreSQL (works table): {count_postgres if count_postgres else 'N/A'}")
print(f"Archivos parciales: {total_in_parts}")
print(f"Parquet consolidado: {count_parquet}")
print(f"\nDiferencia: {1985 - count_parquet} trabajos")

print("\n💡 POSIBLES CAUSAS:")
if total_in_parts > count_parquet:
    print("  ✅ Hay trabajos en archivos parciales no consolidados")
    print("     Solución: Ejecutar consolidate_files_stream.py")
elif count_postgres and count_postgres > count_parquet:
    print("  ✅ Hay trabajos en PostgreSQL no descargados")
    print("     Solución: Re-ejecutar extract_postgres.py sin --journals-only")
else:
    print("  ⚠️ Los trabajos no están en PostgreSQL")
    print("     Posible causa: Snapshot incompleto o filtros en la carga")
