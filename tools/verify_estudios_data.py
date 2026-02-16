"""
Script para verificar los datos de 'Estudios Demográficos y Urbanos' directamente en PostgreSQL
y comparar con lo que está en el archivo parquet.
"""
import psycopg2
import pandas as pd
import json
from pathlib import Path

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

def check_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Buscar la revista por nombre
        query = """
        SELECT 
            id,
            display_name,
            issn_l,
            works_count,
            cited_by_count,
            is_oa,
            is_in_doaj,
            summary_stats,
            oa_works_count,
            is_in_scielo,
            is_ojs,
            is_core,
            is_scopus
        FROM openalex.sources
        WHERE display_name ILIKE '%Estudios Demográficos%Urbanos%'
        LIMIT 1;
        """
        
        cursor.execute(query)
        row = cursor.fetchone()
        
        if row:
            print("="*70)
            print("DATOS EN POSTGRESQL")
            print("="*70)
            print(f"ID: {row[0]}")
            print(f"Display Name: {row[1]}")
            print(f"ISSN-L: {row[2]}")
            print(f"works_count: {row[3]}")
            print(f"cited_by_count: {row[4]}")
            print(f"is_oa: {row[5]}")
            print(f"is_in_doaj: {row[6]}")
            
            # Parse summary_stats
            summary_stats = row[7]
            if summary_stats:
                if isinstance(summary_stats, str):
                    summary_stats = json.loads(summary_stats)
                print(f"\nsummary_stats:")
                print(f"  h_index: {summary_stats.get('h_index', 'N/A')}")
                print(f"  i10_index: {summary_stats.get('i10_index', 'N/A')}")
                print(f"  2yr_mean_citedness: {summary_stats.get('2yr_mean_citedness', 'N/A')}")
            
            print(f"\noa_works_count: {row[8]}")
            print(f"is_in_scielo: {row[9]}")
            print(f"is_ojs: {row[10]}")
            print(f"is_core: {row[11]}")
            print(f"is_scopus: {row[12]}")
        else:
            print("No se encontró la revista en PostgreSQL")
            
        conn.close()
        
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {e}")
        print("Esto es normal si estás en una máquina diferente al servidor.")

def check_parquet():
    journals_file = Path('data/latin_american_journals.parquet')
    if journals_file.exists():
        df = pd.read_parquet(journals_file)
        target = df[df['display_name'].str.contains('Estudios Demográficos', case=False, na=False)]
        
        if not target.empty:
            journal = target.iloc[0]
            print("\n" + "="*70)
            print("DATOS EN PARQUET (latin_american_journals.parquet)")
            print("="*70)
            print(f"ID: {journal['id']}")
            print(f"Display Name: {journal['display_name']}")
            print(f"works_count: {journal.get('works_count', 'N/A')}")
            print(f"cited_by_count: {journal.get('cited_by_count', 'N/A')}")
            print(f"h_index: {journal.get('h_index', 'N/A')}")
            print(f"i10_index: {journal.get('i10_index', 'N/A')}")
            print(f"2yr_mean_citedness: {journal.get('2yr_mean_citedness', 'N/A')}")
            print(f"is_oa: {journal.get('is_oa', 'N/A')}")
            print(f"is_in_doaj: {journal.get('is_in_doaj', 'N/A')}")
            print(f"oa_works_count: {journal.get('oa_works_count', 'N/A')}")
            print(f"is_in_scielo: {journal.get('is_in_scielo', 'N/A')}")
            print(f"is_ojs: {journal.get('is_ojs', 'N/A')}")
            print(f"is_core: {journal.get('is_core', 'N/A')}")
            print(f"is_scopus: {journal.get('is_scopus', 'N/A')}")

if __name__ == "__main__":
    check_database()
    check_parquet()
