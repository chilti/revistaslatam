import psycopg2
import sys
import os
from pathlib import Path

# Intentar importar la configuración del script existente
sys.path.append(os.getcwd())
try:
    from pipeline_revistaslatam.extract_postgres import DB_CONFIG
except ImportError:
    DB_CONFIG = {
        'host': 'localhost',
        'database': 'openalex_db',
        'user': 'postgres',
        'password': 'tu_contasena',
        'port': 5432
    }

def check_concepts():
    print(f"Verificando tabla 'openalex.works_concepts'...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'openalex'
                AND    table_name   = 'works_concepts'
            );
        """)
        exists = cur.fetchone()[0]
        
        if exists:
            print("✅ La tabla 'openalex.works_concepts' existe.")
            cur.execute("SELECT count(*) FROM openalex.works_concepts;")
            count = cur.fetchone()[0]
            print(f"📊 Registros encontrados: {count:,}")
        else:
            print("❌ La tabla 'openalex.works_concepts' NO existe.")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_concepts()
