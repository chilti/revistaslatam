import psycopg2
import sys
import os
from pathlib import Path

# Intentar importar la configuración del script existente
sys.path.append(os.getcwd())
try:
    from pipeline_revistaslatam.extract_postgres import DB_CONFIG
except ImportError:
    print("No se pudo importar DB_CONFIG de pipeline_revistaslatam/extract_postgres.py")
    # Configuración por defecto si falla la importación
    DB_CONFIG = {
        'host': 'localhost',
        'database': 'openalex_db',
        'user': 'postgres',
        'password': 'tu_contasena',
        'port': 5432
    }

def check_table():
    print(f"Intentando conectar a {DB_CONFIG['database']} como {DB_CONFIG['user']}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Verificar si la tabla existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'openalex'
                AND    table_name   = 'works_topics'
            );
        """)
        exists = cur.fetchone()[0]
        
        if exists:
            print("✅ La tabla 'openalex.works_topics' existe.")
            # Contar filas para ver si tiene datos
            cur.execute("SELECT count(*) FROM openalex.works_topics LIMIT 1 OFFSET 100;")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"📊 La tabla tiene datos (confirmado al menos 100 filas).")
            else:
                print("⚠️ La tabla existe pero parece estar vacía.")
        else:
            print("❌ La tabla 'openalex.works_topics' NO existe en el esquema 'openalex'.")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error de conexión o consulta: {e}")
        print("\nSugerencia: Revisa que el 'password' en pipeline_revistaslatam/extract_postgres.py sea correcto.")

if __name__ == "__main__":
    check_table()
