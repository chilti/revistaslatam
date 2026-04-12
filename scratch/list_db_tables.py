import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

def list_tables():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # Buscar todas las tablas en el esquema 'openalex'
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'openalex' ORDER BY table_name;"
        df = pd.read_sql_query(query, conn)
        print("\n=== TABLAS EN ESQUEMA 'openalex' ===")
        print(df['table_name'].tolist())
        
        # También buscar en el esquema public por si acaso
        query_public = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
        df_public = pd.read_sql_query(query_public, conn)
        print("\n=== TABLAS EN ESQUEMA 'public' ===")
        print(df_public['table_name'].tolist())
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_tables()
