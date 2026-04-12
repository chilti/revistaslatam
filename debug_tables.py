import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

def check_all_tables():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("--- ALL TABLES IN openalex SCHEMA ---")
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'openalex'
        ORDER BY table_name;
        """
        df = pd.read_sql_query(query, conn)
        print(df)
        
        # Also check if there's data in 'works' in public schema or elsewhere
        print("\n--- SEARCHING FOR ANY TABLE WITH 'works' IN NAME ---")
        query_any = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%works%'
        ORDER BY table_schema, table_name;
        """
        df_any = pd.read_sql_query(query_any, conn)
        print(df_any)
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_all_tables()
