import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

def debug_query():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        print("--- TABLE STATS ---")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM openalex.sources;")
            print(f"Sources count: {cur.fetchone()[0]}")
            cur.execute("SELECT COUNT(*) FROM openalex.works;")
            print(f"Works count: {cur.fetchone()[0]}")
            cur.execute("SELECT COUNT(*) FROM openalex.works_primary_location;")
            print(f"Works_primary_location count: {cur.fetchone()[0]}")
        
        print("\n--- SAMPLE WORKS IDs ---")
        query_works = "SELECT id, title FROM openalex.works LIMIT 10;"
        df_works = pd.read_sql_query(query_works, conn)
        print(df_works)
        
        print("\n--- TRYING CROSS-TABLE MATCH ---")
        query_cross = """
        SELECT wpl.work_id as wpl_id, w.id as w_id 
        FROM openalex.works_primary_location wpl 
        INNER JOIN openalex.works w ON w.id = wpl.work_id 
        LIMIT 5;
        """
        df_cross = pd.read_sql_query(query_cross, conn)
        print("Direct Join Result:")
        print(df_cross)
        
        if df_cross.empty:
            print("\nNo direct match found! Checking for format difference (Short vs Long)...")
            query_format = """
            SELECT wpl.work_id as wpl_id, w.id as w_id 
            FROM openalex.works_primary_location wpl, openalex.works w 
            WHERE w.id = split_part(wpl.work_id, '/', 4) 
            LIMIT 5;
            """
            df_format = pd.read_sql_query(query_format, conn)
            print("Join with Split Result (w.id = short):")
            print(df_format)

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_query()
