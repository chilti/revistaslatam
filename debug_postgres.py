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
        
        print("--- SAMPLE SOURCES ---")
        query_sources = "SELECT id, display_name FROM openalex.sources LIMIT 5;"
        df_sources = pd.read_sql_query(query_sources, conn)
        print(df_sources)
        
        if not df_sources.empty:
            sample_id = df_sources.iloc[0]['id']
            print(f"\n--- CHECKING WORKS FOR ID: {sample_id} ---")
            
            # Check format in works_primary_location
            query_wpl = "SELECT work_id, source_id FROM openalex.works_primary_location WHERE source_id = %s LIMIT 3;"
            df_wpl = pd.read_sql_query(query_wpl, conn, params=(sample_id,))
            print("\nIDs in works_primary_location for this source:")
            print(df_wpl)
            
            if not df_wpl.empty:
                sample_work_id = df_wpl.iloc[0]['work_id']
                print(f"\n--- CHECKING FORMAT IN works TABLE FOR: {sample_work_id} ---")
                
                query_w = "SELECT id, title FROM openalex.works WHERE id = %s LIMIT 1;"
                df_w = pd.read_sql_query(query_w, conn, params=(sample_work_id,))
                print("\nMatch in works table:")
                print(df_w)
                
                if df_w.empty:
                    short_work_id = sample_work_id.split('/')[-1]
                    print(f"No match with long ID. Trying short ID: {short_work_id}")
                    query_w_short = "SELECT id, title FROM openalex.works WHERE id = %s LIMIT 1;"
                    df_w_short = pd.read_sql_query(query_w_short, conn, params=(short_work_id,))
                    print("Match with short ID:")
                    print(df_w_short)
                else:
                    print("Found match with long ID in works table.")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_query()
