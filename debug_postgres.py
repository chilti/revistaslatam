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
            query_wpl = "SELECT source_id, COUNT(*) as count FROM openalex.works_primary_location WHERE source_id IS NOT NULL GROUP BY source_id LIMIT 5;"
            df_wpl = pd.read_sql_query(query_wpl, conn)
            print("\nSample source_ids in works_primary_location:")
            print(df_wpl)
            
            # Direct count for the sample ID
            query_count = "SELECT COUNT(*) FROM openalex.works_primary_location WHERE source_id = %s;"
            with conn.cursor() as cur:
                cur.execute(query_count, (sample_id,))
                count = cur.fetchone()[0]
                print(f"\nWorks count for {sample_id}: {count}")
                
            # Try with short ID just in case
            short_id = sample_id.split('/')[-1]
            if short_id != sample_id:
                cur.execute(query_count, (short_id,))
                count_short = cur.fetchone()[0]
                print(f"Works count for short ID {short_id}: {count_short}")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_query()
