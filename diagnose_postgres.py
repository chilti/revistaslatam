"""
Script to diagnose PostgreSQL database schema and test connection.
"""
import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

def test_connection():
    """Test database connection and show schema information."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Database connection successful!\n")
        
        # Get all tables in openalex schema
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'openalex'
        ORDER BY table_name;
        """
        
        tables_df = pd.read_sql_query(query, conn)
        
        print("="*70)
        print("TABLES IN openalex SCHEMA")
        print("="*70)
        for table in tables_df['table_name']:
            print(f"  - {table}")
        
        print(f"\nTotal tables: {len(tables_df)}")
        
        # Get row counts for main tables
        print("\n" + "="*70)
        print("ROW COUNTS")
        print("="*70)
        
        main_tables = ['sources', 'works', 'institutions', 'authors', 'works_authorships', 
                       'works_primary_location', 'works_concepts', 'works_open_access']
        
        for table in main_tables:
            try:
                count_query = f"SELECT COUNT(*) as count FROM openalex.{table};"
                count_df = pd.read_sql_query(count_query, conn)
                count = count_df.iloc[0]['count']
                print(f"  {table:30s}: {count:>15,}")
            except Exception as e:
                print(f"  {table:30s}: Table not found or error")
        
        # Check for LATAM data
        print("\n" + "="*70)
        print("LATAM DATA CHECK")
        print("="*70)
        
        latam_countries = ['MX', 'BR', 'AR', 'CL', 'CO', 'PE', 'VE', 'EC', 'CU', 'UY',
                          'CR', 'BO', 'DO', 'GT', 'PY', 'SV', 'HN', 'NI', 'PA', 'PR']
        
        # Count institutions by country
        inst_query = """
        SELECT country_code, COUNT(*) as count
        FROM openalex.institutions
        WHERE country_code = ANY(%s)
        GROUP BY country_code
        ORDER BY count DESC;
        """
        
        inst_df = pd.read_sql_query(inst_query, conn, params=(latam_countries,))
        
        print("\nInstitutions by LATAM country:")
        for _, row in inst_df.iterrows():
            print(f"  {row['country_code']}: {row['count']:,}")
        
        print(f"\nTotal LATAM institutions: {inst_df['count'].sum():,}")
        
        # Sample query to find journals
        print("\n" + "="*70)
        print("SAMPLE: Finding LATAM journals")
        print("="*70)
        
        sample_query = """
        SELECT DISTINCT wpl.source_id
        FROM openalex.works_primary_location wpl
        INNER JOIN openalex.works_authorships wa ON wa.work_id = wpl.work_id
        INNER JOIN openalex.institutions i ON i.id = wa.institution_id
        WHERE i.country_code = ANY(%s)
        LIMIT 10;
        """
        
        sample_df = pd.read_sql_query(sample_query, conn, params=(latam_countries,))
        print(f"\nFound {len(sample_df)} sample journal IDs")
        
        if len(sample_df) > 0:
            # Get details for first journal
            first_journal_id = sample_df.iloc[0]['source_id']
            
            journal_query = """
            SELECT * FROM openalex.sources
            WHERE id = %s;
            """
            
            journal_df = pd.read_sql_query(journal_query, conn, params=(first_journal_id,))
            
            if len(journal_df) > 0:
                print(f"\nSample journal:")
                print(f"  ID: {journal_df.iloc[0]['id']}")
                print(f"  Name: {journal_df.iloc[0]['display_name']}")
                print(f"  Works: {journal_df.iloc[0]['works_count']:,}")
        
        conn.close()
        
        print("\n" + "="*70)
        print("DIAGNOSIS COMPLETE")
        print("="*70)
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_connection()
