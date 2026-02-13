"""
Data collector from local PostgreSQL database (OpenAlex snapshot).
This script queries the local PostgreSQL database instead of the OpenAlex API.
"""
import psycopg2
import pandas as pd
import json
import os
from pathlib import Path
import datetime

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',  # Debe coincidir con load_missing_tables.py
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

# Data directory
DATA_DIR = Path(__file__).parent.parent / 'data'
JOURNALS_FILE = DATA_DIR / 'latin_american_journals.parquet'
WORKS_FILE = DATA_DIR / 'latin_american_works.parquet'


def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}") 
        raise


def fetch_latin_american_journals():
    """
    Fetch all journals from Latin American countries from PostgreSQL.
    """
    print("Fetching Latin American journals from PostgreSQL...")
    
    conn = get_db_connection()
    
    try:
        # Query to get sources (journals) directly
        # We assume the sources table only contains LATAM journals (loaded via load_missing_tables.py)
        # and has the country_code column
        query = """
        SELECT 
            id,
            issn_l,
            issn,
            display_name,
            publisher,
            works_count,
            cited_by_count,
            is_oa,
            is_in_doaj,
            homepage_url,
            works_api_url,
            updated_date,
            country_code,
            is_scopus,
            summary_stats
        FROM openalex.sources
        ORDER BY works_count DESC;
        """
        
        print(f"Querying all journals from openalex.sources table...")
        
        try:
            df = pd.read_sql_query(query, conn)
        except Exception as e:
            print(f"Warning: Could not query with country_code/is_scopus. Trying without them: {e}")
            # Fallback if user didn't reload table with country_code
            query_fallback = """
            SELECT 
                id,
                issn_l,
                issn,
                display_name,
                publisher,
                works_count,
                cited_by_count,
                is_oa,
                is_in_doaj,
                homepage_url,
                works_api_url,
                updated_date,
                'UNKNOWN' as country_code,
                False as is_scopus
            FROM openalex.sources
            ORDER BY works_count DESC;
            """
            df = pd.read_sql_query(query_fallback, conn)
        
        # Add download metadata
        df['download_date'] = datetime.datetime.now().isoformat()
        
        print(f"Found {len(df)} Latin American journals.")
        
        # Procesar métricas adicionales desde summary_stats (si existen)
        if 'summary_stats' in df.columns:
            print("Extrayendo métricas de summary_stats (h-index, i10, impacto)...")
            
            def extract_metric(row, metric, default=0):
                stats = row.get('summary_stats')
                if not isinstance(stats, dict) and pd.notna(stats):
                    # Intentar parsear si es string
                    try: stats = json.loads(stats)
                    except: pass
                
                if isinstance(stats, dict):
                    val = stats.get(metric, default)
                    return val if val is not None else default
                return default

            df['h_index'] = df.apply(lambda row: extract_metric(row, 'h_index', 0), axis=1)
            df['i10_index'] = df.apply(lambda row: extract_metric(row, 'i10_index', 0), axis=1)
            df['2yr_mean_citedness'] = df.apply(lambda row: extract_metric(row, '2yr_mean_citedness', 0.0), axis=1)
            
            # Limpiar columna raw para ahorrar espacio
            df = df.drop(columns=['summary_stats'])
        else:
            print("Warning: summary_stats column not found. Metrics will be 0.")
            df['h_index'] = 0
            df['i10_index'] = 0 
            df['2yr_mean_citedness'] = 0.0

        print(f"Found {len(df)} Latin American journals.")
        
        return df
        
    finally:
        conn.close()


def fetch_works_for_journal(journal_id, journal_name, conn):
    """
    Fetch all works for a specific journal from PostgreSQL.
    
    Args:
        journal_id: OpenAlex ID of the journal
        journal_name: Display name of the journal
        conn: Database connection
    
    Returns:
        DataFrame with works data
    """
    print(f"  Fetching works for: {journal_name}...")
    
    # Main works query
    query = """
    SELECT 
        w.id,
        w.doi,
        w.title,
        w.display_name,
        w.publication_year,
        w.publication_date,
        w.type,
        w.cited_by_count,
        w.is_retracted,
        w.is_paratext,
        w.cited_by_api_url,
        w.abstract_inverted_index,
        w.language,
        w.fwci,
        w.citation_normalized_percentile,
        wpl.source_id as journal_id
    FROM openalex.works w
    INNER JOIN openalex.works_primary_location wpl ON wpl.work_id = w.id
    WHERE wpl.source_id = %s;
    """
    
    try:
        works_df = pd.read_sql_query(query, conn, params=(journal_id,))
    except Exception as e:
        print(f"  ⚠️ Error querying columns (fwci/percentile missing?): {e}")
        # Fallback query without new columns IF database schema is old
        query_fallback = """
        SELECT 
            w.id,
            w.doi,
            w.title,
            w.display_name,
            w.publication_year,
            w.publication_date,
            w.type,
            w.cited_by_count,
            w.is_retracted,
            w.is_paratext,
            w.cited_by_api_url,
            w.abstract_inverted_index,
            w.language,
            wpl.source_id as journal_id
        FROM openalex.works w
        INNER JOIN openalex.works_primary_location wpl ON wpl.work_id = w.id
        WHERE wpl.source_id = %s;
        """
        works_df = pd.read_sql_query(query_fallback, conn, params=(journal_id,))
        works_df['fwci'] = 0.0
        works_df['citation_normalized_percentile'] = 0.0
    
    if len(works_df) == 0:
        print(f"  No works found for {journal_name}")
        return pd.DataFrame()
    
    # Add journal name
    works_df['journal_name'] = journal_name
    
    # Compute derived metrics locally (since they might not be in DB explicit columns)
    if 'citation_normalized_percentile' in works_df.columns:
        # Check if it was extracted as decimal (0-100) or we need to handle it
        # Ensure numeric type strictly as per guide
        pct_col = pd.to_numeric(works_df['citation_normalized_percentile'], errors='coerce').fillna(0.0)
        
        # Calculate boolean flags
        # Top 10%: Percentile >= 90.0
        works_df['is_in_top_10_percent'] = pct_col >= 90.0
        
        # Top 1%: Percentile >= 99.0
        works_df['is_in_top_1_percent'] = pct_col >= 99.0
    
    # Fetch additional data for each work
    print(f"  Fetching additional data for {len(works_df)} works...")
    
    # Get authorships
    authorships_query = """
    SELECT 
        wa.work_id,
        json_agg(
            json_build_object(
                'author_position', wa.author_position,
                'author_id', wa.author_id,
                'institution_id', wa.institution_id,
                'raw_affiliation_string', wa.raw_affiliation_string
            )
        ) as authorships
    FROM openalex.works_authorships wa
    WHERE wa.work_id = ANY(%s)
    GROUP BY wa.work_id;
    """
    
    work_ids = works_df['id'].tolist()
    authorships_df = pd.read_sql_query(authorships_query, conn, params=(work_ids,))
    
    # Merge authorships
    works_df = works_df.merge(authorships_df, left_on='id', right_on='work_id', how='left')
    
    # Get concepts (if table exists and has data)
    try:
        concepts_query = """
        SELECT 
            wc.work_id,
            json_agg(
                json_build_object(
                    'concept_id', wc.concept_id,
                    'score', wc.score
                )
            ) as concepts
        FROM openalex.works_concepts wc
        WHERE wc.work_id = ANY(%s)
        GROUP BY wc.work_id;
        """
        
        concepts_df = pd.read_sql_query(concepts_query, conn, params=(work_ids,))
        works_df = works_df.merge(concepts_df, left_on='id', right_on='work_id', how='left', suffixes=('', '_concepts'))
    except:
        print("  Note: works_concepts table empty or not found, skipping concepts")
    
    # Get topics (if exists)
    try:
        topics_query = """
        SELECT 
            wt.work_id,
            json_agg(
                json_build_object(
                    'topic_id', wt.topic_id,
                    'score', wt.score
                )
            ) as topics
        FROM openalex.works_topics wt
        WHERE wt.work_id = ANY(%s)
        GROUP BY wt.work_id;
        """
        
        topics_df = pd.read_sql_query(topics_query, conn, params=(work_ids,))
        works_df = works_df.merge(topics_df, left_on='id', right_on='work_id', how='left', suffixes=('', '_topics'))
    except:
        print("  Note: works_topics table not found, skipping topics")
    
    # Get open access info (if exists)
    try:
        oa_query = """
        SELECT 
            woa.work_id,
            woa.is_oa,
            woa.oa_status,
            woa.oa_url
        FROM openalex.works_open_access woa
        WHERE woa.work_id = ANY(%s);
        """
        
        oa_df = pd.read_sql_query(oa_query, conn, params=(work_ids,))
        works_df = works_df.merge(oa_df, left_on='id', right_on='work_id', how='left', suffixes=('', '_oa'))
    except:
        print("  Note: works_open_access table not found, skipping OA info")
    
    # Add download metadata
    works_df['download_date'] = datetime.datetime.now().isoformat()
    
    # Clean up duplicate columns
    works_df = works_df.loc[:, ~works_df.columns.duplicated()]
    
    print(f"  Found {len(works_df)} works for {journal_name}")
    
    return works_df


def update_data_from_postgres(update_journals=True, update_works=True):
    """
    Main function to update data from PostgreSQL database.
    
    Args:
        update_journals: If True, fetch and update journal metadata
        update_works: If True, fetch works for all journals
    """
    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)
    
    # Step 1: Fetch journals
    if update_journals or not JOURNALS_FILE.exists():
        print("\n" + "="*70)
        print("FETCHING JOURNALS FROM POSTGRESQL")
        print("="*70)
        
        journals_df = fetch_latin_american_journals()
        
        if len(journals_df) > 0:
            print(f"\nSaving {len(journals_df)} journals to {JOURNALS_FILE}...")
            journals_df.to_parquet(JOURNALS_FILE, index=False)
            print("✓ Journals saved successfully")
        else:
            print("No journals found!")
            return
    else:
        print("\n" + "="*70)
        print("LOADING EXISTING JOURNALS")
        print("="*70)
        journals_df = pd.read_parquet(JOURNALS_FILE)
        print(f"Loaded {len(journals_df)} journals from cache")
    
    # Step 2: Fetch works
    if update_works:
        print("\n" + "="*70)
        print("FETCHING WORKS FROM POSTGRESQL (OPTIMIZED MODE)")
        print("="*70)
        
        # Directory for partial files
        PARTS_DIR = DATA_DIR / 'works_parts'
        PARTS_DIR.mkdir(exist_ok=True)
        
        # Check which journals already have works
        downloaded_journal_ids = set()
        
        # 1. Check main file if exists
        if WORKS_FILE.exists():
            try:
                print("Reading existing IDs from main file...")
                existing_works = pd.read_parquet(WORKS_FILE, columns=['journal_id'])
                ids_main = set(existing_works['journal_id'].unique())
                downloaded_journal_ids.update(ids_main)
                print(f"  - Found {len(ids_main)} journals in main file")
            except Exception as e:
                print(f"  Warning reading main file: {e}")
        
        # 2. Check partial files
        print("Reading existing IDs from partial files...")
        part_files = list(PARTS_DIR.glob('*.parquet'))
        for f in part_files:
            try:
                # Read only necessary column, very fast
                ids_part = pd.read_parquet(f, columns=['journal_id'])['journal_id'].unique()
                downloaded_journal_ids.update(ids_part)
            except Exception:
                continue
                
        print(f"Total unique journals already downloaded: {len(downloaded_journal_ids)}")
        
        # Filter journals to process
        journals_to_process = journals_df[~journals_df['id'].isin(downloaded_journal_ids)]
        
        if len(journals_to_process) == 0:
            print("All journals already have works downloaded!")
            return
        
        print(f"Will process {len(journals_to_process)} journals")
        print("="*70 + "\n")
        
        # Open database connection (reuse for all queries)
        conn = get_db_connection()
        
        try:
            all_works = []
            batch_count = 0
            
            for idx, journal in journals_to_process.iterrows():
                print(f"[{idx+1}/{len(journals_to_process)}] {journal['display_name']} ({journal.get('country_code', 'UNKNOWN')})")
                
                try:
                    works_df = fetch_works_for_journal(journal['id'], journal['display_name'], conn)
                    
                    if len(works_df) > 0:
                        all_works.append(works_df)
                except Exception as e:
                    print(f"  ❌ Error fetching works for {journal['display_name']}: {e}")
                    # Continue to next journal
                
                # Save incrementally every 50 journals (larger batch is fine now since we don't read huge file)
                # Or if list gets too big in memory
                if len(all_works) >= 20:
                    batch_count += 1
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    batch_filename = f"batch_{timestamp}_{batch_count}.parquet"
                    batch_path = PARTS_DIR / batch_filename
                    
                    print(f"\n  → Saving batch {batch_count} to {batch_filename}...")
                    try:
                        batch_df = pd.concat(all_works, ignore_index=True)
                        batch_df.to_parquet(batch_path, index=False)
                        print(f"  ✓ Saved {len(batch_df)} works")
                        all_works = [] # Clear memory
                    except Exception as e:
                        print(f"  ❌ Error saving batch: {e}")
                
                print()
            
            # Save remaining works
            if len(all_works) > 0:
                print("\n→ Saving final batch...")
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                batch_filename = f"batch_{timestamp}_final.parquet"
                batch_path = PARTS_DIR / batch_filename
                
                batch_df = pd.concat(all_works, ignore_index=True)
                batch_df.to_parquet(batch_path, index=False)
                print(f"✓ Saved {len(batch_df)} works")
            
            print("\n" + "="*70)
            print("DATA UPDATE COMPLETE")
            print("Run consolidate_works.py to merge all files if needed.")
            print("="*70)
            
        finally:
            conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch OpenAlex data from PostgreSQL')
    parser.add_argument('--journals-only'cd data, action='store_true', help='Only fetch journals, not works')
    parser.add_argument('--works-only', action='store_true', help='Only fetch works, not journals')
    
    args = parser.parse_args()
    
    update_journals = not args.works_only
    update_works = not args.journals_only
    
    update_data_from_postgres(update_journals=update_journals, update_works=update_works)
