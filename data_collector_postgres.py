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
    'database': 'openalex',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

# Data directory
DATA_DIR = Path(__file__).parent.parent / 'data'
JOURNALS_FILE = DATA_DIR / 'latin_american_journals.parquet'
WORKS_FILE = DATA_DIR / 'latin_american_works.parquet'

# Latin American country codes
LATAM_COUNTRIES = [
    'MX', 'BR', 'AR', 'CL', 'CO', 'PE', 'VE', 'EC', 'CU', 'UY',
    'CR', 'BO', 'DO', 'GT', 'PY', 'SV', 'HN', 'NI', 'PA', 'PR'
]


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
        # Query to get sources (journals) with institution information
        # We need to join with institutions to get country_code
        query = """
        SELECT DISTINCT
            s.id,
            s.issn_l,
            s.issn,
            s.display_name,
            s.publisher,
            s.works_count,
            s.cited_by_count,
            s.is_oa,
            s.is_in_doaj,
            s.homepage_url,
            s.works_api_url,
            s.updated_date,
            i.country_code
        FROM openalex.sources s
        INNER JOIN openalex.works w ON w.id LIKE '%'
        INNER JOIN openalex.works_primary_location wpl ON wpl.work_id = w.id
        INNER JOIN openalex.works_authorships wa ON wa.work_id = w.id
        INNER JOIN openalex.institutions i ON i.id = wa.institution_id
        WHERE wpl.source_id = s.id
            AND i.country_code = ANY(%s)
        LIMIT 1000;
        """
        
        # Simpler approach: Get journals that have works from LATAM authors
        # This might be too slow, let's try a different approach
        
        # Alternative: If sources table has country_code directly
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
            updated_date
        FROM openalex.sources
        WHERE id IN (
            SELECT DISTINCT wpl.source_id
            FROM openalex.works_primary_location wpl
            INNER JOIN openalex.works_authorships wa ON wa.work_id = wpl.work_id
            INNER JOIN openalex.institutions i ON i.id = wa.institution_id
            WHERE i.country_code = ANY(%s)
        )
        ORDER BY works_count DESC;
        """
        
        print(f"Querying journals with authors from: {', '.join(LATAM_COUNTRIES)}")
        
        df = pd.read_sql_query(query, conn, params=(LATAM_COUNTRIES,))
        
        # Add country_code by finding the most common country for each journal
        print("Determining primary country for each journal...")
        
        for idx, row in df.iterrows():
            # Get the most common country for this journal's works
            country_query = """
            SELECT i.country_code, COUNT(*) as count
            FROM openalex.works_primary_location wpl
            INNER JOIN openalex.works_authorships wa ON wa.work_id = wpl.work_id
            INNER JOIN openalex.institutions i ON i.id = wa.institution_id
            WHERE wpl.source_id = %s
                AND i.country_code = ANY(%s)
            GROUP BY i.country_code
            ORDER BY count DESC
            LIMIT 1;
            """
            
            country_df = pd.read_sql_query(country_query, conn, params=(row['id'], LATAM_COUNTRIES))
            
            if len(country_df) > 0:
                df.at[idx, 'country_code'] = country_df.iloc[0]['country_code']
            else:
                df.at[idx, 'country_code'] = None
            
            if (idx + 1) % 100 == 0:
                print(f"  Processed {idx + 1}/{len(df)} journals...")
        
        # Remove journals without a country code
        df = df[df['country_code'].notna()]
        
        # Add download metadata
        df['download_date'] = datetime.datetime.now().isoformat()
        
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
        wpl.source_id as journal_id
    FROM openalex.works w
    INNER JOIN openalex.works_primary_location wpl ON wpl.work_id = w.id
    WHERE wpl.source_id = %s;
    """
    
    works_df = pd.read_sql_query(query, conn, params=(journal_id,))
    
    if len(works_df) == 0:
        print(f"  No works found for {journal_name}")
        return pd.DataFrame()
    
    # Add journal name
    works_df['journal_name'] = journal_name
    
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
    
    # Get concepts
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
        print("FETCHING WORKS FROM POSTGRESQL")
        print("="*70)
        
        # Check which journals already have works
        downloaded_journal_ids = set()
        if WORKS_FILE.exists():
            existing_works = pd.read_parquet(WORKS_FILE, columns=['journal_id'])
            downloaded_journal_ids = set(existing_works['journal_id'].unique())
            print(f"Found {len(downloaded_journal_ids)} journals already downloaded")
        
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
            
            for idx, journal in journals_to_process.iterrows():
                print(f"[{idx+1}/{len(journals_to_process)}] {journal['display_name']} ({journal['country_code']})")
                
                works_df = fetch_works_for_journal(journal['id'], journal['display_name'], conn)
                
                if len(works_df) > 0:
                    all_works.append(works_df)
                
                # Save incrementally every 10 journals
                if len(all_works) >= 10:
                    print("\n  → Saving batch to disk...")
                    batch_df = pd.concat(all_works, ignore_index=True)
                    
                    if WORKS_FILE.exists():
                        # Append to existing file
                        existing = pd.read_parquet(WORKS_FILE)
                        combined = pd.concat([existing, batch_df], ignore_index=True)
                        combined.to_parquet(WORKS_FILE, index=False)
                    else:
                        batch_df.to_parquet(WORKS_FILE, index=False)
                    
                    print(f"  ✓ Saved {len(batch_df)} works")
                    all_works = []
                
                print()
            
            # Save remaining works
            if len(all_works) > 0:
                print("\n→ Saving final batch...")
                batch_df = pd.concat(all_works, ignore_index=True)
                
                if WORKS_FILE.exists():
                    existing = pd.read_parquet(WORKS_FILE)
                    combined = pd.concat([existing, batch_df], ignore_index=True)
                    combined.to_parquet(WORKS_FILE, index=False)
                else:
                    batch_df.to_parquet(WORKS_FILE, index=False)
                
                print(f"✓ Saved {len(batch_df)} works")
            
            print("\n" + "="*70)
            print("DATA UPDATE COMPLETE")
            print("="*70)
            
        finally:
            conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch OpenAlex data from PostgreSQL')
    parser.add_argument('--journals-only', action='store_true', help='Only fetch journals, not works')
    parser.add_argument('--works-only', action='store_true', help='Only fetch works, not journals')
    
    args = parser.parse_args()
    
    update_journals = not args.works_only
    update_works = not args.journals_only
    
    update_data_from_postgres(update_journals=update_journals, update_works=update_works)
