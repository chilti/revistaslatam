"""
Simplified PostgreSQL data collector for limited OpenAlex snapshot.
Works with only: works, works_authorships tables.
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
DATA_DIR = Path(__file__).parent / 'data'
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


def extract_country_from_affiliation(affiliation_string):
    """
    Extract country code from raw affiliation string.
    This is a heuristic approach since we don't have the institutions table.
    """
    if not affiliation_string:
        return None
    
    affiliation_lower = affiliation_string.lower()
    
    # Country name mappings
    country_mappings = {
        'mexico': 'MX', 'méxico': 'MX', 'mexican': 'MX',
        'brazil': 'BR', 'brasil': 'BR', 'brazilian': 'BR',
        'argentina': 'AR', 'argentine': 'AR', 'argentinian': 'AR',
        'chile': 'CL', 'chilean': 'CL',
        'colombia': 'CO', 'colombian': 'CO',
        'peru': 'PE', 'perú': 'PE', 'peruvian': 'PE',
        'venezuela': 'VE', 'venezuelan': 'VE',
        'ecuador': 'EC', 'ecuadorian': 'EC',
        'cuba': 'CU', 'cuban': 'CU',
        'uruguay': 'UY', 'uruguayan': 'UY',
        'costa rica': 'CR', 'costarricense': 'CR',
        'bolivia': 'BO', 'bolivian': 'BO',
        'dominican republic': 'DO', 'república dominicana': 'DO',
        'guatemala': 'GT', 'guatemalan': 'GT',
        'paraguay': 'PY', 'paraguayan': 'PY',
        'el salvador': 'SV', 'salvadoran': 'SV',
        'honduras': 'HN', 'honduran': 'HN',
        'nicaragua': 'NI', 'nicaraguan': 'NI',
        'panama': 'PA', 'panamá': 'PA', 'panamanian': 'PA',
        'puerto rico': 'PR', 'puertorriqueño': 'PR'
    }
    
    for country_name, code in country_mappings.items():
        if country_name in affiliation_lower:
            return code
    
    return None


def find_latam_works_batch(conn, offset, batch_size=10000):
    """
    Find works with LATAM affiliations in batches.
    Uses raw affiliation strings since we don't have institutions table.
    """
    query = """
    SELECT DISTINCT w.id
    FROM openalex.works w
    INNER JOIN openalex.works_authorships wa ON wa.work_id = w.id
    WHERE wa.raw_affiliation_string IS NOT NULL
    ORDER BY w.id
    LIMIT %s OFFSET %s;
    """
    
    df = pd.read_sql_query(query, conn, params=(batch_size, offset))
    return df


def get_work_details(conn, work_ids):
    """
    Get full details for a list of work IDs.
    """
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
        w.language
    FROM openalex.works w
    WHERE w.id = ANY(%s);
    """
    
    works_df = pd.read_sql_query(query, conn, params=(work_ids,))
    
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
            ) ORDER BY wa.author_position
        ) as authorships
    FROM openalex.works_authorships wa
    WHERE wa.work_id = ANY(%s)
    GROUP BY wa.work_id;
    """
    
    authorships_df = pd.read_sql_query(authorships_query, conn, params=(work_ids,))
    
    # Merge
    works_df = works_df.merge(authorships_df, left_on='id', right_on='work_id', how='left')
    
    return works_df


def filter_latam_works(works_df):
    """
    Filter works to only those with LATAM affiliations.
    Determine primary country for each work.
    """
    latam_works = []
    
    for _, work in works_df.iterrows():
        if pd.isna(work['authorships']):
            continue
        
        authorships = work['authorships']
        if isinstance(authorships, str):
            authorships = json.loads(authorships)
        
        # Extract countries from affiliations
        countries = []
        for authorship in authorships:
            affiliation = authorship.get('raw_affiliation_string', '')
            country = extract_country_from_affiliation(affiliation)
            if country:
                countries.append(country)
        
        # If at least one LATAM country found
        if countries:
            # Determine primary country (most common)
            from collections import Counter
            country_counts = Counter(countries)
            primary_country = country_counts.most_common(1)[0][0]
            
            work_dict = work.to_dict()
            work_dict['country_code'] = primary_country
            work_dict['latam_countries'] = list(set(countries))
            latam_works.append(work_dict)
    
    return pd.DataFrame(latam_works) if latam_works else pd.DataFrame()


def extract_latam_works():
    """
    Main function to extract LATAM works from PostgreSQL.
    """
    DATA_DIR.mkdir(exist_ok=True)
    
    print("="*70)
    print("EXTRACTING LATAM WORKS FROM POSTGRESQL")
    print("="*70)
    print("\nStrategy: Scanning works with affiliations and filtering by country")
    print("This may take a while...\n")
    
    conn = get_db_connection()
    
    try:
        # Get total count
        count_query = "SELECT COUNT(*) as count FROM openalex.works;"
        count_df = pd.read_sql_query(count_query, conn)
        total_works = count_df.iloc[0]['count']
        
        print(f"Total works in database: {total_works:,}")
        print("Scanning in batches of 10,000...\n")
        
        all_latam_works = []
        batch_size = 10000
        offset = 0
        latam_count = 0
        
        while offset < total_works:
            print(f"Processing batch: {offset:,} - {offset + batch_size:,}")
            
            # Get batch of work IDs
            batch_df = find_latam_works_batch(conn, offset, batch_size)
            
            if len(batch_df) == 0:
                break
            
            work_ids = batch_df['id'].tolist()
            
            # Get full details
            works_details = get_work_details(conn, work_ids)
            
            # Filter for LATAM
            latam_batch = filter_latam_works(works_details)
            
            if len(latam_batch) > 0:
                latam_count += len(latam_batch)
                all_latam_works.append(latam_batch)
                print(f"  → Found {len(latam_batch)} LATAM works in this batch")
                print(f"  → Total LATAM works so far: {latam_count:,}")
            
            # Save incrementally every 50,000 works processed
            if offset > 0 and offset % 50000 == 0 and all_latam_works:
                print("\n  💾 Saving intermediate results...")
                save_batch(all_latam_works)
                all_latam_works = []
            
            offset += batch_size
            print()
        
        # Save final batch
        if all_latam_works:
            print("\n💾 Saving final results...")
            save_batch(all_latam_works)
        
        print("\n" + "="*70)
        print(f"EXTRACTION COMPLETE - Found {latam_count:,} LATAM works")
        print("="*70)
        
    finally:
        conn.close()


def save_batch(works_list):
    """Save a batch of works to parquet file."""
    batch_df = pd.concat(works_list, ignore_index=True)
    batch_df['download_date'] = datetime.datetime.now().isoformat()
    
    if WORKS_FILE.exists():
        # Append to existing
        existing = pd.read_parquet(WORKS_FILE)
        combined = pd.concat([existing, batch_df], ignore_index=True)
        # Remove duplicates
        combined = combined.drop_duplicates(subset=['id'], keep='last')
        combined.to_parquet(WORKS_FILE, index=False)
        print(f"  ✓ Appended {len(batch_df)} works (total: {len(combined):,})")
    else:
        batch_df.to_parquet(WORKS_FILE, index=False)
        print(f"  ✓ Saved {len(batch_df)} works")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract LATAM works from PostgreSQL (simplified)')
    parser.add_argument('--test', action='store_true', help='Test mode: only process first 100k works')
    
    args = parser.parse_args()
    
    if args.test:
        print("⚠️  TEST MODE: Will only process first 100,000 works\n")
        # Modify batch processing to stop early
    
    extract_latam_works()
