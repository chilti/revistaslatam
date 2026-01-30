import pyalex
from pyalex import Works, Authors, Sources, Institutions, Topics, Publishers, Funders
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import datetime
import time
import json

# Configure PyAlex
pyalex.config.email = "jlja@ciencias.unam.mx" # User email from the notebook

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
PARQUET_FILE = os.path.join(DATA_DIR, 'latin_american_journals.parquet')
WORKS_PARQUET_FILE = os.path.join(DATA_DIR, 'latin_american_works.parquet')

# List of Latin American country codes (ISO 2-letter codes)
LATAM_COUNTRIES = [
    'MX', # Mexico
    'BR', # Brazil
    'AR', # Argentina
    'CL', # Chile
    'CO', # Colombia
    'PE', # Peru
    'VE', # Venezuela
    'EC', # Ecuador
    'CU', # Cuba
    'UY', # Uruguay
    'CR', # Costa Rica
    'BO', # Bolivia
    'DO', # Dominican Republic
    'GT', # Guatemala
    'PY', # Paraguay
    'SV', # El Salvador
    'HN', # Honduras
    'NI', # Nicaragua
    'PA', # Panama
    'PR', # Puerto Rico (often included in LATAM analysis)
]

def fetch_journals_by_country(country_code):
    """
    Fetches sources (journals) from OpenAlex for a specific country code.
    """
    try:
        # Filter sources by country
        # We are looking for journals (type='journal') hosted in the specified country
        query = Sources().filter(country_code=country_code, type='journal')
        # Use paginate to get all results (generator yields pages)
        results = query.paginate(per_page=200)
        
        journals_data = []
        for page in results:
            for source in page:
                # Store complete OpenAlex record
                journal = dict(source)
                # Add metadata
                journal['download_date'] = datetime.datetime.now().isoformat()
                journals_data.append(journal)
            
        return journals_data
    except Exception as e:
        print(f"Error fetching data for {country_code}: {e}")
        return []

def fetch_works_for_journal(journal_id, journal_name):
    """
    Fetches all works (articles) for a specific journal.
    Implements rate limiting to comply with OpenAlex policies (max 10 requests/second for polite pool).
    """
    try:
        print(f"  Fetching works for: {journal_name}...")
        
        # Query works by journal (primary_location.source.id)
        query = Works().filter(primary_location={'source': {'id': journal_id}})
        
        # Use paginate to get all results
        results = query.paginate(per_page=200)
        
        works_data = []
        page_count = 0
        
        for page in results:
            page_count += 1
            
            # Rate limiting: OpenAlex polite pool allows ~10 req/sec
            # We'll be conservative with 0.15s between pages (~6.6 req/sec)
            if page_count > 1:
                time.sleep(0.15)
            
            for work in page:
                # Store complete OpenAlex record
                work_record = dict(work)
                # Add metadata for easier filtering
                work_record['journal_id'] = journal_id
                work_record['journal_name'] = journal_name
                work_record['download_date'] = datetime.datetime.now().isoformat()
                works_data.append(work_record)
            
            # Progress indicator every 5 pages
            if page_count % 5 == 0:
                print(f"    Processed {page_count} pages, {len(works_data)} works so far...")
        
        print(f"  Found {len(works_data)} works for {journal_name}.")
        return works_data
        
    except Exception as e:
        print(f"  Error fetching works for {journal_name}: {e}")
        return []

def update_data(include_works=True):
    """
    Main function to update the dataset for all LATAM countries.
    
    Args:
        include_works: If True, also downloads all works (articles) for each journal.
    """
    all_journals = []
    print("Starting data update from OpenAlex...")
    
    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    for country in LATAM_COUNTRIES:
        print(f"Fetching journals for {country}...")
        country_journals = fetch_journals_by_country(country)
        all_journals.extend(country_journals)
        print(f"Found {len(country_journals)} journals for {country}.")

    if all_journals:
        df = pd.DataFrame(all_journals)
        
        # Save journals to parquet
        print(f"Saving {len(df)} journal records to {PARQUET_FILE}...")
        df.to_parquet(PARQUET_FILE, index=False)
        print("Journal data update complete.")
        
        # Download works if requested
        if include_works:
            print("\n" + "="*60)
            print("Starting Works (articles) download...")
            print(f"This will download articles for {len(df)} journals.")
            print("This may take a considerable amount of time.")
            print("="*60 + "\n")
            
            # Incremental writing with ParquetWriter to avoid reading file into memory
            batch_works = []
            batch_size = 1  # Write every journal
            writer = None
            
            for idx, journal in enumerate(all_journals, 1):
                print(f"[{idx}/{len(all_journals)}] Processing {journal['display_name']}...")
                journal_works = fetch_works_for_journal(journal['id'], journal['display_name'])
                batch_works.extend(journal_works)
                
                # Write batch to parquet and clear memory every batch_size journals
                if idx % batch_size == 0 or idx == len(all_journals):
                    if batch_works:
                        batch_df = pd.DataFrame(batch_works)
                        
                        # PyArrow strict schema enforcement with nested structs is very fragile for evolving data like OpenAlex
                        # Best practice for robustness: Convert all complex types (list, dict) to JSON strings
                        # This ensures the schema is always simple (mostly strings/numbers) and never fails on new nested fields
                        for col in batch_df.columns:
                            # Check if column contains complex types
                            if batch_df[col].dtype == 'object':
                                # Check a sample of non-null values to verify if it's a dict/list
                                sample = batch_df[col].dropna().head(1)
                                if not sample.empty and isinstance(sample.iloc[0], (dict, list)):
                                     # Convert entire column to json string
                                     batch_df[col] = batch_df[col].apply(lambda x: json.dumps(x) if x is not None else None)
                        
                        # Convert to PyArrow Table
                        table = pa.Table.from_pandas(batch_df)
                        
                        if writer is None:
                            # Initialize writer with the schema of the first batch
                            # Fix: Coerce Null-inferred columns to String to accommodate future data
                            # If a column is all nulls in the first batch, PyArrow infers it as Null type.
                            # This causes failures when subsequent batches contain data.
                            new_fields = []
                            for field in table.schema:
                                if field.type == pa.null():
                                    new_fields.append(pa.field(field.name, pa.string()))
                                else:
                                    new_fields.append(field)
                            
                            robust_schema = pa.schema(new_fields)
                            
                            # Cast table to the robust schema
                            try:
                                table = table.cast(robust_schema)
                            except:
                                pass
                            
                            writer = pq.ParquetWriter(WORKS_PARQUET_FILE, robust_schema)
                            print(f"  → Created new Parquet file: {WORKS_PARQUET_FILE}")
                        
                        # Ensure table matches schema of writer (handle missing/extra columns naively)
                        if not table.schema.equals(writer.schema):
                            # Reindex columns to match writer schema (adds missing as null, drops extra)
                            batch_df = batch_df.reindex(columns=writer.schema.names)
                            # Re-apply json conversion to be safe on new nulls
                            
                            # Fallback: recreate table
                            try:
                                table = pa.Table.from_pandas(batch_df, schema=writer.schema)
                            except Exception as e:
                                # Fallback 2: If schema validation fails (e.g. type mismatch), convert confusing columns to string
                                print(f"  Warning: Schema mismatch for batch ({e}). converting object columns to string.")
                                
                                # Iterate over writer schema to find string columns and force conversion
                                for field in writer.schema:
                                    if field.name in batch_df.columns:
                                        # If target is string, ensure input is string
                                        if pa.types.is_string(field.type):
                                            batch_df[field.name] = batch_df[field.name].astype(str)
                                            # Handle "None" stringification if necessary, but astype(str) usually makes 'None' or 'nan'
                                            # Better: apply json.dumps if regex/complex? Or just str()
                                            
                                # Also, for any object column in current batch that isn't in writer schema (dropped above) 
                                # or is object but writer expects something else (unlikely if first batch defined it)
                                
                                # Brute force: Convert ANY object column in batch that causes issues to string representation
                                # But we must respect writer schema.
                                # If writer schema expects "string" for a column (because first batch had json string),
                                # but current batch has "None" (null) -> PyArrow handles null to string fine.
                                # The error "Invalid null value" for "apc_list" suggests apc_list in writer schema might be Struct?
                                # If first batch had apc_list as None, schema might be Null or something else?
                                # No, if first batch had apc_list as complex dict, I converted it to json string.
                                # So writer schema expects String. 
                                # If current batch has apc_list as None (NaN), astype(str) makes "nan".
                                
                                # Let's convert ALL object columns to string if they are not null, to satisfy "String" expectation
                                for col in batch_df.columns:
                                     if batch_df[col].dtype == 'object':
                                         batch_df[col] = batch_df[col].apply(lambda x: str(x) if pd.notnull(x) else None)
                                
                                table = pa.Table.from_pandas(batch_df, schema=writer.schema)
                        
                        writer.write_table(table)
                        print(f"  → Wrote batch ({len(batch_df)} works) to disk.")
                        
                        # Clear memory
                        batch_works = []
                        del batch_df
                        del table
                    
                    print(f"\n--- Progress: {idx}/{len(all_journals)} journals processed ---\n")
            
            if writer:
                writer.close()
                print("Works data update complete.")
        
        return len(df)
    else:
        print("No data found.")
        return 0

def load_data():
    """
    Loads the dataset from the parquet file.
    """
    if os.path.exists(PARQUET_FILE):
        return pd.read_parquet(PARQUET_FILE)
    else:
        print("Data file not found. Please update data first.")
        return pd.DataFrame()

if __name__ == "__main__":
    update_data()
