import pandas as pd
from pathlib import Path
import json

def check_oa_values():
    data_dir = Path(__file__).parent.parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    
    if not works_file.exists():
        print(f"File not found: {works_file}")
        return

    print(f"Scanning {works_file} for OA status values...")
    
    try:
        df = pd.read_parquet(works_file, columns=['open_access'])
    except Exception:
        # Fallback if open_access column is missing or diff name
        try:
            df = pd.read_parquet(works_file)
            if 'oa_status' in df.columns:
                print("Found oa_status column directly.")
                print(df['oa_status'].value_counts())
                return
        except Exception as e:
            print(f"Error reading file: {e}")
            return

    # Helper to parse
    def get_status(x):
        try:
            if isinstance(x, str):
                return json.loads(x).get('oa_status')
            return x.get('oa_status')
        except:
            return None

    if 'open_access' in df.columns:
        print("Parsing open_access JSON column...")
        # Sample if too large? No, value_counts needs all
        # To avoid memory issues, read in chunks? 
        # But parquet reads usually robust. Let's try direct apply if memory allows.
        # Check if column is string (json) or struct
        sample = df['open_access'].iloc[0]
        print(f"Sample value type: {type(sample)}")
        
        status_series = df['open_access'].apply(get_status)
        print("\nValue Counts for oa_status:")
        print(status_series.value_counts())
    else:
        print("Column 'open_access' not found.")

if __name__ == "__main__":
    check_oa_values()
