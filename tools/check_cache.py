import pandas as pd
from pathlib import Path
import os

def check_cache():
    cache_dir = Path(__file__).parent.parent / 'data' / 'cache'
    
    print(f"Checking cache directory: {cache_dir}")
    if not cache_dir.exists():
        print("Cache directory not found!")
        return

    files = list(cache_dir.glob('*.parquet'))
    if not files:
        print("No parquet files found.")
        return

    print(f"Found {len(files)} files:")
    for f in sorted(files):
        size = f.stat().st_size
        try:
            df = pd.read_parquet(f)
            rows = len(df)
            cols = list(df.columns)
            print(f"- {f.name}: {size} bytes, {rows} rows")
            if 'period_2021_2025' in f.name:
                print(f"  Columns: {cols[:5]}...")
                if 'pct_oa_diamond' in cols:
                    print("  ✅ pct_oa_diamond found")
                else:
                    print("  ❌ pct_oa_diamond MISSING")
        except Exception as e:
            print(f"- {f.name}: {size} bytes. Error reading: {e}")

if __name__ == "__main__":
    check_cache()
