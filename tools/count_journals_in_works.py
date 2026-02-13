import pandas as pd
from pathlib import Path

FILE = Path('data/latin_american_works.parquet')

if FILE.exists():
    df = pd.read_parquet(FILE, columns=['journal_id'])
    unique_journals = df['journal_id'].nunique()
    print(f"Total rows: {len(df)}")
    print(f"Unique Journals in Works: {unique_journals}")
else:
    print("File not found.")
