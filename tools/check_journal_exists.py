import pandas as pd
from pathlib import Path

FILE = Path('data/latin_american_journals.parquet')
TARGET = 'Estudios Demográficos y Urbanos'

if not FILE.exists():
    print("File not found.")
else:
    df = pd.read_parquet(FILE)
    print(f"Total Journals: {len(df)}")
    if 'display_name' in df.columns:
        match = df[df['display_name'].str.contains(TARGET, case=False, na=False)]
        if not match.empty:
            print(f"✅ Found journal: {len(match)} matches.")
            print(match[['id', 'display_name', 'issn_l', 'works_count']].to_string())
        else:
            print(f"❌ Journal '{TARGET}' not found.")
            # Search partial
            partial = df[df['display_name'].str.contains('Demogr', case=False, na=False)]
            if not partial.empty:
                print("Partial matches for 'Demogr':")
                print(partial[['id', 'display_name']].head())
    else:
        print("Column 'display_name' missing.")
