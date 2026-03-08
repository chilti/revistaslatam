import pandas as pd
import os

file_path = r'c:\Users\jlja\Documents\Proyectos\revistaslatam\data\journals_topics_sunburst.parquet'

if os.path.exists(file_path):
    df = pd.read_parquet(file_path)
    print(f"File: {file_path}")
    print("\nColumns and Dtypes:")
    print(df.dtypes)
    print("\nFirst 5 rows for 'share':")
    print(df[['journal_id', 'share']].head())
else:
    print(f"File not found: {file_path}")
