import pandas as pd
from pathlib import Path

data_dir = Path('data')
works_file = data_dir / 'latin_american_works.parquet'
mapping_file = data_dir / 'works_topics_mapping.parquet'

print("--- IDs en latin_american_works.parquet ---")
if works_file.exists():
    df_w = pd.read_parquet(works_file, columns=['id'])
    print(df_w['id'].head().tolist())
else:
    print("FALTA latin_american_works.parquet")

print("\n--- IDs en works_topics_mapping.parquet ---")
if mapping_file.exists():
    df_m = pd.read_parquet(mapping_file, columns=['work_id'])
    print(df_m['work_id'].head().tolist())
else:
    print("FALTA works_topics_mapping.parquet")
