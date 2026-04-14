import pandas as pd
import os
import pyarrow.parquet as pq

file_path = 'data/latin_american_works.parquet'

if os.path.exists(file_path):
    try:
        # Usar pyarrow para leer solo metadatos (muy rápido)
        parquet_file = pq.ParquetFile(file_path)
        cols = parquet_file.schema.names
        print(f"Total columns: {len(cols)}")
        
        if 'language' in cols:
             print("✅ 'language' column FOUND.")
        else:
             print("❌ 'language' column NOT FOUND.")
             
        # Print all columns for debug
        print("Columns:", cols)
            
    except Exception as e:
        print(f"Error reading parquet: {e}")
else:
    print(f"File not found: {file_path}")
