import pandas as pd
import os

file_path = r'c:\Users\jlja\Documents\Proyectos\revistaslatam\data\cache\thematic_evolution_latam.parquet'
output_log = r'c:\Users\jlja\Documents\Proyectos\revistaslatam\tmp_evo_diag.txt'

with open(output_log, 'w', encoding='utf-8') as f:
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        f.write(f"File: {file_path}\n")
        f.write(f"Rows: {len(df)}\n")
        f.write("\nColumns and Dtypes:\n")
        f.write(str(df.dtypes) + "\n")
        f.write("\nFirst 10 rows:\n")
        f.write(str(df.head(10)) + "\n")
        
        f.write("\nSummary statistics for num_documents:\n")
        f.write(str(df['num_documents'].describe()) + "\n")
        
        f.write("\nTotal documents in the whole file: " + str(df['num_documents'].sum()) + "\n")
        
        f.write("\nUnique identifiers count:\n")
        f.write(f"Unique journal_ids: {df['journal_id'].nunique()}\n")
        f.write(f"Unique years: {df['year'].nunique()}\n")
        f.write(f"Unique domains: {df['domain'].nunique()}\n")
        
        # Check for zeros
        zero_docs = (df['num_documents'] == 0).sum()
        f.write(f"\nRows with 0 documents: {zero_docs} ({zero_docs/len(df)*100:.2f}%)\n")
        
        # Sample check for years
        f.write("\nCounts per year (top 10):\n")
        year_counts = df.groupby('year')['num_documents'].sum().sort_index().tail(10)
        f.write(str(year_counts) + "\n")
        
        # Check for a specific journal if possible
        if 'journal_id' in df.columns:
            f.write("\nTop 5 Journals by documents:\n")
            top_journals = df.groupby('journal_id')['num_documents'].sum().sort_values(ascending=False).head(5)
            f.write(str(top_journals) + "\n")
            
        # Check for column names specifically
        f.write(f"\nColumn names: {list(df.columns)}\n")
        
    else:
        f.write(f"File not found: {file_path}\n")

print(f"Diagnostics written to {output_log}")
