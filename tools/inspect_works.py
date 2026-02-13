
import pandas as pd
from pathlib import Path

def inspect_parquet():
    file_path = Path('data/latin_american_works.parquet')
    
    with open('tools/inspect_works_report.txt', 'w', encoding='utf-8') as f:
        f.write(f"Inspecting columns of {file_path.name}:\n")
        try:
            df = pd.read_parquet(file_path)
            cols = list(df.columns)
            f.write(str(cols) + "\n\n")
            
            if 'is_in_top_10_percent' in df.columns:
                f.write("'is_in_top_10_percent' Value Counts:\n")
                f.write(str(df['is_in_top_10_percent'].value_counts(dropna=False)) + "\n")
            else:
                f.write("❌ 'is_in_top_10_percent' NOT FOUND.\n")
                
            if 'is_in_top_1_percent' in df.columns:
                 f.write("'is_in_top_1_percent' Value Counts:\n")
                 f.write(str(df['is_in_top_1_percent'].value_counts(dropna=False)) + "\n")
            else:
                 f.write("❌ 'is_in_top_1_percent' NOT FOUND.\n")

            if 'citation_normalized_percentile' in df.columns:
                 f.write("\n'citation_normalized_percentile' Head:\n")
                 f.write(str(df['citation_normalized_percentile'].head(10)) + "\n")
                 f.write("Type: " + str(df['citation_normalized_percentile'].dtype) + "\n")
                 f.write("Stats:\n" + str(df['citation_normalized_percentile'].describe()) + "\n")
  
        except Exception as e:
            f.write(f"Error: {e}\n")

if __name__ == "__main__":
    inspect_parquet()
