import pandas as pd
from pathlib import Path

def check():
    works_file = Path('data/latin_american_works.parquet')
    if not works_file.exists():
        print(f"File {works_file} not found.")
        return
    
    try:
        # Load the file
        full_df = pd.read_parquet(works_file)
        print(f"Successfully loaded {len(full_df)} works.")
        
        # Check specific indicators
        indicators = ['is_in_top_10_percent', 'is_in_top_1_percent', 'citation_normalized_percentile']
        for ind in indicators:
            if ind in full_df.columns:
                n_true = (full_df[ind] == True).sum()
                n_false = (full_df[ind] == False).sum()
                n_null = full_df[ind].isna().sum()
                print(f"\nIndicator: {ind}")
                print(f"  True:  {n_true:,}")
                print(f"  False: {n_false:,}")
                print(f"  Null:  {n_null:,}")
                
                if ind == 'citation_normalized_percentile':
                    print(f"  Mean:  {full_df[ind].mean():.2f}")
                    print(f"  Max:   {full_df[ind].max():.2f}")
            else:
                print(f"\nIndicator '{ind}' NOT found.")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check()
