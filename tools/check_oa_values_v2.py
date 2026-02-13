import pandas as pd
from pathlib import Path
import json

def check_oa_values():
    data_dir = Path(__file__).parent.parent / 'data'
    works_file = data_dir / 'latin_american_works.parquet'
    
    output_file = Path('oa_values_report.txt')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"Scanning {works_file}...\n")
        
        try:
            df = pd.read_parquet(works_file, columns=['oa_status'])
            counts = df['oa_status'].value_counts()
            f.write("Values found in 'oa_status' column:\n")
            f.write(str(counts))
            f.write("\n")
            
            if 'diamond' in counts.index:
                f.write("CONFIRMATION: 'diamond' found in data!\n")
            else:
                f.write("Warning: 'diamond' NOT found in data.\n")
                
        except Exception as e:
            f.write(f"Error: {e}\n")

if __name__ == "__main__":
    check_oa_values()
