import pandas as pd
import glob
from pathlib import Path

def diagnose():
    cache_dir = Path('data/cache')
    files = ['sunburst_metrics_latam.parquet', 'sunburst_metrics_country.parquet', 'sunburst_metrics_journal.parquet']
    
    for f_name in files:
        path = cache_dir / f_name
        if not path.exists():
            print(f"❌ {f_name} does not exist")
            continue
        
        df = pd.read_parquet(path)
        print(f"\n--- DIAGNOSIS: {f_name} ---")
        print(f"Rows: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Unique Levels: {df['level'].unique().tolist()}")
        
        # Check if topic level exists
        if 'topic' in df['level'].unique():
            topic_rows = df[df['level'] == 'topic']
            print(f"Topic level rows: {len(topic_rows)}")
            # Sample a topic row
            if not topic_rows.empty:
                 print("Sample topic data:")
                 print(topic_rows[['domain', 'field', 'subfield', 'topic']].iloc[0].to_dict())
        else:
            print("⚠️ Topic level MISSING in 'level' column")
            
        # Check for ALL values in levels
        for level in ['domain', 'field', 'subfield', 'topic']:
            if level in df.columns:
                all_count = (df[level] == 'ALL').sum()
                print(f"Level '{level}' has 'ALL' count: {all_count}")

if __name__ == "__main__":
    diagnose()
