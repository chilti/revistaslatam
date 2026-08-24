#!/usr/bin/env python3
"""
Pipeline Step: Build International Collaboration Networks & Discipline Sankey Flows
"""
import sys
import os
from pathlib import Path
import pandas as pd
import json

# Add src to path
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR / 'src'))

from network_analysis import (
    build_country_collaboration_network,
    build_discipline_sankey_flows
)

def main():
    print("=" * 70)
    print("BUILDING INTERNATIONAL NETWORKS & DISCIPLINE FLOWS")
    print("=" * 70)
    
    data_dir = BASE_DIR / 'data'
    cache_dir = data_dir / 'cache'
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    works_file = data_dir / 'latin_american_works.parquet'
    topics_file = data_dir / 'journals_topics_sunburst.parquet'
    
    # 1. Collaboration Network
    print("\n1. Building Country-to-Country Collaboration Network...")
    works_df = None
    if works_file.exists():
        try:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(works_file)
            first_batch = next(pf.iter_batches(batch_size=50000, columns=['journal_id', 'is_domestic_author', 'publication_year']))
            works_df = first_batch.to_pandas()
        except Exception as e:
            print(f"Notice: Works sample note ({e})")
            
    collab_df = build_country_collaboration_network(works_df)
    collab_output = cache_dir / 'collaboration_network.parquet'
    collab_df.to_parquet(collab_output, index=False)
    print(f"Saved Collaboration Network: {collab_output} ({len(collab_df)} edges)")
    
    # 2. Discipline Sankey Flows
    print("\n2. Building Discipline-to-Topic Sankey Flows...")
    if topics_file.exists():
        topics_df = pd.read_parquet(topics_file)
        sankey_data = build_discipline_sankey_flows(topics_df)
        
        # Save as json and parquet
        sankey_json_file = cache_dir / 'discipline_sankey.json'
        with open(sankey_json_file, 'w', encoding='utf-8') as f:
            json.dump(sankey_data, f, ensure_ascii=False, indent=2)
        print(f"Saved Discipline Sankey JSON: {sankey_json_file}")
    else:
        print(f"Notice: {topics_file} not found. Skipping Sankey generation.")
        
    print("\n" + "=" * 70)
    print("NETWORK ANALYSIS PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 70)

if __name__ == '__main__':
    main()
