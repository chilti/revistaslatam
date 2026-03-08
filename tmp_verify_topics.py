import pandas as pd
import os

evo_file = r'c:\Users\jlja\Documents\Proyectos\revistaslatam\data\cache\thematic_evolution_latam.parquet'
sunburst_file = r'c:\Users\jlja\Documents\Proyectos\revistaslatam\data\journals_topics_sunburst.parquet'

print("--- Topic Verification ---")

if os.path.exists(evo_file):
    df_evo = pd.read_parquet(evo_file)
    unique_topics_evo = df_evo['topic'].nunique()
    print(f"Unique Topics in thematic_evolution_latam.parquet: {unique_topics_evo}")
    
    # Check if there are combinations of (year, topic) that are repeated unexpectedly
    # (though they should be summed up later in the dashboard)
    
if os.path.exists(sunburst_file):
    df_sun = pd.read_parquet(sunburst_file)
    if 'topic_name' in df_sun.columns:
        unique_topics_sun = df_sun['topic_name'].nunique()
        print(f"Unique Topics in journals_topics_sunburst.parquet: {unique_topics_sun}")
    elif 'topic' in df_sun.columns:
        unique_topics_sun = df_sun['topic'].nunique()
        print(f"Unique Topics in journals_topics_sunburst.parquet: {unique_topics_sun}")

# Check for potential issues
if os.path.exists(evo_file) and os.path.exists(sunburst_file):
    if unique_topics_evo > 4520: # OpenAlex has ~4516
        print("\nWARNING: Number of topics exceeds OpenAlex standard (~4516).")
        print("Checking for leading/trailing spaces or case differences...")
        unique_lower = df_evo['topic'].str.lower().str.strip().nunique()
        print(f"Unique Topics (normalized): {unique_lower}")
        if unique_lower < unique_topics_evo:
            print("Found duplicates due to casing or spaces!")
else:
    print("Files not found for comparison.")
