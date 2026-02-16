"""
Quick script to rename columns in latin_american_works.parquet
to match dashboard expectations.
"""
import pandas as pd
from pathlib import Path

# Paths
BASE_PATH = Path(__file__).parent.parent
WORKS_FILE = BASE_PATH / 'data' / 'latin_american_works.parquet'
BACKUP_FILE = BASE_PATH / 'data' / 'latin_american_works_backup.parquet'

print("="*70)
print("FIXING WORKS COLUMN NAMES")
print("="*70)

if not WORKS_FILE.exists():
    print(f"❌ File not found: {WORKS_FILE}")
    exit(1)

print(f"\n📂 Loading {WORKS_FILE}...")
df = pd.read_parquet(WORKS_FILE)
print(f"✅ Loaded {len(df):,} works")

print(f"\n📋 Current columns:")
for col in df.columns:
    print(f"  - {col}")

# Backup original file
print(f"\n💾 Creating backup at {BACKUP_FILE}...")
df.to_parquet(BACKUP_FILE, index=False)
print("✅ Backup created")

# Rename columns
column_mapping = {
    'citation_normalized_percentile': 'percentile',
    'is_in_top_1_percent': 'is_top_1',
    'is_in_top_10_percent': 'is_top_10'
}

print(f"\n🔄 Renaming columns:")
for old_name, new_name in column_mapping.items():
    if old_name in df.columns:
        print(f"  ✓ {old_name} → {new_name}")
    else:
        print(f"  ⚠️ {old_name} not found (skipping)")

df = df.rename(columns=column_mapping)

print(f"\n📋 New columns:")
for col in df.columns:
    print(f"  - {col}")

# Save updated file
print(f"\n💾 Saving updated file to {WORKS_FILE}...")
df.to_parquet(WORKS_FILE, index=False)
print("✅ File updated successfully")

print("\n" + "="*70)
print("COLUMN RENAMING COMPLETE")
print("="*70)
print(f"\nBackup saved at: {BACKUP_FILE}")
print("If something went wrong, you can restore from the backup.")
