import psycopg2
import sys

# Database configuration (copiada de extract_postgres.py)
DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

print("Connecting to database...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # 1. Check columns in 'works' table
    print("\nCOLUMNS IN openalex.works TABLE:")
    print("-" * 40)
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'openalex' AND table_name = 'works'
        ORDER BY column_name;
    """)
    rows = cur.fetchall()
    
    found_fwci = False
    json_col = None
    
    for col, dtype in rows:
        print(f" - {col} ({dtype})")
        if col == 'fwci': found_fwci = True
        if 'json' in dtype or dtype == 'text': json_col = col

    print("-" * 40)
    
    if not found_fwci:
        print("❌ 'fwci' column NOT found!")
        if json_col:
            print(f"💡 Found potential JSON/Text column: '{json_col}'")
            print("   We might be able to extract FWCI from there.")
            
            # Try to peek into JSON
            print("\nPeeking into JSON structure (first row)...")
            try:
                # Intenta leer fwci del json
                query = f"SELECT {json_col}->>'fwci' as fwci_json FROM openalex.works LIMIT 1;"
                # Si es text, cast to jsonb
                if 'json' not in dtype:
                     query = f"SELECT {json_col}::jsonb->>'fwci' as fwci_json FROM openalex.works LIMIT 1;"
                
                cur.execute(query)
                res = cur.fetchone()
                print(f"   Value of fwci in json: {res}")
            except Exception as e:
                print(f"   Could not query json: {e}")

    else:
        print("✅ 'fwci' column found!")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals(): conn.close()
