import psycopg2
from pprint import pprint

# Configuración (ajusta si es necesario)
DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("Obteniendo columnas de openalex.works...")
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'openalex' AND table_name = 'works';
    """)
    columns = cur.fetchall()
    
    print(f"Encontradas {len(columns)} columnas:")
    found_metrics = []
    for col in columns:
        print(f"  - {col[0]} ({col[1]})")
        if col[0] in ['fwci', 'citation_normalized_percentile', 'is_in_top_10_percent']:
            found_metrics.append(col[0])
            
    print("\nMétricas encontradas:", found_metrics)
    
    conn.close()

except Exception as e:
    print(f"Error: {e}")
