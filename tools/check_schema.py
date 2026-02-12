"""
Script para verificar el schema de la tabla sources.
"""
import psycopg2

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Obtener columnas de la tabla sources
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'openalex' 
        AND table_name = 'sources'
        ORDER BY ordinal_position;
    """)
    
    columns = cur.fetchall()
    
    print("="*70)
    print("COLUMNAS DE openalex.sources")
    print("="*70)
    
    has_country_code = False
    for col_name, col_type in columns:
        print(f"  {col_name:30s} {col_type}")
        if col_name == 'country_code':
            has_country_code = True
    
    print("\n" + "="*70)
    if has_country_code:
        print("✓ La tabla TIENE la columna 'country_code'")
        print("\nPuedes ejecutar directamente:")
        print("  python data_collector_postgres.py")
    else:
        print("✗ La tabla NO TIENE la columna 'country_code'")
        print("\nDebes recargar la tabla:")
        print("  1. Ejecuta: python -c \"import psycopg2; conn = psycopg2.connect(host='localhost', database='openalex', user='postgres', password='tu_contasena'); cur = conn.cursor(); cur.execute('DROP TABLE IF EXISTS openalex.sources'); conn.commit(); print('Tabla borrada')\"")
        print("  2. Ejecuta: python load_missing_tables.py")
    print("="*70)
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
