"""
Script para verificar qué campos están disponibles en PostgreSQL
para la revista Estudios Demográficos y Urbanos
"""
import psycopg2
import json

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Primero, ver qué columnas tiene la tabla sources
    print("="*70)
    print("COLUMNAS DISPONIBLES EN openalex.sources")
    print("="*70)
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'openalex' 
        AND table_name = 'sources'
        ORDER BY ordinal_position;
    """)
    
    columns = cursor.fetchall()
    for col_name, col_type in columns:
        print(f"  {col_name:30s} {col_type}")
    
    # Ahora buscar la revista específica
    print("\n" + "="*70)
    print("DATOS DE ESTUDIOS DEMOGRÁFICOS Y URBANOS EN POSTGRESQL")
    print("="*70)
    
    # Construir query dinámicamente basado en columnas disponibles
    available_cols = [col[0] for col in columns]
    
    # Campos que queremos verificar
    desired_fields = [
        'id', 'display_name', 'issn_l', 'works_count', 'cited_by_count',
        'h_index', 'i10_index', '2yr_mean_citedness',
        'summary_stats', 'oa_works_count', 'is_in_scielo', 'is_ojs', 
        'is_core', 'is_scopus', 'is_oa', 'is_in_doaj'
    ]
    
    # Filtrar solo los que existen
    fields_to_query = [f for f in desired_fields if f in available_cols]
    
    query = f"""
        SELECT {', '.join(fields_to_query)}
        FROM openalex.sources
        WHERE issn_l = '0186-7210'
        LIMIT 1;
    """
    
    cursor.execute(query)
    row = cursor.fetchone()
    
    if row:
        print("\nValores encontrados:")
        for i, field in enumerate(fields_to_query):
            value = row[i]
            
            # Si es summary_stats, parsearlo
            if field == 'summary_stats' and value:
                if isinstance(value, str):
                    value = json.loads(value)
                print(f"\n{field}:")
                if isinstance(value, dict):
                    for k, v in value.items():
                        print(f"    {k}: {v}")
            else:
                print(f"  {field}: {value}")
        
        # Verificar campos faltantes
        missing = [f for f in desired_fields if f not in available_cols]
        if missing:
            print(f"\n⚠️ CAMPOS NO DISPONIBLES EN POSTGRESQL:")
            for f in missing:
                print(f"  - {f}")
    else:
        print("❌ No se encontró la revista")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    print("\nPosibles causas:")
    print("  1. PostgreSQL no está corriendo")
    print("  2. Credenciales incorrectas")
    print("  3. Base de datos no cargada")
