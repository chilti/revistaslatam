"""
Script para recargar solo la tabla institutions.
"""
import sys
sys.path.insert(0, '.')

from load_missing_tables import load_institutions

if __name__ == "__main__":
    print("Recargando solo institutions...")
    
    # Primero vaciar la tabla
    import psycopg2
    from load_missing_tables import DB_PARAMS
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE openalex.institutions")
        conn.commit()
        print("✓ Tabla institutions vaciada")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Advertencia: {e}")
    
    # Cargar de nuevo
    result = load_institutions()
    
    if result:
        print(f"\n✓ Éxito: {len(result)} instituciones cargadas")
    else:
        print("\n✗ Error al cargar institutions")
