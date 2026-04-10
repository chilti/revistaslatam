"""
Script para crear índices en PostgreSQL.
"""
import psycopg2

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "openalex_db",  # Debe coincidir con load_missing_tables.py
    "user": "postgres",
    "password": "tu_contasena"
}

# Índices críticos para data_collector_postgres.py
INDEXES = [
    # Para works_primary_location (JOIN principal)
    "CREATE INDEX IF NOT EXISTS idx_wpl_work_id ON openalex.works_primary_location(work_id);",
    "CREATE INDEX IF NOT EXISTS idx_wpl_source_id ON openalex.works_primary_location(source_id);",
    
    # Para works (JOIN con primary_location)
    "CREATE INDEX IF NOT EXISTS idx_works_id ON openalex.works(id);",
    
    # Para works_authorships (JOIN para obtener autores)
    "CREATE INDEX IF NOT EXISTS idx_wa_work_id ON openalex.works_authorships(work_id);",
    
    # Para works_open_access (JOIN para info OA)
    "CREATE INDEX IF NOT EXISTS idx_woa_work_id ON openalex.works_open_access(work_id);",
    
    # Para works_concepts (JOIN para conceptos, si existe)
    "CREATE INDEX IF NOT EXISTS idx_wc_work_id ON openalex.works_concepts(work_id);",
    
    # Para sources (ya tiene PRIMARY KEY, pero por si acaso)
    "CREATE INDEX IF NOT EXISTS idx_sources_id ON openalex.sources(id);",
    
    # Actualizar estadísticas
    "ANALYZE openalex.works;",
    "ANALYZE openalex.works_primary_location;",
    "ANALYZE openalex.works_authorships;",
    "ANALYZE openalex.works_open_access;",
    "ANALYZE openalex.sources;",
]

if __name__ == "__main__":
    print("="*70)
    print("CREANDO ÍNDICES EN POSTGRESQL")
    print("="*70)
    print("\nEsto puede tomar 5-15 minutos dependiendo del tamaño de los datos...")
    print("="*70 + "\n")
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        for idx, sql in enumerate(INDEXES, 1):
            print(f"[{idx}/{len(INDEXES)}] {sql[:60]}...")
            try:
                cur.execute(sql)
                conn.commit()
                print(f"  ✓ Completado")
            except Exception as e:
                print(f"  ⚠️ Advertencia: {e}")
                conn.rollback()
        
        cur.close()
        conn.close()
        
        print("\n" + "="*70)
        print("✓ ÍNDICES CREADOS EXITOSAMENTE")
        print("="*70)
        print("\nAhora puedes ejecutar:")
        print("  python data_collector_postgres.py")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
