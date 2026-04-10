"""
Script para agregar PRIMARY KEYs a las tablas después de la carga.
ADVERTENCIA: Esto puede tomar tiempo si hay duplicados.
"""
import psycopg2

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "openalex",
    "user": "postgres",
    "password": "tu_contasena"
}

PRIMARY_KEYS = [
    # Tablas principales
    ("works", "id"),
    ("sources", "id"),  # Ya debería tenerla
    ("institutions", "id"),  # Ya debería tenerla
    
    # Tablas de relación
    ("works_primary_location", "work_id"),
    ("works_open_access", "work_id"),
    ("works_authorships", None),  # No tiene PK natural (múltiples autores por work)
    ("works_concepts", None),  # No tiene PK natural
]

if __name__ == "__main__":
    print("="*70)
    print("AGREGANDO PRIMARY KEYS")
    print("="*70)
    print("\n⚠️  ADVERTENCIA: Esto puede tomar tiempo y fallar si hay duplicados")
    print("="*70 + "\n")
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        for table, pk_column in PRIMARY_KEYS:
            if pk_column is None:
                print(f"⊘ {table}: No tiene PRIMARY KEY natural, omitiendo")
                continue
            
            print(f"Procesando {table}...")
            
            # Verificar si ya tiene PRIMARY KEY
            cur.execute(f"""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_schema = 'openalex' 
                AND table_name = '{table}' 
                AND constraint_type = 'PRIMARY KEY';
            """)
            
            if cur.fetchone():
                print(f"  ✓ {table} ya tiene PRIMARY KEY")
                continue
            
            # Verificar duplicados primero
            cur.execute(f"""
                SELECT {pk_column}, COUNT(*) as cnt
                FROM openalex.{table}
                GROUP BY {pk_column}
                HAVING COUNT(*) > 1
                LIMIT 5;
            """)
            
            duplicates = cur.fetchall()
            
            if duplicates:
                print(f"  ✗ {table} tiene {len(duplicates)} duplicados:")
                for dup_id, count in duplicates[:3]:
                    print(f"    - {dup_id}: {count} veces")
                print(f"  ⚠️  Omitiendo PRIMARY KEY (hay duplicados)")
                continue
            
            # Agregar PRIMARY KEY
            try:
                print(f"  Agregando PRIMARY KEY en {pk_column}...")
                cur.execute(f"""
                    ALTER TABLE openalex.{table} 
                    ADD CONSTRAINT {table}_pkey 
                    PRIMARY KEY ({pk_column});
                """)
                conn.commit()
                print(f"  ✓ PRIMARY KEY agregada")
            except Exception as e:
                print(f"  ✗ Error: {e}")
                conn.rollback()
        
        cur.close()
        conn.close()
        
        print("\n" + "="*70)
        print("PROCESO COMPLETADO")
        print("="*70)
        
    except Exception as e:
        print(f"\n✗ Error de conexión: {e}")
