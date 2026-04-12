import os
import pandas as pd
from dotenv import load_dotenv
import clickhouse_connect

# Cargar variables de entorno
load_dotenv()

# Configuración de ClickHouse
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def main():
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST, 
            port=CH_PORT, 
            username=CH_USER, 
            password=CH_PASSWORD,
            database=CH_DATABASE
        )
        print(f"✅ Conectado a ClickHouse: {CH_HOST}:{CH_PORT} (DB: {CH_DATABASE})\n")
        
        # 1. Listar Tablas y su tamaño aproximado
        print("📊 TABLAS ENCONTRADAS")
        print("-" * 50)
        query_tables = f"""
        SELECT 
            table, 
            formatReadableSize(sum(data_compressed_bytes)) as compressed,
            formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed,
            count() as parts
        FROM system.parts
        WHERE database = '{CH_DATABASE}' AND active
        GROUP BY table
        ORDER BY sum(data_uncompressed_bytes) DESC
        """
        tables = client.query_df(query_tables)
        print(tables.to_string(index=False))
        
        # 2. Listar Columnas Materializadas por Tabla
        print("\n💎 COLUMNAS MATERIALIZADAS (Cálculo Físico)")
        print("-" * 80)
        query_columns = f"""
        SELECT 
            table,
            name,
            type,
            default_kind,
            default_expression
        FROM system.columns
        WHERE database = '{CH_DATABASE}' 
          AND (default_kind IN ('MATERIALIZED', 'ALIAS', 'DEFAULT') OR name != 'raw_data')
          AND table NOT LIKE '.inner%' -- Ocultar tablas internas de MV
        ORDER BY table, name
        """
        cols = client.query_df(query_columns)
        
        if cols.empty:
            print("No se encontraron columnas materializadas explícitas (solo raw_data).")
        else:
            # Agrupar para mejor visualización
            for table_name, group in cols.groupby('table'):
                print(f"\n📍 TABLA: {table_name}")
                formatted_group = group[['name', 'type', 'default_kind', 'default_expression']]
                print(formatted_group.to_string(index=False))
        
        # 3. Identificar si existe el motor de búsqueda
        print("\n🚀 RESUMEN DE OPTIMIZACIÓN")
        print("-" * 50)
        for table_name in tables['table'].unique():
            table_cols = cols[cols['table'] == table_name]['name'].tolist()
            if 'raw_data' in table_cols:
                print(f" - {table_name}: Usa 'raw_data' (JSON).")
                if len(table_cols) > 1:
                    print(f"   💡 Ya tiene {len(table_cols)-1} columnas optimizadas.")
                else:
                    print(f"   ⚠️ Extrae todo desde el JSON (Lento si la tabla es grande).")
        
        client.close()
    except Exception as e:
        print(f"❌ Error al inspeccionar ClickHouse: {e}")

if __name__ == "__main__":
    main()
