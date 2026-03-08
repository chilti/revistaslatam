import clickhouse_connect
import pandas as pd

def diagnose():
    try:
        client = clickhouse_connect.get_client(host='127.0.0.1', username='admin', password='admin', database='openalex')
        print("✅ Conectado a ClickHouse")
        
        # 1. Probar visibilidad de archivo básico
        test_file = 'test_snapshot/sources/sample.jsonl.gz'
        count = client.command(f"SELECT count() FROM file('{test_file}', 'JSONEachRow')")
        print(f"✅ Archivo '{test_file}' leído exitosamente. Registros: {count}")
        
        # 2. Ver esquema inferido
        df_schema = client.query_df(f"DESCRIBE file('{test_file}', 'JSONEachRow')")
        print("📋 Esquema inferido:")
        print(df_schema[['name', 'type']])
        
        # 3. Probar el archivo de Works (el real de OpenAlex)
        works_file = 'test_snapshot/works/part_0000.gz'
        print(f"Probar lectura de {works_file}...")
        try:
            w_count = client.command(f"SELECT count() FROM file('{works_file}', 'JSONEachRow')")
            print(f"✅ Archivo Works leído. Registros: {w_count}")
        except Exception as e:
            print(f"❌ Error leyendo Works: {e}")
            
    except Exception as e:
        print(f"❌ Error de diagnóstico: {e}")

if __name__ == "__main__":
    diagnose()
