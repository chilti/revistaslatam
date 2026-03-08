import clickhouse_connect

def minimal_ingest():
    client = clickhouse_connect.get_client(host='127.0.0.1', username='admin', password='admin', database='openalex')
    print("✅ Conectado")
    
    # Limpiar
    client.command("DROP TABLE IF EXISTS openalex_sources")
    
    # Crear y Cargar en un solo paso (sin LIMIT 0)
    # Esto es lo más rudo: crear la tabla con los datos del primer archivo.
    q_create = """
    CREATE TABLE openalex_sources 
    ENGINE = MergeTree() 
    ORDER BY tuple() 
    AS SELECT * FROM file('test_snapshot/sources/sample.jsonl.gz', 'JSONEachRow')
    """
    client.command(q_create)
    print("✅ Tabla creada con AS SELECT")
    
    count = client.command("SELECT COUNT() FROM openalex_sources")
    print(f"Registros después de AS SELECT: {count}")

if __name__ == "__main__":
    minimal_ingest()
