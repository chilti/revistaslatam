"""
Módulo de Ingesta Dinámica de OpenAlex a ClickHouse (OLAP Mundial)

Este script tiene como objetivo evitar el "hardcodeo" de entidades y esquemas.
Explora de manera dinámica un directorio base asumiendo la estructura estándar de un 
snapshot de OpenAlex (ej. `data/works/updated_date=.../*.gz`, `data/authors/...`)
y utiliza la inferencia de esquemas nativa de ClickHouse para crear las tablas 
y cargar todos los campos disponibles (incluso aquellos nuevos introducidos en versiones recientes).
"""
import os
import glob
import logging
import argparse
from pathlib import Path

# Se requiere tener instalado clickhouse-connect (pip install clickhouse-connect)
try:
    import clickhouse_connect
except ImportError:
    print("Por favor instala clickhouse-connect: pip install clickhouse-connect")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración por defecto de ClickHouse
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'openalex')

def get_clickhouse_client():
    """Conecta al servidor ClickHouse y crea la base de datos si no existe."""
    client = clickhouse_connect.get_client(
        host=CH_HOST, 
        port=CH_PORT, 
        username=CH_USER, 
        password=CH_PASSWORD
    )
    
    # Crear la BD
    client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE}")
    logger.info(f"Conectado a ClickHouse. Usando base de datos: {CH_DATABASE}")
    
    # Reconectarse especificando la BD
    return clickhouse_connect.get_client(
        host=CH_HOST, 
        port=CH_PORT, 
        username=CH_USER, 
        password=CH_PASSWORD,
        database=CH_DATABASE
    )

def discover_entities(snapshot_path: Path):
    """
    Escanea la raíz del snapshot y descubre qué entidades existen.
    OpenAlex organiza su snapshot en carpetas de nivel superior 
    (ej: works, authors, concepts, institutions, venues, sources, publishers, funders).
    """
    entities = []
    
    if not snapshot_path.exists() or not snapshot_path.is_dir():
        logger.error(f"El directorio especificado no existe o no es carpeta: {snapshot_path}")
        return entities
        
    for item in snapshot_path.iterdir():
        if item.is_dir() and not item.name.startswith('.'):
            # Verificar si adentro tiene particiones por fecha o archivos .gz
            has_data = len(list(item.glob('**/*.gz'))) > 0
            if has_data:
                entities.append(item.name)
                
    logger.info(f"🔍 Entidades dinámicas descubiertas en el snapshot: {entities}")
    return entities

def infer_and_create_schema(client, entity_name: str, sample_file_path: str):
    """
    Utiliza el motor de ClickHouse para leer un fragmento de un archivo .json.gz,
    inferir la estructura de todas sus claves y crear una tabla tipo MergeTree.
    """
    table_name = f"openalex_{entity_name}"
    
    logger.info(f"[{entity_name}] Infiriendo esquema desde muestra: {sample_file_path}...")
    
    # 1. Utilizar función file() de ClickHouse local para que descubra la estructura
    # Esta es una magia de ClickHouse: DESCRIBE (SELECT * FROM file('path', 'JSONEachRow'))
    try:
        # Aseguramos que file_path sea absoluto y adaptado para que el server CH lo lea
        # (Nota: En un CH Server Dockerizado, tal vez se requiera S3 o rutas montadas)
        # Aquí usaremos la funcionalidad de CH para describir el formato
        describe_query = f"DESCRIBE file('{sample_file_path}', 'JSONEachRow')"
        
        # En caso de que se corra local de verdad
        schema_df = client.query_df(describe_query)
        
        if schema_df.empty:
            logger.error(f"No se pudo inferir el esquema para {entity_name}")
            return False
            
    except Exception as e:
        logger.warning(f"La inferencia por file() falló (probablemente el server CH no tiene acceso a la ruta local).")
        logger.warning(f"Error nativo: {e}")
        logger.info(f"Alternativa: Se creará la tabla como un repositorio JSON crudo para post-procesado (Variante B).")
        
        # Fallback dinámico total: Crear una tabla con una columna tipo String cruda o JSON object.
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id String,
            raw_data String
        ) ENGINE = MergeTree()
        ORDER BY id
        """
        client.command(create_query)
        logger.info(f"[{entity_name}] Tabla Fallback cruda creada.")
        return True

    # Si funcionó la inferencia local estructurada:
    columns_def = []
    has_id = False
    
    for _, row in schema_df.iterrows():
        col_name = row['name']
        col_type = row['type']
        
        # ClickHouse a veces infiere Nullable(String). Eso está bien.
        columns_def.append(f"`{col_name}` {col_type}")
        if col_name == 'id':
            has_id = True
            
    columns_str = ",\n    ".join(columns_def)
    
    # La primary key ideal para OpenAlex es su "id"
    order_by = "id" if has_id else "tuple()"

    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    (
        {columns_str}
    )
    ENGINE = MergeTree()
    ORDER BY {order_by}
    """
    
    client.command(f"DROP TABLE IF EXISTS {table_name}") # Para recrearlo limpio basando en la inferencia
    client.command(create_query)
    
    logger.info(f"[{entity_name}] Tabla columnar creada exitosamente con {len(schema_df)} campos inferidos.")
    return True

def ingest_entity(client, entity_name: str, snapshot_path: Path):
    """
    Busca recursivamente todos los archivos .json.gz de la entidad 
    e inserta los datos en ClickHouse.
    """
    entity_dir = snapshot_path / entity_name
    files = list(entity_dir.glob('**/*.gz'))
    
    if not files:
        logger.warning(f"[{entity_name}] No se encontraron archivos .gz")
        return

    table_name = f"openalex_{entity_name}"
    
    # Tomar el primer archivo para inferir esquema
    sample_file = str(files[0].absolute())
    # Reemplazar barras invertidas en Windows para que CH lo entienda
    sample_file = sample_file.replace('\\', '/')
    
    success = infer_and_create_schema(client, entity_name, sample_file)
    if not success:
        return
        
    logger.info(f"[{entity_name}] Iniciando ingesta de {len(files)} archivos...")
    
    # Bucle de inserción archivo por archivo
    # (En producción real con S3, esto sería un solo INSERT INTO ... SELECT FROM s3(...))
    for i, file_path in enumerate(files, 1):
        clean_path = str(file_path.absolute()).replace('\\', '/')
        try:
            logger.info(f"  -> [{i}/{len(files)}] Ingiriendo {file_path.name}")
            # Insertar los datos infiriendo que los tipos incompletos se autocompleten.
            client.command("SET input_format_json_infer_incomplete_types=1;")
            
            # Chequeamos si la tabla tiene 'raw_data' (modo Fallback)
            tbl_info = client.query_df(f"DESCRIBE {table_name}")
            if 'raw_data' in tbl_info['name'].values:
                # Inserción ruda (todo el JSON como string)
                query = f"""
                INSERT INTO {table_name} 
                SELECT JSONExtractString(_raw, 'id') as id, _raw as raw_data 
                FROM file('{clean_path}', 'LineAsString', '_raw String')
                """
            else:
                # Inserción estructurada
                query = f"INSERT INTO {table_name} SELECT * FROM file('{clean_path}', 'JSONEachRow')"
                
            client.command(query)
            
        except Exception as e:
            logger.error(f"  ❌ Error en archivo {file_path.name}: {e}")

    # Chequear cuenta
    count = client.command(f"SELECT COUNT() FROM {table_name}")
    logger.info(f"[{entity_name}] ✅ Ingesta finalizada. Total registros: {count:,}")
    print("-" * 50)


def main():
    parser = argparse.ArgumentParser(description="Ingesta dinámica de Snapshot de OpenAlex hacia ClickHouse.")
    parser.add_argument("snapshot_dir", help="Directorio raíz del snapshot descomprimido o base de AWS S3.")
    parser.add_argument("--entities", nargs="+", help="Lista de entidades a procesar (ej. works sources). Si no se pasa, descubre todas.")
    args = parser.parse_args()
    
    snapshot_path = Path(args.snapshot_dir)
    
    client = get_clickhouse_client()
    
    if args.entities:
        entities_to_process = args.entities
    else:
        entities_to_process = discover_entities(snapshot_path)
        
    if not entities_to_process:
        logger.error("No hay entidades a procesar. Saliendo.")
        return

    for entity in entities_to_process:
        ingest_entity(client, entity, snapshot_path)
        
    logger.info("🎉 Ingesta Global Finalizada. Los datos están disponibles nativamente para Análisis OLAP en el Dashboard.")


if __name__ == "__main__":
    main()
