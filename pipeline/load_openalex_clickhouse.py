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
from dotenv import load_dotenv

# Cargar variables de entorno desde .env si existe
load_dotenv()

# Se requiere tener instalado clickhouse-connect (pip install clickhouse-connect)
try:
    import clickhouse_connect
except ImportError:
    print("Por favor instala clickhouse-connect: pip install clickhouse-connect")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración de ClickHouse (se priorizan variables de entorno / .env)
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def get_clickhouse_client():
    """Conecta al servidor ClickHouse y asegura la base de datos."""
    try:
        # Intento 1: Conectar directamente (asume que la BD ya existe o el usuario tiene acceso limitado)
        client = clickhouse_connect.get_client(
            host=CH_HOST, 
            port=CH_PORT, 
            username=CH_USER, 
            password=CH_PASSWORD,
            database=CH_DATABASE
        )
        logger.info(f"Conectado exitosamente a la base de datos: {CH_DATABASE}")
    except Exception as e:
        logger.warning(f"No se pudo conectar directamente a '{CH_DATABASE}'. Intentando crearla si hay permisos...")
        try:
            # Intento 2: Conectar sin BD para intentar crearla
            temp_client = clickhouse_connect.get_client(
                host=CH_HOST, 
                port=CH_PORT, 
                username=CH_USER, 
                password=CH_PASSWORD
            )
            temp_client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE}")
            
            # Re-conectar a la BD ya creada
            client = clickhouse_connect.get_client(
                host=CH_HOST, 
                port=CH_PORT, 
                username=CH_USER, 
                password=CH_PASSWORD,
                database=CH_DATABASE
            )
        except Exception as final_err:
            logger.error(f"Error de acceso: El usuario '{CH_USER}' no tiene permisos para crear la BD y ésta no parece existir.")
            raise final_err
    
    # Crear tabla de control de archivos procesados
    client.command(f"""
        CREATE TABLE IF NOT EXISTS _processed_files (
            entity String,
            file_name String,
            processed_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (entity, file_name)
    """)
    
    return client

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
            files = list(item.glob('**/*.gz'))
            if len(files) > 0:
                logger.info(f"Escaneando {item.name}: found {len(files)} files")
                entities.append(item.name)
                
    logger.info(f"🔍 Entidades dinámicas descubiertas en el snapshot: {entities}")
    return entities

def infer_and_create_schema(client, entity_name: str):
    """Crea una tabla simple para almacenar los documentos JSON crudos de la entidad."""
    table_name = f"`{entity_name}`"
    logger.info(f"[{entity_name}] Asegurando tabla de repositorio JSON...")
    
    # Crear tabla con ID y el JSON rudo (Idempotente)
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        raw_data String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY id
    """
    client.command(create_query)
    logger.info(f"[{entity_name}] Tabla {table_name} lista.")
    return True

def ingest_entity(client, entity_name: str, snapshot_path: Path, docker_path: str = None):
    import json
    import gzip
    
    entity_dir = snapshot_path / entity_name
    files = list(entity_dir.glob('**/*.gz'))
    
    if not files:
        logger.warning(f"[{entity_name}] No se encontraron archivos .gz")
        return

    table_name = f"`{entity_name}`"
    
    # Asegurar tabla de repositorio
    infer_and_create_schema(client, entity_name)
    
    # Obtener archivos ya procesados para esta entidad
    try:
        result = client.query(
            f"SELECT file_name FROM _processed_files WHERE entity = '{entity_name}'"
        )
        processed_files = set(result.result_columns[0]) if result.result_columns else set()
    except Exception as e:
        logger.warning(f"[{entity_name}] No se pudo consultar _processed_files: {e}")
        processed_files = set()
    
    logger.info(f"[{entity_name}] Iniciando ingesta por ID + Raw JSON...")
    
    for i, file_path in enumerate(files, 1):
        # Usamos la ruta relativa para distinguir archivos con el mismo nombre en diferentes carpetas de fecha
        relative_path = str(file_path.relative_to(entity_dir))
        
        # Compatibilidad: saltar si la ruta completa O solo el nombre del archivo ya fueron procesados
        if relative_path in processed_files or file_path.name in processed_files:
            logger.info(f"  -> [{i}/{len(files)}] Saltando (ya procesado): {relative_path}")
            continue
            
        try:
            logger.info(f"  -> [{i}/{len(files)}] Procesando: {relative_path}")
            
            rows = []
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    if not line.strip(): continue
                    obj = json.loads(line)
                    # Guardamos el ID limpio y el original como string
                    rows.append([obj.get('id', ''), line.strip()])
                    
                    if len(rows) >= 5000:
                        client.insert(table_name, rows, column_names=['id', 'raw_data'])
                        rows = []
            
            if rows:
                client.insert(table_name, rows, column_names=['id', 'raw_data'])
            
            # Registrar archivo como completado (usando ruta relativa)
            client.command(
                f"INSERT INTO _processed_files (entity, file_name) VALUES ('{entity_name}', '{relative_path}')"
            )
                
        except Exception as e:
            logger.error(f"  ❌ Error en archivo {file_path.name}: {e}")

    # Verificar cuenta
    count = client.command(f"SELECT COUNT() FROM {table_name}")
    logger.info(f"[{entity_name}] ✅ Estado actual. Total registros en tabla: {count:,}")


def main():
    parser = argparse.ArgumentParser(description="Ingesta dinámica de Snapshot de OpenAlex hacia ClickHouse.")
    parser.add_argument("snapshot_dir", help="Directorio raíz del snapshot descomprimido.")
    parser.add_argument("--entities", nargs="+", help="Lista de entidades a procesar. Si no se pasa, descubre todas.")
    parser.add_argument("--docker-path", help="Ruta interna de ClickHouse (ej: test_snapshot) si se usa Docker con volumen mapeado.")
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
        ingest_entity(client, entity, snapshot_path, args.docker_path)
        
    logger.info("🎉 Ingesta Global Finalizada. Los datos están disponibles nativamente para Análisis OLAP en el Dashboard.")


if __name__ == "__main__":
    main()
