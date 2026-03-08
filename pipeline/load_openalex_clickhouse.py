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
CH_HOST = os.environ.get('CH_HOST', '10.90.0.87')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'rag_user')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '$B3tt3r-R4g-3veR-d0N3++')
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
    client = clickhouse_connect.get_client(
        host=CH_HOST, 
        port=CH_PORT, 
        username=CH_USER, 
        password=CH_PASSWORD,
        database=CH_DATABASE
    )
    
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
            logger.info(f"Escaneando {item.name}: found {len(files)} files")
            if len(files) > 0:
                entities.append(item.name)
                
    logger.info(f"🔍 Entidades dinámicas descubiertas en el snapshot: {entities}")
    return entities

def infer_and_create_schema(client, entity_name: str):
    """Crea una tabla simple para almacenar los documentos JSON crudos de la entidad."""
    table_name = f"openalex_{entity_name}"
    logger.info(f"[{entity_name}] Asegurando tabla de repositorio JSON...")
    
    # Crear tabla con ID y el JSON rudo (Idempotente)
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        raw_data String
    ) ENGINE = MergeTree()
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

    table_name = f"openalex_{entity_name}"
    
    # Asegurar tabla de repositorio
    infer_and_create_schema(client, entity_name)
    
    # Obtener archivos ya procesados para esta entidad
    processed_files = set(client.query(
        f"SELECT file_name FROM _processed_files WHERE entity = '{entity_name}'"
    ).result_rows_flatten)
    
    logger.info(f"[{entity_name}] Iniciando ingesta por ID + Raw JSON...")
    
    for i, file_path in enumerate(files, 1):
        if file_path.name in processed_files:
            logger.info(f"  -> [{i}/{len(files)}] Saltando (ya procesado): {file_path.name}")
            continue
            
        try:
            logger.info(f"  -> [{i}/{len(files)}] Procesando: {file_path.name}")
            
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
            
            # Registrar archivo como completado
            client.command(
                f"INSERT INTO _processed_files (entity, file_name) VALUES ('{entity_name}', '{file_path.name}')"
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
