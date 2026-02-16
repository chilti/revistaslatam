"""
Script para buscar y extraer datos de 'Estudios Demográficos y Urbanos' 
directamente desde los archivos del snapshot de OpenAlex.

Esto permite verificar que los datos originales son correctos antes de la carga a PostgreSQL.
"""
import gzip
import json
import os
from pathlib import Path

# Configuración: Ajusta esta ruta al directorio donde están los snapshots
SNAPSHOT_BASE = Path('/mnt/expansion/openalex-snapshot/data')  # Ajustar según tu sistema
SOURCES_DIR = SNAPSHOT_BASE / 'sources'

# Identificadores conocidos para búsqueda
TARGET_JOURNAL_NAME = "Estudios Demográficos y Urbanos"
TARGET_ISSN = "0186-7210"  # ISSN-L conocido de esta revista

def search_in_snapshot():
    """
    Busca la revista en los archivos .gz del snapshot.
    """
    print("="*70)
    print("BÚSQUEDA EN SNAPSHOT DE OPENALEX")
    print("="*70)
    print(f"Directorio de búsqueda: {SOURCES_DIR}")
    
    if not SOURCES_DIR.exists():
        print(f"\n❌ ERROR: No se encontró el directorio {SOURCES_DIR}")
        print("\nPor favor, ajusta la variable SNAPSHOT_BASE en el script.")
        print("Rutas comunes:")
        print("  - /mnt/expansion/openalex-snapshot/data/sources")
        print("  - /data/openalex-snapshot/data/sources")
        print("  - ~/openalex-snapshot/data/sources")
        return
    
    # Listar archivos .gz
    gz_files = list(SOURCES_DIR.glob('*.gz'))
    
    if not gz_files:
        print(f"\n❌ No se encontraron archivos .gz en {SOURCES_DIR}")
        return
    
    print(f"\nEncontrados {len(gz_files)} archivos .gz para procesar...")
    print(f"Buscando: '{TARGET_JOURNAL_NAME}' o ISSN-L '{TARGET_ISSN}'")
    print("-"*70)
    
    found = False
    files_processed = 0
    
    for gz_file in gz_files:
        files_processed += 1
        
        # Mostrar progreso cada 10 archivos
        if files_processed % 10 == 0:
            print(f"  Procesados {files_processed}/{len(gz_files)} archivos...", end='\r')
        
        try:
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        
                        # Verificar si es la revista que buscamos
                        display_name = record.get('display_name', '')
                        issn_l = record.get('issn_l', '')
                        
                        if (TARGET_JOURNAL_NAME.lower() in display_name.lower() or 
                            issn_l == TARGET_ISSN):
                            
                            found = True
                            print(f"\n\n✅ ¡ENCONTRADO en {gz_file.name} (línea {line_num})!")
                            print("="*70)
                            
                            # Extraer campos relevantes
                            print(f"ID: {record.get('id')}")
                            print(f"Display Name: {record.get('display_name')}")
                            print(f"ISSN-L: {record.get('issn_l')}")
                            print(f"ISSN: {record.get('issn')}")
                            print(f"Publisher: {record.get('publisher')}")
                            print(f"Country Code: {record.get('country_code')}")
                            
                            print(f"\n--- MÉTRICAS BÁSICAS ---")
                            print(f"works_count: {record.get('works_count')}")
                            print(f"cited_by_count: {record.get('cited_by_count')}")
                            print(f"is_oa: {record.get('is_oa')}")
                            print(f"is_in_doaj: {record.get('is_in_doaj')}")
                            
                            print(f"\n--- CAMPOS ADICIONALES ---")
                            print(f"oa_works_count: {record.get('oa_works_count', 'NO EXISTE')}")
                            print(f"is_in_scielo: {record.get('is_in_scielo', 'NO EXISTE')}")
                            print(f"is_ojs: {record.get('is_ojs', 'NO EXISTE')}")
                            print(f"is_core: {record.get('is_core', 'NO EXISTE')}")
                            print(f"is_scopus: {record.get('is_scopus', 'NO EXISTE')}")
                            
                            print(f"\n--- SUMMARY STATS (JSON) ---")
                            summary_stats = record.get('summary_stats')
                            if summary_stats:
                                print(f"h_index: {summary_stats.get('h_index')}")
                                print(f"i10_index: {summary_stats.get('i10_index')}")
                                print(f"2yr_mean_citedness: {summary_stats.get('2yr_mean_citedness')}")
                                print(f"2yr_cited_by_count: {summary_stats.get('2yr_cited_by_count')}")
                                print(f"\nSummary Stats completo:")
                                print(json.dumps(summary_stats, indent=2))
                            else:
                                print("summary_stats: NO EXISTE")
                            
                            print(f"\n--- REGISTRO COMPLETO (JSON) ---")
                            print(json.dumps(record, indent=2, ensure_ascii=False))
                            
                            return  # Terminar después de encontrar
                            
                    except json.JSONDecodeError:
                        continue  # Línea corrupta, continuar
                        
        except Exception as e:
            print(f"\n⚠️ Error procesando {gz_file.name}: {e}")
            continue
    
    if not found:
        print(f"\n\n❌ No se encontró '{TARGET_JOURNAL_NAME}' en {files_processed} archivos procesados.")
        print("\nPosibles causas:")
        print("  1. La revista no está en el snapshot (país no incluido)")
        print("  2. El nombre o ISSN es diferente en OpenAlex")
        print("  3. Los archivos están en un directorio diferente")

def list_snapshot_structure():
    """
    Muestra la estructura del snapshot para ayudar a configurar rutas.
    """
    print("\n" + "="*70)
    print("ESTRUCTURA DEL SNAPSHOT")
    print("="*70)
    
    # Intentar encontrar el snapshot en ubicaciones comunes
    possible_paths = [
        Path('/mnt/expansion/openalex-snapshot'),
        Path('/data/openalex-snapshot'),
        Path('/mnt/openalex-snapshot'),
        Path.home() / 'openalex-snapshot',
    ]
    
    for base_path in possible_paths:
        if base_path.exists():
            print(f"\n✅ Encontrado: {base_path}")
            
            # Listar subdirectorios
            try:
                subdirs = [d for d in base_path.iterdir() if d.is_dir()]
                if subdirs:
                    print("  Subdirectorios:")
                    for subdir in sorted(subdirs)[:10]:  # Mostrar primeros 10
                        print(f"    - {subdir.name}")
                        
                        # Si es 'data', mostrar su contenido
                        if subdir.name == 'data':
                            data_subdirs = [d for d in subdir.iterdir() if d.is_dir()]
                            for ds in sorted(data_subdirs)[:5]:
                                gz_count = len(list(ds.glob('*.gz')))
                                print(f"      └─ {ds.name}/ ({gz_count} archivos .gz)")
            except PermissionError:
                print("  (Sin permisos para listar)")
        else:
            print(f"❌ No existe: {base_path}")

if __name__ == "__main__":
    # Primero intentar buscar
    search_in_snapshot()
    
    # Si falla, mostrar estructura para ayudar
    if not SOURCES_DIR.exists():
        list_snapshot_structure()
        print("\n💡 TIP: Actualiza la variable SNAPSHOT_BASE en este script con la ruta correcta.")
