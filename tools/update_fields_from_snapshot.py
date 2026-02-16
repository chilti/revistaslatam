"""
Script para actualizar campos faltantes (is_ojs, is_in_scielo, is_core) 
desde el snapshot de OpenAlex al parquet de journals.

Este script lee los archivos .gz del snapshot y actualiza solo los campos
que no están disponibles en PostgreSQL.
"""
import gzip
import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# Configuración
SNAPSHOT_BASE = Path('/mnt/expansion/openalex-snapshot/data')
SOURCES_DIR = SNAPSHOT_BASE / 'sources'
JOURNALS_FILE = Path('data/latin_american_journals.parquet')

def extract_fields_from_snapshot():
    """
    Lee el snapshot y extrae is_ojs, is_in_scielo, is_core para todas las revistas.
    Retorna un diccionario {journal_id: {is_ojs, is_in_scielo, is_core}}
    """
    print("="*70)
    print("EXTRAYENDO CAMPOS DESDE SNAPSHOT")
    print("="*70)
    
    if not SOURCES_DIR.exists():
        print(f"❌ No se encontró {SOURCES_DIR}")
        return None
    
    # Cargar journals actuales para saber qué IDs buscar
    print(f"\n1. Cargando journals actuales desde {JOURNALS_FILE}...")
    df_journals = pd.read_parquet(JOURNALS_FILE)
    journal_ids = set(df_journals['id'].tolist())
    print(f"   Encontradas {len(journal_ids)} revistas a actualizar")
    
    # Buscar archivos .gz
    print(f"\n2. Buscando archivos .gz en snapshot...")
    gz_files = list(SOURCES_DIR.rglob('*.gz'))
    print(f"   Encontrados {len(gz_files)} archivos")
    
    # Diccionario para almacenar los campos extraídos
    extracted_data = {}
    found_count = 0
    
    print(f"\n3. Procesando archivos del snapshot...")
    for gz_file in tqdm(gz_files, desc="Procesando"):
        try:
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        journal_id = record.get('id')
                        
                        # Solo procesar si es una de nuestras revistas
                        if journal_id in journal_ids:
                            extracted_data[journal_id] = {
                                'is_ojs': record.get('is_ojs', False),
                                'is_in_scielo': record.get('is_in_scielo', False),
                                'is_core': record.get('is_core', False),
                            }
                            found_count += 1
                            
                            # Si ya encontramos todas, podemos parar
                            if found_count >= len(journal_ids):
                                print(f"\n   ✅ Encontradas todas las {found_count} revistas")
                                return extracted_data
                    
                    except json.JSONDecodeError:
                        continue
        
        except Exception as e:
            print(f"\n   ⚠️ Error en {gz_file.name}: {e}")
            continue
    
    print(f"\n   ✅ Encontradas {found_count}/{len(journal_ids)} revistas en el snapshot")
    
    if found_count < len(journal_ids):
        missing = len(journal_ids) - found_count
        print(f"   ⚠️ {missing} revistas no encontradas (usarán valores por defecto)")
    
    return extracted_data

def update_journals_parquet(extracted_data):
    """
    Actualiza el parquet de journals con los campos extraídos del snapshot.
    """
    print(f"\n4. Actualizando {JOURNALS_FILE}...")
    
    # Cargar parquet actual
    df = pd.read_parquet(JOURNALS_FILE)
    
    # Crear columnas temporales con los nuevos valores
    df['is_ojs_new'] = df['id'].map(lambda x: extracted_data.get(x, {}).get('is_ojs', False))
    df['is_in_scielo_new'] = df['id'].map(lambda x: extracted_data.get(x, {}).get('is_in_scielo', False))
    df['is_core_new'] = df['id'].map(lambda x: extracted_data.get(x, {}).get('is_core', False))
    
    # Reemplazar las columnas originales
    df['is_ojs'] = df['is_ojs_new']
    df['is_in_scielo'] = df['is_in_scielo_new']
    df['is_core'] = df['is_core_new']
    
    # Eliminar columnas temporales
    df = df.drop(columns=['is_ojs_new', 'is_in_scielo_new', 'is_core_new'])
    
    # Guardar
    df.to_parquet(JOURNALS_FILE, index=False)
    
    print(f"   ✅ Archivo actualizado")
    
    # Estadísticas
    print(f"\n5. Estadísticas de actualización:")
    print(f"   is_ojs=True: {df['is_ojs'].sum()} revistas")
    print(f"   is_in_scielo=True: {df['is_in_scielo'].sum()} revistas")
    print(f"   is_core=True: {df['is_core'].sum()} revistas")

def verify_update():
    """
    Verifica que la actualización fue exitosa para Estudios Demográficos.
    """
    print(f"\n6. Verificando actualización para 'Estudios Demográficos y Urbanos'...")
    
    df = pd.read_parquet(JOURNALS_FILE)
    target = df[df['issn_l'] == '0186-7210']
    
    if not target.empty:
        journal = target.iloc[0]
        print(f"   is_ojs: {journal['is_ojs']} (esperado: True)")
        print(f"   is_in_scielo: {journal['is_in_scielo']} (esperado: False)")
        print(f"   is_core: {journal['is_core']} (esperado: False)")
        
        if journal['is_ojs'] == True:
            print(f"\n   ✅ Actualización exitosa!")
        else:
            print(f"\n   ⚠️ is_ojs sigue siendo False")
    else:
        print(f"   ❌ Revista no encontrada")

if __name__ == "__main__":
    # Paso 1: Extraer datos del snapshot
    extracted_data = extract_fields_from_snapshot()
    
    if extracted_data:
        # Paso 2: Actualizar parquet
        update_journals_parquet(extracted_data)
        
        # Paso 3: Verificar
        verify_update()
        
        print("\n" + "="*70)
        print("PROCESO COMPLETADO")
        print("="*70)
    else:
        print("\n❌ No se pudo extraer datos del snapshot")
