"""
Script para contar trabajos de 'Estudios Demográficos y Urbanos' 
directamente en el snapshot de OpenAlex.

Esto verificará si los 88 trabajos faltantes están en el snapshot original.
"""
import gzip
import json
from pathlib import Path
from tqdm import tqdm

# Configuración
SNAPSHOT_BASE = Path('/mnt/expansion/openalex-snapshot/data')
WORKS_DIR = SNAPSHOT_BASE / 'works'
JOURNAL_ID = 'https://openalex.org/S2737081250'
JOURNAL_NAME = 'Estudios Demográficos y Urbanos'

print("="*70)
print(f"CONTEO EN SNAPSHOT: {JOURNAL_NAME}")
print("="*70)
print(f"Journal ID: {JOURNAL_ID}")

if not WORKS_DIR.exists():
    print(f"\n❌ No se encontró {WORKS_DIR}")
    print("Verifica la ruta del snapshot")
    exit(1)

# Buscar archivos .gz recursivamente
print(f"\nBuscando archivos .gz en {WORKS_DIR}...")
gz_files = list(WORKS_DIR.rglob('*.gz'))
print(f"Encontrados {len(gz_files)} archivos")

if len(gz_files) == 0:
    print("❌ No hay archivos .gz en el directorio de works")
    exit(1)

# Contar trabajos
print(f"\nProcesando archivos (esto puede tardar varios minutos)...")
works_found = []
files_processed = 0

for gz_file in tqdm(gz_files, desc="Procesando"):
    files_processed += 1
    
    try:
        with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    record = json.loads(line.strip())
                    
                    # Verificar si el trabajo pertenece a esta revista
                    # Puede estar en primary_location o locations
                    primary_location = record.get('primary_location', {})
                    if primary_location:
                        source = primary_location.get('source', {})
                        if source and source.get('id') == JOURNAL_ID:
                            works_found.append({
                                'id': record.get('id'),
                                'year': record.get('publication_year'),
                                'title': record.get('display_name', '')[:80],
                                'file': gz_file.name
                            })
                            continue
                    
                    # También verificar en locations (por si no está en primary)
                    locations = record.get('locations', [])
                    for loc in locations:
                        source = loc.get('source', {})
                        if source and source.get('id') == JOURNAL_ID:
                            works_found.append({
                                'id': record.get('id'),
                                'year': record.get('publication_year'),
                                'title': record.get('display_name', '')[:80],
                                'file': gz_file.name
                            })
                            break
                
                except json.JSONDecodeError:
                    continue
    
    except Exception as e:
        print(f"\n⚠️ Error en {gz_file.name}: {e}")
        continue

# Resultados
print("\n" + "="*70)
print("RESULTADOS")
print("="*70)
print(f"Archivos procesados: {files_processed}")
print(f"Trabajos encontrados en snapshot: {len(works_found)}")

# Distribución por año
if works_found:
    from collections import Counter
    year_dist = Counter(w['year'] for w in works_found)
    
    print(f"\nDistribución por año (primeros 10 y últimos 5):")
    sorted_years = sorted(year_dist.items())
    for year, count in sorted_years[:10]:
        print(f"  {year}: {count}")
    if len(sorted_years) > 15:
        print(f"  ...")
        for year, count in sorted_years[-5:]:
            print(f"  {year}: {count}")
    
    # Muestra de trabajos
    print(f"\nPrimeros 5 trabajos encontrados:")
    for work in works_found[:5]:
        print(f"  {work['year']} | {work['id']}")
        print(f"       {work['title']}...")

# Comparación
print("\n" + "="*70)
print("COMPARACIÓN")
print("="*70)
print(f"Metadata de journal (sources): 1,985")
print(f"Snapshot (works):              {len(works_found)}")
print(f"PostgreSQL:                    1,897")
print(f"Parquet consolidado:           1,889")

print(f"\n📊 ANÁLISIS:")
if len(works_found) >= 1985:
    print(f"✅ El snapshot tiene TODOS los trabajos ({len(works_found)})")
    missing_in_pg = len(works_found) - 1897
    print(f"⚠️ PostgreSQL le faltan {missing_in_pg} trabajos")
    print(f"   Esto indica que la carga a PostgreSQL fue INCOMPLETA")
    print(f"   TODAS las revistas podrían tener trabajos faltantes")
elif len(works_found) >= 1897:
    print(f"✅ El snapshot tiene suficientes trabajos ({len(works_found)})")
    print(f"⚠️ Pero menos que el metadata (1985)")
    print(f"   Diferencia: {1985 - len(works_found)} trabajos")
else:
    print(f"⚠️ El snapshot tiene MENOS trabajos que PostgreSQL")
    print(f"   Esto es extraño y requiere investigación")

print("\n" + "="*70)
