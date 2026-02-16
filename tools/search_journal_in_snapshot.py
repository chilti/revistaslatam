"""
Script genérico para buscar cualquier revista en el snapshot de OpenAlex.

Uso:
    python tools/search_journal_in_snapshot.py --name "Nombre de la Revista"
    python tools/search_journal_in_snapshot.py --issn "0186-7210"
    python tools/search_journal_in_snapshot.py --id "S2737081250"
"""
import gzip
import json
import argparse
from pathlib import Path

# Configuración por defecto
DEFAULT_SNAPSHOT_BASE = Path('/mnt/expansion/openalex-snapshot/data')

def search_journal(search_term, search_type='name', snapshot_dir=None):
    """
    Busca una revista en el snapshot.
    El snapshot está particionado en carpetas updated_date=YYYY-MM-DD
    
    Args:
        search_term: Término de búsqueda (nombre, ISSN, o ID)
        search_type: Tipo de búsqueda ('name', 'issn', 'id')
        snapshot_dir: Directorio del snapshot (opcional)
    """
    if snapshot_dir is None:
        snapshot_dir = DEFAULT_SNAPSHOT_BASE
    
    sources_dir = Path(snapshot_dir) / 'sources'
    
    print("="*70)
    print("BÚSQUEDA EN SNAPSHOT DE OPENALEX")
    print("="*70)
    print(f"Directorio: {sources_dir}")
    print(f"Buscando por {search_type}: '{search_term}'")
    print("-"*70)
    
    if not sources_dir.exists():
        print(f"\n❌ ERROR: No se encontró {sources_dir}")
        print("\nIntenta especificar la ruta con --snapshot-dir")
        return None
    
    # Búsqueda recursiva en todas las particiones
    print("Buscando archivos .gz en particiones...")
    gz_files = list(sources_dir.rglob('*.gz'))
    
    if not gz_files:
        print(f"❌ No hay archivos .gz en {sources_dir}")
        
        # Diagnóstico: mostrar estructura
        subdirs = [d for d in sources_dir.iterdir() if d.is_dir()]
        if subdirs:
            print(f"\nEncontradas {len(subdirs)} carpetas de partición:")
            for subdir in sorted(subdirs)[:5]:
                gz_count = len(list(subdir.glob('*.gz')))
                print(f"  - {subdir.name} ({gz_count} archivos .gz)")
        
        return None
    
    print(f"Encontrados {len(gz_files)} archivos .gz en total")
    print(f"Procesando...")
    
    search_term_lower = search_term.lower()
    files_processed = 0
    
    for gz_file in gz_files:
        files_processed += 1
        
        if files_processed % 10 == 0:
            print(f"  Progreso: {files_processed}/{len(gz_files)}", end='\r')
        
        try:
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        
                        # Determinar si coincide según el tipo de búsqueda
                        match = False
                        
                        if search_type == 'name':
                            display_name = record.get('display_name', '').lower()
                            match = search_term_lower in display_name
                        
                        elif search_type == 'issn':
                            issn_l = record.get('issn_l', '')
                            issn = record.get('issn', [])
                            if isinstance(issn, str):
                                issn = [issn]
                            match = (search_term == issn_l or 
                                   search_term in issn)
                        
                        elif search_type == 'id':
                            openalex_id = record.get('id', '')
                            # Manejar tanto "S123" como "https://openalex.org/S123"
                            match = (search_term in openalex_id or 
                                   openalex_id.endswith(search_term))
                        
                        if match:
                            print(f"\n\n✅ ENCONTRADO en {gz_file.relative_to(sources_dir)} (línea {line_num})")
                            print_journal_details(record)
                            return record
                    
                    except json.JSONDecodeError:
                        continue
        
        except Exception as e:
            print(f"\n⚠️ Error en {gz_file.name}: {e}")
            continue
    
    print(f"\n\n❌ No se encontró en {files_processed} archivos.")
    return None

def print_journal_details(record):
    """Imprime los detalles de una revista de forma estructurada."""
    print("="*70)
    print("INFORMACIÓN DE LA REVISTA")
    print("="*70)
    
    print(f"\n📌 IDENTIFICACIÓN")
    print(f"  ID: {record.get('id')}")
    print(f"  Display Name: {record.get('display_name')}")
    print(f"  ISSN-L: {record.get('issn_l')}")
    print(f"  ISSN: {record.get('issn')}")
    print(f"  Publisher: {record.get('publisher')}")
    print(f"  Country: {record.get('country_code')}")
    print(f"  Homepage: {record.get('homepage_url')}")
    
    print(f"\n📊 MÉTRICAS DE PRODUCCIÓN")
    print(f"  works_count: {record.get('works_count'):,}")
    print(f"  cited_by_count: {record.get('cited_by_count'):,}")
    print(f"  oa_works_count: {record.get('oa_works_count', 'N/A')}")
    
    print(f"\n🔓 ACCESO ABIERTO")
    print(f"  is_oa: {record.get('is_oa')}")
    print(f"  is_in_doaj: {record.get('is_in_doaj')}")
    
    print(f"\n🔍 INDEXACIÓN")
    print(f"  is_scopus: {record.get('is_scopus', 'N/A')}")
    print(f"  is_in_scielo: {record.get('is_in_scielo', 'N/A')}")
    print(f"  is_ojs: {record.get('is_ojs', 'N/A')}")
    print(f"  is_core: {record.get('is_core', 'N/A')}")
    
    print(f"\n📈 SUMMARY STATS")
    summary_stats = record.get('summary_stats')
    if summary_stats:
        print(f"  h_index: {summary_stats.get('h_index')}")
        print(f"  i10_index: {summary_stats.get('i10_index')}")
        print(f"  2yr_mean_citedness: {summary_stats.get('2yr_mean_citedness')}")
        print(f"  2yr_cited_by_count: {summary_stats.get('2yr_cited_by_count')}")
        
        # Mostrar otros campos si existen
        other_fields = {k: v for k, v in summary_stats.items() 
                       if k not in ['h_index', 'i10_index', '2yr_mean_citedness', '2yr_cited_by_count']}
        if other_fields:
            print(f"\n  Otros campos en summary_stats:")
            for key, value in other_fields.items():
                print(f"    {key}: {value}")
    else:
        print("  (No disponible)")
    
    print(f"\n📄 REGISTRO COMPLETO (JSON)")
    print("-"*70)
    print(json.dumps(record, indent=2, ensure_ascii=False))

def main():
    parser = argparse.ArgumentParser(
        description='Buscar revistas en el snapshot de OpenAlex',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python search_journal_in_snapshot.py --name "Estudios Demográficos"
  python search_journal_in_snapshot.py --issn "0186-7210"
  python search_journal_in_snapshot.py --id "S2737081250"
  python search_journal_in_snapshot.py --name "Salud Pública" --snapshot-dir /data/openalex
        """
    )
    
    # Grupo mutuamente exclusivo para tipo de búsqueda
    search_group = parser.add_mutually_exclusive_group(required=True)
    search_group.add_argument('--name', help='Buscar por nombre de revista')
    search_group.add_argument('--issn', help='Buscar por ISSN o ISSN-L')
    search_group.add_argument('--id', help='Buscar por ID de OpenAlex (ej: S2737081250)')
    
    parser.add_argument('--snapshot-dir', 
                       default=DEFAULT_SNAPSHOT_BASE,
                       help=f'Directorio del snapshot (default: {DEFAULT_SNAPSHOT_BASE})')
    
    args = parser.parse_args()
    
    # Determinar tipo de búsqueda y término
    if args.name:
        search_type = 'name'
        search_term = args.name
    elif args.issn:
        search_type = 'issn'
        search_term = args.issn
    else:
        search_type = 'id'
        search_term = args.id
    
    # Ejecutar búsqueda
    result = search_journal(search_term, search_type, args.snapshot_dir)
    
    if result:
        print("\n✅ Búsqueda completada exitosamente.")
    else:
        print("\n❌ No se encontró la revista.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
