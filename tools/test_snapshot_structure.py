"""
Script de prueba para verificar la estructura del snapshot y la búsqueda recursiva.
"""
from pathlib import Path

SNAPSHOT_BASE = Path('/mnt/expansion/openalex-snapshot/data')
SOURCES_DIR = SNAPSHOT_BASE / 'sources'

print("="*70)
print("VERIFICACIÓN DE ESTRUCTURA DEL SNAPSHOT")
print("="*70)

if not SOURCES_DIR.exists():
    print(f"❌ No se encontró: {SOURCES_DIR}")
    print("\nIntenta ajustar SNAPSHOT_BASE en el script.")
else:
    print(f"✅ Directorio encontrado: {SOURCES_DIR}")
    
    # Listar subdirectorios (particiones)
    subdirs = sorted([d for d in SOURCES_DIR.iterdir() if d.is_dir()])
    
    if subdirs:
        print(f"\n📁 Encontradas {len(subdirs)} carpetas de partición:")
        
        total_gz = 0
        for i, subdir in enumerate(subdirs):
            gz_files = list(subdir.glob('*.gz'))
            gz_count = len(gz_files)
            total_gz += gz_count
            
            # Mostrar primeras 10 y últimas 5
            if i < 10 or i >= len(subdirs) - 5:
                print(f"  {i+1:3d}. {subdir.name:30s} → {gz_count:4d} archivos .gz")
            elif i == 10:
                print(f"  ... ({len(subdirs) - 15} carpetas más) ...")
        
        print(f"\n📊 TOTAL: {total_gz:,} archivos .gz en todas las particiones")
        
        # Verificar búsqueda recursiva
        print(f"\n🔍 Verificando búsqueda recursiva con rglob...")
        rglob_files = list(SOURCES_DIR.rglob('*.gz'))
        print(f"  rglob encontró: {len(rglob_files):,} archivos")
        
        if len(rglob_files) == total_gz:
            print("  ✅ La búsqueda recursiva funciona correctamente")
        else:
            print(f"  ⚠️ Discrepancia: {abs(len(rglob_files) - total_gz)} archivos de diferencia")
        
        # Mostrar ejemplo de ruta completa
        if rglob_files:
            example = rglob_files[0]
            print(f"\n📄 Ejemplo de ruta completa:")
            print(f"  {example}")
            print(f"  Relativa a sources/: {example.relative_to(SOURCES_DIR)}")
    else:
        print("\n⚠️ No se encontraron subdirectorios de partición")
        
        # Buscar .gz directamente en sources/
        direct_gz = list(SOURCES_DIR.glob('*.gz'))
        if direct_gz:
            print(f"  Pero hay {len(direct_gz)} archivos .gz directamente en sources/")
        else:
            print("  Tampoco hay archivos .gz directamente en sources/")
