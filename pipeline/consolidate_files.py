import os
import pandas as pd
from pathlib import Path
import time
import sys

# Configuración de Rutas
DATA_DIR = Path(__file__).parent.parent / 'data'
PARTS_DIR = DATA_DIR / 'works_parts'
OUTPUT_FILE = DATA_DIR / 'latin_american_works.parquet'

def consolidate():
    print("="*60)
    print("CONSOLIDACIÓN ROBUSTA DE ARCHIVOS PARQUET (Pandas Engine)")
    print("="*60)
    
    if not PARTS_DIR.exists():
        print(f"❌ No existe el directorio de partes: {PARTS_DIR}")
        return

    # Listar archivos
    files = list(PARTS_DIR.glob('*.parquet'))
    if not files:
        print("⚠️ No hay archivos .parquet en works_parts/")
        return
        
    # Ordenar para consistencia
    files.sort(key=lambda x: x.name)

    print(f"📂 Encontrados {len(files)} archivos parciales.")
    total_size_mb = sum(f.stat().st_size for f in files) / (1024*1024)
    print(f"📊 Tamaño total en disco (partes): {total_size_mb:.2f} MB")
    
    start_time = time.time()
    
    # Leer y concatenar
    dfs = []
    success_count = 0
    fail_count = 0
    
    print("\n📖 Leyendo archivos...")
    for i, f in enumerate(files):
        try:
            # Imprimir progreso visualmente
            if i % 5 == 0 or i == len(files)-1:
                sys.stdout.write(f"\r  Procesando {i+1}/{len(files)}: {f.name} ({len(dfs)} ok)")
                sys.stdout.flush()
            
            df = pd.read_parquet(f)
            if not df.empty:
                # Optimización preliminar de tipos para ahorrar memoria si es posible
                # (Opcional, pero pandas suele ser eficiente con parquet)
                dfs.append(df)
                success_count += 1
            else:
                print(f"\n  ⚠️ Archivo vacío: {f.name}")
                
        except Exception as e:
            print(f"\n  ❌ Error leyendo {f.name}: {e}")
            fail_count += 1
            
    print(f"\n\n✅ Leídos exitosamente: {success_count} archivos.")
    if fail_count > 0:
        print(f"❌ Fallaron: {fail_count} archivos.")
    
    if not dfs:
        print("⛔ No se pudieron leer datos válidos. Abortando.")
        return
        
    print(f"\n🧩 Concatenando {len(dfs)} DataFrames en memoria...")
    try:
        # Concatenación con alineación automática de columnas (outer join implícito)
        # Esto maneja diferencias de esquema (columnas faltantes se llenan con NaN)
        full_df = pd.concat(dfs, ignore_index=True, sort=False)
        print(f"  ✓ Filas totales (bruto): {len(full_df):,}")
        
        # Deduplicar
        print("🔍 Eliminando duplicados por ID...")
        if 'id' in full_df.columns:
            before = len(full_df)
            full_df = full_df.drop_duplicates(subset=['id'], keep='last')
            print(f"  ✓ Eliminados {before - len(full_df):,} duplicados.")
        
        # Verificar integridad mínima
        print("🛡️ Verificando integridad...")
        print(f"  Columnas ({len(full_df.columns)}): {list(full_df.columns)}")
        
        print(f"💾 Guardando archivo maestro: {OUTPUT_FILE}...")
        full_df.to_parquet(OUTPUT_FILE, index=False)
        
        final_size_mb = OUTPUT_FILE.stat().st_size / (1024*1024)
        print(f"\n🎉 CONSOLIDACIÓN EXITOSA")
        print(f"  📄 Filas finales: {len(full_df):,}")
        print(f"  📦 Tamaño final: {final_size_mb:.2f} MB")
        print(f"  ⏱️ Tiempo total: {time.time() - start_time:.2f} s")
        
    except MemoryError:
        print(f"\n❌ ERROR DE MEMORIA: No se pudo concatenar todo en RAM.")
        print("Sugerencia: Ejecuta en una máquina con más RAM o implementa consolidación incremental.")
        
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO en concatenación/guardado: {e}")

if __name__ == "__main__":
    consolidate()
