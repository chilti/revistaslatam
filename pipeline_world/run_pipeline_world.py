import subprocess
import sys
import time
import os
from datetime import datetime
from pathlib import Path

# Configuración de carpetas
BASE_DIR = Path(__file__).parent
PIPELINE_DIR = "pipeline_world"

def run_step(script_name, description, args=None):
    """Ejecuta un script de Python como subproceso y maneja errores."""
    print(f"\n{'='*80}")
    print(f"🚀 PASO INICIADO (GLOBAL): {description}")
    print(f"   Archivo: {script_name}")
    print(f"{'='*80}\n")
    
    start_time = time.time()
    
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    
    try:
        # Ejecutar el proceso
        process = subprocess.run(
            cmd, 
            check=True,
            text=True
        )
        
        elapsed_time = time.time() - start_time
        print(f"\n{'='*80}")
        print(f"✅ PASO COMPLETADO EXITOSAMENTE")
        print(f"⏱️ Tiempo: {elapsed_time:.2f} segundos ({elapsed_time/60:.2f} minutos)")
        print(f"{'='*80}\n")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n{'!'*80}")
        print(f"❌ ERROR CRÍTICO EN EL PASO: {description}")
        print(f"   El script {script_name} falló con código de salida {e.returncode}")
        print(f"{'!'*80}\n")
        return False
        
    except FileNotFoundError:
        print(f"\n❌ ERROR: No se encontró el archivo {script_name}")
        return False
        
    except Exception as e:
        print(f"\n❌ ERROR INESPERADO: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Orquestador del Pipeline Global (ClickHouse)')
    parser.add_argument('--skip-metrics', action='store_true', help='Salta el cálculo OLAP en ClickHouse (usa Parquets existentes)')
    parser.add_argument('--skip-maps', action='store_true', help='Omite el cálculo de mapas UMAP/SOM')
    
    args = parser.parse_args()

    print(f"\n{'*'*80}")
    print(f"🌟 INICIANDO PIPELINE GLOBAL (CLICKHOUSE) 🌟")
    print(f"   Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'*'*80}\n")
    
    total_start = time.time()

    # --- FASE 1: CÁLCULO ANALÍTICO (OLAP) ---
    if not args.skip_metrics:
        # 1. Ejecutar métricas base, sunburst y evolución temática en ClickHouse
        if not run_step(f"{PIPELINE_DIR}/compute_metrics_clickhouse.py", "Cálculo de Métricas Globales (OLAP - ClickHouse)"):
            sys.exit(1)
    else:
        print("⏭️ Omitiendo Fase de Métricas ClickHouse (usando archivos Parquet locales)")

    # --- FASE 2: PROYECCIONES Y MAPAS ---
    if not args.skip_maps:
        print("\n" + "-"*80)
        print("📈 INICIANDO FASE DE PROYECCIONES GLOBALES (UMAP/SOM)")
        print("-"*80)

        map_scripts = [
            (f"{PIPELINE_DIR}/calculate_umap_global.py", "UMAP Global (Macro Regiones y Países Mundiales)"),
            (f"{PIPELINE_DIR}/calculate_umap.py", "UMAP Detallado (Países/Revistas Globales)"),
            (f"{PIPELINE_DIR}/calculate_som.py", "SOM Global (U-Matrix)"),
            (f"{PIPELINE_DIR}/calculate_som_trajectories.py", "SOM de Trayectorias Globales")
        ]

        for script, desc in map_scripts:
            # Los mapas estáticos consumen los Parquets generados en la Fase 1
            run_step(script, desc)
    else:
        print("⏭️ Omitiendo Fase de Mapas/Proyecciones")

    total_elapsed = time.time() - total_start
    print(f"\n{'*'*80}")
    print(f"🎉 ¡PIPELINE GLOBAL FINALIZADO EXITOSAMENTE! 🎉")
    print(f"   Tiempo Total de Ejecución: {total_elapsed:.2f} segundos ({total_elapsed/60:.2f} minutos)")
    print(f"{'*'*80}")
