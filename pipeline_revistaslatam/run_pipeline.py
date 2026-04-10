import subprocess
import sys
import time
import os
from datetime import datetime
from pathlib import Path

# Configuración de carpetas
BASE_DIR = Path(__file__).parent
PIPELINE_DIR = "pipeline_legacy"

def run_step(script_name, description, args=None):
    """Ejecuta un script de Python como subproceso y maneja errores."""
    print(f"\n{'='*80}")
    print(f"🚀 PASO INICIADO: {description}")
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
    
    parser = argparse.ArgumentParser(description='Orquestador del Pipeline Legacy (Postgres)')
    parser.add_argument('--skip-extraction', action='store_true', help='Omite la extracción de Postgres (usa datos Parquet existentes)')
    parser.add_argument('--only-compute', action='store_true', help='Versión rápida: Solo corre cálculos de métricas y mapas (omite extracción y API)')
    parser.add_argument('--skip-maps', action='store_true', help='Omite el cálculo de mapas UMAP/SOM (más rápido)')
    
    args = parser.parse_args()

    # Definir lógica de ejecución basada en flags
    run_infra = not (args.skip_extraction or args.only_compute)
    run_enrich = not args.only_compute
    run_compute = True # Siempre corre cálculos si estamos aquí
    run_maps = not args.skip_maps
    
    print(f"\n{'*'*80}")
    print(f"🌟 INICIANDO PIPELINE DE ACTUALIZACIÓN (MODO: {'Sólo Cálculo' if args.only_compute else 'Completo'}) 🌟")
    print(f"   Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'*'*80}\n")
    
    total_start = time.time()
    email = os.environ.get('OPENALEX_EMAIL')

    # --- FASE 1: DATOS CRUDOS ---
    if run_infra:
        # 1. Extracción de datos
        if not run_step(f"{PIPELINE_DIR}/extract_postgres.py", "Extracción de Datos (Postgres -> Parquet Parts)"):
            sys.exit(1)

        # 2. Consolidación
        if not run_step(f"{PIPELINE_DIR}/consolidate_files.py", "Consolidación de Archivos (Unir partes -> Works Completo)"):
            pass
    else:
        print("⏭️ Omitiendo Fase de Infraestructura/Postgres (usando archivos Parquet locales)")

    # --- FASE 2: ENRIQUECIMIENTO Y MÉTRICAS BASE ---

    if run_enrich:
        # 3. Enriquecimiento API (Tópicos)
        enrich_args = []
        if email:
            enrich_args = ["--email", email]
        
        if not run_step(f"{PIPELINE_DIR}/enrich_journals_api.py", "Enriquecimiento Temático (API OpenAlex)", args=enrich_args):
            print("⚠️ Advertencia: El enriquecimiento temático falló.")
    else:
        print("⏭️ Omitiendo Fase de Enriquecimiento API")

    if run_compute:
        # 4. Cálculo de métricas de desempeño (Paralelo)
        if not run_step(f"{PIPELINE_DIR}/precompute_metrics_parallel.py", "Cálculo de Métricas de Desempeño (Anuales y Periodo)"):
            sys.exit(1)

        # --- FASE 3: VISUALIZACIÓN ANALÍTICA ---

        # 5. Métricas Temáticas (4 niveles + Indicadores)
        run_step(f"{PIPELINE_DIR}/compute_topics_metrics_postgres.py", "Cálculo de Indicadores Jerárquicos (Sunburst 4 Niveles)")

        # 6. Sunburst Regional
        run_step(f"{PIPELINE_DIR}/generate_country_sunburst.py", "Generación de Sunburst a nivel País (Agregación)")

    # --- FASE 4: MAPAS Y PROYECCIONES ---
    if run_maps:
        print("\n" + "-"*80)
        print("📈 INICIANDO FASE DE PROYECCIONES (MAPAS UMAP Y SOM)")
        print("-"*80)

        map_scripts = [
            (f"{PIPELINE_DIR}/process_trajectories.py", "Procesamiento de Trayectorias Dinámicas"),
            (f"{PIPELINE_DIR}/calculate_umap.py", "UMAP Estático (Países y Revistas)"),
            (f"{PIPELINE_DIR}/calculate_som.py", "SOM de Países (U-Matrix)"),
            (f"{PIPELINE_DIR}/calculate_som_trajectories.py", "SOM de Trayectorias (Global)")
        ]

        for script, desc in map_scripts:
            run_step(script, desc)
    else:
        print("⏭️ Omitiendo Fase de Mapas/Proyecciones")

    total_elapsed = time.time() - total_start
    print(f"\n{'*'*80}")
    print(f"🎉 ¡PIPELINE COMPLETO FINALIZADO EXITOSAMENTE! 🎉")
    print(f"   Tiempo Total de Ejecución: {total_elapsed:.2f} segundos ({total_elapsed/60:.2f} minutos)")
    print(f"{'*'*80}")
    print("\nSiguientes pasos recomendados:")
    print("1. Verifica los archivos generados en 'data/cache/', 'data/umap/' y 'data/som/'.")
    print("2. Ejecuta el dashboard para visualizar los cambios:")
    print("   streamlit run dashboard.py\n")
