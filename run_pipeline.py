import subprocess
import sys
import time
import os
from datetime import datetime

def run_step(script_name, description):
    """Ejecuta un script de Python como subproceso y maneja errores."""
    print(f"\n{'='*70}")
    print(f"PASO INICIADO: {description}")
    print(f"Ejecutando: {script_name}")
    print(f"{'='*70}\n")
    
    start_time = time.time()
    
    try:
        # Usa el mismo ejecutable de python
        process = subprocess.run(
            [sys.executable, script_name], 
            check=True,
            text=True
        )
        
        elapsed_time = time.time() - start_time
        print(f"\n{'='*70}")
        print(f"✅ PASO COMPLETADO EXITOSAMENTE")
        print(f"Tiempo: {elapsed_time:.2f} segundos ({elapsed_time/60:.2f} minutos)")
        print(f"{'='*70}\n")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n{'='*70}")
        print(f"❌ ERROR CRÍTICO EN EL PASO: {description}")
        print(f"El script {script_name} falló con código de salida {e.returncode}")
        print(f"{'='*70}\n")
        return False
        
    except FileNotFoundError:
        print(f"\n❌ ERROR: No se encontró el archivo {script_name}")
        return False
        
    except Exception as e:
        print(f"\n❌ ERROR INESPERADO: {e}")
        return False

if __name__ == "__main__":
    print(f"Iniciando Pipeline de Actualización de Datos y Métricas v2.0")
    print(f"Fecha/Hora: {datetime.now().isoformat()}")
    
    total_start = time.time()

    # PASO 1: Extracción de datos (PostgreSQL)
    step1_success = run_step(
        "pipeline/extract_postgres.py", 
        "Extracción de Datos de Revistas y Trabajos (PostgreSQL -> Parquet)"
    )
    
    if not step1_success:
        print("🛑 Deteniendo el pipeline debido a error en la extracción.")
        sys.exit(1)

    # PASO 1.5: Consolidación de archivos Parquet
    # Une los archivos parciales generados por extract_postgres.py en un solo archivo maestro
    # (Si consolidated_files.py no existe, esto fallará pero no es crítico si extract_postgres hizo su trabajo)
    # PASO 1.5: Consolidación de archivos Parquet
    # Priorizar versión streaming (bajo consumo RAM) si existe
    consolidation_script = "pipeline/consolidate_files_stream.py"
    if not os.path.exists(consolidation_script):
        consolidation_script = "pipeline/consolidate_files.py"

    if os.path.exists(consolidation_script):
        step15_success = run_step(
            consolidation_script, 
            f"Consolidación de Archivos Parquet ({consolidation_script})"
        )
        if not step15_success:
            print("⚠️ Advertencia: La consolidación falló. El cálculo de métricas podría usar datos incompletos.")
    else:
        print("ℹ️ Paso 1.5 omitido: Script de consolidación no encontrado.")

    # PASO 2: Enriquecimiento con API (Topics para Sunburst)
    print("\n" + "="*70)
    print("PASO EXTRA: Enriquecimiento de Revistas (API OpenAlex)")
    print("="*70)
    
    email = os.environ.get('OPENALEX_EMAIL')
    api_args = [sys.executable, "pipeline/enrich_journals_api.py"]
    if email:
        print(f"Usando email de variable de entorno: {email}")
        api_args.extend(['--email', email])
    else:
        print("No se detectó variable OPENALEX_EMAIL (usando modo lento)")

    if os.path.exists("pipeline/enrich_journals_api.py"):
        try:
            subprocess.run(api_args, check=False) # check=False para no detener si falla la API
            print("✅ Enriquecimiento completado (o intentado)")
        except Exception as e:
            print(f"⚠️ Falló el enriquecimiento API (no crítico): {e}")
    else:
        print("ℹ️ Paso 2 omitido: enrich_journals_api.py no encontrado.")

    # PASO 3: Cálculo de métricas
    step3_success = run_step(
        "pipeline/transform_metrics.py", 
        "Precomputación de Métricas de Desempeño (Parquet -> Métricas)"
    )
    
    if not step3_success:
        print("🛑 Deteniendo el pipeline debido a error en el cálculo de métricas.")
        sys.exit(1)

    # PASO 3.5: Cálculo de UMAP para países y revistas
    step35_success = run_step(
        "pipeline/calculate_umap.py", 
        "Cálculo de Embeddings UMAP (Países y Revistas)"
    )
    
    if not step35_success:
        print("⚠️ Advertencia: Falló el cálculo de UMAP. Las visualizaciones UMAP no estarán disponibles.")

    # PASO 4: Cálculo de Trayectorias y Suavizado (UMAP)
    step4_success = run_step(
        "pipeline/process_trajectories.py", 
        "Cálculo de Trayectorias (Suavizado + UMAP)"
    )
    
    if not step4_success:
        print("⚠️ Advertencia: Falló el cálculo de trayectorias. El dashboard no mostrará esta sección.")

    total_elapsed = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"🎉 PIPELINE COMPLETO FINALIZADO EXITOSAMENTE")
    print(f"Tiempo Total: {total_elapsed:.2f} segundos ({total_elapsed/60:.2f} minutos)")
    print(f"{'='*70}")
    print("\nAhora puedes ejecutar el dashboard para ver los resultados actualizados:")
    print("  streamlit run dashboard.py")
