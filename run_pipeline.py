
import subprocess
import sys
import time
from datetime import datetime

def run_step(script_name, description):
    """Ejecuta un script de Python como subproceso y maneja errores."""
    print(f"\n{'='*70}")
    print(f"PASO INICIADO: {description}")
    print(f"Ejecutando: {script_name}")
    print(f"{'='*70}\n")
    
    start_time = time.time()
    
    # Ejecuta el script usando el mismo intérprete de Python actual
    try:
        # check=True lanza una excepción si el subproceso devuelve error
        process = subprocess.run(
            [sys.executable, script_name], 
            check=True,
            text=True  # Manejo de texto para stdout/stderr
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
    print(f"Iniciando Pipeline de Actualización de Datos y Métricas")
    print(f"Fecha/Hora: {datetime.now().isoformat()}")
    
    total_start = time.time()

    # PASO 1: Extracción de datos desde PostgreSQL
    # Este script genera los archivos Parquet base en data/
    step1_success = run_step(
        "data_collector_postgres.py", 
        "Extracción de Datos de Revistas y Trabajos (PostgreSQL -> Parquet)"
    )
    
    if not step1_success:
        print("🛑 Deteniendo el pipeline debido a error en la extracción.")
        sys.exit(1)
        
    # PASO 2: Cálculo de métricas
    # Este script lee los Parquet generados y calcula métricas complejas
    step2_success = run_step(
        "precompute_metrics_parallel_optimized.py", 
        "Precomputación de Métricas de Desempeño (Parquet -> Métricas)"
    )
    
    if not step2_success:
        print("🛑 Deteniendo el pipeline debido a error en el cálculo de métricas.")
        sys.exit(1)

    total_elapsed = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"🎉 PIPELINE COMPLETO FINALIZADO EXITOSAMENTE")
    print(f"Tiempo Total: {total_elapsed:.2f} segundos ({total_elapsed/60:.2f} minutos)")
    print(f"{'='*70}")
    print("\nAhora puedes ejecutar el dashboard para ver los resultados actualizados:")
    print("  streamlit run dashboard.py")
