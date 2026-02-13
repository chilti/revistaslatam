import pandas as pd
import os
from pathlib import Path

# Configuración
WORKS_FILE = Path('data/latin_american_works.parquet')
TARGET_JOURNAL = 'Estudios Demográficos y Urbanos'

def inspect_journal():
    if not WORKS_FILE.exists():
        print(f"❌ Error: No se encontró el archivo {WORKS_FILE}")
        return

    print(f"📂 Cargando {WORKS_FILE} ...")
    try:
        # Cargamos el dataframe completo (300MB aprox, cabe en memoria)
        df = pd.read_parquet(WORKS_FILE)
        print(f"   Total de trabajos en el archivo: {len(df)}")
        
        # Verificar columna de nombre de revista
        # A veces es 'journal_name', 'display_name', o dentro de 'process_work' logic se extrajo distinto.
        # En inspect anterior vimos 'journal_name'.
        
        if 'journal_name' not in df.columns:
            print("⚠️ Columna 'journal_name' no encontrada. Columnas disponibles:")
            print(df.columns.tolist())
            return

        # Filtrar por nombre (case insensitive)
        print(f"🔍 Buscando trabajos de: '{TARGET_JOURNAL}' ...")
        mask = df['journal_name'].str.contains(TARGET_JOURNAL, case=False, na=False)
        journal_works = df[mask]
        
        count = len(journal_works)
        print(f"✅ Se encontraron {count} trabajos.")
        
        if count > 0:
            print("\n--- Ejemplo de Trabajos (Top 5) ---")
            # Columnas relevantes para mostrar
            cols_to_show = ['id', 'publication_year', 'title', 'cited_by_count', 'fwci', 'citation_normalized_percentile']
            # Filtrar solo columnas que existen
            cols = [c for c in cols_to_show if c in df.columns]
            
            print(journal_works[cols].head().to_string(index=False))
            
            print("\n--- Distribución por Año ---")
            print(journal_works['publication_year'].value_counts().sort_index())
            
            # Exportar a CSV
            output_csv = 'estudios_demograficos_works.csv'
            journal_works.to_csv(output_csv, index=False)
            print(f"\n💾 Trabajos exportados a: {output_csv}")
            
        else:
            print("⚠️ No se encontraron trabajos con ese nombre exacto.")
            print("Nombres de revistas similares encontrados:")
            # Buscar coincidencias parciales si falla
            mask_partial = df['journal_name'].str.contains('Demogr', case=False, na=False)
            print(df[mask_partial]['journal_name'].unique())

    except Exception as e:
        print(f"❌ Error procesando el archivo: {e}")

if __name__ == "__main__":
    inspect_journal()
