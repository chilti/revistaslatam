import pandas as pd
from pathlib import Path

# Cargar datos
journals_file = Path('data/latin_american_journals.parquet')
works_file = Path('data/latin_american_works.parquet')

if journals_file.exists():
    journals_df = pd.read_parquet(journals_file)
    
    # Buscar "Estudios Demográficos y Urbanos"
    target = journals_df[journals_df['display_name'].str.contains('Estudios Demográficos', case=False, na=False)]
    
    if not target.empty:
        journal = target.iloc[0]
        print("="*70)
        print("DIAGNÓSTICO: Estudios Demográficos y Urbanos")
        print("="*70)
        print(f"\nID: {journal['id']}")
        print(f"Display Name: {journal['display_name']}")
        print(f"ISSN-L: {journal['issn_l']}")
        print(f"\n--- MÉTRICAS DEL ARCHIVO JOURNALS ---")
        print(f"works_count: {journal.get('works_count', 'N/A')}")
        print(f"cited_by_count: {journal.get('cited_by_count', 'N/A')}")
        print(f"h_index: {journal.get('h_index', 'N/A')}")
        print(f"i10_index: {journal.get('i10_index', 'N/A')}")
        print(f"2yr_mean_citedness: {journal.get('2yr_mean_citedness', 'N/A')}")
        print(f"is_oa: {journal.get('is_oa', 'N/A')}")
        print(f"is_in_doaj: {journal.get('is_in_doaj', 'N/A')}")
        
        # Verificar campos adicionales solicitados
        print(f"\n--- CAMPOS ADICIONALES SOLICITADOS ---")
        for field in ['oa_works_count', 'is_in_scielo', 'is_ojs', 'is_core']:
            val = journal.get(field, 'NO EXISTE')
            print(f"{field}: {val}")
        
        # Contar trabajos reales en works file
        if works_file.exists():
            print(f"\n--- CONTEO REAL EN WORKS FILE ---")
            works_df = pd.read_parquet(works_file, columns=['journal_id'])
            actual_count = (works_df['journal_id'] == journal['id']).sum()
            print(f"Trabajos reales en latin_american_works.parquet: {actual_count}")
            print(f"Diferencia con works_count: {journal['works_count'] - actual_count}")
        
        # Mostrar todas las columnas disponibles
        print(f"\n--- TODAS LAS COLUMNAS DISPONIBLES ---")
        print(journals_df.columns.tolist())
        
    else:
        print("No se encontró la revista")
else:
    print("Archivo de revistas no encontrado")
