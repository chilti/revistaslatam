"""
Comparación lado a lado: PostgreSQL vs Snapshot vs Parquet actual

Este script ayuda a identificar dónde está el problema en la cadena de datos.
"""
import pandas as pd
from pathlib import Path

print("="*70)
print("COMPARACIÓN DE FUENTES DE DATOS")
print("="*70)

# 1. Datos del Parquet actual
print("\n1️⃣ PARQUET ACTUAL (latin_american_journals.parquet)")
print("-"*70)
parquet_file = Path('data/latin_american_journals.parquet')
if parquet_file.exists():
    df = pd.read_parquet(parquet_file)
    target = df[df['issn_l'] == '0186-7210']
    
    if not target.empty:
        journal = target.iloc[0]
        print(f"works_count: {journal.get('works_count')}")
        print(f"h_index: {journal.get('h_index')}")
        print(f"i10_index: {journal.get('i10_index')}")
        print(f"2yr_mean_citedness: {journal.get('2yr_mean_citedness')}")
        print(f"oa_works_count: {journal.get('oa_works_count')}")
        print(f"is_ojs: {journal.get('is_ojs')}")
        print(f"is_in_scielo: {journal.get('is_in_scielo')}")
        print(f"is_core: {journal.get('is_core')}")
    else:
        print("❌ No encontrada")
else:
    print("❌ Archivo no existe")

# 2. Datos del Snapshot (según búsqueda manual)
print("\n2️⃣ SNAPSHOT DE OPENALEX (2025-10-27)")
print("-"*70)
print("works_count: 1985")
print("h_index: 25")
print("i10_index: 162")
print("2yr_mean_citedness: 0.5119047619047619")
print("oa_works_count: 1985")
print("is_ojs: True")
print("is_in_scielo: False")
print("is_core: False")

# 3. Resumen de discrepancias
print("\n3️⃣ ANÁLISIS DE DISCREPANCIAS")
print("-"*70)

if parquet_file.exists() and not target.empty:
    discrepancies = []
    
    expected = {
        'h_index': 25,
        'i10_index': 162,
        '2yr_mean_citedness': 0.5119,
        'oa_works_count': 1985,
        'is_ojs': True,
    }
    
    for field, expected_val in expected.items():
        actual_val = journal.get(field)
        if actual_val != expected_val:
            # Para floats, comparar con tolerancia
            if isinstance(expected_val, float):
                if abs(actual_val - expected_val) > 0.001:
                    discrepancies.append(f"❌ {field}: {actual_val} (esperado: {expected_val})")
            else:
                discrepancies.append(f"❌ {field}: {actual_val} (esperado: {expected_val})")
        else:
            print(f"✅ {field}: OK")
    
    if discrepancies:
        print("\n⚠️ PROBLEMAS ENCONTRADOS:")
        for d in discrepancies:
            print(f"  {d}")
        
        print("\n🔍 POSIBLES CAUSAS:")
        print("  1. PostgreSQL no tiene la columna 'summary_stats'")
        print("  2. Los campos nuevos (oa_works_count, is_ojs, etc.) no existen en PostgreSQL")
        print("  3. El script de extracción no está parseando correctamente")
        print("\n💡 SIGUIENTE PASO:")
        print("  Ejecuta: python tools/check_postgres_columns.py")
        print("  Esto te dirá exactamente qué columnas tiene PostgreSQL")
