import os
import sys
import pandas as pd
import argparse
from pathlib import Path
from dotenv import load_dotenv
import clickhouse_connect

# Add src to path to get regions
sys.path.append(str(Path(__file__).parent.parent))
from src.regions import GLOBAL_REGIONS

def get_ch_client():
    load_dotenv()
    CH_HOST = os.environ.get('CH_HOST', 'localhost')
    CH_PORT = int(os.environ.get('CH_PORT', 8124))
    CH_USER = os.environ.get('CH_USER', 'default')
    CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
    CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')
    
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, 
            username=CH_USER, password=CH_PASSWORD, 
            database=CH_DATABASE
        )
        return client
    except Exception as e:
        print(f"Error conectando a ClickHouse: {str(e)}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Verificar indicadores de excelencia en ClickHouse")
    parser.add_argument("--country", type=str, help="Filtrar por código de país (ej. MX)")
    parser.add_argument("--journal", type=str, help="Filtrar por nombre de revista (ej. 'Estudios Demográficos y Urbanos')")
    args = parser.parse_args()

    client = get_ch_client()
    latam_countries = GLOBAL_REGIONS['Latinoamérica y Caribe']
    
    # Construcción dinámica de filtros para SQLite/Clickhouse
    filters = ["type IN ('journal', 'conference')"]
    if args.journal:
        safe_journal = args.journal.replace("'", "''")
        filters.append(f"display_name ILIKE '%{safe_journal}%'")
    
    if args.country:
        filters.append(f"country_code = '{args.country.upper()}'")
    elif not args.journal:
        # Solo aplicar filtro LATAM si no hay revista epecífica ni país
        filters.append(f"country_code IN {tuple(latam_countries)}")
        
    filter_clause = " AND ".join(filters)
    
    # 1. Obtener conteo de revistas (sources) ÚNICAS de LATAM
    print(f"Buscando revistas en ClickHouse con filtros: {filter_clause} ...")
    sources_query = f"""
    SELECT count(DISTINCT id) FROM sources 
    WHERE {filter_clause}
    AND works_count > 0
    """
    unique_journals = client.command(sources_query)
    print(f"Encontradas {unique_journals:,} revistas únicas activas.")
    
    if unique_journals == 0:
        print("No se encontraron revistas para estos filtros.")
        return

    # 2. Verificar datos de excelencia con una query OPTIMIZADA
    print("Calculando indicadores de excelencia para artículos...")
    
    # Optimizamos: Deduplicamos por id de obra usando argMax para tener conteos reales (Unique IDs)
    query = f"""
    WITH filtered_journals AS (
        SELECT id FROM sources 
        WHERE {filter_clause}
        GROUP BY id
        HAVING argMax(works_count, updated_date) > 0
    ),
    dedup_works AS (
        SELECT 
            id,
            argMax(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_10_percent'), updated_date) as top_10_native,
            argMax(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_1_percent'), updated_date) as top_1_native,
            argMax(JSONExtractFloat(raw_data, 'citation_normalized_percentile', 'value'), updated_date) as percentile_val,
            argMax(JSONExtractFloat(raw_data, 'fwci'), updated_date) as fwci_val
        FROM works
        WHERE source_id IN (SELECT id FROM filtered_journals)
        GROUP BY id
    )
    SELECT 
        count() as total,
        sum(top_10_native) as top_10_n,
        sum(top_1_native) as top_1_n,
        countIf(percentile_val >= 0.90) as top_10_c,
        countIf(percentile_val >= 0.99) as top_1_c,
        countIf(percentile_val > 0) as has_percentile,
        countIf(fwci_val > 0) as has_fwci
    FROM dedup_works
    """
    
    print("Ejecutando query de agregación en ClickHouse (Deduplicación de revistas activa)...")
    result = client.query(query)
    
    for row in result.result_set:
        total, top_10_native, top_1_native, top_10_calc, top_1_calc, has_percentile, has_fwci = row
        
        context_name = args.journal or args.country or "LATAM JOURNALS"
        print("\n" + "="*50)
        print(f"RESULTADOS DE EXCELENCIA ({context_name})")
        print("="*50)
        print(f"Artículos totales: {total:,}")
        print("\n--- Campos Nativos (JSON) ---")
        print(f"Top 10% (native):  {top_10_native:,} ({ (top_10_native/total*100) if total > 0 else 0:.2f}%)")
        print(f"Top 1% (native):   {top_1_native:,} ({ (top_1_native/total*100) if total > 0 else 0:.4f}%)")
        print("\n--- Campos Calculados (Percentil Escala 0-1) ---")
        # Aquí confirmamos mi sospecha de la escala
        print(f"Top 10% (calc 0.9): {top_10_calc:,} ({ (top_10_calc/total*100) if total > 0 else 0:.2f}%)")
        print(f"Top 1% (calc 0.99): {top_1_calc:,} ({ (top_1_calc/total*100) if total > 0 else 0:.4f}%)")
        print("-" * 50)
        print(f"Con Percentil > 0: {has_percentile:,} ({ (has_percentile/total*100) if total > 0 else 0:.2f}%)")
        print(f"Con FWCI > 0:      {has_fwci:,} ({ (has_fwci/total*100) if total > 0 else 0:.2f}%)")
        print("="*50)
        
        if top_10_native == 0 and top_10_calc > 0:
            print("\nLOGRADO: Se confirma que el campo nativo 'is_in_top_10_percent' es cero,")
            print("pero los artículos SÍ tienen percentiles altos (>0.90).")
            print("Debemos calcular los indicadores basándonos en el percentil.")
            print("\nALERTA: El indicador Top 10% es CERO absoluto en la base de datos.")
            print("Posibles causas:")
            print("1. El campo 'is_in_top_10_percent' no existe en el JSON raw_data de este snapshot.")
            print("2. Los datos de OpenAlex para revistas regionales no incluyen este cálculo.")
            print("3. La extracción de JSON falló por estructura inesperada.")
            
            # Prueba de inspección de un registro
            print("\nInspeccionando un registro aleatorio que debería tener estos campos...")
            sample_query = f"""
            SELECT raw_data FROM works 
            WHERE source_id IN (SELECT id FROM sources WHERE {filter_clause})
            LIMIT 1
            """
            sample = client.command(sample_query)
            if sample:
                import json
                data = json.loads(sample)
                print("Campos presentes en raw_data (primer nivel):")
                print(list(data.keys()))
                print(f"\n¿Existe 'is_in_top_10_percent'? {'SÍ' if 'is_in_top_10_percent' in data else 'NO'}")
                print(f"¿Existe 'citation_normalized_percentile'? {'SÍ' if 'citation_normalized_percentile' in data else 'NO'}")
                if 'citation_normalized_percentile' in data:
                    print(f"Valor de percentil: {data['citation_normalized_percentile']}")

if __name__ == "__main__":
    main()
