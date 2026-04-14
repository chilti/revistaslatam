import os
import sys
import pandas as pd
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
    client = get_ch_client()
    latam_countries = GLOBAL_REGIONS['Latinoamérica y Caribe']
    
    print(f"Paises LATAM (n={len(latam_countries)}): {', '.join(latam_countries)}")
    
    # 1. Obtener IDs de revistas (sources) de LATAM
    print("Buscando revistas latinoamericanas en ClickHouse...")
    sources_query = f"""
    SELECT id FROM sources 
    WHERE country_code IN {tuple(latam_countries)} 
    AND type = 'journal'
    """
    sources_df = client.query_df(sources_query)
    journal_ids = sources_df['id'].tolist()
    print(f"Encontradas {len(journal_ids)} revistas.")
    
    if not journal_ids:
        print("No se encontraron revistas para estos países.")
        return

    # 2. Verificar existencia de campos en raw_data y contar artículos
    # OpenAlex fields: is_in_top_1_percent, is_in_top_10_percent
    print("Calculando indicadores de excelencia para artículos en estas revistas...")
    
    # Query optimizada: Usamos una subconsulta para evitar el error de tamaño máximo de consulta
    # y comparamos con percentil >= 0.90 dado que la escala parece ser 0-1
    
    query = f"""
    SELECT 
        count() as total,
        sum(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_10_percent')) as top_10_native,
        sum(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_1_percent')) as top_1_native,
        countIf(JSONExtractFloat(raw_data, 'citation_normalized_percentile', 'value') >= 0.90) as top_10_calc,
        countIf(JSONExtractFloat(raw_data, 'citation_normalized_percentile', 'value') >= 0.99) as top_1_calc,
        countIf(JSONExtractFloat(raw_data, 'citation_normalized_percentile', 'value') > 0) as has_percentile,
        countIf(JSONExtractFloat(raw_data, 'fwci') > 0) as has_fwci
    FROM works
    WHERE source_id IN (
        SELECT id FROM sources 
        WHERE country_code IN {tuple(latam_countries)} 
        AND type = 'journal'
    )
    """
    
    print("Ejecutando query de agregación en ClickHouse (puede tardar)...")
    result = client.query(query)
    
    for row in result.result_set:
        total, top_10_native, top_1_native, top_10_calc, top_1_calc, has_percentile, has_fwci = row
        
        print("\n" + "="*50)
        print("RESULTADOS DE EXCELENCIA (LATAM JOURNALS)")
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
            WHERE source_id IN {tuple(journal_ids)}
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
