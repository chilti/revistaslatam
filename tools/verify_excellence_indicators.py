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
    
    # Vamos a procesar por partes para no saturar si hay millones
    batch_size = 500
    total_works = 0
    top_10_count = 0
    top_1_count = 0
    works_with_percentile = 0
    works_with_fwci = 0
    
    # Query optimizada para contar directamente en ClickHouse
    # Nota: Usamos JSONExtractBool para extraer del campo raw_data
    # Pero primero verificamos si existen como columnas materializadas o en el JSON
    
    query = f"""
    SELECT 
        count() as total,
        sum(JSONExtractBool(raw_data, 'is_in_top_10_percent')) as top_10,
        sum(JSONExtractBool(raw_data, 'is_in_top_1_percent')) as top_1,
        countIf(JSONExtractFloat(raw_data, 'citation_normalized_percentile') > 0) as has_percentile,
        countIf(JSONExtractFloat(raw_data, 'fwci') > 0) as has_fwci
    FROM works
    WHERE source_id IN {tuple(journal_ids)}
    """
    
    print("Ejecutando query de agregación en ClickHouse (puede tardar)...")
    result = client.query(query)
    
    for row in result.result_set:
        total, top_10, top_1, has_percentile, has_fwci = row
        
        print("\n" + "="*50)
        print("RESULTADOS DE EXCELENCIA (LATAM JOURNALS)")
        print("="*50)
        print(f"Artículos totales: {total:,}")
        print(f"En Top 10%:        {top_10:,} ({ (top_10/total*100) if total > 0 else 0:.2f}%)")
        print(f"En Top 1%:         {top_10:,} ({ (top_1/total*100) if total > 0 else 0:.4f}%)")
        print("-" * 50)
        print(f"Con Percentil > 0: {has_percentile:,} ({ (has_percentile/total*100) if total > 0 else 0:.2f}%)")
        print(f"Con FWCI > 0:      {has_fwci:,} ({ (has_fwci/total*100) if total > 0 else 0:.2f}%)")
        print("="*50)
        
        if top_10 == 0:
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
