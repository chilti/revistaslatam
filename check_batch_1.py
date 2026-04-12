import os
import pandas as pd
from dotenv import load_dotenv
import clickhouse_connect

load_dotenv()

def main():
    client = clickhouse_connect.get_client(
        host=os.environ.get('CH_HOST'),
        port=int(os.environ.get('CH_PORT')),
        username=os.environ.get('CH_USER'),
        password=os.environ.get('CH_PASSWORD'),
        database=os.environ.get('CH_DATABASE')
    )
    
    countries = ("AR", "BO", "BR", "CL", "CO", "CR", "CU", "DO", "EC", "SV", "GT", "HN", "MX", "NI", "PA", "PY", "PE", "PR", "UY", "VE")
    
    query = f"""
    SELECT 
        id, 
        display_name, 
        works_count, 
        country_code,
        JSONExtractString(raw_data, 'type') as source_type
    FROM sources 
    WHERE type = 'journal' 
      AND country_code IN {countries} 
      AND works_count > 0 
    ORDER BY works_count DESC 
    LIMIT 15
    """
    
    df = client.query_df(query)
    print("🔝 TOP 15 FUENTES POR VOLUMEN EN CLICKHOUSE")
    print("-" * 100)
    print(df.to_string(index=False))
    
    # Comprobar si hay duplicados en la consulta de works
    top_ids = df['id'].tolist()
    query_works_check = f"""
    SELECT source_id, count() 
    FROM works 
    WHERE source_id IN {tuple(top_ids)}
    GROUP BY source_id
    ORDER BY count() DESC
    """
    df_works = client.query_df(query_works_check)
    print("\n📦 CONTEO REAL DE ARTÍCULOS EN LA TABLA 'works' PARA ESTOS IDS")
    print("-" * 100)
    print(df_works.to_string(index=False))

if __name__ == "__main__":
    main()
