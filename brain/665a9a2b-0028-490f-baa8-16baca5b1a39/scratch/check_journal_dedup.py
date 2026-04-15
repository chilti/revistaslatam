import os
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def check_deduplication():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, 
        username=CH_USER, password=CH_PASSWORD, 
        database=CH_DATABASE
    )
    
    journal_name = "Estudios Demográficos y Urbanos"
    
    query = f"""
    WITH filtered_journals AS (
        SELECT id FROM sources 
        WHERE display_name ILIKE '%{journal_name}%'
        AND type IN ('journal', 'conference')
        GROUP BY id
        HAVING argMax(works_count, updated_date) > 0
    )
    SELECT 
        count() as raw_count,
        count(DISTINCT id) as unique_count
    FROM works
    WHERE source_id IN (SELECT id FROM filtered_journals)
    """
    
    result = client.query(query)
    for row in result.result_set:
        print(f"Journal: {journal_name}")
        print(f"Raw Rows in ClickHouse: {row[0]}")
        print(f"Unique Work IDs: {row[1]}")

if __name__ == "__main__":
    check_deduplication()
