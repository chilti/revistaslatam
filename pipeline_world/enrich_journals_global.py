import os
import requests
import json
import clickhouse_connect
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ClickHouse Env
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'openalex')

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DATABASE
    )

def fetch_journal_metadata(name_or_issn):
    url = f"https://api.openalex.org/sources?search={name_or_issn}&per_page=1"
    resp = requests.get(url).json()
    if resp['results']:
        return resp['results'][0]
    return None

def fetch_journal_works(journal_id, limit=300):
    # Fetch works from 2021-2025 to enrich recent metrics
    url = f"https://api.openalex.org/works?filter=primary_location.source.id:{journal_id},publication_year:2021-2025&per_page=100"
    works = []
    for page in range(1, (limit // 100) + 1):
        r = requests.get(f"{url}&page={page}").json()
        if not r['results']:
            break
        works.extend(r['results'])
        if len(works) >= limit:
            break
    return works

def inject_to_clickhouse(client, source, works):
    # Inject Source
    source_json = json.dumps(source)
    client.insert('openalex_sources', [[source['id'], source_json]], column_names=['id', 'raw_data'])
    logger.info(f"Inyectada Source: {source['display_name']} ({source['id']})")
    
    # Inject Works
    rows = []
    for w in works:
        rows.append([w['id'], json.dumps(w)])
    if rows:
        client.insert('openalex_works', rows, column_names=['id', 'raw_data'])
        logger.info(f"Inyectados {len(rows)} trabajos para {source['display_name']}")

def main():
    client = get_client()
    
    # Lista de revistas representativas
    target_journals = [
        "Nature", 
        "Science", 
        "The Lancet", 
        "NEJM", 
        "IEEE Transactions on Pattern Analysis and Machine Intelligence",
        "National Science Review", # China
        "Angewandte Chemie", # Germany
        "Progress of Theoretical and Experimental Physics", # Japan
        "Medical Journal of Australia",
        "South African Journal of Science",
        "Scientific Reports" # Global/Multi
    ]
    
    for jt in target_journals:
        logger.info(f"Procesando {jt}...")
        source = fetch_journal_metadata(jt)
        if source:
            works = fetch_journal_works(source['id'], limit=400)
            inject_to_clickhouse(client, source, works)
        else:
            logger.warning(f"No se encontró metadata para {jt}")

if __name__ == "__main__":
    main()
