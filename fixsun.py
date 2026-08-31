import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

parquet_file = 'data/journals_topics_sunburst.parquet'
df = pd.read_parquet(parquet_file)
unique_topics = df['topic_id'].unique()
print(f'Descargando "subfields" para {len(unique_topics)} tópicos únicos vía API...')

topic_subfield_map = {}

def get_subfield(topic_url):
    try:
        clean_id = topic_url.split('/')[-1]
        resp = requests.get(f'https://api.openalex.org/topics/{clean_id}', timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return topic_url, data.get('subfield', {}).get('display_name', 'Unknown')
    except Exception as e:
        pass
    return topic_url, 'Unknown'

# Usa ThreadPoolExecutor para paralelizar y hacerlo en segundos en vez de horas
with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(get_subfield, unique_topics)
    for i, (topic_url, subfield) in enumerate(results):
        topic_subfield_map[topic_url] = subfield
        if i % 100 == 0 and i > 0:
            print(f"  Procesados {i} tópicos...")

df['subfield'] = df['topic_id'].map(topic_subfield_map)

# Reordenar las columnas para dejar subfield en un lugar lógico (antes de field)
cols = ['journal_id', 'journal_name', 'topic_name', 'topic_id', 'subfield', 'field', 'domain', 'count', 'share']
df = df[cols]

# Sobreescribir parquet
df.to_parquet(parquet_file, index=False)
print('✅ ¡Completado! El archivo parquet de Sunburst ha sido actualizado.')
