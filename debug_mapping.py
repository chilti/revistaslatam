import requests
import os
import pandas as pd
from pathlib import Path

# Configuración idéntica al script
OPENALEX_LOCAL_API = os.environ.get('OPENALEX_LOCAL_API', 'http://localhost:5012')
DATA_DIR = Path(__file__).parent.parent / 'data'
MAPPING_FILE = DATA_DIR / 'works_topics_mapping.parquet'

print(f"DEBUG: DATA_DIR detectado como: {DATA_DIR.absolute()}")
print(f"DEBUG: MAPPING_FILE detectado como: {MAPPING_FILE.absolute()}")

# Test de API Local con una revista conocida (S4306423602 - Journal of Latin American Studies)
test_journal = "S4306423602"
url = f"{OPENALEX_LOCAL_API}/works"
params = {
    'filter': f"primary_location.source.id:{test_journal}",
    'per_page': 1
}

try:
    print(f"DEBUG: Consultando API Local en {url} con filtro {test_journal}...")
    resp = requests.get(url, params=params, timeout=5)
    print(f"DEBUG: Status Code: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        count = data.get('meta', {}).get('count', 0)
        print(f"DEBUG: Se encontraron {count} artículos para esa revista.")
        if count == 0:
            print("⚠️ ADVERTENCIA: El API Local no devolvió artículos. Es probable que no tenga los datos cargados.")
    else:
        print(f"❌ ERROR: El API Local respondió con status {resp.status_code}")
except Exception as e:
    print(f"❌ EXCEPCIÓN: No se pudo conectar con el API Local: {e}")
