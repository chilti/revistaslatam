import argparse
import json
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de URLs
OPENALEX_OFFICIAL_API = "https://api.openalex.org"
OPENALEX_LOCAL_API = os.environ.get('OPENALEX_LOCAL_API', 'http://localhost:5012')

# Directorio de datos
DATA_DIR = Path(__file__).parent.parent / 'data'
JOURNALS_FILE = DATA_DIR / 'latin_american_journals.parquet'
MAPPING_FILE = DATA_DIR / 'works_topics_mapping.parquet'

def save_partial(data_list, output_path=MAPPING_FILE):
    if not data_list:
        return
    try:
        df = pd.DataFrame(data_list)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
    except Exception as e:
        print(f"Error guardando parcial: {e}")

def enrich_works_topics(email=None):
    if not JOURNALS_FILE.exists():
        print(f"❌ No se encontró el archivo de revistas: {JOURNALS_FILE}")
        return

    print("="*70)
    print("ENRIQUECIMIENTO DE TÓPICOS POR ARTÍCULO (API OPENALEX)")
    print("Objetivo: Obtener el mapeo Artículo <-> Tópicos para Sunburst granular")
    if email:
        print(f"Usando email: {email} (Polite Pool)")
    print("="*70)

    # Cargar revistas
    journals_df = pd.read_parquet(JOURNALS_FILE)
    # Ordenar por cantidad de trabajos para procesar las más grandes primero o viceversa
    # journals_df = journals_df.sort_values('works_count', ascending=False)
    journal_ids = journals_df['id'].tolist()
    
    # Progreso existente
    existing_mapping = []
    processed_journal_ids = set()
    
    if MAPPING_FILE.exists():
        try:
            old_df = pd.read_parquet(MAPPING_FILE)
            if not old_df.empty:
                existing_mapping = old_df.to_dict('records')
                # Necesitamos saber qué revistas ya terminamos. 
                # Podríamos guardar una columna 'journal_id' en el mapeo para esto.
                if 'journal_id' in old_df.columns:
                    processed_journal_ids = set(old_df['journal_id'].unique())
                    print(f"✓ {len(processed_journal_ids)} revistas ya tienen sus artículos mapeados.")
        except Exception as e:
            print(f"Advertencia leyendo mapeo existente: {e}")

    ids_to_process = [jid for jid in journal_ids if jid not in processed_journal_ids]
    
    if not ids_to_process:
        print("\n✅ ¡Todos los artículos ya tienen tópicos mapeados!")
        return

    print(f"Procesando {len(ids_to_process)} revistas para extraer tópicos de sus artículos...")
    
    new_mapping = []
    session = requests.Session()
    
    # Decidir URL base
    base_url = OPENALEX_LOCAL_API
    print(f"📡 Usando API Local: {base_url}")
    
    start_time = time.time()
    
    for i, jid in enumerate(ids_to_process):
        try:
            clean_id = jid.split('/')[-1]
            journal_works_found = 0
            
            # --- INTENTO: API (Local o Oficial) ---
            cursor = "*"
            while cursor:
                url = f"{base_url}/works"
                params = {
                    'filter': f"primary_location.source.id:{clean_id}",
                    'select': 'id,topics',
                    'per_page': 200,
                    'cursor': cursor
                }
                if email and base_url == OPENALEX_OFFICIAL_API:
                    params['mailto'] = email
                
                try:
                    resp = session.get(url, params=params, timeout=20)
                except requests.exceptions.ConnectionError:
                    if base_url == OPENALEX_LOCAL_API:
                        print(f"  ⚠️ Error de conexión con API Local. Cambiando a API Oficial...")
                        base_url = OPENALEX_OFFICIAL_API
                        continue
                    else:
                        raise

                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get('results', [])
                    
                    for work in results:
                        work_id = work.get('id')
                        topics = work.get('topics', [])
                        
                        if topics:
                            for t in topics:
                                new_mapping.append({
                                    'work_id': work_id,
                                    'journal_id': jid,
                                    'topic_id': t.get('id'),
                                    'topic_name': t.get('display_name'),
                                    'subfield': t.get('subfield', {}).get('display_name'),
                                    'field': t.get('field', {}).get('display_name'),
                                    'domain': t.get('domain', {}).get('display_name'),
                                    'score': t.get('score', 0)
                                })
                        else:
                            new_mapping.append({
                                'work_id': work_id, 'journal_id': jid, 'topic_id': None, 'topic_name': None,
                                'subfield': 'Unknown', 'field': 'Unknown', 'domain': 'Unknown', 'score': 0
                            })
                    
                    journal_works_found += len(results)
                    next_cursor = data.get('meta', {}).get('next_cursor')
                    
                    if not results or not next_cursor or next_cursor == cursor:
                        cursor = None
                    else:
                        cursor = next_cursor
                        
                elif resp.status_code == 429:
                    time.sleep(5)
                else:
                    print(f"  ❌ Error {resp.status_code} para revista {clean_id}")
                    cursor = None
            
            # print(f"  [{i+1}/{len(ids_to_process)}] {clean_id}: {journal_works_found} artículos.")
                # (conservando la lógica del cursor que escribí antes)
                url = f"https://api.openalex.org/works"
                params = {
                    'filter': f"primary_location.source.id:{clean_id}",
                    'select': 'id,topics',
                    'per_page': 200,
                    'cursor': cursor
                }
                if email:
                    params['mailto'] = email
                
                resp = session.get(url, params=params, timeout=20)
                
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get('results', [])
                    
                    for work in results:
                        work_id = work.get('id')
                        topics = work.get('topics', [])
                        
                        if topics:
                            for t in topics:
                                new_mapping.append({
                                    'work_id': work_id,
                                    'journal_id': jid,
                                    'topic_id': t.get('id'),
                                    'topic_name': t.get('display_name'),
                                    'subfield': t.get('subfield', {}).get('display_name'),
                                    'field': t.get('field', {}).get('display_name'),
                                    'domain': t.get('domain', {}).get('display_name'),
                                    'score': t.get('score', 0)
                                })
                        else:
                            # Guardar registro sin tópico para marcar que se procesó el artículo
                            new_mapping.append({
                                'work_id': work_id,
                                'journal_id': jid,
                                'topic_id': None,
                                'topic_name': None,
                                'subfield': 'Unknown',
                                'field': 'Unknown',
                                'domain': 'Unknown',
                                'score': 0
                            })
                    
                    journal_works_found += len(results)
                    next_cursor = data.get('meta', {}).get('next_cursor')
                    
                    if not results or not next_cursor or next_cursor == cursor:
                        cursor = None
                    else:
                        cursor = next_cursor
                        
                elif resp.status_code == 429:
                    time.sleep(5)
                else:
                    print(f"  ❌ Error {resp.status_code} para revista {clean_id}")
                    cursor = None
            
            # print(f"  [{i+1}/{len(ids_to_process)}] {clean_id}: {journal_works_found} artículos mapeados.")
            
        except Exception as e:
            print(f"  ❌ Excepción en revista {jid}: {e}")

        # Guardado parcial cada 10 revistas
        if (i + 1) % 10 == 0:
            save_partial(existing_mapping + new_mapping)
            print(f"  [{i+1}/{len(ids_to_process)}] Revistas procesadas...")

    # Guardado final
    save_partial(existing_mapping + new_mapping)
    print(f"\n✅ Mapeo completado.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enrich works with topics from OpenAlex API')
    parser.add_argument('--email', help='Email for OpenAlex API politeness pool')
    args = parser.parse_args()
    
    # Priorizar email de .env si no se pasa por argumento
    email = args.email or os.environ.get('OPENALEX_EMAIL')
    
    enrich_works_topics(email=email)
