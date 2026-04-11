"""
Script para enriquecer los datos de las revistas consultando la API de OpenAlex.
Descarga la jerarquía de tópicos (Topics -> Fields -> Domains) para generar el gráfico Sunburst.

Este script hace peticiones a la API (1 por revista).
"""
import pandas as pd
import requests
import time
import os
import argparse
import json
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Directorio de datos
DATA_DIR = Path(__file__).parent.parent / 'data'
JOURNALS_FILE = DATA_DIR / 'latin_american_journals.parquet'
OUTPUT_FILE = DATA_DIR / 'journals_topics_sunburst.parquet'
ENRICHED_FILE = DATA_DIR / 'journals_enriched.parquet'

def save_partial(data_list, output_path=OUTPUT_FILE):
    if not data_list:
        return
    try:
        df = pd.DataFrame(data_list)
        # Crear carpeta si no existe
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        # print(f"  (Guardado parcial: {len(df)} registros)")
    except Exception as e:
        print(f"Error guardando parcial: {e}")

def enrich_journals(email=None):
    if not JOURNALS_FILE.exists():
        print(f"❌ No se encontró el archivo de revistas: {JOURNALS_FILE}")
        return

    print("="*70)
    print("ENRIQUECIMIENTO DE REVISTAS (API OPENALEX)")
    print("Objetivo: Descargar jerarquía de tópicos para Sunburst")
    if email:
        print(f"Usando email: {email} (Polite Pool)")
    else:
        print("⚠️ Sin email configurado (Lento: 1 req/s)")
    print("="*70)

    # Cargar revistas
    try:
        journals_df = pd.read_parquet(JOURNALS_FILE)
        journal_ids = journals_df['id'].tolist()
    except Exception as e:
        print(f"Error leyendo archivo de revistas: {e}")
        return
    
    # Verificar progreso existente para metadatos enriquecidos
    enriched_data = []
    processed_ids = set()
    
    if ENRICHED_FILE.exists():
        try:
            old_enriched = pd.read_parquet(ENRICHED_FILE)
            if not old_enriched.empty:
                enriched_data = old_enriched.to_dict('records')
                processed_ids = set(old_enriched['id'].unique())
                print(f"✓ Encontrados {len(processed_ids)} revistas con metadata previa.")
        except Exception as e:
            print(f"Advertencia leyendo metadata enriquecida: {e}")
            
    # También cargar tópicos existentes
    existing_topics = []
    if OUTPUT_FILE.exists():
        try:
            old_topics = pd.read_parquet(OUTPUT_FILE)
            existing_topics = old_topics.to_dict('records')
        except Exception:
            pass

    # Filtrar IDs pendientes
    ids_to_process = [jid for jid in journal_ids if jid not in processed_ids]
    
    if not ids_to_process:
        print("\n✅ ¡Todas las revistas ya están enriquecidas!")
        return

    print(f"Procesando {len(ids_to_process)} revistas restantes...")
    
    new_topics = []
    new_enriched = []
    total_new = 0
    
    session = requests.Session()
    session.headers.update({'User-Agent': 'RevistasLatam/1.0 (mailto:' + (email or 'test@example.com') + ')'})
    
    start_time = time.time()
    sleep_time = 0.1 if email else 0.5 
    
    for i, jid in enumerate(ids_to_process):
        try:
            clean_id = jid.split('/')[-1]
            data = None
            
            # --- USO DE API OFICIAL (Como solicitaste) ---
            url = f"https://api.openalex.org/sources/{clean_id}"
            params = {} # Objeto completo
            if email: params['mailto'] = email
            
            resp = session.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
            elif resp.status_code == 429:
                print(f"  ⚠️ Rate limit API oficial. Esperando 5s...")
                time.sleep(5)
                continue
            else:
                print(f"  ❌ Error {resp.status_code} para {clean_id} en API oficial")
                continue

            if data:
                # 1. Guardar metadata completa (denormalizando algunas listas)
                processed_item = data.copy()
                for complex_col in ['apc_prices', 'ids', 'societies', 'summary_stats', 'x_concepts']:
                    if complex_col in processed_item:
                         processed_item[complex_col] = str(processed_item[complex_col])
                
                new_enriched.append(processed_item)
                
                # 2. Extraer Tópicos para Sunburst
                topics = data.get('topics', [])
                for topic in topics:
                    share = topic.get('share', 0) or (topic.get('percentage', 0) / 100.0 if 'percentage' in topic else 0)
                    count = topic.get('count', 0) or topic.get('works_count', 0)

                    t_data = {
                        'journal_id': jid,
                        'journal_name': data.get('display_name'),
                        'topic_name': topic['display_name'],
                        'topic_id': topic['id'],
                        'subfield': topic['subfield']['display_name'] if 'subfield' in topic else 'Unknown',
                        'field': topic['field']['display_name'] if 'field' in topic else 'Unknown',
                        'domain': topic['domain']['display_name'] if 'domain' in topic else 'Unknown',
                        'count': count,
                        'share': share
                    }
                    new_topics.append(t_data)
                    total_new += 1
                
                # Marcar como procesado aunque no tenga tópicos (para no reintentar infinitamente)
                if not topics:
                    # Agregar un registro dummy o simplemente confiar en `processed_ids` logic next time
                    # Pero `processed_ids` se basa en `journal_id` unique en el output file.
                    # Si no guardamos nada, se reintentará.
                    # Guardamos un dummy con topic_name="Unknown" para marcarlo?
                    # Mejor no ensuciar. Asumimos que si no está en output, no tiene tópicos.
                    pass

            elif resp.status_code == 429:
                print(f"  ⚠️ Rate limit. Esperando 5s...")
                time.sleep(5)
            else:
                print(f"  ❌ Error {resp.status_code} para {clean_id}")

        except Exception as e:
            print(f"  ❌ Excepción para {clean_id}: {e}")
            
        # Log progreso cada 20
        if (i + 1) % 20 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print(f"  [{i+1}/{len(ids_to_process)}] Procesados ({rate:.1f} req/s)")
            
            # Guardado parcial
            save_partial(enriched_data + new_enriched, ENRICHED_FILE)
            save_partial(existing_topics + new_topics, OUTPUT_FILE)
        
        time.sleep(sleep_time)
        
    # Guardado final
    save_partial(enriched_data + new_enriched, ENRICHED_FILE)
    save_partial(existing_topics + new_topics, OUTPUT_FILE)
    print(f"\n✅ Enriquecimiento completado. Revistas totals: {len(enriched_data) + len(new_enriched)}")

if __name__ == "__main__":
    import argparse
    from generate_country_sunburst import generate_country_sunburst
    
    parser = argparse.ArgumentParser(description='Enrich journals with OpenAlex API data')
    parser.add_argument('--email', help='Email for OpenAlex API politeness pool')
    args = parser.parse_args()
    
    # Priorizar email de .env si no se pasa por argumento
    email = args.email or os.environ.get('OPENALEX_EMAIL')
    
    enrich_journals(email=email)
    
    # También generar automáticamente el nivel país
    generate_country_sunburst()
