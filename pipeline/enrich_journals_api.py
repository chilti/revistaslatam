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
from pathlib import Path

# Directorio de datos
DATA_DIR = Path(__file__).parent.parent / 'data'
JOURNALS_FILE = DATA_DIR / 'latin_american_journals.parquet'
OUTPUT_FILE = DATA_DIR / 'journals_topics_sunburst.parquet'

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
    
    # Verificar progreso existente
    existing_data = []
    processed_ids = set()
    
    if OUTPUT_FILE.exists():
        try:
            old_df = pd.read_parquet(OUTPUT_FILE)
            if not old_df.empty:
                existing_data = old_df.to_dict('records')
                processed_ids = set(old_df['journal_id'].unique())
                print(f"✓ Encontrados {len(processed_ids)} revistas ya procesadas.")
        except Exception as e:
            print(f"Advertencia leyendo archivo existente: {e}")

    # Filtrar IDs pendientes
    ids_to_process = [jid for jid in journal_ids if jid not in processed_ids]
    
    if not ids_to_process:
        print("\n✅ ¡Todas las revistas ya tienen sus tópicos descargados!")
        return

    print(f"Procesando {len(ids_to_process)} revistas restantes...")
    
    new_data = []
    total_new = 0
    
    session = requests.Session()
    session.headers.update({'User-Agent': 'RevistasLatam/1.0 (mailto:' + (email or 'test@example.com') + ')'})
    
    start_time = time.time()
    sleep_time = 0.1 if email else 0.5 # Acelerado con email
    
    for i, jid in enumerate(ids_to_process):
        try:
            clean_id = jid.split('/')[-1]
            url = f"https://api.openalex.org/sources/{clean_id}"
            params = {'select': 'id,display_name,topics'}
            if email:
                params['mailto'] = email
            
            resp = session.get(url, params=params, timeout=15)
            
            if resp.status_code == 200:
                data = resp.json()
                topics = data.get('topics', [])
                
                # Procesar cada tópico
                for topic in topics:
                    # Normalizar share/percentage
                    share = topic.get('share', 0)
                    if share == 0:
                        share = topic.get('percentage', 0) / 100.0 if 'percentage' in topic else 0
                    
                    count = topic.get('count', 0)
                    if count == 0 and 'works_count' in topic: # A veces viene como works_count
                         count = topic['works_count']

                    # Estructura jerárquica para Sunburst
                    t_data = {
                        'journal_id': jid,
                        'journal_name': data.get('display_name'),
                        'topic_name': topic['display_name'],
                        'topic_id': topic['id'],
                        'field': topic['field']['display_name'],
                        'domain': topic['domain']['display_name'],
                        'count': count,
                        'share': share
                    }
                    new_data.append(t_data)
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
            save_partial(existing_data + new_data)
        
        time.sleep(sleep_time)
        
    # Guardado final
    final_list = existing_data + new_data
    save_partial(final_list)
    print(f"\n✅ Enriquecimiento completado. Total registros topics: {len(final_list)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enrich journals with OpenAlex API data')
    parser.add_argument('--email', help='Email for OpenAlex API politeness pool')
    args = parser.parse_args()
    
    enrich_journals(email=args.email)
