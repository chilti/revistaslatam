#!/usr/bin/env python3
"""
export_articles_openalex.py - Exportador de Registros Científicos Completos OpenAlex
===================================================================================
Recupera registros completos con todas las 88 dimensiones bibliométricas
(autores, filiaciones, ORCIDs, RORs, resumen, frentes/tópicos, referencias citadas,
ODS, citas, FWCI y estatus OA) desde la API local de OpenAlex (localhost:5012)
y DuckDB / Parquet de RevistasLATAM.

Formatos soportados:
  - json   : Array JSON con los objetos OpenAlex completos jerárquicos.
  - jsonl  : Un objeto JSON completo por línea (recomendado para big data y streaming).
  - csv    : Formato estándar OpenAlex de 88 columnas con delimitadores pipe (|).
"""

import os
import sys
import json
import csv
import time
import argparse
import urllib.request
import urllib.parse
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import pandas as pd
from dotenv import load_dotenv

# Cargar entorno
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)

DEFAULT_OPENALEX_API = os.getenv('OPENALEX_LOCAL_URL', 'http://localhost:5012')
DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
DUCKDB_PATH = DATA_DIR / 'revistaslatam.duckdb'
PARQUET_PATH = DATA_DIR / 'latin_american_works.parquet'


# =============================================================================
# RECONSTRUCCIÓN DE ABSTRACT Y CAMPOS AUXILIARES
# =============================================================================

def reconstruct_abstract(inv_idx: Optional[Dict[str, List[int]]]) -> str:
    """Reconstruye el texto plano del abstract a partir del inverted index."""
    if not inv_idx or not isinstance(inv_idx, dict):
        return ""
    word_positions = []
    for word, positions in inv_idx.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort(key=lambda x: x[0])
    return " ".join(w for _, w in word_positions)


def normalize_id(entity_id: str) -> str:
    """Extrae el ID corto (W..., S...) de un URI o string."""
    if not entity_id:
        return ""
    return entity_id.split('/')[-1].strip()


# =============================================================================
# MAPEO A LAS 88 COLUMNAS OFICIALES DE OPENALEX CSV
# =============================================================================

OPENALEX_CSV_COLUMNS = [
    "Title", "Author", "Year", "Citation count", "Open access", "Concept", "Concept IDs",
    "Domain", "Domain IDs", "Field", "Field IDs", "Keyword", "Keyword IDs", "SDG", "SDG IDs",
    "Subfield", "Subfield IDs", "Topic", "Topic IDs", "Author IDs", "Authors count",
    "Corresponding author", "Any location source", "Any location source IDs", "CWTS core",
    "DOAJ", "Has repository fulltext", "OA source", "Source", "Source IDs", "Awards",
    "Funder", "Funder IDs", "Corresponding institution", "Institution", "Institution IDs",
    "Institutions count", "Continent", "Countries count", "Country", "Global south",
    "Language", "OA accepted", "OA published", "Open Access status", "PDF-linked",
    "Cited by", "Cites", "FWCI", "Reference count", "Related to", "Date", "Has DOI",
    "DOI", "Has ORCID", "Has PMCID", "ISSN", "MAG-only", "Work ID", "ORCID", "PubMed",
    "ROR ID", "Abstract", "Any location accepted", "Any location CWTS core",
    "Any location DOAJ", "Any location OA", "Any location published", "APC sum",
    "Best OA source DOAJ", "Citation percentile by subfield", "Citations sum",
    "Estimated APC paid", "Has abstract", "Has fulltext", "Has ISSN",
    "Has oa submitted version", "Has references", "Is oa", "Is paratext",
    "Locations count", "Primary accepted", "Primary OA", "Primary published",
    "Retracted", "Top 1% cited", "Top 10% cited", "Type"
]


def safe_pipe_join(items, key=None) -> str:
    """Une elementos con '|' de forma segura, convirtiendo diccionarios o tipos no-string."""
    if not items or not isinstance(items, list):
        return ""
    result = []
    for it in items:
        if it is None:
            continue
        if isinstance(it, dict):
            if key and key in it:
                val = it[key]
            else:
                val = it.get('display_name') or it.get('name') or it.get('id') or it.get('value') or str(it)
            if val is not None:
                result.append(str(val).strip())
        else:
            result.append(str(it).strip())
    seen = set()
    unique = []
    for r in result:
        if r and r not in seen:
            seen.add(r)
            unique.append(r)
    return "|".join(unique)


def map_work_to_openalex_csv_row(w: Dict[str, Any]) -> Dict[str, Any]:
    """Mapea un objeto JSON completo de OpenAlex a las 88 columnas del CSV estándar."""
    # Autores y afiliaciones
    authorships = w.get('authorships', []) or []
    authors = [a.get('author', {}).get('display_name', '') for a in authorships if isinstance(a, dict) and a.get('author')]
    author_ids = [normalize_id(a.get('author', {}).get('id', '')) for a in authorships if isinstance(a, dict) and a.get('author')]
    orcids = [a.get('author', {}).get('orcid', '') for a in authorships if isinstance(a, dict) and a.get('author', {}).get('orcid')]
    
    institutions = []
    institution_ids = []
    rors = []
    countries = []
    for a in authorships:
        if not isinstance(a, dict):
            continue
        for inst in a.get('institutions', []) or []:
            if isinstance(inst, dict):
                if inst.get('display_name'):
                    institutions.append(str(inst['display_name']).strip())
                if inst.get('id'):
                    institution_ids.append(normalize_id(inst['id']))
                if inst.get('ror'):
                    rors.append(str(inst['ror']).strip())
                if inst.get('country_code'):
                    countries.append(str(inst['country_code']).strip())
        for c in a.get('countries', []) or []:
            if c and str(c).strip() not in countries:
                countries.append(str(c).strip())

    # Tópicos y conceptos
    topics = w.get('topics', []) or []
    topic_names = [t.get('display_name', '') for t in topics if isinstance(t, dict) and t.get('display_name')]
    topic_ids = [normalize_id(t.get('id', '')) for t in topics if isinstance(t, dict) and t.get('id')]
    subfields = [t.get('subfield', {}).get('display_name', '') for t in topics if isinstance(t, dict) and t.get('subfield')]
    subfield_ids = [str(t.get('subfield', {}).get('id', '')) for t in topics if isinstance(t, dict) and t.get('subfield')]
    fields = [t.get('field', {}).get('display_name', '') for t in topics if isinstance(t, dict) and t.get('field')]
    field_ids = [str(t.get('field', {}).get('id', '')) for t in topics if isinstance(t, dict) and t.get('field')]
    domains = [t.get('domain', {}).get('display_name', '') for t in topics if isinstance(t, dict) and t.get('domain')]
    domain_ids = [str(t.get('domain', {}).get('id', '')) for t in topics if isinstance(t, dict) and t.get('domain')]

    concepts = w.get('concepts', []) or []
    concept_names = [c.get('display_name', '') for c in concepts if isinstance(c, dict) and c.get('display_name')]
    concept_ids = [normalize_id(c.get('id', '')) for c in concepts if isinstance(c, dict) and c.get('id')]

    keywords = w.get('keywords', []) or []
    kw_names = [k.get('display_name', '') if isinstance(k, dict) else str(k) for k in keywords if k]
    kw_ids = [k.get('id', '') if isinstance(k, dict) else str(k) for k in keywords if k]

    sdgs = w.get('sustainable_development_goals', []) or []
    sdg_names = [s.get('display_name', '') if isinstance(s, dict) else str(s) for s in sdgs if s]
    sdg_ids = [s.get('id', '') if isinstance(s, dict) else str(s) for s in sdgs if s]

    # Ubicación primaria y revista
    prim_loc = w.get('primary_location', {}) or {}
    prim_source = prim_loc.get('source', {}) or {} if isinstance(prim_loc, dict) else {}
    source_name = prim_source.get('display_name', '') if isinstance(prim_source, dict) else ''
    source_id = normalize_id(prim_source.get('id', '')) if isinstance(prim_source, dict) else ''
    issn_l = prim_source.get('issn_l', '') if isinstance(prim_source, dict) else ''
    if not issn_l and isinstance(prim_source, dict):
        issn_l = safe_pipe_join(prim_source.get('issn', []) or [])

    # Referencias citadas
    ref_works = [normalize_id(str(r)) for r in w.get('referenced_works', []) or [] if r]
    ref_count = w.get('referenced_works_count') or len(ref_works)
    related = [normalize_id(str(r)) for r in w.get('related_works', []) or [] if r]

    # Abstract
    abstract_text = w.get('abstract') or reconstruct_abstract(w.get('abstract_inverted_index'))

    # Open Access
    oa = w.get('open_access', {}) or {}
    is_oa = oa.get('is_oa', False) if isinstance(oa, dict) else False
    oa_status = oa.get('oa_status', 'closed') if isinstance(oa, dict) else 'closed'

    # Citaciones y percentiles
    cited_count = w.get('cited_by_count', 0) or 0
    fwci = w.get('fwci', '')
    perc = w.get('citation_normalized_percentile', {}).get('value', '') if isinstance(w.get('citation_normalized_percentile'), dict) else (w.get('citation_normalized_percentile') or '')

    # Corresponding
    corr_authors = safe_pipe_join(w.get('corresponding_author_ids', []) or [])
    corr_insts = safe_pipe_join(w.get('corresponding_institution_ids', []) or [])

    # Funders & Awards
    funders = w.get('funders', []) or []
    funder_names = safe_pipe_join(funders, key='display_name')
    funder_ids = safe_pipe_join([normalize_id(f.get('id', '') if isinstance(f, dict) else str(f)) for f in funders])
    awards_val = safe_pipe_join(w.get('awards', []) or [])

    row = {
        "Title": str(w.get('title') or w.get('display_name') or '').strip(),
        "Author": safe_pipe_join(authors),
        "Year": w.get('publication_year', ''),
        "Citation count": cited_count,
        "Open access": "Open Access" if is_oa else "Closed Access",
        "Concept": safe_pipe_join(concept_names),
        "Concept IDs": safe_pipe_join(concept_ids),
        "Domain": safe_pipe_join(domains),
        "Domain IDs": safe_pipe_join(domain_ids),
        "Field": safe_pipe_join(fields),
        "Field IDs": safe_pipe_join(field_ids),
        "Keyword": safe_pipe_join(kw_names),
        "Keyword IDs": safe_pipe_join(kw_ids),
        "SDG": safe_pipe_join(sdg_names),
        "SDG IDs": safe_pipe_join(sdg_ids),
        "Subfield": safe_pipe_join(subfields),
        "Subfield IDs": safe_pipe_join(subfield_ids),
        "Topic": safe_pipe_join(topic_names),
        "Topic IDs": safe_pipe_join(topic_ids),
        "Author IDs": safe_pipe_join(author_ids),
        "Authors count": len(authors),
        "Corresponding author": corr_authors,
        "Any location source": source_name,
        "Any location source IDs": source_id,
        "CWTS core": "In CWTS Core" if (isinstance(prim_source, dict) and prim_source.get('is_core')) else "Not CWTS Core source",
        "DOAJ": "In DOAJ" if (isinstance(prim_source, dict) and prim_source.get('is_in_doaj')) else "Not in DOAJ",
        "Has repository fulltext": "In a repository" if any(isinstance(loc, dict) and loc.get('is_oa') for loc in w.get('locations', []) or []) else "",
        "OA source": "Open Access" if is_oa else "",
        "Source": source_name,
        "Source IDs": source_id,
        "Awards": awards_val,
        "Funder": funder_names,
        "Funder IDs": funder_ids,
        "Corresponding institution": corr_insts,
        "Institution": safe_pipe_join(institutions),
        "Institution IDs": safe_pipe_join(institution_ids),
        "Institutions count": len(set(institutions)),
        "Continent": "Latin America" if any(c in ['MX', 'BR', 'CO', 'CL', 'AR', 'PE', 'EC', 'CR', 'CU', 'UY', 'VE', 'BO', 'PY', 'PA', 'GT', 'HN', 'SV', 'NI', 'DO', 'PR'] for c in countries) else "",
        "Countries count": len(set(countries)),
        "Country": safe_pipe_join(countries),
        "Global south": "Global South" if any(c in ['MX', 'BR', 'CO', 'CL', 'AR', 'PE', 'EC', 'CR', 'CU', 'UY', 'VE', 'BO', 'PY', 'PA', 'GT', 'HN', 'SV', 'NI', 'DO', 'PR'] for c in countries) else "",
        "Language": w.get('language', ''),
        "OA accepted": "Open Access" if is_oa else "",
        "OA published": "Open Access" if is_oa else "",
        "Open Access status": oa_status,
        "PDF-linked": "linked to a PDF" if (isinstance(prim_loc, dict) and prim_loc.get('pdf_url')) else "",
        "Cited by": cited_count,
        "Cites": ref_count,
        "FWCI": fwci,
        "Reference count": ref_count,
        "Related to": safe_pipe_join(related[:10]),
        "Date": w.get('publication_date', ''),
        "Has DOI": "has DOI" if w.get('doi') else "",
        "DOI": w.get('doi', ''),
        "Has ORCID": "has ORCID" if orcids else "",
        "Has PMCID": "",
        "ISSN": issn_l,
        "MAG-only": "",
        "Work ID": normalize_id(w.get('id', '')),
        "ORCID": safe_pipe_join(orcids),
        "PubMed": w.get('ids', {}).get('pmid', '') if isinstance(w.get('ids'), dict) else '',
        "ROR ID": safe_pipe_join(rors),
        "Abstract": abstract_text,
        "Any location accepted": "",
        "Any location CWTS core": "",
        "Any location DOAJ": "",
        "Any location OA": "",
        "Any location published": "",
        "APC sum": "",
        "Best OA source DOAJ": "",
        "Citation percentile by subfield": perc,
        "Citations sum": cited_count,
        "Estimated APC paid": w.get('apc_paid', {}).get('value', '') if isinstance(w.get('apc_paid'), dict) else '',
        "Has abstract": "has abstract" if abstract_text else "",
        "Has fulltext": "has fulltext" if w.get('has_fulltext') else "",
        "Has ISSN": "has ISSN" if issn_l else "",
        "Has oa submitted version": "",
        "Has references": "has references" if ref_count > 0 else "",
        "Is oa": "is oa" if is_oa else "",
        "Is paratext": "is paratext" if w.get('is_paratext') else "not is paratext",
        "Locations count": len(w.get('locations', []) or []),
        "Primary accepted": "",
        "Primary OA": "primary OA" if is_oa else "",
        "Primary published": "primary published" if prim_loc else "",
        "Retracted": "Retracted" if w.get('is_retracted') else "Isn't retracted",
        "Top 1% cited": "top 1% cited" if w.get('is_in_top_1_percent') else "not top 1% cited",
        "Top 10% cited": "top 10% cited" if w.get('is_in_top_10_percent') else "not top 10% cited",
        "Type": w.get('type', 'article')
    }
    return row


# =============================================================================
# RECUPERACIÓN MULTIHILO DESDE LA API LOCAL OPENALEX
# =============================================================================

def fetch_single_work(work_id: str, api_url: str = DEFAULT_OPENALEX_API) -> Optional[Dict[str, Any]]:
    """Descarga un objeto Work completo desde la API local OpenAlex."""
    clean_wid = normalize_id(work_id)
    url = f"{api_url.rstrip('/')}/entities/works/{clean_wid}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'RevistasLATAM-Exporter/1.0'})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode('utf-8'))
            if data and isinstance(data, dict) and not data.get('error'):
                return data
    except Exception:
        pass
    return None


def fetch_works_batch(work_ids: List[str], api_url: str = DEFAULT_OPENALEX_API, max_workers: int = 16) -> List[Dict[str, Any]]:
    """Descarga concurrentemente una lista de Work IDs."""
    results = []
    total = len(work_ids)
    print(f"  -> Recuperando {total:,} registros completos desde {api_url} (concurrency={max_workers})...")
    
    t0 = time.time()
    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_wid = {executor.submit(fetch_single_work, wid, api_url): wid for wid in work_ids}
        for future in as_completed(future_to_wid):
            res = future.result()
            if res:
                results.append(res)
            completed += 1
            if completed % 100 == 0 or completed == total:
                elapsed = time.time() - t0
                rate = completed / elapsed if elapsed > 0 else 0
                print(f"     Progreso: {completed:,}/{total:,} ({completed/total*100:.1f}%) — {rate:.1f} reg/s", end='\r')

    print(f"\n  ✅ {len(results):,} registros completos recuperados exitosamente en {time.time()-t0:.2f}s.")
    return results


# =============================================================================
# CONSULTA DE WORK IDS DESDE DUCKDB / PARQUET
# =============================================================================

def get_work_ids_from_db(
    journal_id: Optional[str] = None,
    country_code: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    limit: Optional[int] = None
) -> List[str]:
    """Obtiene la lista de Work IDs filtrados desde DuckDB / Parquet."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    conditions = ["1=1"]
    params = []
    
    if journal_id:
        jid_clean = normalize_id(journal_id)
        conditions.append("(journal_id LIKE ? OR journal_id = ?)")
        params.extend([f"%{jid_clean}%", f"https://openalex.org/{jid_clean}"])
        
    if country_code:
        conditions.append("""
            journal_id IN (
                SELECT id FROM journals WHERE country_code = ?
            )
        """)
        params.append(country_code.upper())
        
    if year_min is not None:
        conditions.append("publication_year >= ?")
        params.append(year_min)
        
    if year_max is not None:
        conditions.append("publication_year <= ?")
        params.append(year_max)
        
    where_clause = " AND ".join(conditions)
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    
    sql = f"""
        SELECT id FROM works 
        WHERE {where_clause}
        ORDER BY publication_year DESC, cited_by_count DESC
        {limit_clause}
    """
    
    df = con.execute(sql, params).df()
    con.close()
    
    return df['id'].tolist()


# =============================================================================
# EXPORTADOR PRINCIPAL
# =============================================================================

def export_works(
    journal_id: Optional[str] = None,
    country_code: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    format_type: str = 'json',
    output_path: Optional[str] = None,
    limit: Optional[int] = None,
    max_workers: int = 16,
    api_url: str = DEFAULT_OPENALEX_API
) -> str:
    """Ejecuta el pipeline completo de exportación y guarda el archivo."""
    print("=" * 80)
    print("🚀 EXPORTADOR DE REGISTROS CIENTÍFICOS OPENALEX (RevistasLATAM -> knoMap)")
    print("=" * 80)
    
    # 1. Obtener lista de IDs
    work_ids = get_work_ids_from_db(
        journal_id=journal_id,
        country_code=country_code,
        year_min=year_min,
        year_max=year_max,
        limit=limit
    )
    
    if not work_ids:
        print("⚠️ No se encontraron artículos para los criterios especificados.")
        return ""
        
    print(f"✅ Se encontraron {len(work_ids):,} artículos para exportar.")
    
    # 2. Descargar registros completos desde API local
    works = fetch_works_batch(work_ids, api_url=api_url, max_workers=max_workers)
    if not works:
        print("❌ No se pudieron recuperar registros de la API OpenAlex.")
        return ""

    # 3. Determinar ruta de salida por defecto
    if not output_path:
        out_dir = Path(__file__).resolve().parent.parent / 'exports'
        out_dir.mkdir(exist_ok=True)
        
        tag = normalize_id(journal_id) if journal_id else (country_code or 'works')
        years_tag = f"_{year_min}_{year_max}" if (year_min or year_max) else "_todos"
        ext = 'jsonl' if format_type == 'jsonl' else ('csv' if format_type == 'csv' else 'json')
        output_path = str(out_dir / f"openalex_export_{tag}{years_tag}.{ext}")

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    # 4. Escribir según formato
    print(f"💾 Guardando archivo en formato [{format_type.upper()}] en: {out_file}")
    
    if format_type == 'jsonl':
        with open(out_file, 'w', encoding='utf-8') as f:
            for w in works:
                f.write(json.dumps(w, ensure_ascii=False) + '\n')
                
    elif format_type == 'csv':
        with open(out_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=OPENALEX_CSV_COLUMNS)
            writer.writeheader()
            for w in works:
                row = map_work_to_openalex_csv_row(w)
                writer.writerow(row)
                
    else:  # json array
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(works, f, ensure_ascii=False, indent=2)

    file_size_mb = out_file.stat().st_size / (1024 * 1024)
    print(f"✨ ¡Exportación completada exitosamente! Tamaño: {file_size_mb:.2f} MB ({len(works):,} registros)")
    return str(out_file)


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Exportador de artículos OpenAlex completos para knoMap")
    parser.add_argument('--journal-id', type=str, help="ID OpenAlex de la revista (ej: S2737081250)")
    parser.add_argument('--country', type=str, help="Código de país ISO-2 (ej: MX, BR, CO)")
    parser.add_argument('--years', type=str, help="Rango de años (ej: 2021-2025 o 2020)")
    parser.add_argument('--format', type=str, choices=['json', 'jsonl', 'csv'], default='json', help="Formato de salida")
    parser.add_argument('--output', type=str, help="Ruta del archivo de salida")
    parser.add_argument('--limit', type=int, help="Límite máximo de artículos a exportar")
    parser.add_argument('--concurrency', type=int, default=16, help="Número de hilos concurrentes")
    parser.add_argument('--api-url', type=str, default=DEFAULT_OPENALEX_API, help="URL de la API local OpenAlex")

    args = parser.parse_args()

    year_min, year_max = None, None
    if args.years:
        if '-' in args.years:
            parts = args.years.split('-')
            year_min, year_max = int(parts[0]), int(parts[1])
        else:
            year_min = year_max = int(args.years)

    export_works(
        journal_id=args.journal_id,
        country_code=args.country,
        year_min=year_min,
        year_max=year_max,
        format_type=args.format,
        output_path=args.output,
        limit=args.limit,
        max_workers=args.concurrency,
        api_url=args.api_url
    )


if __name__ == '__main__':
    main()
