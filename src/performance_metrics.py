import pandas as pd
import numpy as np
import json
import os
from pathlib import Path
import pyarrow.parquet as pq

def safe_get(obj, *keys, default=None):
    """Safely navigate nested dictionaries."""
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key, default)
        else:
            return default
    return obj if obj is not None else default

def parse_json_field(value):
    """Parse JSON string to dict/list if needed."""
    if isinstance(value, str) and (value.startswith('{') or value.startswith('[')):
        try:
            return json.loads(value)
        except:
            return value
    return value

def get_cache_dir():
    """Returns the cache directory path."""
    cache_dir = Path(__file__).parent.parent / 'data' / 'cache'
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

# ============================================================================
# Core Bibliometric & Scientometric Mathematical Functions (Phase 1)
# ============================================================================

def compute_h_index(citations):
    """
    Computes Hirsch's h-index: max h such that h works have >= h citations.
    """
    if citations is None or len(citations) == 0:
        return 0
    arr = np.asarray(citations, dtype=int)
    arr = arr[arr > 0]
    if len(arr) == 0:
        return 0
    arr = np.sort(arr)[::-1]
    ranks = np.arange(1, len(arr) + 1)
    h = int(np.max(np.where(arr >= ranks, ranks, 0), initial=0))
    return h

def compute_g_index(citations):
    """
    Computes Egghe's g-index: highest rank g such that top g works have >= g^2 cumulative citations.
    """
    if citations is None or len(citations) == 0:
        return 0
    arr = np.asarray(citations, dtype=int)
    arr = arr[arr > 0]
    if len(arr) == 0:
        return 0
    arr = np.sort(arr)[::-1]
    cumsum = np.cumsum(arr)
    ranks = np.arange(1, len(arr) + 1)
    g = int(np.max(np.where(cumsum >= ranks**2, ranks, 0), initial=0))
    return g

def compute_m_index(h_index, first_year, last_year):
    """
    Computes Hirsch's m-quotient: m = h / (T_years_active).
    """
    if not first_year or not last_year or first_year > last_year:
        return round(float(h_index), 2)
    years_active = max(1, int(last_year) - int(first_year) + 1)
    return round(float(h_index) / years_active, 2)

def compute_price_index(years, reference_year=None, window=5):
    """
    Computes Price Index: percentage of references/works published in the last window years.
    """
    if years is None or len(years) == 0:
        return 0.0
    arr = np.asarray(years, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0:
        return 0.0
    ref = reference_year if reference_year is not None else np.max(arr)
    recent_count = np.sum((ref - arr) < window)
    return round(float((recent_count / len(arr)) * 100), 2)

def compute_shannon_diversity(counts_dict_or_array):
    """
    Computes Shannon Entropy / Diversity Index: H = -sum(p_i * ln(p_i)).
    """
    if counts_dict_or_array is None:
        return 0.0
    if isinstance(counts_dict_or_array, dict):
        counts = np.array(list(counts_dict_or_array.values()), dtype=float)
    else:
        counts = np.asarray(counts_dict_or_array, dtype=float)
    counts = counts[counts > 0]
    if len(counts) == 0:
        return 0.0
    total = counts.sum()
    if total == 0:
        return 0.0
    p = counts / total
    return round(float(-np.sum(p * np.log(p + 1e-12))), 3)

class MetricsAccumulator:
    """
    Accumulates metrics across chunks for memory-efficient processing.
    """
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset all accumulators."""
        self.count = 0
        self.fwci_sum = 0.0
        self.percentile_sum = 0.0
        self.top_10_count = 0
        self.top_1_count = 0
        self.citations_list = []
        self.total_citations = 0
        self.years_list = []
        self.domestic_author_count = 0
        self.foreign_author_count = 0
        self.has_doi_count = 0
        self.is_oa_count = 0
        self.has_oa_url_count = 0
        self.institution_counts = {}
        self.oa_counts = {
            'gold': 0,
            'diamond': 0,
            'green': 0,
            'hybrid': 0,
            'bronze': 0,
            'closed': 0
        }
        # Language counters
        self.lang_counts = {
            'en': 0, 'fr': 0, 'de': 0, 'it': 0, 'la': 0,
            'nd': 0, 'pt': 0, 'ru': 0, 'es': 0, 'other': 0
        }

    def add_batch(self, chunk):
        """
        Process a DataFrame chunk and update metrics.
        """
        if len(chunk) == 0:
            return

        self.count += len(chunk)
        
        # Citations
        if 'cited_by_count' in chunk.columns:
            cits = pd.to_numeric(chunk['cited_by_count'], errors='coerce').fillna(0).astype(int)
            self.total_citations += int(cits.sum())
            self.citations_list.extend(cits[cits > 0].tolist())

        # Years
        if 'publication_year' in chunk.columns:
            years = pd.to_numeric(chunk['publication_year'], errors='coerce').dropna().astype(int)
            self.years_list.extend(years.tolist())

        # FWCI (Field Weighted Citation Impact)
        if 'fwci' in chunk.columns:
            self.fwci_sum += chunk['fwci'].fillna(0.0).sum()

        # Percentile
        if 'citation_normalized_percentile' in chunk.columns:
            if chunk['citation_normalized_percentile'].dtype == 'object':
                vals = pd.to_numeric(chunk['citation_normalized_percentile'], errors='coerce').fillna(0.0)
            else:
                vals = chunk['citation_normalized_percentile'].fillna(0.0)
            
            self.percentile_sum += vals.sum()
            self.top_10_count += (vals >= 90.0).sum()
            self.top_1_count += (vals >= 99.0).sum()
        elif 'percentile' in chunk.columns:
            vals = pd.to_numeric(chunk['percentile'], errors='coerce').fillna(0.0)
            self.percentile_sum += vals.sum()
            self.top_10_count += (vals >= 90.0).sum()
            self.top_1_count += (vals >= 99.0).sum()

        # Open Access Status
        if 'oa_status' in chunk.columns:
            counts = chunk['oa_status'].value_counts()
            for status in ['gold', 'diamond', 'green', 'hybrid', 'bronze', 'closed']:
                self.oa_counts[status] += counts.get(status, 0)
                
        # Domestic vs Foreign Authors
        if 'is_domestic_author' in chunk.columns:
            dom = pd.to_numeric(chunk['is_domestic_author'], errors='coerce').fillna(-1)
            self.domestic_author_count += int((dom == 1.0).sum())
            self.foreign_author_count += int((dom == 0.0).sum())

        # Institutional diversity
        if 'institution_id' in chunk.columns:
            for inst, cnt in chunk['institution_id'].dropna().value_counts().items():
                self.institution_counts[inst] = self.institution_counts.get(inst, 0) + cnt

        # DOI and OA flags
        if 'doi' in chunk.columns:
            self.has_doi_count += int(chunk['doi'].notna().sum())
        if 'is_oa' in chunk.columns:
            self.is_oa_count += int((chunk['is_oa'] == True).sum())
        if 'oa_url' in chunk.columns:
            self.has_oa_url_count += int(chunk['oa_url'].notna().sum())

        # Language Stats
        if 'language' in chunk.columns:
            lang_counts = chunk['language'].fillna('unknown').value_counts()
            target_langs = ['en', 'fr', 'de', 'it', 'la', 'nd', 'pt', 'ru', 'es']
            for lang in target_langs:
                self.lang_counts[lang] += lang_counts.get(lang, 0)
            self.lang_counts['other'] += lang_counts[~lang_counts.index.isin(target_langs)].sum()

    def get_metrics(self):
        """
        Return dictionary with calculated average metrics.
        """
        if self.count == 0:
            return {
                'num_documents': 0,
                'total_citations': 0,
                'citations_per_doc': 0.0,
                'h_index': 0,
                'g_index': 0,
                'm_index': 0.0,
                'price_index': 0.0,
                'shannon_diversity': 0.0,
                'fwci_avg': 0.0,
                'pct_top_10': 0.0,
                'pct_top_1': 0.0,
                'avg_percentile': 0.0,
                'pct_oa_gold': 0.0,
                'pct_oa_diamond': 0.0,
                'pct_oa_green': 0.0,
                'pct_oa_hybrid': 0.0,
                'pct_oa_bronze': 0.0,
                'pct_oa_closed': 0.0,
                'pct_authors_domestic': 0.0,
                'pct_authors_foreign': 0.0,
                'pct_has_doi': 0.0,
                'pct_is_oa': 0.0,
                'num_institutions': 0,
                'pct_lang_en': 0.0,
                'pct_lang_fr': 0.0,
                'pct_lang_de': 0.0,
                'pct_lang_it': 0.0,
                'pct_lang_la': 0.0,
                'pct_lang_nd': 0.0,
                'pct_lang_pt': 0.0,
                'pct_lang_ru': 0.0,
                'pct_lang_es': 0.0,
                'pct_lang_other': 0.0
            }
        
        h_idx = compute_h_index(self.citations_list)
        g_idx = compute_g_index(self.citations_list)
        first_yr = min(self.years_list) if self.years_list else None
        last_yr = max(self.years_list) if self.years_list else None
        m_idx = compute_m_index(h_idx, first_yr, last_yr)
        price_idx = compute_price_index(self.years_list)
        shannon_div = compute_shannon_diversity(self.institution_counts)

        total_author_tracked = self.domestic_author_count + self.foreign_author_count
        pct_dom = round((self.domestic_author_count / total_author_tracked) * 100, 2) if total_author_tracked > 0 else 0.0
        pct_for = round((self.foreign_author_count / total_author_tracked) * 100, 2) if total_author_tracked > 0 else 0.0

        metrics = {
            'num_documents': self.count,
            'total_citations': int(self.total_citations),
            'citations_per_doc': round(self.total_citations / self.count, 2),
            'h_index': int(h_idx),
            'g_index': int(g_idx),
            'm_index': float(m_idx),
            'price_index': float(price_idx),
            'shannon_diversity': float(shannon_div),
            'fwci_avg': round(self.fwci_sum / self.count, 2),
            'pct_top_10': round((self.top_10_count / self.count) * 100, 2),
            'pct_top_1': round((self.top_1_count / self.count) * 100, 2),
            'avg_percentile': round(self.percentile_sum / self.count, 2),
            'pct_oa_gold': round((self.oa_counts['gold'] / self.count) * 100, 2),
            'pct_oa_diamond': round((self.oa_counts['diamond'] / self.count) * 100, 2),
            'pct_oa_green': round((self.oa_counts['green'] / self.count) * 100, 2),
            'pct_oa_hybrid': round((self.oa_counts['hybrid'] / self.count) * 100, 2),
            'pct_oa_bronze': round((self.oa_counts['bronze'] / self.count) * 100, 2),
            'pct_oa_closed': round((self.oa_counts['closed'] / self.count) * 100, 2),
            'pct_authors_domestic': pct_dom,
            'pct_authors_foreign': pct_for,
            'pct_has_doi': round((self.has_doi_count / self.count) * 100, 2),
            'pct_is_oa': round((self.is_oa_count / self.count) * 100, 2),
            'num_institutions': len(self.institution_counts)
        }
        
        # Calculate Language Percentages
        for lang in ['en', 'fr', 'de', 'it', 'la', 'nd', 'pt', 'ru', 'es', 'other']:
            metrics[f'pct_lang_{lang}'] = round((self.lang_counts[lang] / self.count) * 100, 2)
            
        return metrics

def process_works_in_chunks(works_filepath, filter_func=None, chunk_size=50000):
    """
    Process works file in chunks and calculate metrics.
    """
    parquet_file = pq.ParquetFile(works_filepath)
    accumulator = MetricsAccumulator()
    total_rows = parquet_file.metadata.num_rows
    chunks_processed = 0
    
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        df_chunk = batch.to_pandas()
        
        if 'open_access' in df_chunk.columns and 'oa_status' not in df_chunk.columns:
            df_chunk['oa_status'] = df_chunk['open_access'].apply(
                lambda x: safe_get(parse_json_field(x), 'oa_status', default='closed')
            )
        
        if 'publication_year' not in df_chunk.columns and 'biblio' in df_chunk.columns:
            df_chunk['publication_year'] = df_chunk['biblio'].apply(
                lambda x: safe_get(parse_json_field(x), 'year')
            )
        
        if filter_func is not None:
            df_chunk = filter_func(df_chunk)
        
        if len(df_chunk) > 0:
            accumulator.add_batch(df_chunk)
        
        chunks_processed += 1
        if chunks_processed % 20 == 0:
            print(f"    Processed {chunks_processed * chunk_size:,} / {total_rows:,} rows...")
    
    return accumulator.get_metrics()

def get_year_range(works_filepath):
    """
    Detect the range of years available in the works data.
    """
    parquet_file = pq.ParquetFile(works_filepath)
    years = set()
    for i, batch in enumerate(parquet_file.iter_batches(batch_size=100000)):
        df_chunk = batch.to_pandas()
        if 'publication_year' not in df_chunk.columns and 'biblio' in df_chunk.columns:
            df_chunk['publication_year'] = df_chunk['biblio'].apply(
                lambda x: safe_get(parse_json_field(x), 'year')
            )
        if 'publication_year' in df_chunk.columns:
            chunk_years = pd.to_numeric(df_chunk['publication_year'], errors='coerce').dropna()
            years.update(chunk_years.unique())
        if i >= 4:
            break
    if years:
        return int(min(years)), int(max(years))
    else:
        return 2000, 2025

def calculate_annual_metrics_chunked(works_filepath, start_year=None, end_year=None):
    if start_year is None or end_year is None:
        detected_start, detected_end = get_year_range(works_filepath)
        start_year = start_year or detected_start
        end_year = end_year or detected_end
    
    annual_data = []
    for year in range(start_year, end_year + 1):
        filter_func = lambda df, y=year: df[df['publication_year'] == y]
        metrics = process_works_in_chunks(works_filepath, filter_func)
        metrics['year'] = year
        annual_data.append(metrics)
    
    annual_metrics_df = pd.DataFrame(annual_data)
    
    filter_func = lambda df: df[
        (df['publication_year'] >= start_year) & 
        (df['publication_year'] <= end_year)
    ]
    period_metrics = process_works_in_chunks(works_filepath, filter_func)
    period_metrics['period'] = f'{start_year}-{end_year}'
    
    return annual_metrics_df, period_metrics

def calculate_journal_metrics_chunked(works_filepath, journals_df, journal_id, start_year=None, end_year=None):
    journal_info = journals_df[journals_df['id'] == journal_id]
    if len(journal_info) == 0:
        return None, None
    journal_info = journal_info.iloc[0]
    
    is_scopus = safe_get(journal_info, 'is_indexed_in_scopus', default=False)
    is_core = safe_get(journal_info, 'is_core', default=False)
    is_doaj = safe_get(journal_info, 'is_in_doaj', default=False)
    
    journal_indexing = {
        'is_scopus': bool(is_scopus),
        'is_core': bool(is_core),
        'is_doaj': bool(is_doaj)
    }
    
    if start_year is None or end_year is None:
        start_year, end_year = get_year_range(works_filepath)
    
    annual_data = []
    for year in range(start_year, end_year + 1):
        year_filter = lambda df, jid=journal_id, y=year: df[(df['journal_id'] == jid) & (df['publication_year'] == y)]
        metrics = process_works_in_chunks(works_filepath, year_filter)
        metrics['year'] = year
        metrics['journal_id'] = journal_id
        metrics.update(journal_indexing)
        annual_data.append(metrics)
    
    annual_metrics_df = pd.DataFrame(annual_data)
    
    period_filter = lambda df, jid=journal_id: df[
        (df['journal_id'] == jid) & 
        (df['publication_year'] >= start_year) & 
        (df['publication_year'] <= end_year)
    ]
    period_metrics = process_works_in_chunks(works_filepath, period_filter)
    period_metrics['journal_id'] = journal_id
    period_metrics['period'] = f'{start_year}-{end_year}'
    period_metrics.update(journal_indexing)
    
    return annual_metrics_df, period_metrics

def calculate_country_metrics_chunked(works_filepath, journals_df, country_code, start_year=None, end_year=None):
    country_journals = journals_df[journals_df['country_code'] == country_code]
    if len(country_journals) == 0:
        return None, None, None
    
    if start_year is None or end_year is None:
        start_year, end_year = get_year_range(works_filepath)
    
    num_journals = len(country_journals)
    journal_ids = country_journals['id'].tolist()
    
    pct_scopus = (country_journals.apply(lambda x: safe_get(x, 'is_indexed_in_scopus', default=False), axis=1).sum() / num_journals) * 100
    pct_core = (country_journals.apply(lambda x: safe_get(x, 'is_core', default=False), axis=1).sum() / num_journals) * 100
    pct_doaj = (country_journals.apply(lambda x: safe_get(x, 'is_in_doaj', default=False), axis=1).sum() / num_journals) * 100
    
    journal_metrics = {
        'num_journals': num_journals,
        'pct_scopus': round(pct_scopus, 2),
        'pct_core': round(pct_core, 2),
        'pct_doaj': round(pct_doaj, 2)
    }
    
    annual_data = []
    for year in range(start_year, end_year + 1):
        year_filter = lambda df, jids=journal_ids, y=year: df[
            (df['journal_id'].isin(jids)) & 
            (df['publication_year'] == y)
        ]
        metrics = process_works_in_chunks(works_filepath, year_filter)
        metrics['year'] = year
        metrics['country_code'] = country_code
        annual_data.append(metrics)
    
    annual_metrics_df = pd.DataFrame(annual_data)
    
    period_filter = lambda df, jids=journal_ids: df[
        (df['journal_id'].isin(jids)) & 
        (df['publication_year'] >= start_year) & 
        (df['publication_year'] <= end_year)
    ]
    period_metrics = process_works_in_chunks(works_filepath, period_filter)
    period_metrics.update(journal_metrics)
    period_metrics['country_code'] = country_code
    period_metrics['period'] = '2021-2025'
    
    return annual_metrics_df, period_metrics, journal_metrics

def calculate_latam_metrics_chunked(works_filepath, journals_df, start_year=None, end_year=None):
    num_journals = len(journals_df)
    if num_journals == 0:
        return None, None, None
    
    if start_year is None or end_year is None:
        start_year, end_year = get_year_range(works_filepath)
    
    pct_scopus = (journals_df.apply(lambda x: safe_get(x, 'is_indexed_in_scopus', default=False), axis=1).sum() / num_journals) * 100
    pct_core = (journals_df.apply(lambda x: safe_get(x, 'is_core', default=False), axis=1).sum() / num_journals) * 100
    pct_doaj = (journals_df.apply(lambda x: safe_get(x, 'is_in_doaj', default=False), axis=1).sum() / num_journals) * 100
    
    journal_metrics = {
        'num_journals': num_journals,
        'pct_scopus': round(pct_scopus, 2),
        'pct_core': round(pct_core, 2),
        'pct_doaj': round(pct_doaj, 2)
    }
    
    annual_data = []
    for year in range(start_year, end_year + 1):
        year_filter = lambda df, y=year: df[df['publication_year'] == y]
        metrics = process_works_in_chunks(works_filepath, year_filter)
        metrics['year'] = year
        annual_data.append(metrics)
    
    annual_metrics_df = pd.DataFrame(annual_data)
    
    period_filter = lambda df: df[
        (df['publication_year'] >= start_year) & 
        (df['publication_year'] <= end_year)
    ]
    period_metrics = process_works_in_chunks(works_filepath, period_filter)
    period_metrics.update(journal_metrics)
    period_metrics['period'] = f'{start_year}-{end_year}'
    
    return annual_metrics_df, period_metrics, journal_metrics

def compute_and_cache_all_metrics(works_filepath, journals_filepath, force_recalculate=False):
    cache_dir = get_cache_dir()
    
    print("⚙️ Loading journals data...")
    try:
        journals_df = pd.read_parquet(journals_filepath)
    except Exception as e:
        print(f"⚠️ Error loading journals data: {e}")
        return None
    
    if journals_df.empty:
        print("⚠️ No journals data available")
        return None
    
    print(f"✓ Loaded {len(journals_df):,} journals")
    
    if not os.path.exists(works_filepath):
        print(f"⚠️ Works file not found: {works_filepath}")
        return None
    
    start_year, end_year = get_year_range(works_filepath)
    print(f"✓ Year range: {start_year}-{end_year}")
    
    # 1. LATAM level
    print("\n📊 LATAM metrics...")
    latam_annual, latam_period, _ = calculate_latam_metrics_chunked(works_filepath, journals_df, start_year, end_year)
    if latam_annual is not None:
        latam_annual.to_parquet(cache_dir / 'metrics_latam_annual.parquet', index=False)
    if latam_period is not None:
        pd.DataFrame([latam_period]).to_parquet(cache_dir / 'metrics_latam_period.parquet', index=False)
    
    # 2. Country level
    print("\n📊 Country metrics...")
    country_annual_list = []
    country_period_list = []
    for country_code in journals_df['country_code'].unique():
        annual, period, _ = calculate_country_metrics_chunked(works_filepath, journals_df, country_code, start_year, end_year)
        if annual is not None: country_annual_list.append(annual)
        if period is not None: country_period_list.append(period)
    
    country_annual_df = pd.concat(country_annual_list, ignore_index=True) if country_annual_list else pd.DataFrame()
    country_period_df = pd.DataFrame(country_period_list) if country_period_list else pd.DataFrame()
    if not country_annual_df.empty:
        country_annual_df.to_parquet(cache_dir / 'metrics_country_annual.parquet', index=False)
    if not country_period_df.empty:
        country_period_df.to_parquet(cache_dir / 'metrics_country_period.parquet', index=False)
    
    # 3. Journal level
    print("\n📊 Journal metrics...")
    journal_annual_list = []
    journal_period_list = []
    for idx, journal_id in enumerate(journals_df['id'].unique(), 1):
        if idx % 100 == 0:
            print(f" Progress: {idx}/{len(journals_df)} journals...")
        annual, period = calculate_journal_metrics_chunked(works_filepath, journals_df, journal_id, start_year, end_year)
        if annual is not None and len(annual) > 0: journal_annual_list.append(annual)
        if period is not None: journal_period_list.append(period)
    
    journal_annual_df = pd.concat(journal_annual_list, ignore_index=True) if journal_annual_list else pd.DataFrame()
    journal_period_df = pd.DataFrame(journal_period_list) if journal_period_list else pd.DataFrame()
    if not journal_annual_df.empty:
        journal_annual_df.to_parquet(cache_dir / 'metrics_journal_annual.parquet', index=False)
    if not journal_period_df.empty:
        journal_period_df.to_parquet(cache_dir / 'metrics_journal_period.parquet', index=False)
    
    print("\n✅ All metrics computed and cached successfully!")
    return {
        'journal_annual': journal_annual_df,
        'journal_period': journal_period_df,
        'country_annual': country_annual_df,
        'country_period': country_period_df,
        'latam_annual': latam_annual,
        'latam_period': latam_period
    }

def load_cached_metrics(level, metric_type='period'):
    cache_dir = get_cache_dir()
    cache_file = cache_dir / f'metrics_{level}_{metric_type}.parquet'
    if cache_file.exists():
        try:
            return pd.read_parquet(cache_file)
        except Exception as e:
            print(f"Error loading cache: {e}")
            return None
    return None
