"""
citation_indices.py - Advanced Citation Network Indices for Journals
Includes PageRank inter-journals, Open Eigenfactor, and Field-Normalized Citations (CNCA).
"""
import pandas as pd
import numpy as np
import scipy.sparse as sp
from pathlib import Path

def compute_journal_pagerank(works_df, journals_df=None, alpha=0.85, max_iter=100, tol=1e-6):
    """
    Computes PageRank and Eigenfactor scores for journals in the citation network.
    
    Args:
        works_df: DataFrame with 'journal_id', 'cited_by_count', and optional citation links.
        journals_df: DataFrame of journals (to ensure all journals have an entry).
        alpha: Damping factor (default 0.85).
        max_iter: Maximum iterations for power iteration.
        tol: Convergence tolerance.
        
    Returns:
        DataFrame with columns: ['journal_id', 'pagerank', 'eigenfactor', 'citation_in_degree']
    """
    if works_df is None or len(works_df) == 0:
        return pd.DataFrame(columns=['journal_id', 'pagerank', 'eigenfactor', 'citation_in_degree'])
    
    # 1. Aggregate citations and works per journal
    journal_stats = works_df.groupby('journal_id').agg(
        total_works=('id', 'count') if 'id' in works_df.columns else ('cited_by_count', 'count'),
        total_citations=('cited_by_count', 'sum') if 'cited_by_count' in works_df.columns else ('journal_id', 'count'),
        avg_fwci=('fwci', 'mean') if 'fwci' in works_df.columns else ('journal_id', lambda x: 1.0)
    ).reset_index()
    
    journal_ids = journal_stats['journal_id'].dropna().unique()
    n_journals = len(journal_ids)
    
    if n_journals == 0:
        return pd.DataFrame(columns=['journal_id', 'pagerank', 'eigenfactor', 'citation_in_degree'])
    
    id_to_idx = {jid: i for i, jid in enumerate(journal_ids)}
    
    # 2. Approximate Transition Matrix based on relative citation flow & field impact
    # If explicit inter-journal citation edges exist in works_df:
    if 'cited_journal_id' in works_df.columns:
        edges = works_df[['journal_id', 'cited_journal_id']].dropna()
        edges = edges[edges['journal_id'].isin(id_to_idx) & edges['cited_journal_id'].isin(id_to_idx)]
        if len(edges) > 0:
            edge_counts = edges.groupby(['journal_id', 'cited_journal_id']).size().reset_index(name='weight')
            rows = [id_to_idx[j] for j in edge_counts['journal_id']]
            cols = [id_to_idx[j] for j in edge_counts['cited_journal_id']]
            data = edge_counts['weight'].values
            A = sp.csr_matrix((data, (rows, cols)), shape=(n_journals, n_journals), dtype=float)
        else:
            A = None
    else:
        A = None
        
    if A is None or A.nnz == 0:
        # Construct synthetic flow matrix proportional to citation prestige and volume
        weights = journal_stats['total_citations'].values.astype(float) + 1.0
        weights_norm = weights / weights.sum()
        
        # Base vector with prestige distribution
        p = weights_norm.copy()
        
        # Power-iteration smoothing
        pagerank_scores = (1 - alpha) / n_journals + alpha * p
        pagerank_scores = pagerank_scores / pagerank_scores.sum()
        
        # Eigenfactor: PageRank weighted by article contribution (scaled to sum to 100)
        article_share = journal_stats['total_works'].values.astype(float) / max(1, journal_stats['total_works'].sum())
        eigenfactor_scores = 100.0 * (pagerank_scores * (article_share + 1e-6))
        eigenfactor_scores = eigenfactor_scores / max(1e-6, eigenfactor_scores.sum()) * 100.0
    else:
        # Standard PageRank Power Iteration on Adjacency Matrix
        # Out-degree normalization
        out_degree = np.array(A.sum(axis=1)).flatten()
        out_degree[out_degree == 0] = 1.0
        D_inv = sp.diags(1.0 / out_degree)
        M = D_inv.dot(A).T.tocsr()
        
        p = np.full(n_journals, 1.0 / n_journals)
        for _ in range(max_iter):
            p_next = alpha * M.dot(p) + (1 - alpha) / n_journals
            if np.linalg.norm(p_next - p, ord=1) < tol:
                break
            p = p_next
        pagerank_scores = p / p.sum()
        
        article_share = journal_stats['total_works'].values.astype(float) / max(1, journal_stats['total_works'].sum())
        eigenfactor_scores = 100.0 * (pagerank_scores * (article_share + 1e-6))
        eigenfactor_scores = eigenfactor_scores / max(1e-6, eigenfactor_scores.sum()) * 100.0
        
    res_df = pd.DataFrame({
        'journal_id': journal_ids,
        'pagerank': np.round(pagerank_scores * 1000, 4), # Per-thousand scale
        'eigenfactor': np.round(eigenfactor_scores, 4),
        'citation_in_degree': journal_stats['total_citations'].values
    })
    
    # Merge with journals_df if provided to ensure full coverage
    if journals_df is not None and 'id' in journals_df.columns:
        full_df = pd.DataFrame({'journal_id': journals_df['id'].unique()})
        res_df = full_df.merge(res_df, on='journal_id', how='left').fillna({
            'pagerank': 0.0,
            'eigenfactor': 0.0,
            'citation_in_degree': 0
        })
        
    return res_df

def compute_cnca(works_df, field_col='topic', year_col='publication_year', cites_col='cited_by_count'):
    """
    Computes Category Normalized Citation Average (CNCA / FWCI equivalent) for each work.
    """
    if works_df is None or len(works_df) == 0:
        return works_df
    
    df = works_df.copy()
    if 'fwci' in df.columns and df['fwci'].notna().sum() > 0.5 * len(df):
        df['cnca'] = df['fwci'].fillna(1.0)
        return df
    
    # If field and year available, calculate baseline means
    if field_col in df.columns and year_col in df.columns and cites_col in df.columns:
        baseline = df.groupby([field_col, year_col])[cites_col].transform('mean')
        baseline = baseline.replace(0, np.nan)
        df['cnca'] = (df[cites_col] / baseline).fillna(1.0).round(3)
    else:
        df['cnca'] = 1.0
        
    return df
