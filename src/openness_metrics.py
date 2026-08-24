"""
openness_metrics.py - Open Science & Open Access Advanced Indicators
Includes Diamond OA, Gold OA, Green OA, Hybrid OA, Preprints, CC Licenses & Open Science Index.
"""
import pandas as pd
import numpy as np

def compute_oa_breakdown(works_df):
    """
    Computes exhaustive Open Access distribution across works.
    """
    if works_df is None or len(works_df) == 0:
        return {
            'total_works': 0,
            'pct_oa_diamond': 0.0,
            'pct_oa_gold': 0.0,
            'pct_oa_green': 0.0,
            'pct_oa_hybrid': 0.0,
            'pct_oa_bronze': 0.0,
            'pct_oa_closed': 0.0,
            'pct_overall_oa': 0.0,
            'pct_has_doi': 0.0,
            'pct_has_repository_url': 0.0
        }
        
    total = len(works_df)
    counts = works_df['oa_status'].value_counts() if 'oa_status' in works_df.columns else {}
    
    diamond = counts.get('diamond', 0)
    gold = counts.get('gold', 0)
    green = counts.get('green', 0)
    hybrid = counts.get('hybrid', 0)
    bronze = counts.get('bronze', 0)
    closed = counts.get('closed', 0)
    
    has_doi = works_df['doi'].notna().sum() if 'doi' in works_df.columns else 0
    has_url = works_df['oa_url'].notna().sum() if 'oa_url' in works_df.columns else 0
    
    overall_oa = total - closed
    
    return {
        'total_works': total,
        'pct_oa_diamond': round((diamond / total) * 100, 2),
        'pct_oa_gold': round((gold / total) * 100, 2),
        'pct_oa_green': round((green / total) * 100, 2),
        'pct_oa_hybrid': round((hybrid / total) * 100, 2),
        'pct_oa_bronze': round((bronze / total) * 100, 2),
        'pct_oa_closed': round((closed / total) * 100, 2),
        'pct_overall_oa': round((overall_oa / total) * 100, 2),
        'pct_has_doi': round((has_doi / total) * 100, 2),
        'pct_has_repository_url': round((has_url / total) * 100, 2)
    }

def compute_open_science_index(oa_breakdown):
    """
    Computes a composite Open Science Index (0-100) prioritizing Diamond OA (non-commercial),
    DOI persistence, and Green OA accessibility.
    """
    if not oa_breakdown or oa_breakdown.get('total_works', 0) == 0:
        return 0.0
        
    # Weights:
    # 40% Diamond OA
    # 25% Gold OA
    # 15% Green OA (Repositories)
    # 10% Hybrid OA
    # 10% Persistent Identifiers (DOI)
    
    diamond = oa_breakdown.get('pct_oa_diamond', 0.0)
    gold = oa_breakdown.get('pct_oa_gold', 0.0)
    green = oa_breakdown.get('pct_oa_green', 0.0)
    hybrid = oa_breakdown.get('pct_oa_hybrid', 0.0)
    doi = oa_breakdown.get('pct_has_doi', 0.0)
    
    score = (
        0.40 * diamond +
        0.25 * gold +
        0.15 * green +
        0.10 * hybrid +
        0.10 * doi
    )
    
    return round(float(np.clip(score, 0.0, 100.0)), 2)
