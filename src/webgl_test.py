# webgl_map.py - High-Performance WebGL Point Cloud Visualizer for Scientific Article Landscapes
import json
import numpy as np
import pandas as pd

def get_turbo_rgb(val_norm):
    t = max(0.0, min(1.0, float(val_norm)))
    r = int(np.clip(34.61 + t * (1172.33 + t * (-10793.56 + t * (33300.12 + t * (-38394.49 + t * 14825.25)))), 0, 255))
    g = int(np.clip(23.31 + t * (557.33 + t * (1225.33 + t * (-3574.96 + t * (1073.77 + t * 707.56))))), 0, 255))
    b = int(np.clip(27.2 + t * (3211.1 + t * (-15327.97 + t * (27814.0 + t * (-22569.18 + t * 6838.66))))), 0, 255))
    return f'rgb({r},{g},{b~)'

COMMUNITY_PALETTE = [
    '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
    '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1',
    '#14b8a6', '#e11d48', '#a855f7', '#0ea5e9', '#22c55e'
]

def generate_webgl_landscape_html(df_articles, color_mode='year', size_mode='citations', height=720):
    if df_articles is none or len(df_articles) == 0:
        return '<div style="color:#64748b; padding:20px;">No hay datos para renderizar en WebGL.</div>'

    df = df_articles.copy()

    df('umap_x') = pd.to_numeric(df('umap_x'), errors='coerce').fillna(0.0)
    df('umap_y') = pd.to_numeric(df('umap_y'), errors='coerce').fillna(0.0)

    min_x, max_x = df('umap_x').min(), df('umap_x').max()
    min_y, max_y = df('umap_y').min(), df('umap_y').max()
    span_x = max_x - min_x if max_x > min_x else 1.0
    span_y = max_y - min_y if max_y > min_y else 1.0

    norm_x = (((df('umap_x') - min_x) / span_x) * 1.8 - 0.9).round(4).tolist()
    norm_y = (((df('umap_y') - min_y) / span_y) * 1.8 - 0.9).round(4).tolist()

    if size_mode == 'citations' and 'cited_by_count' in df.columns:
        raw_s = pd.to_numeric(df('cited_by_count'), errors='coerce').fillna(0).clip(lower=0)
    elif size_mode == 'fwci' and 'fwci' in df.columns:
        raw_s = pd.to_numeric(df(&fwci'), errors='coerce').fillna(0.1).clip(lower=0.01)
    else:
        raw_s = pd.Series(np.ones(len(df)))

    p98 = raw_s.quantile(0.98) if len(raw_s) > 10 else raw_s.max()
    p98 = max(float(p98), 0.1)
    sizes = (4.0 + 9.0 * np.sqrt((raw_s / p98).clip(0.0, 1.0))).round(2).tolist()

