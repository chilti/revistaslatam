"""
Cálculo de UMAP Global (Olap ClickHouse)

Proyecta un mapa UMAP bidimensional para las Macro Regiones (Global South, Global North y sus subdivisiones) 
y para todos los países del mundo basándose en las métricas generadas por ClickHouse 
durante el período 2021-2025 (o el cálculo anual derivado a un solo periodo).
"""
import pandas as pd
import numpy as np
from pathlib import Path
from umap import UMAP
from sklearn.preprocessing import StandardScaler
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = Path(__file__).parent.parent
CACHE_DIR = BASE_PATH / 'data' / 'cache'

# Features idénticos a los del paper/LATAM
feature_cols = [
    'num_journals',       # Volumen
    'pct_oa_diamond',     # Acceso Diamante
    'fwci_avg',           # Impacto Numérico
    'pct_top_10',         # Excelencia de Calidad
    'pct_top_1',          # Élite
    'avg_percentile',     # Distribución
    'pct_lang_en'         # Alcance Lingüístico
]

def load_period_data(entity_type):
    """
    Carga el parquet de la métrica por país o región calculada por `compute_metrics_clickhouse.py`.
    Para generar mapas estáticos 2D, agrupamos temporalmente los datos métricos de todo el periodo 
    disponible (ej. Promedio del 2021-2025) en un solo vector por entidad.
    """
    if entity_type == 'region':
        filepath = CACHE_DIR / 'metrics_global_region_annual.parquet'
        id_col = 'region'
    else:
        filepath = CACHE_DIR / 'metrics_global_country_annual.parquet'
        id_col = 'country_code'
        
    if not filepath.exists():
        logger.error(f"Archivo no encontrado: {filepath}")
        return None
        
    df = pd.read_parquet(filepath)
    # Filtrar solo la ventana actual de estudio
    df = df[(df['year'] >= 2021) & (df['year'] <= 2025)]
    
    if df.empty:
        return None
        
    # Colapsar el quinquenio promediando las variables
    collapsed = df.groupby(id_col, as_index=False).mean(numeric_only=True)
    return collapsed

def compute_umap():
    # 1. Mapa de las 8 MACRO REGIONES
    df_region = load_period_data('region')
    if df_region is not None:
        logger.info(f"Calculando UMAP para {len(df_region)} Regiones...")
        
        missing = [c for c in feature_cols if c not in df_region.columns]
        if missing:
            logger.warning(f"Faltan features en region: {missing}. No se hará UMAP.")
        else:
            # Escalado
            valid = df_region.dropna(subset=feature_cols).copy()
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(valid[feature_cols])
            
            # Parametría UMAP (Pocos datos = n_neighbors chicos)
            n_neighbors = min(4, len(valid) - 1)
            reducer = UMAP(n_neighbors=n_neighbors, min_dist=0.1, random_state=42)
            embedding = reducer.fit_transform(X_scaled)
            
            valid['umap_x'] = embedding[:, 0]
            valid['umap_y'] = embedding[:, 1]
            
            out_file = CACHE_DIR / 'umap_global_regions.parquet'
            valid.to_parquet(out_file, index=False)
            logger.info(f"✅ UMAP Regiones guardado en {out_file}")

    # 2. Mapa Mundial de PAÍSES (Los ~200 países rastreados)
    df_country = load_period_data('country')
    if df_country is not None:
        logger.info(f"Calculando UMAP para {len(df_country)} Países Globales...")
        
        missing = [c for c in feature_cols if c not in df_country.columns]
        if missing:
            logger.warning(f"Faltan features en country: {missing}")
        else:
            valid = df_country.dropna(subset=feature_cols).copy()
            
            # Enriqueciendo Países con su región para coloreo
            valid_merge = valid.copy() # Aquí asumo que df_country tiene ya el join pero lo revisamos
            if len(valid_merge) > 10:
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(valid_merge[feature_cols])
                
                reducer = UMAP(n_neighbors=15, min_dist=0.1, random_state=42)
                embedding = reducer.fit_transform(X_scaled)
                
                valid_merge['umap_x'] = embedding[:, 0]
                valid_merge['umap_y'] = embedding[:, 1]
                
                out_file = CACHE_DIR / 'umap_global_countries.parquet'
                valid_merge.to_parquet(out_file, index=False)
                logger.info(f"✅ UMAP Países Globales guardado en {out_file}")

if __name__ == "__main__":
    compute_umap()
