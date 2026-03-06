import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import os

# Configuración de página
st.set_page_config(
    page_title="Dashboard Bibliométrico Global",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Rutas y Carga de datos
BASE_PATH = Path(__file__).parent.parent if '__file__' in locals() else Path(os.getcwd())
CACHE_DIR = BASE_PATH / 'data' / 'cache'

# Referencia a src/regions.py
sys.path.append(str(BASE_PATH / 'src'))
try:
    from regions import GLOBAL_REGIONS, get_region_for_country
except ImportError:
    st.error("Error cargando `src/regions.py`. Asegúrate de que existe.")
    st.stop()

@st.cache_data
def load_data(filename: str):
    file_path = CACHE_DIR / filename
    if not file_path.exists():
        return None
    return pd.read_parquet(file_path)

# --- CARGA DE DATOS ---
df_region_annual = load_data('metrics_global_region_annual.parquet')
df_country_annual = load_data('metrics_global_country_annual.parquet')
df_umap_macro = load_data('umap_global_regions.parquet')

# Variables globales para filtrar el año más reciente disponible en toda la base
latest_year = 2025 # Por default
if df_region_annual is not None and not df_region_annual.empty:
    latest_year = int(df_region_annual['year'].max())

# --- SIDEBAR ---
st.sidebar.title("🌍 Navegación Mundial")
level = st.sidebar.radio(
    "Nivel de Análisis:",
    ["1. Mundo (Macro)", "2. Exploración por Región", "3. Análisis de País"]
)

st.sidebar.markdown("---")
st.sidebar.info(
    "Este dashboard opera bajo un modelo OLAP "
    "procesando las métricas calculadas "
    "directamente de ClickHouse."
)

if level == "1. Mundo (Macro)":
    st.title("Panorama Científico Mundial")
    st.markdown("Comparativa general entre las grandes regiones geoeconómicas.")
    
    if df_region_annual is not None:
        # Datos del último año para las KPIs Globales
        df_latest = df_region_annual[df_region_annual['year'] == latest_year]
        
        # --- TABLERO DE RESUMEN MACRO ---
        col1, col2, col3, col4 = st.columns(4)
        
        total_docs = df_latest['num_documents'].sum() if not df_latest.empty else 0
        total_journals = df_latest['num_journals'].sum() if not df_latest.empty else 0
        total_diamond = (df_latest['num_documents'] * (df_latest['pct_oa_diamond']/100)).sum() if not df_latest.empty else 0
        
        col1.metric("Volumen (Doc. Totales)", f"{int(total_docs):,}")
        col2.metric("Total de Revistas", f"{int(total_journals):,}")
        col3.metric("Acceso Diamante (Estimado)", f"{int(total_diamond):,}")
        
        st.markdown("---")
        
        # --- UMAP DE REGIONES ---
        st.subheader("Similitud Estructural de Regiones (UMAP 2021-2025)")
        st.markdown("Proyección 2D basada en perfiles de Acceso Abierto, Idiomas e Impacto (FWCI).")
        
        if df_umap_macro is not None and not df_umap_macro.empty:
            fig_umap = px.scatter(
                df_umap_macro, 
                x='umap_x', 
                y='umap_y',
                text='region',
                color='region', # Idealmente crearíamos un dict de colores para Norte vs Sur
                size='num_documents',
                hover_data=['num_journals', 'fwci_avg', 'pct_oa_diamond', 'pct_lang_en'],
                title="Mapa de Similitud - Macro Regiones"
            )
            fig_umap.update_traces(textposition='top center', marker=dict(line=dict(width=1, color='DarkSlateGrey')))
            fig_umap.update_layout(showlegend=False, height=500)
            st.plotly_chart(fig_umap, use_container_width=True)
        else:
            st.warning("No se encontró `umap_global_regions.parquet`. Ejecute el pipeline estático global.")
            
        # --- TRAYECTORIAS (LINE CHARTS) ---
        st.subheader("Evolución Temporal: Impacto vs Idioma")
        colA, colB = st.columns(2)
        
        with colA:
            fig_fwci = px.line(
                df_region_annual, x='year', y='fwci_avg', color='region',
                title="Evolución FWCI Promedio (2000-Presente)"
            )
            st.plotly_chart(fig_fwci, use_container_width=True)
            
        with colB:
            fig_en = px.line(
                df_region_annual, x='year', y='pct_lang_en', color='region',
                title="Penetración del Idioma Inglés (%)"
            )
            st.plotly_chart(fig_en, use_container_width=True)
            
    else:
        st.error("No hay datos de agregación macro disponibles. Asegúrese de correr `compute_metrics_clickhouse.py`.")

elif level == "2. Exploración por Región":
    st.title("Análisis Intrarregional")
    
    regiones_list = list(GLOBAL_REGIONS.keys())
    selected_region = st.selectbox("Seleccione la Región a analizar:", regiones_list)
    
    if df_country_annual is not None:
        # Filtrar solo países de la región elegida
        paises_region = GLOBAL_REGIONS[selected_region]
        df_region_countries = df_country_annual[df_country_annual['country_code'].isin(paises_region)]
        
        if not df_region_countries.empty:
            df_latest = df_region_countries[df_region_countries['year'] == latest_year]
            
            # --- MAPA Coroplético ---
            st.subheader(f"Distribución Geográfica - {selected_region} ({latest_year})")
            
            fig_map = px.choropleth(
                df_latest,
                locations="country_code",
                color="fwci_avg",
                hover_name="country_code",
                hover_data=["num_journals", "num_documents", "pct_oa_diamond"],
                color_continuous_scale=px.colors.sequential.Teal,
                title=f"Impacto Normatizado (FWCI) por País en {selected_region}"
            )
            fig_map.update_layout(geo=dict(showcoastlines=True, projection_type='equirectangular'))
            st.plotly_chart(fig_map, use_container_width=True)
            
            # --- TABLA COMPARATIVA ---
            st.subheader("Estadísticas Recientes por País")
            display_cols = ['country_code', 'num_journals', 'num_documents', 'fwci_avg', 'pct_oa_diamond', 'pct_lang_en']
            if set(display_cols).issubset(df_latest.columns):
                sorted_df = df_latest[display_cols].sort_values(by='num_documents', ascending=False)
                st.dataframe(sorted_df, use_container_width=True)
                
        else:
            st.info(f"No hay datos procesados en la base para la región {selected_region}.")
            
    else:
        st.error("Faltan datos de países `metrics_global_country_annual.parquet`.")

elif level == "3. Análisis de País":
    st.title("Análisis Estructural de País")
    
    # --- FILTRO CASCADA ---
    col1, col2 = st.columns(2)
    with col1:
        regiones_list = list(GLOBAL_REGIONS.keys())
        selected_region_for_filter = st.selectbox("1. Filtrar por Región:", ["Todas"] + regiones_list)
        
    with col2:
        if df_country_annual is not None:
            available_countries = df_country_annual['country_code'].unique().tolist()
            
            if selected_region_for_filter != "Todas":
                # Filtrar lista
                valid_in_region = GLOBAL_REGIONS[selected_region_for_filter]
                available_countries = [c for c in available_countries if c in valid_in_region]
                
            selected_country = st.selectbox("2. Seleccionar País:", sorted(available_countries))
        else:
            selected_country = None
            st.warning("Sin datos de países cargados.")
            
    if selected_country and df_country_annual is not None:
        st.markdown("---")
        df_target = df_country_annual[df_country_annual['country_code'] == selected_country].sort_values('year')
        
        st.subheader(f"Evolución de {selected_country}")
        
        if not df_target.empty:
            kpi1, kpi2, kpi3 = st.columns(3)
            latest = df_target.iloc[-1]
            kpi1.metric("Producción en el último año", f"{int(latest['num_documents']):,}")
            kpi2.metric("Impacto Promedio (FWCI)", f"{latest['fwci_avg']:.2f}")
            kpi3.metric("Revistas Publicadoras", f"{int(latest['num_journals']):,}")
            
            fig = px.area(
                df_target, x="year", y=["num_documents"],
                title="Historial de Producción Documental (OLAP)",
                color_discrete_sequence=['#ff7f0e']
            )
            st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("### Tabla histórica completa")
            st.dataframe(df_target.sort_values('year', ascending=False), use_container_width=True)
            
        else:
            st.info("Sin registros históricos para este país.")
