import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np
import somoclu  
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from data_collector import update_data
from data_processor import load_data as collector_load_data
from performance_metrics import compute_and_cache_all_metrics, load_cached_metrics, get_cache_dir
from som_utils import hex_center, hex_polygon

# Page config
st.set_page_config(
    page_title="Dashboard Bibliométrico LATAM",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Sidebar ---
st.sidebar.title("Bibliometría LATAM")
st.sidebar.markdown("---")

# Navigation
level = st.sidebar.radio(
    "Nivel de Análisis",
    ["Region (Latinoamérica)", "País", "Revista", "Acerca de..."]
)

# === SCROLL RESET LOGIC ===
# Reset scroll position when switching sections
if 'current_view_level' not in st.session_state:
    st.session_state.current_view_level = level

if st.session_state.current_view_level != level:
    st.session_state.current_view_level = level
    # Inject JavaScript to scroll to top
    import streamlit.components.v1 as components
    components.html(
        """
        <script>
            // Target the main scrollable container in Streamlit
            var scrollable = window.parent.document.querySelector('[data-testid="stAppViewContainer"]');
            if (scrollable) {
                scrollable.scrollTo({top: 0, behavior: 'instant'});
            }
        </script>
        """,
        height=0,
        width=0
    )

st.sidebar.markdown("---")

# Show cache status
cache_dir = get_cache_dir()
latam_cache = cache_dir / 'metrics_latam_annual.parquet'

# Defines trajectory paths globally
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
traj_coords_file = os.path.join(BASE_PATH, 'data', 'cache', 'trajectory_coordinates.parquet')
traj_raw_file = os.path.join(BASE_PATH, 'data', 'cache', 'trajectory_data_raw.parquet')
traj_smooth_file = os.path.join(BASE_PATH, 'data', 'cache', 'trajectory_data_smoothed.parquet')
traj_smooth_w5_file = os.path.join(BASE_PATH, 'data', 'cache', 'trajectory_data_smoothed_w5.parquet')
MAP_COUNTRIES_FILE = os.path.join(BASE_PATH, 'data', 'cache', 'trajectory_countries_coords.parquet')
MAP_JOURNALS_FILE = os.path.join(BASE_PATH, 'data', 'cache', 'trajectory_journals_coords.parquet')
TOPICS_FILE = os.path.join(BASE_PATH, 'data', 'journals_topics_sunburst.parquet')
COUNTRIES_TOPICS_FILE = os.path.join(BASE_PATH, 'data', 'countries_topics_sunburst.parquet')

# Country Names Mapping
COUNTRY_NAMES = {
    'AR': 'Argentina', 'BO': 'Bolivia', 'BR': 'Brasil', 'BS': 'Bahamas', 
    'BZ': 'Belice', 'CL': 'Chile', 'CO': 'Colombia', 'CR': 'Costa Rica', 
    'CU': 'Cuba', 'DO': 'Rep. Dominicana', 'EC': 'Ecuador', 'GT': 'Guatemala', 
    'GY': 'Guyana', 'HN': 'Honduras', 'HT': 'Haití', 'JM': 'Jamaica', 
    'MX': 'México', 'NI': 'Nicaragua', 'PA': 'Panamá', 'PE': 'Perú', 
    'PR': 'Puerto Rico', 'PY': 'Paraguay', 'SV': 'El Salvador', 'UY': 'Uruguay', 
    'VE': 'Venezuela', 'ES': 'España', 'PT': 'Portugal', 'LATAM': 'Latinoamérica',
    'BB': 'Barbados', 'SR': 'Surinam', 'TT': 'Trinidad y Tobago'
}

if latam_cache.exists():
    import datetime
    mtime = os.path.getmtime(latam_cache)
    cache_time = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
    st.sidebar.info(f"📊 Caché de métricas: {cache_time}")
else:
    st.sidebar.warning("⚠️ Sin caché de métricas")

# Snapshot info
st.sidebar.caption("📅 Datos base: OpenAlex Snapshot 2025-10-27")
st.sidebar.caption("ℹ️ Métricas como `2yr_mean_citedness` reflejan el estado en esa fecha")

# --- Main Content ---
st.title("Revistas Científicas de Latinoamérica")
st.caption("OpenAlex Snapshot 2025-10-27")

# Load Data
@st.cache_data
def load_data():
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    journals_path = os.path.join(data_dir, 'latin_american_journals.parquet')
    df = collector_load_data(journals_path)
    return df

df = load_data()

if df.empty:
    st.warning("⚠️ No hay datos disponibles. Por favor, pulsa 'Actualizar Datos' en la barra lateral para comenzar.")
    st.stop()

# Check if cached metrics exist
has_cached_metrics = latam_cache.exists()

def load_and_scale(entity_type, period_key):
    """Wrapper to load metrics and auto-scale avg_percentile to 0-100."""
    df = load_cached_metrics(entity_type, period_key)
    if df is not None and not df.empty and 'avg_percentile' in df.columns:
        # If max value is <= 1.0 (e.g. 0.45), assume 0-1 scale and multiply by 100
        if df['avg_percentile'].max() <= 1.05 and df['avg_percentile'].max() > 0:
            df['avg_percentile'] = df['avg_percentile'] * 100
    return df

def create_profile_table(df_data, level_col, index_col, index_name, total_name):
    """Agrega y pivotea métricas temáticas para Perfiles Temáticos."""
    # 1. Agrupar por Entidad (País/Revista) y Nivel Temático (Dominio/Campo/Subcampo)
    grouped = df_data.groupby([index_col, level_col], as_index=False)['count'].sum()
    
    # 2. Pivotear para que los temas sean columnas
    pivoted = grouped.pivot(index=index_col, columns=level_col, values='count').fillna(0).astype(int)
    
    # 3. Formatear el índice y columnas
    if index_col == 'country_code':
        # Translate Country Codes
        pivoted.index = pivoted.index.map(lambda x: COUNTRY_NAMES.get(x, x))
    
    pivoted.index.name = index_name
    pivoted.columns.name = None
    pivoted = pivoted.reset_index()
    
    # 4. Calcular el Total General por Entidad (para ordenar y mostrar)
    topic_cols = [c for c in pivoted.columns if c != index_name]
    pivoted['Total'] = pivoted[topic_cols].sum(axis=1)
    
    # Ordenar por el Total descendente
    pivoted = pivoted.sort_values(by='Total', ascending=False)
    
    # 5. Calcular la Fila de Suma Total
    total_row = {index_name: total_name}
    for col in topic_cols + ['Total']:
        total_row[col] = pivoted[col].sum()
    
    # Añadir fila inferior (usando concat para evitar problemas de append en nuevas vars pandas)
    total_df = pd.DataFrame([total_row])
    final_table = pd.concat([pivoted, total_df], ignore_index=True)
    
    return final_table

# Filter by Level
if level == "Region (Latinoamérica)":
    st.header("Panorama Regional")
    
    # Basic KPIs from journals
    col1, col2 = st.columns(2)
    col1.metric("Revistas Indexadas", len(df))
    col2.metric("Total Artículos", f"{df['works_count'].sum():,}")
    
    # Geographic Map Section
    if has_cached_metrics:
        st.markdown("---")
        st.subheader("Mapa Regional por Indicador")
        
        # Load country metrics for map
        country_period = load_and_scale('country', 'period')
        
        if country_period is not None and len(country_period) > 0:
            # Indicator selector
            indicator_options = {
                'Número de Revistas': 'num_journals',
                'Artículos': 'num_documents',
                'FWCI Promedio': 'fwci_avg',
                '% Top 10%': 'pct_top_10',
                '% Top 1%': 'pct_top_1',
                '% OA Total': 'pct_oa_total',
                '% OA Diamante': 'pct_oa_diamond',
                '% OA Gold': 'pct_oa_gold',
                '% OA Verde': 'pct_oa_green',
                '% OA Híbrido': 'pct_oa_hybrid',
                '% OA Bronce': 'pct_oa_bronze',
                '% Cerrado': 'pct_oa_closed'
            }
            
            selected_indicator = st.selectbox(
                "Selecciona un indicador para visualizar:",
                options=list(indicator_options.keys()),
                index=0
            )
            
            # Calculate % OA Total if needed
            if 'pct_oa_total' not in country_period.columns:
                country_period['pct_oa_total'] = (
                    country_period['pct_oa_gold'] + 
                    country_period['pct_oa_green'] + 
                    country_period['pct_oa_hybrid'] + 
                    country_period['pct_oa_bronze']
                )
            
            # Get the column name for the selected indicator
            indicator_col = indicator_options[selected_indicator]
            
            # Validar que la columna existe y tiene datos
            if indicator_col not in country_period.columns:
                st.warning(f"⚠️ La columna '{indicator_col}' no existe en los datos.")
                st.write("Columnas disponibles:", list(country_period.columns))
            elif country_period[indicator_col].isna().all():
                st.warning(f"⚠️ No hay datos para '{selected_indicator}'")
            else:
                # Filtrar solo países con datos válidos
                map_data = country_period[country_period[indicator_col].notna()].copy()
                
                if len(map_data) == 0:
                    st.warning(f"⚠️ No hay países con datos válidos para '{selected_indicator}'")
                else:
                    # Detectar formato de código de país y convertir si es necesario
                    sample_code = str(map_data['country_code'].iloc[0])
                    
                    if len(sample_code) == 2:
                        # Códigos ISO-2 (MX, BR, AR) - necesitan conversión a ISO-3
                        # Mapeo manual de los principales países latinoamericanos
                        iso2_to_iso3 = {
                            'AR': 'ARG', 'BO': 'BOL', 'BR': 'BRA', 'CL': 'CHL', 'CO': 'COL',
                            'CR': 'CRI', 'CU': 'CUB', 'DO': 'DOM', 'EC': 'ECU', 'SV': 'SLV',
                            'GT': 'GTM', 'HN': 'HND', 'MX': 'MEX', 'NI': 'NIC', 'PA': 'PAN',
                            'PY': 'PRY', 'PE': 'PER', 'UY': 'URY', 'VE': 'VEN', 'PR': 'PRI'
                        }
                        map_data['country_code_iso3'] = map_data['country_code'].map(iso2_to_iso3)
                        location_col = 'country_code_iso3'
                    else:
                        # Ya son ISO-3
                        location_col = 'country_code'
                    
                    # Create choropleth map
                    fig_map = px.choropleth(
                        map_data,
                        locations=location_col,
                        locationmode='ISO-3',
                        color=indicator_col,
                        hover_name='country_code',
                        hover_data={
                            location_col: False,  # No mostrar el código ISO-3
                            'country_code': True,
                            indicator_col: ':.2f',
                            'num_journals': ':,',
                            'num_documents': ':,'
                        },
                        color_continuous_scale='Viridis',
                        labels={
                            indicator_col: selected_indicator,
                            'country_code': 'País',
                            'num_journals': 'Revistas',
                            'num_documents': 'Artículos'
                        },
                        title=f'{selected_indicator} por País'
                    )
            
                    # Configurar el mapa para mostrar toda Latinoamérica
                    # No usar scope para evitar restricciones, en su lugar usar geo settings
                    fig_map.update_geos(
                        showcountries=True,
                        countrycolor="lightgray",
                        showcoastlines=True,
                        coastlinecolor="gray",
                        showland=True,
                        landcolor="white",
                        showocean=True,
                        oceancolor="lightblue",
                        projection_type='natural earth',
                        # Centrar en Latinoamérica completa (México a Argentina)
                        center=dict(lat=-5, lon=-70),
                        # Ajustar el rango para mostrar desde México hasta el sur de Argentina
                        lataxis_range=[-60, 35],
                        lonaxis_range=[-120, -30]
                    )
                    
                    fig_map.update_layout(
                        height=600,
                        margin=dict(l=0, r=0, t=40, b=0),
                        geo=dict(
                            bgcolor='rgba(0,0,0,0)',
                        )
                    )
                    
                    st.plotly_chart(fig_map, use_container_width=True)
    


    if has_cached_metrics:
        st.markdown("---")
        st.subheader("Indicadores de Desempeño")
        
        # Load cached metrics
        latam_annual = load_and_scale('latam', 'annual')
        latam_annual = load_and_scale('latam', 'annual')
        latam_period = load_and_scale('latam', 'period')
        latam_period_recent = load_and_scale('latam', 'period_2021_2025')
        
        if latam_period is not None and len(latam_period) > 0:
            period_data = latam_period.iloc[0]
            
            # Display period metrics
            st.markdown(f"### Periodo Completo: {period_data.get('period', 'N/A')}")
            
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Documentos", f"{period_data.get('num_documents', 0):,}")
            col2.metric("FWCI Promedio", f"{period_data.get('fwci_avg', 0):.2f}")
            col3.metric("% Top 10%", f"{period_data.get('pct_top_10', 0):.1f}%")
            col4.metric("% Top 1%", f"{period_data.get('pct_top_1', 0):.1f}%")
            col5.metric("Percentil Prom. Norm.", f"{period_data.get('avg_percentile', 0):.1f}")
            
            # Recent Period
            if latam_period_recent is not None and len(latam_period_recent) > 0:
                rec_data = latam_period_recent.iloc[0]
                st.markdown(f"### Periodo Reciente: 2021-2025")
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Documentos", f"{rec_data.get('num_documents', 0):,}")
                c2.metric("FWCI Promedio", f"{rec_data.get('fwci_avg', 0):.2f}")
                c3.metric("% Top 10%", f"{rec_data.get('pct_top_10', 0):.1f}%")
                c4.metric("% Top 1%", f"{rec_data.get('pct_top_1', 0):.1f}%")
                c5.metric("Percentil Prom. Norm.", f"{rec_data.get('avg_percentile', 0):.1f}")
            
            # Open Access and Language breakdown
            st.markdown("#### Distribución y Características de las Publicaciones")
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                oa_data = {
                    'Tipo': ['Gold', 'Diamond', 'Green', 'Hybrid', 'Bronze', 'Closed'],
                    'Porcentaje': [
                        period_data.get('pct_oa_gold', 0),
                        period_data.get('pct_oa_diamond', 0),
                        period_data.get('pct_oa_green', 0),
                        period_data.get('pct_oa_hybrid', 0),
                        period_data.get('pct_oa_bronze', 0),
                        period_data.get('pct_oa_closed', 0)
                    ]
                }
                oa_df = pd.DataFrame(oa_data)
                
                fig_oa = px.pie(oa_df, values='Porcentaje', names='Tipo',
                               title='Distribución por Acceso Abierto',
                               color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig_oa, use_container_width=True)

            with col_chart2:
                lang_data = {
                    'Idioma': ['Español', 'Inglés', 'Portugués', 'Francés', 'Alemán', 'Italiano', 'Otros'],
                    'Porcentaje': [
                        period_data.get('pct_lang_es', 0),
                        period_data.get('pct_lang_en', 0),
                        period_data.get('pct_lang_pt', 0),
                        period_data.get('pct_lang_fr', 0),
                        period_data.get('pct_lang_de', 0),
                        period_data.get('pct_lang_it', 0),
                        period_data.get('pct_lang_other', 0)
                    ]
                }
                lang_df = pd.DataFrame(lang_data)
                lang_df = lang_df[lang_df['Porcentaje'] > 0]
                
                if not lang_df.empty:
                    fig_lang = px.pie(lang_df, values='Porcentaje', names='Idioma',
                                     title='Distribución por Idiomas',
                                     color_discrete_sequence=px.colors.qualitative.Pastel)
                    st.plotly_chart(fig_lang, use_container_width=True)
                else:
                    st.info("Sin datos de idioma.")
            
            # Journal indexing
            st.markdown("#### Indexación de Revistas")
            col1, col2, col3 = st.columns(3)
            col1.metric("% Scopus", f"{period_data.get('pct_scopus', 0):.1f}%")
            col2.metric("% CORE", f"{period_data.get('pct_core', 0):.1f}%")
            col3.metric("% DOAJ", f"{period_data.get('pct_doaj', 0):.1f}%")

            # --- Sunburst de Temáticas (Regional Level) ---
            if os.path.exists(COUNTRIES_TOPICS_FILE):
                try:
                    topics_latam = pd.read_parquet(COUNTRIES_TOPICS_FILE)
                    if not topics_latam.empty:
                        st.markdown("---")
                        st.subheader("Temáticas de Investigación a Nivel Regional (Sunburst)")
                        # Agrupar todo LATAM a 3 niveles: domain -> field -> subfield
                        topics_agg = topics_latam.groupby(['domain', 'field', 'subfield'], as_index=False)['count'].sum()
                        topics_agg = topics_agg[topics_agg['count'] > 0]
                        
                        if not topics_agg.empty:
                            fig_sun_latam = px.sunburst(
                                topics_agg,
                                path=['domain', 'field', 'subfield'],
                                values='count',
                                color='domain',
                                color_discrete_sequence=px.colors.qualitative.Prism
                            )
                            fig_sun_latam.update_layout(margin=dict(t=10, l=0, r=0, b=10), height=500)
                            st.plotly_chart(fig_sun_latam, use_container_width=True)
                            
                        with st.expander("📊 Perfiles Temáticos"):
                            tab_dom, tab_field, tab_sub = st.tabs(["Dominio", "Campo", "Subcampo"])
                            
                            with tab_dom:
                                df_dom = create_profile_table(topics_latam, 'domain', 'country_code', 'País', 'Total Región LATAM')
                                st.dataframe(df_dom, use_container_width=True, hide_index=True)
                                
                            with tab_field:
                                df_field = create_profile_table(topics_latam, 'field', 'country_code', 'País', 'Total Región LATAM')
                                st.dataframe(df_field, use_container_width=True, hide_index=True)
                                
                            with tab_sub:
                                df_sub = create_profile_table(topics_latam, 'subfield', 'country_code', 'País', 'Total Región LATAM')
                                st.dataframe(df_sub, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.warning(f"No se pudieron cargar los temas regionales: {e}")
        
        if latam_annual is not None and len(latam_annual) > 0:
            st.markdown("---")
            st.markdown("### Tendencias Anuales")
            
            # Filter for recent years (last 30 years)
            recent_years = latam_annual[latam_annual['year'] >= latam_annual['year'].max() - 30].copy()
            
            # Documents over time
            fig_docs = px.line(recent_years, x='year', y='num_documents',
                              title='Evolución de Documentos Publicados',
                              labels={'year': 'Año', 'num_documents': 'Número de Documentos'},
                              markers=True)
            st.plotly_chart(fig_docs, use_container_width=True)
            
            # FWCI over time
            fig_fwci = px.line(recent_years, x='year', y='fwci_avg',
                              title='Evolución del FWCI Promedio',
                              labels={'year': 'Año', 'fwci_avg': 'FWCI Promedio'},
                              markers=True)
            fig_fwci.add_hline(y=1.0, line_dash="dash", line_color="red",
                              annotation_text="Promedio Mundial (1.0)")
            st.plotly_chart(fig_fwci, use_container_width=True)
            
            # Top percentages over time
            fig_top = go.Figure()
            fig_top.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_top_10'],
                                        mode='lines+markers', name='Top 10%'))
            fig_top.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_top_1'],
                                        mode='lines+markers', name='Top 1%'))
            fig_top.update_layout(title='Evolución de Artículos Altamente Citados',
                                 xaxis_title='Año',
                                 yaxis_title='Porcentaje (%)')
            st.plotly_chart(fig_top, use_container_width=True)
            
            # OA trends
            fig_oa_trend = go.Figure()
            fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_gold'],
                                             mode='lines+markers', name='Gold'))
            fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_green'],
                                             mode='lines+markers', name='Green'))
            fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_hybrid'],
                                             mode='lines+markers', name='Hybrid'))
            fig_oa_trend.update_layout(title='Evolución de Tipos de Acceso Abierto',
                                      xaxis_title='Año',
                                      yaxis_title='Porcentaje (%)')
            st.plotly_chart(fig_oa_trend, use_container_width=True)
            
            # Show annual data table
            with st.expander("📊 Ver Tabla de Datos Anuales"):
                cols_metrics_l = [
                    'year', 'num_documents', 'fwci_avg', 
                    'pct_oa_total', 'pct_oa_diamond', 'pct_oa_gold', 
                    'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed',
                    'avg_percentile', 'pct_top_10', 'pct_top_1',
                    'pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 
                    'pct_lang_fr', 'pct_lang_de', 'pct_lang_it'
                ]
                
                # Default OA Total calculation
                if 'pct_oa_total' not in recent_years.columns:
                    recent_years['pct_oa_total'] = recent_years.get('pct_oa_gold', 0) + recent_years.get('pct_oa_green', 0) + recent_years.get('pct_oa_hybrid', 0) + recent_years.get('pct_oa_bronze', 0)
                
                cols_metrics_l = [c for c in cols_metrics_l if c in recent_years.columns]
                
                cols_map_l = {
                    'year': 'Año',
                    'num_documents': 'Documentos',
                    'fwci_avg': 'FWCI',
                    'pct_oa_total': '% OA Total',
                    'pct_oa_diamond': '% OA Diamante',
                    'pct_oa_gold': '% OA Gold',
                    'pct_oa_green': '% OA Verde',
                    'pct_oa_hybrid': '% OA Híbrido',
                    'pct_oa_bronze': '% OA Bronce',
                    'pct_oa_closed': '% Cerrado',
                    'avg_percentile': 'Percentil Prom.',
                    'pct_top_10': '% Top 10',
                    'pct_top_1': '% Top 1',
                    'pct_lang_es': '% Español',
                    'pct_lang_en': '% Inglés',
                    'pct_lang_pt': '% Portugués',
                    'pct_lang_fr': '% Francés',
                    'pct_lang_de': '% Alemán',
                    'pct_lang_it': '% Italiano'
                }
                
                desired_order_l = ['Año', 'Documentos', 'FWCI', 
                                 '% OA Total', '% OA Diamante', '% OA Gold', 
                                 '% OA Verde', '% OA Híbrido', '% OA Bronce', '% Cerrado',
                                 '% Español', '% Inglés', '% Portugués', '% Francés', '% Alemán', '% Italiano',
                                 'Percentil Prom.', '% Top 10', '% Top 1']
                
                df_display_l = recent_years[cols_metrics_l].copy().sort_values('year', ascending=False)
                df_display_l = df_display_l.rename(columns=cols_map_l)
                final_cols_l = [c for c in desired_order_l if c in df_display_l.columns]
                
                st.dataframe(df_display_l[final_cols_l], use_container_width=True, hide_index=True)

        # Tablas de Países
        st.markdown("---")
        st.subheader("Comparativa por País")
        
        tab_countries_1, tab_countries_2 = st.tabs(["Periodo Completo", "Periodo Reciente (2021-2025)"])
        
        # Columns to display
        cols_display = [
            'country_code', 'num_journals', 'num_documents', 'fwci_avg', 'avg_percentile', 
            'pct_top_10', 'pct_top_1', 'pct_oa_gold', 'pct_oa_diamond', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed',
            'pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 'pct_lang_fr', 'pct_lang_de', 'pct_lang_it'
        ]
        
        with tab_countries_1:
            st.markdown("**Indicadores por País (Periodo Completo)**")
            country_period = load_and_scale('country', 'period')
            if country_period is not None and not country_period.empty:
                # Sort by num_documents
                display_df = country_period.sort_values('num_documents', ascending=False)
                # Filter columns that exist
                # Filter columns that exist
                valid_cols = [c for c in cols_display if c in display_df.columns]
                display_df = display_df[valid_cols].copy()
                
                # Add Country Names
                display_df.insert(1, 'country_name', display_df['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x)))
                
                final_df = display_df.rename(columns={
                    'country_name': 'País', 'country_code': 'Código', 'avg_percentile': 'Percentil Prom. Norm.',
                    'pct_lang_es': '% Español', 'pct_lang_en': '% Inglés', 'pct_lang_pt': '% Portugués', 
                    'pct_lang_fr': '% Francés', 'pct_lang_de': '% Alemán', 'pct_lang_it': '% Italiano'
                })
                cols_final_order = ['Código', 'País'] + [c for c in final_df.columns if c not in ['Código', 'País']]
                
                st.dataframe(final_df[cols_final_order], use_container_width=True, hide_index=True)
            else:
                st.info("No hay datos de países disponibles.")
                
        with tab_countries_2:
            st.markdown("**Indicadores por País (2021-2025)**")
            country_period_recent = load_and_scale('country', 'period_2021_2025')
            if country_period_recent is not None and not country_period_recent.empty:
                # Sort by num_documents
                display_df_recent = country_period_recent.sort_values('num_documents', ascending=False)
                # Filter columns
                # Filter columns
                valid_cols = [c for c in cols_display if c in display_df_recent.columns]
                display_df_recent = display_df_recent[valid_cols].copy()
                
                # Add Country Names
                display_df_recent.insert(1, 'country_name', display_df_recent['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x)))
                
                final_df_recent = display_df_recent.rename(columns={
                    'country_name': 'País', 'country_code': 'Código', 'avg_percentile': 'Percentil Prom. Norm.',
                    'pct_lang_es': '% Español', 'pct_lang_en': '% Inglés', 'pct_lang_pt': '% Portugués', 
                    'pct_lang_fr': '% Francés', 'pct_lang_de': '% Alemán', 'pct_lang_it': '% Italiano'
                })
                cols_final_order = ['Código', 'País'] + [c for c in final_df_recent.columns if c not in ['Código', 'País']]
                
                st.dataframe(final_df_recent[cols_final_order], use_container_width=True, hide_index=True)
            else:
                st.info("No hay datos recientes de países disponibles (es necesario ejecutar pre-cálculo v2).")

        # --- TRAYECTORIAS GLOBALES (RESTAURADO) ---
        if os.path.exists(MAP_COUNTRIES_FILE):
            st.markdown("---")
            st.subheader("Trayectorias de Desempeño Latam (Global)")
            st.caption("Evolución comparativa de todos los países y Latinoamérica (2000-2025) en el espacio UMAP.")
            
            try:
                coords_df = pd.read_parquet(MAP_COUNTRIES_FILE)
                # Filter years: 2000 to 2025
                mask = (coords_df['year'] >= 2000) & (coords_df['year'] <= 2025)
                df_traj_global = coords_df[mask].copy()
                
                if not df_traj_global.empty:
                    fig_traj_global = go.Figure()
                    
                    # Get unique entities sorted so LATAM is last and plotted on top
                    entities = sorted(df_traj_global['id'].unique())
                    if 'LATAM' in entities:
                        entities.remove('LATAM')
                        entities.append('LATAM')
                    
                    # Countries to hide by default (outliers or small scale)
                    excluded_countries = ['BB', 'BS', 'BZ', 'DO', 'EC', 'GY', 'HN', 'NI', 'PA', 'PE', 'PY']
                    
                    for entity_id in entities:
                        entity_data = df_traj_global[df_traj_global['id'] == entity_id].sort_values('year')
                        if entity_data.empty: continue
                        
                        visible_status = True
                        if entity_id in excluded_countries:
                            visible_status = 'legendonly'
                        
                        # Get Name
                        entity_name = COUNTRY_NAMES.get(entity_id, entity_id)
                        
                        if entity_id == 'LATAM':
                            line_color = '#2ca02c' # Green
                            line_width = 5
                            marker_size = 6
                            opacity = 1.0
                            name = 'Iberoamérica (Ref.)'
                            visible_status = True # Always visible
                        else:
                            line_color = None # Auto
                            line_width = 2
                            marker_size = 4
                            opacity = 0.6
                            name = entity_name
                        
                        fig_traj_global.add_trace(go.Scatter(
                            x=entity_data['x'], 
                            y=entity_data['y'],
                            mode='lines+markers',
                            name=name,
                            text=entity_data['year'],
                            hovertemplate=f"<b>{name}</b><br>Año: %{{text}}<br>X: %{{x:.2f}}<br>Y: %{{y:.2f}}",
                            line=dict(width=line_width, color=line_color),
                            marker=dict(size=marker_size, color=line_color),
                            opacity=opacity,
                            visible=visible_status
                        ))
                    
                    fig_traj_global.update_layout(
                        title="Evolución de Trayectorias (2000-2025)",
                        xaxis_title="Dimensión 1",
                        yaxis_title="Dimensión 2",
                        template="plotly_white",
                        height=600,
                        hovermode="closest",
                        legend=dict(itemclick="toggleothers", itemdoubleclick="toggle")
                    )
                    
                    st.plotly_chart(fig_traj_global, use_container_width=True)
                    
                    st.info("💡 Cada línea representa la evolución del perfil bibliométrico a lo largo del tiempo. La línea verde gruesa representa a Iberoamérica como conjunto de referencia.")
                    
                    with st.expander("📊 Ver datos de trayectorias (Global)"):
                        # Attempt to load original metrics to show instead of just coordinates
                        try:
                            # Load annual data
                            country_annual = load_cached_metrics('country', 'annual')
                            latam_annual = load_cached_metrics('latam', 'annual')
                            
                            metrics_data = []
                            
                            # Prepare Country Data
                            if country_annual is not None and not country_annual.empty:
                                c_df = country_annual.copy()
                                # Ensure we have the ID column for merging
                                if 'country_code' in c_df.columns:
                                    c_df['id'] = c_df['country_code']
                                metrics_data.append(c_df)
                                
                            # Prepare LATAM Data
                            if latam_annual is not None and not latam_annual.empty:
                                l_df = latam_annual.copy()
                                l_df['id'] = 'LATAM'
                                metrics_data.append(l_df)
                            
                            if metrics_data:
                                all_metrics = pd.concat(metrics_data, ignore_index=True)
                                
                                # Merge with trajectory coordinates (left join to keep valid coords)
                                # df_traj_global has 'id' and 'year'
                                merged_df = pd.merge(
                                    df_traj_global, 
                                    all_metrics, 
                                    on=['id', 'year'], 
                                    how='left',
                                    suffixes=('', '_orig')
                                )
                                
                                # Add Full Name
                                merged_df['name'] = merged_df['id'].map(lambda x: COUNTRY_NAMES.get(x, x))
                                
                                # Select columns to display
                                cols_map = {
                                    'id': 'Código',
                                    'name': 'País/Región',
                                    'year': 'Año',
                                    'num_documents': 'Documentos',
                                    'fwci_avg': 'FWCI',
                                    'pct_oa_diamond': '% OA Diamante',
                                    'pct_oa_gold': '% OA Gold',
                                    'pct_top_10': '% Top 10',
                                    'avg_percentile': 'Percentil Prom.',
                                    'x': 'Coord. UMAP X',
                                    'y': 'Coord. UMAP Y'
                                }
                                
                                # Get available cols
                                available_cols = [c for c in cols_map.keys() if c in merged_df.columns]
                                
                                # Rename
                                final_view = merged_df[available_cols].rename(columns=cols_map)
                                
                                # Reorder
                                desired_order = ['Código', 'País/Región', 'Año', 'Documentos', 'FWCI', '% OA Diamante', '% OA Gold', '% Top 10', 'Percentil Prom.', 'Coord. UMAP X', 'Coord. UMAP Y']
                                final_order = [c for c in desired_order if c in final_view.columns]
                                
                                st.dataframe(final_view[final_order], use_container_width=True, hide_index=True)
                            else:
                                if 'name' not in df_traj_global.columns:
                                    df_traj_global['name'] = df_traj_global['id'].map(lambda x: COUNTRY_NAMES.get(x, x))
                                st.dataframe(df_traj_global, use_container_width=True)
                                
                        except Exception as e:
                            st.warning(f"No se pudieron enriquecer los datos: {e}")
                            if 'name' not in df_traj_global.columns:
                                df_traj_global['name'] = df_traj_global['id'].map(lambda x: COUNTRY_NAMES.get(x, x))
                            st.dataframe(df_traj_global, use_container_width=True)
                else:
                    st.warning("⚠️ No hay datos de trayectorias para el periodo 2000-2025.")
            except Exception as e:
                st.error(f"❌ Error visualizando trayectorias globales: {e}")

        # --- Radar Analysis ---
        st.markdown("---")
        if st.checkbox("Mostrar Análisis de Radar (Todos los Países)"):
            st.subheader("Perfiles de Desempeño Relativo (Radar)")
            st.info("Nota: Los valores están normalizados (0-1) respecto al máximo regional de cada indicador para permitir la comparación geométrica del perfil.")
            
            if country_period is not None and not country_period.empty:
                # Indicators
                radar_vars = ['fwci_avg', 'avg_percentile', 'pct_top_10', 'pct_top_1', 'pct_oa_diamond']
                radar_labels = ['FWCI', 'Percentil Norm.', 'Top 10%', 'Top 1%', 'OA Diamante']
                
                # Normalize Function
                def normalize_minmax(df, columns):
                    df_norm = df.copy()
                    for col in columns:
                        if col in df.columns:
                            max_val = df[col].max()
                            if max_val > 0:
                                df_norm[col] = df[col] / max_val
                            else:
                                df_norm[col] = 0
                    return df_norm

                # Prepare Normalized DFs
                df_full_norm = normalize_minmax(country_period, radar_vars)
                
                df_recent_norm = None
                if country_period_recent is not None and not country_period_recent.empty:
                    df_recent_norm = normalize_minmax(country_period_recent, radar_vars)
                
                # Grid Layout
                countries_list = sorted(country_period['country_code'].unique())
                
                # Use columns
                cols = st.columns(3)
                
                for i, country in enumerate(countries_list):
                    with cols[i % 3]:
                        fig = go.Figure()
                        
                        # Full Period Trace
                        row_full = df_full_norm[df_full_norm['country_code'] == country]
                        labels_closed = radar_labels + [radar_labels[0]]
                        
                        if not row_full.empty:
                            values_full = [row_full[v].iloc[0] if v in row_full else 0 for v in radar_vars]
                            values_full += [values_full[0]]
                            
                            fig.add_trace(go.Scatterpolar(
                                r=values_full,
                                theta=labels_closed,
                                fill='toself',
                                name='Periodo Completo',
                                line_color='blue',
                                opacity=0.4
                            ))
                        
                        # Recent Period Trace
                        if df_recent_norm is not None:
                            row_recent = df_recent_norm[df_recent_norm['country_code'] == country]
                            if not row_recent.empty:
                                values_recent = [row_recent[v].iloc[0] if v in row_recent else 0 for v in radar_vars]
                                values_recent += [values_recent[0]]
                                
                                fig.add_trace(go.Scatterpolar(
                                    r=values_recent,
                                    theta=labels_closed,
                                    fill='toself',
                                    name='2021-2025',
                                    line_color='red',
                                    opacity=0.4
                                ))
                        
                        fig.update_layout(
                            polar=dict(
                                radialaxis=dict(visible=True, range=[0, 1.05], showticklabels=False),
                            ),
                            margin=dict(t=30, b=20, l=30, r=30),
                            height=250,
                            title=dict(text=country, y=0.95),
                            showlegend=False
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                st.caption("🔵 Azul: Periodo Completo | 🔴 Rojo: Periodo Reciente (2021-2025)")
        
        # UMAP Visualization for Countries
        st.markdown("---")
        st.subheader("Mapa de Similitud entre Países (UMAP)")
        st.caption("Visualización 2D basada en: Revistas, OA Diamante, FWCI, % Top 10%, % Top 1%, Percentil Promedio (2021-2025)")
        
        umap_countries_file = os.path.join(BASE_PATH, 'data', 'umap', 'umap_countries_recent.parquet')
        
        if os.path.exists(umap_countries_file):
            try:
                df_umap_countries = pd.read_parquet(umap_countries_file)
                
                if 'umap_x' in df_umap_countries.columns and 'umap_y' in df_umap_countries.columns:
                    # Create scatter plot
                    fig_umap = px.scatter(
                        df_umap_countries,
                        x='umap_x',
                        y='umap_y',
                        text='country_code',
                        hover_data={
                            'country_code': True,
                            'num_journals': ':,',
                            'fwci_avg': ':.2f',
                            'avg_percentile': ':.1f',
                            'pct_top_10': ':.1f',
                            'pct_top_1': ':.1f',
                            'pct_oa_diamond': ':.1f',
                            'umap_x': False,
                            'umap_y': False
                        },
                        labels={
                            'umap_x': 'UMAP Dimensión 1',
                            'umap_y': 'UMAP Dimensión 2',
                            'country_code': 'País'
                        },
                        title='Países Latinoamericanos - Espacio de Similitud'
                    )
                    
                    fig_umap.update_traces(
                        textposition='top center',
                        marker=dict(size=12, line=dict(width=1, color='white'))
                    )
                    
                    fig_umap.update_layout(
                        height=500,
                        showlegend=False,
                        xaxis=dict(showgrid=True, zeroline=True),
                        yaxis=dict(showgrid=True, zeroline=True)
                    )
                    
                    st.plotly_chart(fig_umap, use_container_width=True)
                    
                    st.info("💡 Los países cercanos en el mapa tienen perfiles bibliométricos similares. La distancia refleja diferencias en producción, impacto y acceso abierto.")
                    
                    with st.expander("📊 Ver tabla de datos UMAP (Países)"):
                        # Add full country name
                        if 'country_name' not in df_umap_countries.columns:
                            df_umap_countries['country_name'] = df_umap_countries['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x))

                        # Select and rename columns for display
                        cols_to_show = {
                            'country_code': 'Código',
                            'country_name': 'País',
                            'num_journals': 'Revistas',
                            'pct_oa_diamond': '% OA Diamante',
                            'fwci_avg': 'FWCI Promedio',
                            'pct_top_10': '% Top 10%',
                            'pct_top_1': '% Top 1%',
                            'avg_percentile': 'Percentil Promedio'
                        }
                        
                        # Reorder columns to have Name first
                        ordered_cols = ['country_code', 'country_name', 'num_journals', 'pct_oa_diamond', 'fwci_avg', 'pct_top_10', 'pct_top_1', 'avg_percentile']
                        final_cols = [c for c in ordered_cols if c in df_umap_countries.columns]
                        
                        st.dataframe(
                            df_umap_countries[final_cols].rename(columns=cols_to_show),
                            use_container_width=True,
                            hide_index=True
                        )
                else:
                    st.warning("⚠️ Archivo UMAP encontrado pero sin coordenadas. Ejecuta el pipeline completo.")
            except Exception as e:
                st.error(f"❌ Error cargando visualización UMAP: {e}")
        else:
            st.info("💡 Ejecuta el pipeline completo (`python run_pipeline.py`) para generar la visualización UMAP.")
        
        # --- MAPA SOM (NUEVO) ---
        som_countries_umatrix = os.path.join(BASE_PATH, 'data', 'som', 'som_countries_umatrix.npy')
        som_countries_bmus = os.path.join(BASE_PATH, 'data', 'som', 'som_countries_bmus.parquet')
        
        if os.path.exists(som_countries_umatrix):
            st.markdown("---")
            st.subheader("Mapa Auto-Organizado (SOM) - Países")
            st.caption("Visualización hexagonal basada en perfiles de similitud. El color de fondo (U-Matrix) y las etiquetas muestran agrupamientos naturales.")
            
            try:
                # Cargar datos
                U = np.load(som_countries_umatrix)
                
                # Check for BMUs
                if os.path.exists(som_countries_bmus):
                    df_bmus = pd.read_parquet(som_countries_bmus)
                else:
                    df_bmus = pd.DataFrame() # Empty if not found
                
                n_rows, n_cols = U.shape
                orientation = "pointy"
                s = 1.0 # Radio del hexágono

                # Mapa de colores invertido (Pinkyl)
                colorscale = px.colors.sequential.Pinkyl[::-1]
                val_flat = U.flatten()
                vmin, vmax = float(np.nanmin(val_flat)), float(np.nanmax(val_flat))

                def get_color(val):
                    if vmax == vmin: return colorscale[0]
                    t = (val - vmin) / (vmax - vmin)
                    idx = int(np.clip(t*(len(colorscale)-1), 0, len(colorscale)-1))
                    return colorscale[idx]

                fig_som = go.Figure()

                xmin, xmax, ymin, ymax = 1e9, -1e9, 1e9, -1e9

                # 1. Dibujar celdas hexagonales
                for r in range(n_rows):
                    for c in range(n_cols):
                        xc, yc = hex_center(r, c, s=s, orientation=orientation)
                        hx, hy = hex_polygon(xc, yc, s=s, orientation=orientation)

                        val = U[r, c]
                        fill_color = get_color(val)
                        
                        fig_som.add_trace(go.Scatter(
                            x=hx, y=hy,
                            mode="lines",
                            fill="toself",
                            line=dict(width=0.5, color="#bbbbbb"),
                            fillcolor=fill_color,
                            hoverinfo="text",
                            text=f"Celda ({r},{c})<br>U-Dist: {val:.3f}",
                            showlegend=False
                        ))
                        
                        xmin = min(xmin, np.min(hx))
                        xmax = max(xmax, np.max(hx))
                        ymin = min(ymin, np.min(hy))
                        ymax = max(ymax, np.max(hy))

                # 2. Dibujar etiquetas de países (BMUs)
                if not df_bmus.empty:
                    labels_by_neuron = {}
                    for idx, row in df_bmus.iterrows():
                        try:
                            if 'bmu_row' in row and 'bmu_col' in row:
                                r, c = int(row['bmu_row']), int(row['bmu_col'])
                                country = row.get('country_code', 'UNK')
                                labels_by_neuron.setdefault((r, c), []).append(country)
                        except:
                            continue

                    text_x = []
                    text_y = []
                    text_labels = []

                    for (r, c), countries in labels_by_neuron.items():
                        xc, yc = hex_center(r, c, s=s, orientation=orientation)
                        label = ", ".join(countries)
                        text_x.append(xc)
                        text_y.append(yc)
                        text_labels.append(label)

                    if text_x:
                        fig_som.add_trace(go.Scatter(
                            x=text_x, 
                            y=text_y,
                            text=text_labels,
                            mode="text",
                            textfont=dict(size=11, color="black", family="Arial Black"),
                            hoverinfo="text",
                            hovertext=[f"Países: {l}" for l in text_labels],
                            showlegend=False
                        ))

                # Ajustar layout
                pad = 1.0
                fig_som.update_layout(
                    title="Mapa Auto-Organizado (SOM 20x15) - Países",
                    xaxis=dict(visible=False, range=[xmin - pad, xmax + pad], scaleanchor="y", scaleratio=1),
                    yaxis=dict(visible=False, range=[ymax + pad, ymin - pad]), # Arriba = Fila 0
                    plot_bgcolor="white",
                    height=700,
                    margin=dict(l=20, r=20, t=50, b=20)
                )

                st.plotly_chart(fig_som, use_container_width=True)
                
                if not df_bmus.empty:
                    with st.expander("📊 Ver detalles de asignación SOM"):
                        # Add Names
                        if 'country_name' not in df_bmus.columns:
                            df_bmus['country_name'] = df_bmus['country_code'].map(lambda x: COUNTRY_NAMES.get(x, x))
                            
                        # Show useful cols
                        cols_show = ['country_code', 'country_name', 'num_journals', 'fwci_avg', 'pct_oa_diamond', 'bmu_row', 'bmu_col']
                        rename_map = {'country_code': 'Código', 'country_name': 'País'}
                        
                        existing = [c for c in cols_show if c in df_bmus.columns]
                        st.dataframe(
                            df_bmus[existing].rename(columns=rename_map).sort_values('País'), 
                            use_container_width=True
                        )

            except Exception as e:
                st.error(f"❌ Error visualizando SOM: {e}")
        
        # Dynamic Scatter Plot for All Journals
        st.markdown("---")
        st.subheader("Explorador de Revistas - Scatter Plot Dinámico")
        st.caption("Visualiza la relación entre diferentes indicadores bibliométricos para todas las revistas latinoamericanas")
        
        # Period selector
        period_option = st.radio(
            "Selecciona el período:",
            options=["Período Reciente (2021-2025)", "Período Completo"],
            index=0,  # Default: Recent period
            horizontal=True
        )
        
        # Load appropriate data based on period selection
        if period_option == "Período Reciente (2021-2025)":
            journal_data = load_and_scale('journal', 'period_2021_2025')
            period_label = "2021-2025"
        else:
            journal_data = load_and_scale('journal', 'period')
            period_label = "Período Completo"
        
        if journal_data is not None and len(journal_data) > 0:
            # Merge with journal metadata to get names and countries
            journals_meta = df[['id', 'display_name', 'country_code']].copy()
            scatter_data = journal_data.merge(
                journals_meta,
                left_on='journal_id',
                right_on='id',
                how='left'
            )
            
            # Map country codes to full names
            country_names = {
                'AR': 'Argentina',
                'BO': 'Bolivia',
                'BR': 'Brasil',
                'CL': 'Chile',
                'CO': 'Colombia',
                'CR': 'Costa Rica',
                'CU': 'Cuba',
                'DO': 'República Dominicana',
                'EC': 'Ecuador',
                'SV': 'El Salvador',
                'GT': 'Guatemala',
                'HN': 'Honduras',
                'MX': 'México',
                'NI': 'Nicaragua',
                'PA': 'Panamá',
                'PY': 'Paraguay',
                'PE': 'Perú',
                'PR': 'Puerto Rico',
                'UY': 'Uruguay',
                'VE': 'Venezuela',
                'ES': 'España',
                'PT': 'Portugal'
            }
            
            # Add country name column
            scatter_data['country_name'] = scatter_data['country_code'].map(country_names).fillna(scatter_data['country_code'])
            
            # Define available indicators
            indicator_options = {
                'Documentos': 'num_documents',
                'FWCI Promedio': 'fwci_avg',
                '% Top 10%': 'pct_top_10',
                '% Top 1%': 'pct_top_1',
                'Percentil Promedio': 'avg_percentile',
                '% OA Total': 'pct_oa_total',
                '% OA Gold': 'pct_oa_gold',
                '% OA Diamond': 'pct_oa_diamond',
                '% OA Green': 'pct_oa_green',
                '% OA Hybrid': 'pct_oa_hybrid',
                '% OA Bronze': 'pct_oa_bronze',
                '% Cerrado': 'pct_oa_closed'
            }
            
            # Filter to only available columns
            available_indicators = {
                k: v for k, v in indicator_options.items() 
                if v in scatter_data.columns
            }
            
            if len(available_indicators) >= 2:
                # Axis selectors in columns
                col_x, col_y = st.columns(2)
                
                with col_x:
                    x_indicator = st.selectbox(
                        "Indicador Eje X:",
                        options=list(available_indicators.keys()),
                        index=0  # Default: Documentos
                    )
                
                with col_y:
                    y_indicator = st.selectbox(
                        "Indicador Eje Y:",
                        options=list(available_indicators.keys()),
                        index=1  # Default: FWCI Promedio
                    )
                
                x_col = available_indicators[x_indicator]
                y_col = available_indicators[y_indicator]
                
                # Filter out rows with missing data for selected indicators
                plot_data = scatter_data[
                    scatter_data[x_col].notna() & 
                    scatter_data[y_col].notna()
                ].copy()
                
                if len(plot_data) > 0:
                    # Create scatter plot
                    fig_scatter = px.scatter(
                        plot_data,
                        x=x_col,
                        y=y_col,
                        color='country_name',
                        hover_data={
                            'display_name': True,
                            'country_name': True,
                            'country_code': False,
                            x_col: ':.2f',
                            y_col: ':.2f',
                            'num_documents': ':,' if 'num_documents' in plot_data.columns else False
                        },
                        labels={
                            x_col: x_indicator,
                            y_col: y_indicator,
                            'country_name': 'País',
                            'display_name': 'Revista'
                        },
                        title=f'{y_indicator} vs {x_indicator} ({period_label})',
                        opacity=0.7
                    )
                    
                    fig_scatter.update_traces(
                        marker=dict(size=8, line=dict(width=0.5, color='white'))
                    )
                    
                    fig_scatter.update_layout(
                        height=600,
                        xaxis=dict(showgrid=True, zeroline=True),
                        yaxis=dict(showgrid=True, zeroline=True),
                        legend=dict(
                            title="País",
                            orientation="v",
                            yanchor="top",
                            y=1,
                            xanchor="left",
                            x=1.02
                        )
                    )
                    
                    st.plotly_chart(fig_scatter, use_container_width=True)
                    
                    # Summary statistics
                    with st.expander("📊 Ver estadísticas descriptivas"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown(f"**{x_indicator}**")
                            st.write(f"Media: {plot_data[x_col].mean():.2f}")
                            st.write(f"Mediana: {plot_data[x_col].median():.2f}")
                            st.write(f"Desv. Est.: {plot_data[x_col].std():.2f}")
                            st.write(f"Min: {plot_data[x_col].min():.2f}")
                            st.write(f"Max: {plot_data[x_col].max():.2f}")
                        
                        with col2:
                            st.markdown(f"**{y_indicator}**")
                            st.write(f"Media: {plot_data[y_col].mean():.2f}")
                            st.write(f"Mediana: {plot_data[y_col].median():.2f}")
                            st.write(f"Desv. Est.: {plot_data[y_col].std():.2f}")
                            st.write(f"Min: {plot_data[y_col].min():.2f}")
                            st.write(f"Max: {plot_data[y_col].max():.2f}")
                        
                        # Correlation
                        correlation = plot_data[x_col].corr(plot_data[y_col])
                        st.markdown(f"**Correlación de Pearson:** {correlation:.3f}")
                else:
                    st.warning("⚠️ No hay datos disponibles para los indicadores seleccionados.")
            else:
                st.warning("⚠️ No hay suficientes indicadores disponibles para el scatter plot.")
        else:
            st.info(f"💡 No hay datos disponibles para {period_label}. Ejecuta el pipeline completo.")
    
        st.markdown("---")
        st.subheader("Indicadores Anuales (Todos los Países y LATAM)")
        st.caption("Detalle histórico de la evolución bibliométrica por país y año. Incluye datos crudos y suavizados (medias móviles).")
        
        tab_raw, tab_w3, tab_w5 = st.tabs(["📊 Datos Crudos", "🌊 Suavizado (w=3)", "🌌 Suavizado (w=5)"])
        
        try:
            # Load annual data
            country_annual_all = load_cached_metrics('country', 'annual')
            latam_annual_all = load_cached_metrics('latam', 'annual')
            
            # Combine Data
            df_list = []
            if country_annual_all is not None:
                df_list.append(country_annual_all)
            if latam_annual_all is not None:
                latam_annual_all = latam_annual_all.copy()
                latam_annual_all['country_code'] = 'Lat'
                df_list.append(latam_annual_all)
            
            if df_list:
                df_full = pd.concat(df_list, ignore_index=True)
                
                # Metrics columns to smooth
                cols_metrics = [
                    'num_journals', 'num_documents', 'fwci_avg', 
                    'pct_oa_total', 'pct_oa_diamond', 'pct_oa_gold', 
                    'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed',
                    'avg_percentile', 'pct_top_10', 'pct_top_1',
                    'pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 
                    'pct_lang_fr', 'pct_lang_de', 'pct_lang_it'
                ]
                # Filter useful columns only
                cols_metrics = [c for c in cols_metrics if c in df_full.columns]
                
                # Function to prepare and display
                def show_table(df_input, window=None):
                    df_work = df_input.copy()
                    
                    if window:
                        # Sort for rolling: Country, Year Asc
                        df_work = df_work.sort_values(['country_code', 'year'], ascending=[True, True])
                        # Apply rolling
                        # Group by country ensures we don't mix data between countries
                        # min_periods=1 allows computing partial means at edges
                        df_work[cols_metrics] = df_work.groupby('country_code')[cols_metrics].rolling(window=window, min_periods=1).mean().reset_index(0, drop=True)
                    
                    # Create ID_Year
                    df_work['id_year'] = df_work['country_code'] + '_' + df_work['year'].astype(str)
                    
                    # Final Sort: Country, Year Desc
                    df_work = df_work.sort_values(['country_code', 'year'], ascending=[True, False])
                    
                    # Formatting Columns
                    cols_map = {
                        'id_year': 'Código_Año',
                        'num_journals': 'Revistas',
                        'num_documents': 'Documentos',
                        'fwci_avg': 'FWCI',
                        'pct_oa_total': '% OA Total',
                        'pct_oa_diamond': '% OA Diamante',
                        'pct_oa_gold': '% OA Gold',
                        'pct_oa_green': '% OA Verde',
                        'pct_oa_hybrid': '% OA Híbrido',
                        'pct_oa_bronze': '% OA Bronce',
                        'pct_oa_closed': '% Cerrado',
                        'avg_percentile': 'Percentil Prom.',
                        'pct_top_10': '% Top 10',
                        'pct_top_1': '% Top 1',
                        'pct_lang_es': '% Español',
                        'pct_lang_en': '% Inglés',
                        'pct_lang_pt': '% Portugués',
                        'pct_lang_fr': '% Francés',
                        'pct_lang_de': '% Alemán',
                        'pct_lang_it': '% Italiano'
                    }
                    
                    desired_order = ['Código_Año', 'Revistas', 'Documentos', 'FWCI', 
                                     '% OA Total', '% OA Diamante', '% OA Gold', 
                                     '% OA Verde', '% OA Híbrido', '% OA Bronce', '% Cerrado',
                                     '% Español', '% Inglés', '% Portugués', '% Francés', '% Alemán', '% Italiano',
                                     'Percentil Prom.', '% Top 10', '% Top 1']
                    
                    final_cols = [c for c in desired_order if c in cols_map.values()]
                    
                    # Rename
                    df_display = df_work.rename(columns=cols_map)
                    
                    # Filter existing in display
                    existing_final_cols = [c for c in final_cols if c in df_display.columns]
                    
                    st.dataframe(df_display[existing_final_cols], use_container_width=True, hide_index=True)

                with tab_raw:
                    show_table(df_full, window=None)
                
                with tab_w3:
                    st.info("💡 Media móvil con ventana de 3 años, centrada en el año final (t, t-1, t-2).")
                    show_table(df_full, window=3)
                    
                with tab_w5:
                    st.info("💡 Media móvil con ventana de 5 años, centrada en el año final (t, t-1, ..., t-4).")
                    show_table(df_full, window=5)

            else:
                st.info("No hay datos anuales disponibles.")
                
        except Exception as e:
            st.error(f"Error cargando tabla anual: {e}")
        

    else:
        st.info("💡 Ejecuta 'Precalcular Indicadores' para ver métricas de desempeño detalladas.")

elif level == "País":
    st.header("Análisis por País")
    
    countries = sorted(df['country_code'].unique())
    
    # Initialize session state for country selection
    if 'selected_country' not in st.session_state:
        # Set MX as default if available, otherwise first country
        st.session_state.selected_country = 'MX' if 'MX' in countries else countries[0]
    
    # Get index for default value
    try:
        default_idx = countries.index(st.session_state.selected_country)
    except ValueError:
        default_idx = 0
        st.session_state.selected_country = countries[0]
    
    selected_country = st.selectbox(
        "Selecciona un País", 
        countries,
        index=default_idx,
        key="country_selector"
    )
    
    # Update session state
    st.session_state.selected_country = selected_country
    
    # Filter data
    country_df = df[df['country_code'] == selected_country]
    
    # Basic KPIs
    col1, col2 = st.columns(2)
    col1.metric("Revistas", len(country_df))
    col2.metric("Artículos", f"{country_df['works_count'].sum():,}")
    
    # Removed Top Journals Table as per request
    
    if has_cached_metrics:
        # Load country metrics
        country_annual = load_and_scale('country', 'annual')
        country_annual = load_and_scale('country', 'annual')
        country_period = load_and_scale('country', 'period')
        country_period_recent = load_and_scale('country', 'period_2021_2025')
        
        if country_period is not None:
            country_data = country_period[country_period['country_code'] == selected_country]
            
            if len(country_data) > 0:
                st.markdown("---")
                st.subheader("Indicadores de Desempeño del País")
                
                period_data = country_data.iloc[0]
                
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("Documentos", f"{period_data.get('num_documents', 0):,}")
                col2.metric("FWCI Promedio", f"{period_data.get('fwci_avg', 0):.2f}")
                col3.metric("% Top 10%", f"{period_data.get('pct_top_10', 0):.1f}%")
                col4.metric("% Top 1%", f"{period_data.get('pct_top_1', 0):.1f}%")
                col5.metric("Percentil Prom. Norm.", f"{period_data.get('avg_percentile', 0):.1f}")

                # Recent Period
                if country_period_recent is not None:
                    country_rec_data = country_period_recent[country_period_recent['country_code'] == selected_country]
                    if len(country_rec_data) > 0:
                        rec_data = country_rec_data.iloc[0]
                        st.markdown(f"### Periodo Reciente: 2021-2025")
                        c1, c2, c3, c4, c5 = st.columns(5)
                        c1.metric("Documentos", f"{rec_data.get('num_documents', 0):,}")
                        c2.metric("FWCI Promedio", f"{rec_data.get('fwci_avg', 0):.2f}")
                        c3.metric("% Top 10%", f"{rec_data.get('pct_top_10', 0):.1f}%")
                        c4.metric("% Top 1%", f"{rec_data.get('pct_top_1', 0):.1f}%")
                        c5.metric("Percentil Prom. Norm.", f"{rec_data.get('avg_percentile', 0):.1f}")
                
                # --- TRAYECTORIA DE DESEMPEÑO (UMAP PAÍS - MAPA GLOBAL) ---
                if os.path.exists(MAP_COUNTRIES_FILE):
                    st.markdown("---")
                    st.subheader("Trayectoria de Desempeño (Perfil Multidimensional)")
                    st.markdown("Proyección UMAP Global de la evolución del desempeño comparativo (2000-2025).")
                    try:
                        coords_df = pd.read_parquet(MAP_COUNTRIES_FILE)
                        # Filter: Selected Country and LATAM, Year 2000-2025
                        mask = ((coords_df['id'] == selected_country) | (coords_df['id'] == 'LATAM')) & (coords_df['year'] >= 2000) & (coords_df['year'] <= 2025)
                        subset_df = coords_df[mask].copy()
                        
                        if not subset_df.empty:
                            fig_traj = go.Figure()
                            
                            # Colors/Names Mapping
                            colors = {selected_country: '#1f77b4', 'LATAM': '#2ca02c'}
                            names = {selected_country: f'País: {selected_country}', 'LATAM': 'Iberoamérica (Ref.)'}
                            
                            for entity_id in subset_df['id'].unique():
                                entity_data = subset_df[subset_df['id'] == entity_id].sort_values('year')
                                if entity_data.empty: continue
                                
                                color = colors.get(entity_id, '#7f7f7f') 
                                name = names.get(entity_id, entity_id)
                                
                                fig_traj.add_trace(go.Scatter(
                                    x=entity_data['x'], 
                                    y=entity_data['y'],
                                    mode='lines+markers+text',
                                    name=name,
                                    text=entity_data['year'].astype(str).str[-2:], 
                                    textposition="top center",
                                    line=dict(shape='spline', width=3, color=color), 
                                    marker=dict(size=6, color=color)
                                ))
                            
                            fig_traj.update_layout(
                                title="Evolución de la Trayectoria (UMAP)",
                                xaxis_title="Dimensión 1",
                                yaxis_title="Dimensión 2",
                                template="plotly_white",
                                hovermode="closest",
                                height=550,
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                            )
                            st.plotly_chart(fig_traj, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error visualizando trayectoria: {e}")
                # Historic indicators moved to Annual Trends section
                
                # UMAP Visualization for Journals in this Country
                st.markdown("---")
                st.subheader("Mapa de Similitud entre Revistas (UMAP)")
                st.caption("Visualización 2D basada en: Documentos, OA Diamante, FWCI, % Top 10%, % Top 1%, Percentil Promedio (2021-2025)")
                
                umap_journals_file = os.path.join(BASE_PATH, 'data', 'umap', 'umap_journals_recent.parquet')
                
                if os.path.exists(umap_journals_file):
                    try:
                        df_umap_journals = pd.read_parquet(umap_journals_file)
                        
                        # Filter for selected country
                        df_country_journals = df_umap_journals[df_umap_journals['country_code'] == selected_country]
                        
                        if len(df_country_journals) >= 3 and 'umap_x' in df_country_journals.columns:
                            # Create scatter plot
                            fig_umap_j = px.scatter(
                                df_country_journals,
                                x='umap_x',
                                y='umap_y',
                               #text='display_name',
                                hover_data={
                                    'display_name': True,
                                    'num_documents': ':,',
                                    'fwci_avg': ':.2f',
                                    'avg_percentile': ':.1f',
                                    'pct_top_10': ':.1f',
                                    'pct_top_1': ':.1f',
                                    'pct_oa_diamond': ':.1f',
                                    'umap_x': False,
                                    'umap_y': False
                                },
                                labels={
                                    'umap_x': 'UMAP Dimensión 1',
                                    'umap_y': 'UMAP Dimensión 2',
                                    'display_name': 'Revista',
                                    'pct_oa_diamond': '% OA Diamante'
                                },
                                title=f'Revistas de {selected_country} - Espacio de Similitud'
                            )
                            
                            fig_umap_j.update_traces(
                                textposition='top center',
                                textfont=dict(size=8),
                                marker=dict(size=10, line=dict(width=1, color='white'))
                            )
                            
                            fig_umap_j.update_layout(
                                height=500,
                                showlegend=False,
                                xaxis=dict(showgrid=True, zeroline=True),
                                yaxis=dict(showgrid=True, zeroline=True)
                            )
                            
                            st.plotly_chart(fig_umap_j, use_container_width=True)
                            
                            st.info("💡 Las revistas cercanas en el mapa tienen perfiles bibliométricos similares. La distancia refleja diferencias en producción, impacto y acceso abierto.")
                            
                            with st.expander("📊 Ver tabla de datos UMAP (Revistas)"):
                                # Select and rename columns for display
                                cols_to_show_j = {
                                    'display_name': 'Revista',
                                    'num_documents': 'Documentos',
                                    'pct_oa_diamond': '% OA Diamante',
                                    'fwci_avg': 'FWCI Promedio',
                                    'pct_top_10': '% Top 10%',
                                    'pct_top_1': '% Top 1%',
                                    'avg_percentile': 'Percentil Promedio'
                                }
                                # Filter only existing columns
                                available_cols_j = [c for c in cols_to_show_j.keys() if c in df_country_journals.columns]
                                st.dataframe(
                                    df_country_journals[available_cols_j].rename(columns=cols_to_show_j),
                                    use_container_width=True,
                                    hide_index=True
                                )
                        elif len(df_country_journals) < 3:
                            st.warning(f"⚠️ {selected_country} tiene menos de 3 revistas con datos. Se necesitan al menos 3 para UMAP.")
                        else:
                            st.warning("⚠️ Archivo UMAP encontrado pero sin coordenadas para este país.")
                    except Exception as e:
                        st.error(f"❌ Error cargando visualización UMAP: {e}")
                else:
                    st.info("💡 Ejecuta el pipeline completo (`python run_pipeline.py`) para generar la visualización UMAP.")
                
                # Dynamic Scatter Plot for Journals in this Country
                st.markdown("---")
                st.subheader("Explorador de Revistas - Scatter Plot Dinámico")
                st.caption(f"Visualiza la relación entre diferentes indicadores bibliométricos para las revistas de {selected_country}")
                
                # Period selector
                period_option_country = st.radio(
                    "Selecciona el período:",
                    options=["Período Reciente (2021-2025)", "Período Completo"],
                    index=0,  # Default: Recent period
                    horizontal=True,
                    key="country_period_selector"  # Unique key to avoid conflicts
                )
                
                # Load appropriate data based on period selection
                if period_option_country == "Período Reciente (2021-2025)":
                    journal_data_country = load_and_scale('journal', 'period_2021_2025')
                    period_label_country = "2021-2025"
                else:
                    journal_data_country = load_and_scale('journal', 'period')
                    period_label_country = "Período Completo"
                
                if journal_data_country is not None and len(journal_data_country) > 0:
                    # Merge with journal metadata
                    journals_meta_country = df[['id', 'display_name', 'country_code']].copy()
                    scatter_data_country = journal_data_country.merge(
                        journals_meta_country,
                        left_on='journal_id',
                        right_on='id',
                        how='left'
                    )
                    
                    # Filter for selected country
                    scatter_data_country = scatter_data_country[
                        scatter_data_country['country_code'] == selected_country
                    ].copy()
                    
                    if len(scatter_data_country) >= 3:
                        # Define available indicators
                        indicator_options_country = {
                            'Documentos': 'num_documents',
                            'FWCI Promedio': 'fwci_avg',
                            '% Top 10%': 'pct_top_10',
                            '% Top 1%': 'pct_top_1',
                            'Percentil Promedio': 'avg_percentile',
                            '% OA Total': 'pct_oa_total',
                            '% OA Gold': 'pct_oa_gold',
                            '% OA Diamond': 'pct_oa_diamond',
                            '% OA Green': 'pct_oa_green',
                            '% OA Hybrid': 'pct_oa_hybrid',
                            '% OA Bronze': 'pct_oa_bronze',
                            '% Cerrado': 'pct_oa_closed'
                        }
                        
                        # Filter to only available columns
                        available_indicators_country = {
                            k: v for k, v in indicator_options_country.items() 
                            if v in scatter_data_country.columns
                        }
                        
                        if len(available_indicators_country) >= 2:
                            # Axis selectors in columns
                            col_x_c, col_y_c = st.columns(2)
                            
                            with col_x_c:
                                x_indicator_c = st.selectbox(
                                    "Indicador Eje X:",
                                    options=list(available_indicators_country.keys()),
                                    index=0,  # Default: Documentos
                                    key="country_x_indicator"
                                )
                            
                            with col_y_c:
                                y_indicator_c = st.selectbox(
                                    "Indicador Eje Y:",
                                    options=list(available_indicators_country.keys()),
                                    index=1,  # Default: FWCI Promedio
                                    key="country_y_indicator"
                                )
                            
                            x_col_c = available_indicators_country[x_indicator_c]
                            y_col_c = available_indicators_country[y_indicator_c]
                            
                            # Filter out rows with missing data
                            plot_data_country = scatter_data_country[
                                scatter_data_country[x_col_c].notna() & 
                                scatter_data_country[y_col_c].notna()
                            ].copy()
                            
                            if len(plot_data_country) > 0:
                                # Create scatter plot
                                fig_scatter_c = px.scatter(
                                    plot_data_country,
                                    x=x_col_c,
                                    y=y_col_c,
                                    hover_data={
                                        'display_name': True,
                                        x_col_c: ':.2f',
                                        y_col_c: ':.2f',
                                        'num_documents': ':,' if 'num_documents' in plot_data_country.columns else False
                                    },
                                    labels={
                                        x_col_c: x_indicator_c,
                                        y_col_c: y_indicator_c,
                                        'display_name': 'Revista'
                                    },
                                    title=f'{y_indicator_c} vs {x_indicator_c} - {selected_country} ({period_label_country})'
                                )
                                
                                fig_scatter_c.update_traces(
                                    marker=dict(size=10, line=dict(width=0.5, color='white'), color='#1f77b4')
                                )
                                
                                fig_scatter_c.update_layout(
                                    height=600,
                                    xaxis=dict(showgrid=True, zeroline=True),
                                    yaxis=dict(showgrid=True, zeroline=True),
                                    showlegend=False
                                )
                                
                                st.plotly_chart(fig_scatter_c, use_container_width=True)
                                
                                # Summary statistics
                                with st.expander("📊 Ver estadísticas descriptivas"):
                                    col1_c, col2_c = st.columns(2)
                                    
                                    with col1_c:
                                        st.markdown(f"**{x_indicator_c}**")
                                        st.write(f"Media: {plot_data_country[x_col_c].mean():.2f}")
                                        st.write(f"Mediana: {plot_data_country[x_col_c].median():.2f}")
                                        st.write(f"Desv. Est.: {plot_data_country[x_col_c].std():.2f}")
                                        st.write(f"Min: {plot_data_country[x_col_c].min():.2f}")
                                        st.write(f"Max: {plot_data_country[x_col_c].max():.2f}")
                                    
                                    with col2_c:
                                        st.markdown(f"**{y_indicator_c}**")
                                        st.write(f"Media: {plot_data_country[y_col_c].mean():.2f}")
                                        st.write(f"Mediana: {plot_data_country[y_col_c].median():.2f}")
                                        st.write(f"Desv. Est.: {plot_data_country[y_col_c].std():.2f}")
                                        st.write(f"Min: {plot_data_country[y_col_c].min():.2f}")
                                        st.write(f"Max: {plot_data_country[y_col_c].max():.2f}")
                                    
                                    # Correlation
                                    correlation_c = plot_data_country[x_col_c].corr(plot_data_country[y_col_c])
                                    st.markdown(f"**Correlación de Pearson:** {correlation_c:.3f}")
                            else:
                                st.warning("⚠️ No hay datos disponibles para los indicadores seleccionados.")
                        else:
                            st.warning("⚠️ No hay suficientes indicadores disponibles para el scatter plot.")
                    else:
                        st.info(f"💡 {selected_country} tiene menos de 3 revistas con datos para el período seleccionado.")
                else:
                    st.info(f"💡 No hay datos disponibles para {period_label_country}. Ejecuta el pipeline completo.")

                # Data Tables Expander
               
                # Open Access and Language breakdown
                st.markdown("#### Distribución y Características de las Publicaciones")
                col_chart1_c, col_chart2_c = st.columns(2)
                
                with col_chart1_c:
                    oa_data = {
                        'Tipo': ['Gold', 'Diamond', 'Green', 'Hybrid', 'Bronze', 'Closed'],
                        'Porcentaje': [
                            period_data.get('pct_oa_gold', 0),
                            period_data.get('pct_oa_diamond', 0),
                            period_data.get('pct_oa_green', 0),
                            period_data.get('pct_oa_hybrid', 0),
                            period_data.get('pct_oa_bronze', 0),
                            period_data.get('pct_oa_closed', 0)
                        ]
                    }
                    oa_df = pd.DataFrame(oa_data)
                    
                    fig_oa = px.pie(oa_df, values='Porcentaje', names='Tipo',
                                   title='Distribución por Acceso Abierto',
                                   color_discrete_sequence=px.colors.qualitative.Set3)
                    st.plotly_chart(fig_oa, use_container_width=True)

                with col_chart2_c:
                    lang_data = {
                        'Idioma': ['Español', 'Inglés', 'Portugués', 'Francés', 'Alemán', 'Italiano', 'Otros'],
                        'Porcentaje': [
                            period_data.get('pct_lang_es', 0),
                            period_data.get('pct_lang_en', 0),
                            period_data.get('pct_lang_pt', 0),
                            period_data.get('pct_lang_fr', 0),
                            period_data.get('pct_lang_de', 0),
                            period_data.get('pct_lang_it', 0),
                            period_data.get('pct_lang_other', 0)
                        ]
                    }
                    lang_df = pd.DataFrame(lang_data)
                    lang_df = lang_df[lang_df['Porcentaje'] > 0]
                    
                    if not lang_df.empty:
                        fig_lang = px.pie(lang_df, values='Porcentaje', names='Idioma',
                                         title='Distribución por Idiomas',
                                         color_discrete_sequence=px.colors.qualitative.Pastel)
                        st.plotly_chart(fig_lang, use_container_width=True)
                    else:
                        st.info("Sin datos de idioma.")
                
                # Indexing
                st.markdown("#### Indexación de Revistas")
                col1, col2, col3 = st.columns(3)
                col1.metric("% Scopus", f"{period_data.get('pct_scopus', 0):.1f}%")
                col2.metric("% CORE", f"{period_data.get('pct_core', 0):.1f}%")
                col3.metric("% DOAJ", f"{period_data.get('pct_doaj', 0):.1f}%")

                # --- Sunburst de Temáticas (Country Level) ---
                if os.path.exists(COUNTRIES_TOPICS_FILE):
                    try:
                        try:
                            # Carga optimizada filtrando por country
                            topics_c = pd.read_parquet(COUNTRIES_TOPICS_FILE, filters=[('country_code', '==', selected_country)])
                        except:
                            # Fallback
                            topics_full = pd.read_parquet(COUNTRIES_TOPICS_FILE)
                            topics_c = topics_full[topics_full['country_code'] == selected_country]
                        
                        if not topics_c.empty:
                            st.markdown("---")
                            st.subheader(f"Temáticas de Investigación en {COUNTRY_NAMES.get(selected_country, selected_country)} (Sunburst)")
                            
                            # Agrupar por niveles: domain -> field -> subfield
                            topics_agg_c = topics_c.groupby(['domain', 'field', 'subfield'], as_index=False)['count'].sum()
                            topics_agg_c = topics_agg_c[topics_agg_c['count'] > 0]
                            
                            if not topics_agg_c.empty:
                                fig_sun_c = px.sunburst(
                                    topics_agg_c,
                                    path=['domain', 'field', 'subfield'],
                                    values='count',
                                    color='domain',
                                    color_discrete_sequence=px.colors.qualitative.Prism
                                )
                                fig_sun_c.update_layout(margin=dict(t=10, l=0, r=0, b=10), height=500)
                                st.plotly_chart(fig_sun_c, use_container_width=True)
                                
                            # --- Tablas de Perfiles Temáticos ---
                            with st.expander("📊 Perfiles Temáticos"):
                                tab_dom_c, tab_field_c, tab_sub_c = st.tabs(["Dominio", "Campo", "Subcampo"])
                                
                                # Obtener IDs de revistas de este país
                                country_j_ids = df[df['country_code'] == selected_country]['id'].tolist()
                                
                                # Cargar temas a nivel revista
                                if os.path.exists(TOPICS_FILE):
                                    topics_j_all = pd.read_parquet(TOPICS_FILE)
                                    topics_cj = topics_j_all[topics_j_all['journal_id'].isin(country_j_ids)]
                                    
                                    if not topics_cj.empty:
                                        total_c_name = f"Total {COUNTRY_NAMES.get(selected_country, selected_country)}"
                                        
                                        with tab_dom_c:
                                            df_dom_c = create_profile_table(topics_cj, 'domain', 'journal_name', 'Revista', total_c_name)
                                            st.dataframe(df_dom_c, use_container_width=True, hide_index=True)
                                            
                                        with tab_field_c:
                                            df_field_c = create_profile_table(topics_cj, 'field', 'journal_name', 'Revista', total_c_name)
                                            st.dataframe(df_field_c, use_container_width=True, hide_index=True)
                                            
                                        with tab_sub_c:
                                            df_sub_c = create_profile_table(topics_cj, 'subfield', 'journal_name', 'Revista', total_c_name)
                                            st.dataframe(df_sub_c, use_container_width=True, hide_index=True)
                                    else:
                                        st.info("No hay datos temáticos detallados por revista para este país.")
                                else:
                                    st.warning("No se encontró el archivo de temas por revista.")
                                
                    except Exception as e:
                        st.warning(f"No se pudieron cargar los temas del país: {e}")
        
        if country_annual is not None:
            country_annual_data = country_annual[country_annual['country_code'] == selected_country]
            
            if len(country_annual_data) > 0:
                st.markdown("---")
                st.markdown("### Tendencias Anuales")
                
                recent_years = country_annual_data[country_annual_data['year'] >= country_annual_data['year'].max() - 30].copy()
                
                # Documents over time
                fig_docs = px.line(recent_years, x='year', y='num_documents',
                                  title='Evolución de Documentos Publicados',
                                  labels={'year': 'Año', 'num_documents': 'Número de Documentos'},
                                  markers=True)
                st.plotly_chart(fig_docs, use_container_width=True)
                
                # FWCI over time
                fig_fwci = px.line(recent_years, x='year', y='fwci_avg',
                                  title='Evolución del FWCI Promedio',
                                  labels={'year': 'Año', 'fwci_avg': 'FWCI Promedio'},
                                  markers=True)
                fig_fwci.add_hline(y=1.0, line_dash="dash", line_color="red",
                                  annotation_text="Promedio Mundial (1.0)")
                st.plotly_chart(fig_fwci, use_container_width=True)
                
                # Top percentages over time
                fig_top = go.Figure()
                fig_top.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_top_10'],
                                            mode='lines+markers', name='Top 10%'))
                fig_top.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_top_1'],
                                            mode='lines+markers', name='Top 1%'))
                fig_top.update_layout(title='Evolución de Artículos Altamente Citados',
                                     xaxis_title='Año',
                                     yaxis_title='Porcentaje (%)')
                st.plotly_chart(fig_top, use_container_width=True)
                
                # OA trends
                fig_oa_trend = go.Figure()
                fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_gold'],
                                                 mode='lines+markers', name='Gold'))
                fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_diamond'],
                                                 mode='lines+markers', name='Diamond'))
                fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_green'],
                                                 mode='lines+markers', name='Green'))
                fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_hybrid'],
                                                 mode='lines+markers', name='Hybrid'))
                fig_oa_trend.update_layout(title='Evolución de Tipos de Acceso Abierto',
                                          xaxis_title='Año',
                                          yaxis_title='Porcentaje (%)')
                st.plotly_chart(fig_oa_trend, use_container_width=True)
                
                st.markdown("---")
                st.subheader(f"Indicadores Históricos de {selected_country}")
                
                tab_c_raw, tab_c_w3, tab_c_w5 = st.tabs(["📊 Datos Crudos", "🌊 Suavizado (w=3)", "🌌 Suavizado (w=5)"])
                
                # Load annual data
                country_annual_all = load_cached_metrics('country', 'annual')
                
                if country_annual_all is not None:
                    # Filter for selected country
                    df_country_annual = country_annual_all[country_annual_all['country_code'] == selected_country].copy()
                    
                    if not df_country_annual.empty:
                        # Metrics columns to smooth and show
                        cols_metrics_c = [
                            'num_journals', 'num_documents', 'fwci_avg', 
                            'pct_oa_total', 'pct_oa_diamond', 'pct_oa_gold', 
                            'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed',
                            'avg_percentile', 'pct_top_10', 'pct_top_1',
                            'pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 
                            'pct_lang_fr', 'pct_lang_de', 'pct_lang_it'
                        ]
                        
                        # Calculate OA total if missing
                        if 'pct_oa_total' not in df_country_annual.columns:
                            df_country_annual['pct_oa_total'] = df_country_annual.get('pct_oa_gold', 0) + df_country_annual.get('pct_oa_green', 0) + df_country_annual.get('pct_oa_hybrid', 0) + df_country_annual.get('pct_oa_bronze', 0)
                            
                        # Filter useful columns only
                        cols_metrics_c = [c for c in cols_metrics_c if c in df_country_annual.columns]

                        def show_country_table(df_input, window=None):
                            df_work = df_input.copy()
                            
                            if window:
                                df_work = df_work.sort_values('year', ascending=True)
                                df_work[cols_metrics_c] = df_work[cols_metrics_c].rolling(window=window, min_periods=1).mean()
                            
                            df_work = df_work.sort_values('year', ascending=False)
                            
                            # Formatting
                            cols_map_c = {
                                'year': 'Año',
                                'num_journals': 'Revistas',
                                'num_documents': 'Documentos',
                                'fwci_avg': 'FWCI',
                                'pct_oa_total': '% OA Total',
                                'pct_oa_diamond': '% OA Diamante',
                                'pct_oa_gold': '% OA Gold',
                                'pct_oa_green': '% OA Verde',
                                'pct_oa_hybrid': '% OA Híbrido',
                                'pct_oa_bronze': '% OA Bronce',
                                'pct_oa_closed': '% Cerrado',
                                'avg_percentile': 'Percentil Prom.',
                                'pct_top_10': '% Top 10',
                                'pct_top_1': '% Top 1',
                                'pct_lang_es': '% Español',
                                'pct_lang_en': '% Inglés',
                                'pct_lang_pt': '% Portugués',
                                'pct_lang_fr': '% Francés',
                                'pct_lang_de': '% Alemán',
                                'pct_lang_it': '% Italiano'
                            }
                            
                            desired_order_c = ['Año', 'Revistas', 'Documentos', 'FWCI', 
                                             '% OA Total', '% OA Diamante', '% OA Gold', 
                                             '% OA Verde', '% OA Híbrido', '% OA Bronce', '% Cerrado',
                                             '% Español', '% Inglés', '% Portugués', '% Francés', '% Alemán', '% Italiano',
                                             'Percentil Prom.', '% Top 10', '% Top 1']
                            
                            df_display = df_work.rename(columns=cols_map_c)
                            final_cols = [c for c in desired_order_c if c in df_display.columns]
                            
                            st.dataframe(df_display[final_cols], use_container_width=True, hide_index=True)

                        with tab_c_raw:
                            show_country_table(df_country_annual, window=None)
                        with tab_c_w3:
                            show_country_table(df_country_annual, window=3)
                        with tab_c_w5:
                            show_country_table(df_country_annual, window=5)
                    else:
                        st.info("No hay datos históricos para este país.")
                else:
                    st.warning("No se pudieron cargar los datos anuales.")
    else:
        st.info("💡 Ejecuta 'Precalcular Indicadores' para ver métricas de desempeño detalladas.")

elif level == "Revista":
    st.header("Detalle de Revista")
    
    # Filters
    countries = sorted(df['country_code'].unique())
    
    # Initialize session state for journal country selection
    if 'selected_country_journal' not in st.session_state:
        st.session_state.selected_country_journal = 'MX' if 'MX' in countries else countries[0]
    
    # Get index for country default
    try:
        country_idx = countries.index(st.session_state.selected_country_journal)
    except ValueError:
        country_idx = 0
        st.session_state.selected_country_journal = countries[0]
    
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        selected_country_journal = st.selectbox(
            "Filtrar por País", 
            countries, 
            index=country_idx,
            key="journal_country_filter"
        )
        # Update session state
        st.session_state.selected_country_journal = selected_country_journal
    
    journals_list = df[df['country_code'] == selected_country_journal]['display_name'].sort_values().unique()
    
    # Initialize session state for journal selection
    if 'selected_journal_name' not in st.session_state:
        # Try to find "Estudios Demográficos y Urbanos" as default
        default_journal = 'Estudios Demográficos y Urbanos'
        if default_journal in journals_list:
            st.session_state.selected_journal_name = default_journal
        else:
            st.session_state.selected_journal_name = journals_list[0] if len(journals_list) > 0 else None
    
    # Get index for journal default
    if st.session_state.selected_journal_name in journals_list:
        journal_idx = list(journals_list).index(st.session_state.selected_journal_name)
    else:
        journal_idx = 0
        st.session_state.selected_journal_name = journals_list[0] if len(journals_list) > 0 else None
    
    with col_filter2:
        selected_journal_name = st.selectbox(
            "Selecciona Revista", 
            journals_list,
            index=journal_idx,
            key="journal_name_selector"
        )
        # Update session state
        st.session_state.selected_journal_name = selected_journal_name
        
    # Get Journal Data
    journal_data = df[df['display_name'] == selected_journal_name].iloc[0]
    
    # Header Info with OpenAlex Link
    st.subheader(journal_data['display_name'])
    openalex_url = journal_data['id']
    st.caption(f"ISSN: {journal_data['issn_l']} | [Ver en OpenAlex]({openalex_url}) | Homepage: {journal_data.get('homepage_url', 'N/A')}")
    
    # Metrics - Primera Fila: Producción y Citación
    st.markdown("#### 📊 Producción y Citación")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Documentos (OpenAlex)", f"{journal_data['works_count']:,}")
    m2.metric("Total Citas", f"{journal_data['cited_by_count']:,}")
    m3.metric("Impacto (2yr)", f"{journal_data.get('2yr_mean_citedness', 0):.3f}")
    m4.metric("Índice H", journal_data.get('h_index', 0))
    
    # Metrics - Segunda Fila: Índices y Acceso Abierto
    st.markdown("#### 📈 Índices y Acceso Abierto")
    m5, m6, m7, m8 = st.columns(4)
    m5.metric("Índice i10", journal_data.get('i10_index', 0))
    m6.metric("Trabajos OA", f"{journal_data.get('oa_works_count', 0):,}")
    m7.metric("Es OA", "✅ Sí" if journal_data.get('is_oa', False) else "❌ No")
    m8.metric("En DOAJ", "✅ Sí" if journal_data.get('is_in_doaj', False) else "❌ No")
    
    # Metrics - Tercera Fila: Indexación
    st.markdown("#### 🔍 Indexación")
    m9, m10, m11, m12 = st.columns(4)
    m9.metric("En SciELO", "✅ Sí" if journal_data.get('is_in_scielo', False) else "❌ No")
    m10.metric("Usa OJS", "✅ Sí" if journal_data.get('is_ojs', False) else "❌ No")
    m11.metric("En CORE", "✅ Sí" if journal_data.get('is_core', False) else "❌ No")
    m12.metric("En Scopus", "✅ Sí" if journal_data.get('is_scopus', False) else "❌ No")
    
    # --- Sunburst de Temáticas (Journal Level) ---
    if os.path.exists(TOPICS_FILE):
        try:
            # Carga optimizada filtrando por journal_id
            jid = journal_data['id']
            try:
                topics_j = pd.read_parquet(TOPICS_FILE, filters=[('journal_id', '==', jid)])
            except:
                # Fallback si falla el filtro (versiones antiguas pandas/pyarrow)
                topics_full = pd.read_parquet(TOPICS_FILE)
                topics_j = topics_full[topics_full['journal_id'] == jid]
            
            if not topics_j.empty:
                st.markdown("---")
                st.subheader("Temáticas de Investigación (Sunburst)")
                
                # Filtrar tópicos con 0 relevancia
                topics_j = topics_j[topics_j['count'] > 0]
                
                if not topics_j.empty:
                    fig_sun = px.sunburst(
                        topics_j,
                        path=['domain', 'field', 'subfield', 'topic_name'],
                        values='count',
                        color='domain',
                        color_discrete_sequence=px.colors.qualitative.Prism
                    )
                    fig_sun.update_layout(margin=dict(t=10, l=0, r=0, b=10), height=500)
                    st.plotly_chart(fig_sun, use_container_width=True)
                    st.caption("Jerarquía: Dominio -> Campo -> Tópico. Tamaño basado en volumen de documentos.")
        except Exception as e:
            st.warning(f"No se pudieron cargar los temas: {e}")

    if has_cached_metrics:
        # Load journal metrics
        journal_annual = load_and_scale('journal', 'annual')
        journal_annual = load_and_scale('journal', 'annual')
        journal_period = load_and_scale('journal', 'period')
        journal_period_recent = load_and_scale('journal', 'period_2021_2025')
        
        if journal_period is not None:
            journal_period_data = journal_period[journal_period['journal_id'] == journal_data['id']]
            
            if len(journal_period_data) > 0:
                st.markdown("---")
                st.subheader("Indicadores de Desempeño")
                
                # Full Period
                st.markdown("### Periodo Completo")
                period_data = journal_period_data.iloc[0]
                
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("Documentos", f"{period_data.get('num_documents', 0):,}")
                col2.metric("FWCI Promedio", f"{period_data.get('fwci_avg', 0):.2f}")
                col3.metric("% Top 10%", f"{period_data.get('pct_top_10', 0):.1f}%")
                col4.metric("% Top 1%", f"{period_data.get('pct_top_1', 0):.1f}%")
                col5.metric("Percentil Prom. Norm.", f"{period_data.get('avg_percentile', 0):.1f}")

                # Recent Period
                if journal_period_recent is not None:
                    journal_rec_data = journal_period_recent[journal_period_recent['journal_id'] == journal_data['id']]
                    if len(journal_rec_data) > 0:
                        rec_data = journal_rec_data.iloc[0]
                        st.markdown(f"### Periodo Reciente: 2021-2025")
                        c1, c2, c3, c4, c5 = st.columns(5)
                        c1.metric("Documentos", f"{rec_data.get('num_documents', 0):,}")
                        c2.metric("FWCI Promedio", f"{rec_data.get('fwci_avg', 0):.2f}")
                        c3.metric("% Top 10%", f"{rec_data.get('pct_top_10', 0):.1f}%")
                        c4.metric("% Top 1%", f"{rec_data.get('pct_top_1', 0):.1f}%")
                        c5.metric("Percentil Prom. Norm.", f"{rec_data.get('avg_percentile', 0):.1f}")
                
                st.markdown("#### Distribución y Características de las Publicaciones")
                col_chart1, col_chart2 = st.columns(2)
                
                with col_chart1:
                    oa_data = {
                        'Tipo': ['Gold', 'Diamond', 'Green', 'Hybrid', 'Bronze', 'Closed'],
                        'Porcentaje': [
                            period_data.get('pct_oa_gold', 0),
                            period_data.get('pct_oa_diamond', 0),
                            period_data.get('pct_oa_green', 0),
                            period_data.get('pct_oa_hybrid', 0),
                            period_data.get('pct_oa_bronze', 0),
                            period_data.get('pct_oa_closed', 0)
                        ]
                    }
                    oa_df = pd.DataFrame(oa_data)
                    
                    fig_oa = px.pie(oa_df, values='Porcentaje', names='Tipo',
                                   title='Distribución por Acceso Abierto',
                                   color_discrete_sequence=px.colors.qualitative.Set3)
                    st.plotly_chart(fig_oa, use_container_width=True)
                
                with col_chart2:
                    lang_data = {
                        'Idioma': ['Español', 'Inglés', 'Portugués', 'Francés', 'Alemán', 'Italiano', 'Otros'],
                        'Porcentaje': [
                            period_data.get('pct_lang_es', 0),
                            period_data.get('pct_lang_en', 0),
                            period_data.get('pct_lang_pt', 0),
                            period_data.get('pct_lang_fr', 0),
                            period_data.get('pct_lang_de', 0),
                            period_data.get('pct_lang_it', 0),
                            period_data.get('pct_lang_other', 0)
                        ]
                    }
                    lang_df = pd.DataFrame(lang_data)
                    # Exclude exactly 0% for cleaner drawing
                    lang_df = lang_df[lang_df['Porcentaje'] > 0]
                    
                    if not lang_df.empty:
                        fig_lang = px.pie(lang_df, values='Porcentaje', names='Idioma',
                                         title='Distribución por Idiomas',
                                         color_discrete_sequence=px.colors.qualitative.Pastel)
                        st.plotly_chart(fig_lang, use_container_width=True)
                    else:
                        st.info("Sin datos de idioma.")
        
                # --- RADAR RECIENTE (2021-2025) ---
                if journal_period_recent is not None and not journal_period_recent.empty:
                    rec_row = journal_period_recent[journal_period_recent['journal_id'] == journal_data['id']]
                    if not rec_row.empty:
                        st.markdown("---")
                        st.subheader("Perfil de Desempeño Reciente (Radar 2021-2025)")
                        st.info("Nota: Valores normalizados (0-1) respecto al máximo observado en todas las revistas.")
                        
                        row_rec = rec_row.iloc[0]
                        
                        # Indicators to plot
                        radar_indicators = ['fwci_avg', 'avg_percentile', 'pct_top_10', 'pct_top_1', 'pct_oa_diamond']
                        radar_labels = ['FWCI', 'Percentil Norm.', 'Top 10%', 'Top 1%', 'OA Diamante']
                        
                        valid_ind = []
                        valid_lbl = []
                        max_vals = []
                        
                        # Find max values for normalization
                        for i, col in enumerate(radar_indicators):
                            if col in journal_period_recent.columns:
                                valid_ind.append(col)
                                valid_lbl.append(radar_labels[i])
                                m = journal_period_recent[col].max()
                                max_vals.append(m if m > 0 else 1.0)
                        
                        if len(valid_ind) >= 3:
                            # Normalize
                            values = [row_rec.get(col, 0) / mx for col, mx in zip(valid_ind, max_vals)]
                            
                            # Close logic loop
                            values += [values[0]]
                            labels_closed = valid_lbl + [valid_lbl[0]]
                            
                            fig_radar = go.Figure()
                            fig_radar.add_trace(go.Scatterpolar(
                                r=values, 
                                theta=labels_closed, 
                                fill='toself',
                                name='2021-2025',
                                line_color='red', 
                                opacity=0.5
                            ))
                            
                            fig_radar.update_layout(
                                polar=dict(
                                    radialaxis=dict(visible=True, range=[0, 1.05], showticklabels=False),
                                ),
                                showlegend=False, 
                                title='Perfil de Desempeño Relativo',
                                height=400,
                                margin=dict(t=40, b=40, l=40, r=40)
                            )
                            st.plotly_chart(fig_radar, use_container_width=True)
                            
        if journal_annual is not None:
            journal_annual_data = journal_annual[journal_annual['journal_id'] == journal_data['id']]
            
            if len(journal_annual_data) > 0:
                st.markdown("---")
                st.markdown("### Tendencias Anuales")
                
                recent_years = journal_annual_data[journal_annual_data['year'] >= journal_annual_data['year'].max() - 20].copy()
                
                # Documents over time
                fig_docs = px.line(recent_years, x='year', y='num_documents',
                                  title='Evolución de Documentos Publicados',
                                  labels={'year': 'Año', 'num_documents': 'Número de Documentos'},
                                  markers=True)
                st.plotly_chart(fig_docs, use_container_width=True)
                
                # FWCI over time
                fig_fwci = px.line(recent_years, x='year', y='fwci_avg',
                                  title='Evolución del FWCI Promedio',
                                  labels={'year': 'Año', 'fwci_avg': 'FWCI Promedio'},
                                  markers=True)
                fig_fwci.add_hline(y=1.0, line_dash="dash", line_color="red",
                                  annotation_text="Promedio Mundial (1.0)")
                st.plotly_chart(fig_fwci, use_container_width=True)
                
                # Top percentages over time
                fig_top = go.Figure()
                fig_top.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_top_10'],
                                            mode='lines+markers', name='Top 10%'))
                fig_top.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_top_1'],
                                            mode='lines+markers', name='Top 1%'))
                fig_top.update_layout(title='Evolución de Artículos Altamente Citados',
                                     xaxis_title='Año',
                                     yaxis_title='Porcentaje (%)')
                st.plotly_chart(fig_top, use_container_width=True)
                
                # OA trends
                fig_oa_trend = go.Figure()
                fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_gold'],
                                                 mode='lines+markers', name='Gold'))
                fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_diamond'],
                                                 mode='lines+markers', name='Diamond'))
                fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_green'],
                                                 mode='lines+markers', name='Green'))
                fig_oa_trend.add_trace(go.Scatter(x=recent_years['year'], y=recent_years['pct_oa_hybrid'],
                                                 mode='lines+markers', name='Hybrid'))
                fig_oa_trend.update_layout(title='Evolución de Tipos de Acceso Abierto',
                                          xaxis_title='Año',
                                          yaxis_title='Porcentaje (%)')
                st.plotly_chart(fig_oa_trend, use_container_width=True)
                
                # Show annual data table nicely formatted
                st.markdown("---")
                st.markdown("### Tabla de Indicadores Anuales")
                
                # Metrics columns to show
                cols_metrics_j = [
                    'year', 'num_documents', 'fwci_avg', 
                    'pct_oa_total', 'pct_oa_diamond', 'pct_oa_gold', 
                    'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed',
                    'avg_percentile', 'pct_top_10', 'pct_top_1',
                    'pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 
                    'pct_lang_fr', 'pct_lang_de', 'pct_lang_it'
                ]
                # Filter useful columns only
                cols_metrics_j = [c for c in cols_metrics_j if c in journal_annual_data.columns]
                
                # Formatting
                cols_map_j = {
                    'year': 'Año',
                    'num_documents': 'Documentos',
                    'fwci_avg': 'FWCI',
                    'pct_oa_total': '% OA Total',
                    'pct_oa_diamond': '% OA Diamante',
                    'pct_oa_gold': '% OA Gold',
                    'pct_oa_green': '% OA Verde',
                    'pct_oa_hybrid': '% OA Híbrido',
                    'pct_oa_bronze': '% OA Bronce',
                    'pct_oa_closed': '% Cerrado',
                    'avg_percentile': 'Percentil Prom.',
                    'pct_top_10': '% Top 10',
                    'pct_top_1': '% Top 1',
                    'pct_lang_es': '% Español',
                    'pct_lang_en': '% Inglés',
                    'pct_lang_pt': '% Portugués',
                    'pct_lang_fr': '% Francés',
                    'pct_lang_de': '% Alemán',
                    'pct_lang_it': '% Italiano'
                }
                
                desired_order_j = ['Año', 'Documentos', 'FWCI', 
                                 '% OA Total', '% OA Diamante', '% OA Gold', 
                                 '% OA Verde', '% OA Híbrido', '% OA Bronce', '% Cerrado',
                                 '% Español', '% Inglés', '% Portugués', '% Francés', '% Alemán', '% Italiano',
                                 'Percentil Prom.', '% Top 10', '% Top 1']
                
                # Prepare DF (Full history, not just recent)
                df_display_j = journal_annual_data[cols_metrics_j].copy().sort_values('year', ascending=False)
                df_display_j = df_display_j.rename(columns=cols_map_j)
                
                final_cols_j = [c for c in desired_order_j if c in df_display_j.columns]
                
                st.dataframe(df_display_j[final_cols_j], use_container_width=True, hide_index=True)
    else:
        st.info("💡 Ejecuta 'Precalcular Indicadores' para ver métricas de desempeño detalladas.")

    # --- TRAYECTORIA DE DESEMPEÑO (UMAP) ---
    st.markdown("---")
    st.subheader("Trayectoria de Desempeño (Perfil Multidimensional)")
    st.markdown("""
    Esta visualización proyecta 5 indicadores clave (Documentos, FWCI, Percentil Promedio, Top 1%, Top 10%) 
    en un plano 2D para observar la evolución del desempeño a lo largo del tiempo.
    """)
    
    
    if os.path.exists(MAP_JOURNALS_FILE):
        try:
            full_coords_df = pd.read_parquet(MAP_JOURNALS_FILE)
            
            # IDs to show
            target_id = journal_data['id']
            country_code = journal_data.get('country_code', '')
            
            # Filter by Context (Country Map)
            if 'map_context' in full_coords_df.columns:
                context_df = full_coords_df[full_coords_df['map_context'] == country_code]
            else:
                context_df = pd.DataFrame() # Should not happen with V2
            
            if not context_df.empty:
                # Filter: Journal and Country (No LATAM), Year 2000-2025
                coords_df = context_df
                mask = ((coords_df['id'] == target_id) | (coords_df['id'] == country_code)) & (coords_df['year'] >= 2000) & (coords_df['year'] <= 2025)
                subset_df = coords_df[mask].copy()
            
            if not subset_df.empty:
                fig_traj = go.Figure()
                
                # Colors/Names Mapping
                colors = {target_id: '#1f77b4', country_code: '#ff7f0e'}
                names = {target_id: selected_journal_name, country_code: f'País: {country_code}'}
                st.caption(f"Visualizando: {selected_journal_name} ({target_id}) vs {country_code}")
                
                # Plot each entity
                for entity_id in subset_df['id'].unique():
                    entity_data = subset_df[subset_df['id'] == entity_id].sort_values('year')
                    if entity_data.empty: continue
                    
                    color = colors.get(entity_id, '#7f7f7f') 
                    name = names.get(entity_id, entity_id)
                    
                    # Line + Markers (Spline)
                    fig_traj.add_trace(go.Scatter(
                        x=entity_data['x'], 
                        y=entity_data['y'],
                        mode='lines+markers+text',
                        name=name,
                        text=entity_data['year'].astype(str).str[-2:], # '19, '20
                        textposition="top center",
                        line=dict(shape='spline', width=3, color=color), 
                        marker=dict(size=6, color=color)
                    ))
                
                fig_traj.update_layout(
                    title="Evolución de la Trayectoria (UMAP)",
                    xaxis_title="Dimensión 1",
                    yaxis_title="Dimensión 2",
                    template="plotly_white",
                    hovermode="closest",
                    height=650,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                
                st.plotly_chart(fig_traj, use_container_width=True)
                
                # Data Tables Expander
                with st.expander("📊 Ver Tablas de Datos (Crudos y Suavizados)"):
                    tab1, tab2, tab3 = st.tabs(["Datos Crudos", "Suavizado (w=3)", "Suavizado (w=5)"])
                    
                    cols_to_show = [
                        'name', 'type', 'year', 'num_documents', 'fwci_avg',
                        'pct_oa_total', 'pct_oa_diamond', 'pct_oa_gold',
                        'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed',
                        'pct_lang_es', 'pct_lang_en', 'pct_lang_pt',
                        'pct_lang_fr', 'pct_lang_de', 'pct_lang_it',
                        'avg_percentile', 'pct_top_10', 'pct_top_1'
                    ]
                    
                    rename_map_traj = {
                        'name': 'Nombre', 'type': 'Tipo', 'year': 'Año',
                        'num_documents': 'Documentos', 'fwci_avg': 'FWCI',
                        'pct_oa_total': '% OA Total', 'pct_oa_diamond': '% OA Diamante',
                        'pct_oa_gold': '% OA Gold', 'pct_oa_green': '% OA Verde',
                        'pct_oa_hybrid': '% OA Híbrido', 'pct_oa_bronze': '% OA Bronce',
                        'pct_oa_closed': '% Cerrado',
                        'pct_lang_es': '% Español', 'pct_lang_en': '% Inglés',
                        'pct_lang_pt': '% Portugués', 'pct_lang_fr': '% Francés',
                        'pct_lang_de': '% Alemán', 'pct_lang_it': '% Italiano',
                        'avg_percentile': 'Percentil Prom.',
                        'pct_top_10': '% Top 10', 'pct_top_1': '% Top 1'
                    }
                    
                    # RAW DATA
                    if os.path.exists(traj_raw_file):
                        raw_df = pd.read_parquet(traj_raw_file)
                        raw_subset = raw_df[raw_df['id'].isin([target_id, country_code]) & (raw_df['year'] >= 2000) & (raw_df['year'] <= 2025)].sort_values(['id', 'year'])
                        existing_cols = [c for c in cols_to_show if c in raw_subset.columns]
                        tab1.dataframe(raw_subset[existing_cols].rename(columns=rename_map_traj), use_container_width=True, hide_index=True)
                    else:
                        tab1.warning("Archivo de datos crudos no encontrado.")
                        
                    # SMOOTHED DATA (w=3)
                    if os.path.exists(traj_smooth_file):
                        smooth_df = pd.read_parquet(traj_smooth_file)
                        smooth_subset = smooth_df[smooth_df['id'].isin([target_id, country_code]) & (smooth_df['year'] >= 2000) & (smooth_df['year'] <= 2025)].sort_values(['id', 'year'])
                        existing_cols_s = [c for c in cols_to_show if c in smooth_subset.columns]
                        tab2.dataframe(smooth_subset[existing_cols_s].rename(columns=rename_map_traj), use_container_width=True, hide_index=True)
                        tab2.caption("Media móvil exponencial (window=3, tau=1).")
                    else:
                        tab2.warning("Archivo w=3 no encontrado.")
                        
                    # SMOOTHED DATA (w=5)
                    if os.path.exists(traj_smooth_w5_file):
                        smooth_w5_df = pd.read_parquet(traj_smooth_w5_file)
                        smooth_w5_subset = smooth_w5_df[smooth_w5_df['id'].isin([target_id, country_code]) & (smooth_w5_df['year'] >= 2000) & (smooth_w5_df['year'] <= 2025)].sort_values(['id', 'year'])
                        existing_cols_w5 = [c for c in cols_to_show if c in smooth_w5_subset.columns]
                        tab3.dataframe(smooth_w5_subset[existing_cols_w5].rename(columns=rename_map_traj), use_container_width=True, hide_index=True)
                        tab3.caption("Media móvil exponencial (window=5, tau=1).")
                    else:
                        tab3.warning("Archivo w=5 no encontrado.")
                        
            else:
                st.info("No hay suficientes datos anuales para proyectar la trayectoria de esta revista.")
                
        except Exception as e:
            st.error(f"Error procesando visualización de trayectorias: {e}")
            
    else:
        st.info("ℹ️ Para ver el Análisis de Trayectorias, por favor ejecuta el pipeline completo.")
    
    # --- SCATTER PLOT DE ARTÍCULOS ---
    st.markdown("---")
    st.subheader("Explorador de Artículos - Scatter Plot Dinámico")
    st.caption(f"Visualiza la relación entre diferentes indicadores bibliométricos para los artículos de {journal_data['display_name']}")
    
    # Checkbox to control loading
    show_works_scatter = st.checkbox(
        "📊 Cargar y mostrar explorador de artículos",
        value=False,
        help="Activa esta opción para cargar los datos de artículos y visualizar el scatter plot. Puede tardar unos segundos."
    )
    
    if show_works_scatter:
        # Load works data
        works_file = os.path.join(BASE_PATH, 'data', 'latin_american_works.parquet')
        
        if os.path.exists(works_file):
            try:
                with st.spinner("Cargando datos de artículos..."):
                    # Load works for this journal only
                    works_df = pd.read_parquet(
                        works_file,
                        filters=[('journal_id', '=', journal_data['id'])]
                    )
                
                if len(works_df) > 0:
                    st.success(f"✅ Cargados {len(works_df):,} artículos")
                    
                    # Define available indicators for works
                    works_indicator_options = {
                        'FWCI': 'fwci',
                        'Percentil': 'percentile',
                        'Citas': 'cited_by_count',
                        'Top 1%': 'is_top_1',
                        'Top 10%': 'is_top_10'
                    }
                    
                    # Filter to only available columns
                    available_works_indicators = {
                        k: v for k, v in works_indicator_options.items() 
                        if v in works_df.columns
                    }
                    
                    if len(available_works_indicators) >= 2:
                        # Axis selectors in columns
                        col_x_w, col_y_w = st.columns(2)
                        
                        with col_x_w:
                            x_indicator_w = st.selectbox(
                                "Indicador Eje X:",
                                options=list(available_works_indicators.keys()),
                                index=0,  # Default: FWCI
                                key="works_x_indicator"
                            )
                        
                        with col_y_w:
                            y_indicator_w = st.selectbox(
                                "Indicador Eje Y:",
                                options=list(available_works_indicators.keys()),
                                index=2 if len(available_works_indicators) > 2 else 1,  # Default: Citas
                                key="works_y_indicator"
                            )
                        
                        x_col_w = available_works_indicators[x_indicator_w]
                        y_col_w = available_works_indicators[y_indicator_w]
                        
                        # Filter out rows with missing data
                        plot_data_works = works_df[
                            works_df[x_col_w].notna() & 
                            works_df[y_col_w].notna()
                        ].copy()
                        
                        if len(plot_data_works) > 0:
                            # Add checkbox for full data
                            show_all_works = st.checkbox(
                                "Mostrar todos los artículos (puede ser lento)",
                                value=False,
                                help="Si se activa, se mostrarán todos los puntos disponibles. Si no, se mostrará una muestra de 1,000 artículos."
                            )

                            # Limit to reasonable number for performance unless requested
                            limit = 1000
                            if not show_all_works and len(plot_data_works) > limit:
                                st.info(f"ℹ️ Mostrando una muestra de {limit:,} artículos de {len(plot_data_works):,} totales para mejor rendimiento.")
                                plot_data_works = plot_data_works.sample(n=limit, random_state=42)
                            elif len(plot_data_works) > limit:
                                st.warning(f"⚠️ Mostrando {len(plot_data_works):,} artículos. La interacción puede ser lenta.")
                            
                            # Prepare hover data
                            hover_cols = {
                                'title': True,
                                x_col_w: ':.2f' if x_col_w not in ['is_top_1', 'is_top_10'] else True,
                                y_col_w: ':.2f' if y_col_w not in ['is_top_1', 'is_top_10'] else True,
                            }
                            
                            # Add publication year if available
                            if 'publication_year' in plot_data_works.columns:
                                hover_cols['publication_year'] = True
                            
                            # Create scatter plot
                            fig_scatter_w = px.scatter(
                                plot_data_works,
                                x=x_col_w,
                                y=y_col_w,
                                hover_data=hover_cols,
                                labels={
                                    x_col_w: x_indicator_w,
                                    y_col_w: y_indicator_w,
                                    'title': 'Título',
                                    'publication_year': 'Año'
                                },
                                title=f'{y_indicator_w} vs {x_indicator_w} - Artículos de {journal_data["display_name"]}',
                                opacity=0.6
                            )
                            
                            fig_scatter_w.update_traces(
                                marker=dict(size=8, line=dict(width=0.3, color='white'), color='#2ca02c')
                            )
                            
                            fig_scatter_w.update_layout(
                                height=600,
                                xaxis=dict(showgrid=True, zeroline=True),
                                yaxis=dict(showgrid=True, zeroline=True),
                                showlegend=False
                            )
                            
                            st.plotly_chart(fig_scatter_w, use_container_width=True)
                            
                            # Summary statistics
                            with st.expander("📊 Ver estadísticas descriptivas"):
                                col1_w, col2_w = st.columns(2)
                                
                                with col1_w:
                                    st.markdown(f"**{x_indicator_w}**")
                                    if x_col_w not in ['is_top_1', 'is_top_10']:
                                        st.write(f"Media: {plot_data_works[x_col_w].mean():.2f}")
                                        st.write(f"Mediana: {plot_data_works[x_col_w].median():.2f}")
                                        st.write(f"Desv. Est.: {plot_data_works[x_col_w].std():.2f}")
                                        st.write(f"Min: {plot_data_works[x_col_w].min():.2f}")
                                        st.write(f"Max: {plot_data_works[x_col_w].max():.2f}")
                                    else:
                                        # Boolean indicator
                                        count_true = plot_data_works[x_col_w].sum()
                                        pct_true = (count_true / len(plot_data_works)) * 100
                                        st.write(f"Artículos: {int(count_true):,} ({pct_true:.1f}%)")
                                
                                with col2_w:
                                    st.markdown(f"**{y_indicator_w}**")
                                    if y_col_w not in ['is_top_1', 'is_top_10']:
                                        st.write(f"Media: {plot_data_works[y_col_w].mean():.2f}")
                                        st.write(f"Mediana: {plot_data_works[y_col_w].median():.2f}")
                                        st.write(f"Desv. Est.: {plot_data_works[y_col_w].std():.2f}")
                                        st.write(f"Min: {plot_data_works[y_col_w].min():.2f}")
                                        st.write(f"Max: {plot_data_works[y_col_w].max():.2f}")
                                    else:
                                        # Boolean indicator
                                        count_true = plot_data_works[y_col_w].sum()
                                        pct_true = (count_true / len(plot_data_works)) * 100
                                        st.write(f"Artículos: {int(count_true):,} ({pct_true:.1f}%)")
                                
                                # Correlation (only for numeric indicators)
                                if x_col_w not in ['is_top_1', 'is_top_10'] and y_col_w not in ['is_top_1', 'is_top_10']:
                                    correlation_w = plot_data_works[x_col_w].corr(plot_data_works[y_col_w])
                                    st.markdown(f"**Correlación de Pearson:** {correlation_w:.3f}**")
                        else:
                            st.warning("⚠️ No hay datos disponibles para los indicadores seleccionados.")
                    else:
                        st.warning("⚠️ No hay suficientes indicadores disponibles en los datos de artículos.")
                else:
                    st.info(f"💡 No se encontraron artículos para esta revista en el archivo de datos.")
            except Exception as e:
                st.error(f"❌ Error cargando datos de artículos: {e}")
        else:
            st.info("💡 Archivo de artículos no encontrado. Ejecuta el pipeline de extracción.")



    else:
        st.info("💡 Ejecuta 'Precalcular Indicadores' para ver métricas de desempeño detalladas.")

elif level == "Acerca de...":
    st.header("Acerca de...")
    
    st.subheader("Grupo de trabajo")
    st.markdown("""
    **Complejidad, Ciencienciometría y Ciencia de la Ciencia**
    
    *   Dr. Humberto Andrés Carrillo Calvet
    *   Dr. Ricardo Arencibia Jorge
    *   Dr. José Luis Jiménez Andrade
    """)
    
    st.divider()
    
    st.subheader("Programación")
    st.markdown("""
    *   José Luis Jiménez Andrade
    *   Antigravity con Gemini 3 Pro, Claude Sonnet 4.5
    """)
    
    st.divider()
    
    st.subheader("Diagrama general del sistema")
    
    # Diagrama Mermaid visualizado con Graphviz para compatibilidad nativa
    st.caption("Flujo de datos y arquitectura del sistema")
    
    # 2. Extract stats for simplified diagram
    n_revistas = len(df)
    n_trabajos = df['works_count'].sum() if 'works_count' in df.columns else 0

    st.graphviz_chart(f"""
    digraph SimplifiedPipeline {{
        rankdir=LR;
        node [shape=box, style=filled, fillcolor="#f0f2f6", fontname="Sans-Serif"];
        edge [fontname="Sans-Serif", fontsize=10];
        
        Sources [label="Fuentes de Datos\\n(PostgreSQL + API)\\n📦 1.2 Tb", shape=cylinder, fillcolor="#ffeba0"];
        ETL [label="Extracción (ETL)\\n📄 {n_revistas:,} Revistas\\n📑 {n_trabajos:,} Trabajos", shape=component, fillcolor="#ffbd45"];
        Processing [label="Procesamiento\\n(Métricas & UMAP)", shape=component, fillcolor="#ffbd45"];
        Cache [label="Datos Procesados\\n(Cache Parquet)", shape=note, fillcolor="#e8fdf5"];
        Dashboard [label="Dashboard\\n(Streamlit)", shape=rect, style=filled, fillcolor="#ff4b4b", fontcolor=white];

        Sources -> ETL;
        ETL -> Processing;
        Processing -> Cache;
        Cache -> Dashboard;
    }}
    """)
    st.caption("Arquitectura General Simplificada")
    st.markdown("---")

    st.graphviz_chart("""
    digraph Pipeline {
        rankdir=TB;
        node [shape=box, style=filled, fillcolor="#f0f2f6", fontname="Sans-Serif"];
        edge [fontname="Sans-Serif", fontsize=10];
        
        subgraph cluster_sources {
            label = "Fuentes de Datos";
            style=dashed;
            color="#555555";
            PostgreSQL [shape=cylinder, fillcolor="#ffeba0", label="PostgreSQL\n(OpenAlex Snapshot)"];
            OpenAlex_API [shape=ellipse, fillcolor="#ffeba0", label="OpenAlex API\n(Enrichment)"];
        }
        
        subgraph cluster_extraction {
            label = "Extracción (ETL)";
            color="#555555";
            extract_postgres [label="extract_postgres.py", shape=component, fillcolor="#ffbd45"];
        }
        
        subgraph cluster_data {
            label = "Datos Crudos (Parquet)";
            color="#555555";
            journals_parquet [label="latin_american_journals.parquet", shape=note, fillcolor="#e8fdf5"];
            works_parquet [label="latin_american_works.parquet", shape=note, fillcolor="#e8fdf5"];
        }
        
        subgraph cluster_processing {
            label = "Procesamiento";
            color="#555555";
            transform_metrics [label="transform_metrics.py", shape=component, fillcolor="#ffbd45"];
            calculate_umap [label="calculate_umap.py", shape=component, fillcolor="#ffbd45"];
            calculate_trajectory [label="calculate_trajectory.py", shape=component, fillcolor="#ffbd45"];
        }
        
        subgraph cluster_cache {
            label = "Datos Procesados (Cache)";
            color="#555555";
            metrics_parquet [label="metrics_*.parquet", shape=note, fillcolor="#e8fdf5"];
            umap_parquet [label="umap_*.parquet", shape=note, fillcolor="#e8fdf5"];
            trajectory_parquet [label="trajectory_*.parquet", shape=note, fillcolor="#e8fdf5"];
        }
        
        Dashboard [label="dashboard.py\n(Streamlit App)", shape=rect, style=filled, fillcolor="#ff4b4b", fontcolor=white, fontsize=14];

        # Edges
        PostgreSQL -> extract_postgres;
        OpenAlex_API -> extract_postgres [style=dotted];
        
        extract_postgres -> journals_parquet;
        extract_postgres -> works_parquet;
        
        journals_parquet -> transform_metrics;
        works_parquet -> transform_metrics;
        
        transform_metrics -> metrics_parquet;
        
        metrics_parquet -> calculate_umap;
        calculate_umap -> umap_parquet;
        
        metrics_parquet -> calculate_trajectory;
        calculate_trajectory -> trajectory_parquet;
        
        journals_parquet -> Dashboard;
        metrics_parquet -> Dashboard;
        umap_parquet -> Dashboard;
        trajectory_parquet -> Dashboard;
    }
    """)
    
    with st.expander("Ver definición Mermaid"):
        st.code("""
graph TD
    subgraph "Fuentes de Datos"
        DB[(PostgreSQL\nOpenAlex Snapshot)]
        API[OpenAlex API\n(Enrichment)]
    end

    subgraph "Extracción (ETL)"
        E1[extract_postgres.py]
        E1 -->|Consulta SQL| DB
        E1 -->|Genera| P1
        E1 -->|Genera| P2
    end
    
    subgraph "Datos Crudos (Parquet)"
        P1[latin_american_journals.parquet]
        P2[latin_american_works.parquet]
    end

    subgraph "Procesamiento y Transformación"
        T1[transform_metrics.py]
        T1 -->|Lee| P1
        T1 -->|Lee| P2
        T1 -->|Calcula| P3
        
        UMAP[calculate_umap.py]
        UMAP -->|Lee| P3
        UMAP -->|Genera| P4
        
        TRAJ[calculate_trajectory.py]
        TRAJ -->|Lee| P3
        TRAJ -->|Genera| P5
    end
    
    subgraph "Datos Procesados (Cache)"
        P3[metrics_*.parquet]
        P4[umap_*.parquet]
        P5[trajectory_*.parquet]
    end
    
    subgraph "Visualización"
        D[dashboard.py]
        D -->|Visualiza| P1
        D -->|Visualiza| P3
        D -->|Visualiza| P4
        D -->|Visualiza| P5
    end
        """, language='mermaid')

    st.markdown("---")
    st.subheader("Descripción Detallada de Componentes del Sistema")
    
    st.markdown("""
    El sistema implementa un flujo de trabajo de procesamiento de datos bibliométricos diseñado para analizar el desempeño de revistas científicas latinoamericanas utilizando grandes volúmenes de datos. A continuación se detalla cada módulo:

    #### 1. Fuentes de Datos (Data Sources)
    La base del sistema reside en la recolección de metadatos científicos robustos y actualizados.
    *   **PostgreSQL (OpenAlex Snapshot)**: Se utiliza una instancia local de PostgreSQL cargada con el snapshot oficial de OpenAlex (2025). Esta base de datos relacional almacena terabytes de información sobre trabajos académicos, autores, instituciones y fuentes de publicación, permitiendo consultas SQL complejas de alto rendimiento sin depender de latencias de red externas.
    *   **OpenAlex API**: Actúa como complemento dinámico. Aunque el snapshot es estático, la API permite enriquecer datos específicos en tiempo real, validando metadatos críticos o recuperando trabajos muy recientes que no se encuentren en la versión local.
    
    #### 2. Extracción (ETL - Extract, Transform, Load)
    Esta fase es crítica para filtrar el universo de datos global y centrarse en el contexto regional.
    *   **Script de Extracción (`extract_postgres.py`)**: Este componente ejecuta consultas SQL optimizadas para identificar y extraer exclusivamente las revistas publicadas en países de Latinoamérica y el Caribe.
        *   *Funcionalidad*: Recupera metadatos de las revistas (ISSN, editorial, país) y descarga todos los trabajos (artículos) asociados a estas revistas.
        *   *Manejo de Datos*: Implementa paginación eficiente para manejar millones de registros sin saturar la memoria RAM.
    *   **Datos Crudos (`latin_american_*.parquet`)**: Los resultados se serializan en formato Parquet, un formato columnar de alta eficiencia que reduce el espacio en disco hasta un 80% comparado con CSV y permite lecturas ultrarrápidas para el procesamiento posterior.
    
    #### 3. Procesamiento y Análisis Avanzado
    El núcleo analítico del sistema transforma los datos crudos en conocimiento accionable.
    *   **Cálculo de Métricas (`transform_metrics.py`)**:
        *   Procesa millones de artículos para calcular indicadores de desempeño a nivel de revista y país.
        *   *Métricas Clave*: Calcula el **FWCI** (Impacto de Citas Ponderado por Campo), conteos de citas, percentiles de impacto (Top 1%, Top 10%), y desglose detallado de las modalidades de Acceso Abierto (Diamante, Dorado, Híbrido).
        *   Genera series temporales anuales para analizar la evolución histórica.
    *   **Proyección UMAP (`calculate_umap.py`)**:
        *   Implementa algoritmos de aprendizaje no supervisado (**UMAP**: *Uniform Manifold Approximation and Projection*) para reducir la complejidad multidimensional de los datos.
        *   Transforma 6+ dimensiones de desempeño (citas, producción, impacto, etc.) en un espacio 2D, permitiendo visualizar qué revistas o países tienen perfiles bibliométricos similares, revelando clústeres naturales por disciplina o calidad.
    *   **Análisis de Trayectorias (`calculate_trajectory.py`)**:
        *   Modela la evolución temporal de cada entidad en el espacio UMAP.
        *   Permite trazar el "camino" que ha recorrido una revista o país a lo largo de los años, facilitando la identificación de tendencias de mejora, estancamiento o cambios en la política editorial.
    
    #### 4. Capa de Datos Procesados (Cache)
    Para garantizar una experiencia de usuario fluida, el sistema no calcula métricas en tiempo real, sino que consume datos pre-procesados.
    *   **Archivos Parquet Optimizados**: Almacenan los resultados finales de los procesos anteriores (`metrics_country_annual.parquet`, `metrics_journal_period.parquet`, etc.).
    *   Esta arquitectura desacoplada permite que el dashboard responda en milisegundos, independientemente de la complejidad de los cálculos matemáticos subyacentes que pueden tomar horas en ejecutarse.
    
    #### 5. Visualización e Interacción (Dashboard)
    El punto de contacto final con el usuario, desarrollado en **Streamlit**.
    *   **Interfaz Dinámica**: Permite navegar entre niveles de análisis (Región, País, Revista).
    *   **Visualizaciones Interactivas**: Utiliza **Plotly** para generar gráficos que responden al usuario (zoom, selección, tooltips).
        *   *Mapas Coropléticos*: Para visualizar indicadores geográficos.
        *   *Scatter Plots Dinámicos*: Para correlacionar variables a elección del usuario.
        *   *Radares de Desempeño*: Para comparar el perfil de una revista contra el promedio regional.
    *   **Transparencia de Datos**: Cada visualización permite acceder a los datos tabulares subyacentes, fomentando la reproducibilidad y el análisis detallado.
    """)

# Footer
st.markdown("---")

