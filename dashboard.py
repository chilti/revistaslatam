import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from data_collector import update_data
from data_processor import load_data as collector_load_data
from performance_metrics import compute_and_cache_all_metrics, load_cached_metrics, get_cache_dir

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
    ["Region (Latinoamérica)", "País", "Revista"]
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
st.title("Sistema de Información Bibliométrica")

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
                '% Acceso Abierto': 'pct_oa_total',
                '% Cerrado': 'pct_oa_closed',
                '% Gold': 'pct_oa_gold',
                '% Green': 'pct_oa_green',
                '% Hybrid': 'pct_oa_hybrid',
                '% Bronze': 'pct_oa_bronze'
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
            
            # Open Access breakdown
            st.markdown("#### Distribución de Acceso Abierto")
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
                           title='Distribución por Tipo de Acceso Abierto',
                           color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig_oa, use_container_width=True)
            
            # Journal indexing
            st.markdown("#### Indexación de Revistas")
            col1, col2, col3 = st.columns(3)
            col1.metric("% Scopus", f"{period_data.get('pct_scopus', 0):.1f}%")
            col2.metric("% CORE", f"{period_data.get('pct_core', 0):.1f}%")
            col3.metric("% DOAJ", f"{period_data.get('pct_doaj', 0):.1f}%")
        
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
                st.dataframe(recent_years, use_container_width=True, hide_index=True)

        # Tablas de Países
        st.markdown("---")
        st.subheader("Comparativa por País")
        
        tab_countries_1, tab_countries_2 = st.tabs(["Periodo Completo", "Periodo Reciente (2021-2025)"])
        
        # Columns to display
        cols_display = [
            'country_code', 'num_journals', 'num_documents', 'fwci_avg', 'avg_percentile', 
            'pct_top_10', 'pct_top_1', 'pct_oa_gold', 'pct_oa_diamond', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed'
        ]
        
        with tab_countries_1:
            st.markdown("**Indicadores por País (Periodo Completo)**")
            country_period = load_and_scale('country', 'period')
            if country_period is not None and not country_period.empty:
                # Sort by num_documents
                display_df = country_period.sort_values('num_documents', ascending=False)
                # Filter columns that exist
                valid_cols = [c for c in cols_display if c in display_df.columns]
                final_df = display_df[valid_cols].rename(columns={'avg_percentile': 'Percentil Prom. Norm.'})
                st.dataframe(final_df, use_container_width=True, hide_index=True)
            else:
                st.info("No hay datos de países disponibles.")
                
        with tab_countries_2:
            st.markdown("**Indicadores por País (2021-2025)**")
            country_period_recent = load_and_scale('country', 'period_2021_2025')
            if country_period_recent is not None and not country_period_recent.empty:
                # Sort by num_documents
                display_df_recent = country_period_recent.sort_values('num_documents', ascending=False)
                # Filter columns
                valid_cols = [c for c in cols_display if c in display_df_recent.columns]
                final_df = display_df_recent[valid_cols].rename(columns={'avg_percentile': 'Percentil Prom. Norm.'})
                st.dataframe(final_df, use_container_width=True, hide_index=True)
            else:
                st.info("No hay datos recientes de países disponibles (es necesario ejecutar pre-cálculo v2).")

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
        st.caption("Visualización 2D basada en: Documentos, OA Diamante, FWCI, % Top 10%, % Top 1%, Percentil Promedio (2021-2025)")
        
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
                else:
                    st.warning("⚠️ Archivo UMAP encontrado pero sin coordenadas. Ejecuta el pipeline completo.")
            except Exception as e:
                st.error(f"❌ Error cargando visualización UMAP: {e}")
        else:
            st.info("💡 Ejecuta el pipeline completo (`python run_pipeline.py`) para generar la visualización UMAP.")
        
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
    
    else:
        st.info("💡 Ejecuta 'Precalcular Indicadores' para ver métricas de desempeño detalladas.")

elif level == "País":
    st.header("Análisis por País")
    
    countries = sorted(df['country_code'].unique())
    selected_country = st.selectbox("Selecciona un País", countries)
    
    # Filter data
    country_df = df[df['country_code'] == selected_country]
    
    # Basic KPIs
    col1, col2 = st.columns(2)
    col1.metric("Revistas", len(country_df))
    col2.metric("Artículos", f"{country_df['works_count'].sum():,}")
    
    # Top Journals Table
    st.markdown("### Top Revistas por Citas")
    top_journals = country_df.sort_values('cited_by_count', ascending=False).head(10)
    
    # Try to enhance with performance metrics
    journal_period = load_and_scale('journal', 'period')
    
    cols_basic = ['display_name', 'issn_l', 'works_count', 'cited_by_count']
    cols_advanced = [
        'fwci_avg', 'avg_percentile', 'pct_top_10', 'pct_top_1', 
        'pct_oa_gold', 'pct_oa_diamond', 'pct_oa_green', 'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed'
    ]
    
    if journal_period is not None:
        # Merge on ID
        merged = pd.merge(top_journals, journal_period, left_on='id', right_on='journal_id', how='left')
        
        # Check which columns are available
        available_cols = [c for c in cols_advanced if c in merged.columns]
        final_cols = cols_basic + available_cols
        
        st.dataframe(merged[final_cols], use_container_width=True, hide_index=True)
    else:
        # Fallback
        st.dataframe(
            top_journals[['display_name', 'issn_l', 'works_count', 'cited_by_count', '2yr_mean_citedness', 'is_oa']],
            use_container_width=True,
            hide_index=True
        )
    
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
                with st.expander("📊 Ver Tablas de Datos (Crudos y Suavizados)"):
                    tab1, tab2, tab3 = st.tabs(["Datos Crudos", "Suavizado (w=3)", "Suavizado (w=5)"])
                    
                    # RAW DATA
                    if os.path.exists(traj_raw_file):
                        raw_df = pd.read_parquet(traj_raw_file)
                        raw_subset = raw_df[raw_df['id'].isin([selected_country, 'LATAM']) & (raw_df['year'] >= 2000) & (raw_df['year'] <= 2025)].sort_values(['id', 'year'])
                        # Format for display
                        cols_to_show = ['name', 'type', 'year', 'num_documents', 'fwci_avg', 'avg_percentile', 'pct_top_10', 'pct_top_1']
                        existing_cols = [c for c in cols_to_show if c in raw_subset.columns]
                        tab1.dataframe(raw_subset[existing_cols], use_container_width=True, hide_index=True)
                    else:
                        tab1.warning("Archivo de datos crudos no encontrado.")
                        
                    # SMOOTHED DATA (w=3)
                    if os.path.exists(traj_smooth_file):
                        smooth_df = pd.read_parquet(traj_smooth_file)
                        smooth_subset = smooth_df[smooth_df['id'].isin([selected_country, 'LATAM']) & (smooth_df['year'] >= 2000) & (smooth_df['year'] <= 2025)].sort_values(['id', 'year'])
                        # Format
                        existing_cols_s = [c for c in cols_to_show if c in smooth_subset.columns]
                        tab2.dataframe(smooth_subset[existing_cols_s], use_container_width=True, hide_index=True)
                        tab2.caption("Nota: Datos suavizados (media móvil exponencial, window=3).")
                    else:
                        tab2.warning("Archivo de datos suavizados (w=3) no encontrado.")

                    # SMOOTHED DATA (w=5)
                    # SMOOTHED DATA (w=5)
                    if os.path.exists(traj_smooth_w5_file):
                        smooth_w5_df = pd.read_parquet(traj_smooth_w5_file)
                        smooth_w5_subset = smooth_w5_df[smooth_w5_df['id'].isin([selected_country, 'LATAM']) & (smooth_w5_df['year'] >= 2000) & (smooth_w5_df['year'] <= 2025)].sort_values(['id', 'year'])
                        # Format
                        existing_cols_w5 = [c for c in cols_to_show if c in smooth_w5_subset.columns]
                        tab3.dataframe(smooth_w5_subset[existing_cols_w5], use_container_width=True, hide_index=True)
                        tab3.caption("Nota: Datos suavizados intensamente (media móvil exponencial, window=5).")
                    else:
                        tab3.warning("Archivo de datos suavizados (w=5) no encontrado.")
                
                # Open Access breakdown
                st.markdown("#### Distribución de Acceso Abierto")
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
                               title='Distribución por Tipo de Acceso Abierto',
                               color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig_oa, use_container_width=True)
                
                # Indexing
                st.markdown("#### Indexación de Revistas")
                col1, col2, col3 = st.columns(3)
                col1.metric("% Scopus", f"{period_data.get('pct_scopus', 0):.1f}%")
                col2.metric("% CORE", f"{period_data.get('pct_core', 0):.1f}%")
                col3.metric("% DOAJ", f"{period_data.get('pct_doaj', 0):.1f}%")
        
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
                
                # Show annual data table
                with st.expander("📊 Ver Tabla de Datos Anuales"):
                    st.dataframe(recent_years, use_container_width=True, hide_index=True)
    else:
        st.info("💡 Ejecuta 'Precalcular Indicadores' para ver métricas de desempeño detalladas.")

elif level == "Revista":
    st.header("Detalle de Revista")
    
    # Filters
    countries = sorted(df['country_code'].unique())
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        selected_country_journal = st.selectbox("Filtrar por País", countries, key="journal_country_filter")
    
    journals_list = df[df['country_code'] == selected_country_journal]['display_name'].sort_values().unique()
    with col_filter2:
        selected_journal_name = st.selectbox("Selecciona Revista", journals_list)
        
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
                        path=['domain', 'field', 'topic_name'],
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
                
                # Open Access breakdown
                st.markdown("#### Distribución de Acceso Abierto")
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
                               title='Distribución por Tipo de Acceso Abierto',
                               color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig_oa, use_container_width=True)
        
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
                
                # Show annual data table
                with st.expander("📊 Ver Tabla de Datos Anuales"):
                    st.dataframe(recent_years, use_container_width=True, hide_index=True)
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
                    
                    cols_to_show = ['name', 'type', 'year', 'num_documents', 'fwci_avg', 'avg_percentile', 'pct_top_10', 'pct_top_1']
                    
                    # RAW DATA
                    if os.path.exists(traj_raw_file):
                        raw_df = pd.read_parquet(traj_raw_file)
                        raw_subset = raw_df[raw_df['id'].isin([target_id, country_code]) & (raw_df['year'] >= 2000) & (raw_df['year'] <= 2025)].sort_values(['id', 'year'])
                        existing_cols = [c for c in cols_to_show if c in raw_subset.columns]
                        tab1.dataframe(raw_subset[existing_cols], use_container_width=True, hide_index=True)
                    else:
                        tab1.warning("Archivo de datos crudos no encontrado.")
                        
                    # SMOOTHED DATA (w=3)
                    if os.path.exists(traj_smooth_file):
                        smooth_df = pd.read_parquet(traj_smooth_file)
                        smooth_subset = smooth_df[smooth_df['id'].isin([target_id, country_code]) & (smooth_df['year'] >= 2000) & (smooth_df['year'] <= 2025)].sort_values(['id', 'year'])
                        existing_cols_s = [c for c in cols_to_show if c in smooth_subset.columns]
                        tab2.dataframe(smooth_subset[existing_cols_s], use_container_width=True, hide_index=True)
                        tab2.caption("Media móvil exponencial (window=3, tau=1).")
                    else:
                        tab2.warning("Archivo w=3 no encontrado.")
                        
                    # SMOOTHED DATA (w=5)
                    if os.path.exists(traj_smooth_w5_file):
                        smooth_w5_df = pd.read_parquet(traj_smooth_w5_file)
                        smooth_w5_subset = smooth_w5_df[smooth_w5_df['id'].isin([target_id, country_code]) & (smooth_w5_df['year'] >= 2000) & (smooth_w5_df['year'] <= 2025)].sort_values(['id', 'year'])
                        existing_cols_w5 = [c for c in cols_to_show if c in smooth_w5_subset.columns]
                        tab3.dataframe(smooth_w5_subset[existing_cols_w5], use_container_width=True, hide_index=True)
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
                            # Limit to reasonable number for performance
                            if len(plot_data_works) > 1000:
                                st.info(f"ℹ️ Mostrando una muestra de 1000 artículos de {len(plot_data_works):,} totales para mejor rendimiento.")
                                plot_data_works = plot_data_works.sample(n=1000, random_state=42)
                            
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



# Footer
st.markdown("---")
st.markdown("Desarrollado por ...")
