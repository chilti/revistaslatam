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
if latam_cache.exists():
    import datetime
    mtime = os.path.getmtime(latam_cache)
    cache_time = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
    st.sidebar.info(f"📊 Caché de métricas: {cache_time}")
else:
    st.sidebar.warning("⚠️ Sin caché de métricas")

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
        country_period = load_cached_metrics('country', 'period')
        
        if country_period is not None and len(country_period) > 0:
            # Indicator selector
            indicator_options = {
                'Número de Revistas': 'num_journals',
                'Artículos': 'num_documents',
                'FWCI Promedio': 'fwci_avg',
                '% Acceso Abierto': 'pct_oa_total',
                '% Cerrado': 'pct_oa_closed',
                '% Gold': 'pct_oa_gold',
                '% Green': 'pct_oa_green',
                '% Hybrid': 'pct_oa_hybrid',
                '% Bronze': 'pct_oa_bronze',
                '% Top 10%': 'pct_top_10'
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
            
            # Create choropleth map
            fig_map = px.choropleth(
                country_period,
                locations='country_code',
                locationmode='ISO-3',
                color=indicator_col,
                hover_name='country_code',
                hover_data={
                    'country_code': True,
                    indicator_col: ':.2f',
                    'num_journals': ':,',
                    'num_documents': ':,'
                },
                color_continuous_scale='Viridis',
                labels={indicator_col: selected_indicator},
                title=f'{selected_indicator} por País'
            )
            
            # Focus on Latin America
            fig_map.update_geos(
                scope='south america',
                showcountries=True,
                countrycolor="lightgray",
                showcoastlines=True,
                coastlinecolor="gray",
                projection_type='natural earth',
                center=dict(lat=-10, lon=-60),
                lataxis_range=[-60, 35],
                lonaxis_range=[-120, -30]
            )
            
            fig_map.update_layout(
                height=600,
                margin=dict(l=0, r=0, t=40, b=0)
            )
            
            st.plotly_chart(fig_map, use_container_width=True)
            
            # Show top 5 countries for selected indicator
            st.markdown(f"**Top 5 Países - {selected_indicator}**")
            top_countries = country_period.nlargest(5, indicator_col)[['country_code', indicator_col, 'num_journals', 'num_documents']]
            top_countries.columns = ['País', selected_indicator, 'Revistas', 'Artículos']
            st.dataframe(top_countries, use_container_width=True, hide_index=True)
    

    if has_cached_metrics:
        st.markdown("---")
        st.subheader("Indicadores de Desempeño")
        
        # Load cached metrics
        latam_annual = load_cached_metrics('latam', 'annual')
        latam_period = load_cached_metrics('latam', 'period')
        
        if latam_period is not None and len(latam_period) > 0:
            period_data = latam_period.iloc[0]
            
            # Display period metrics
            st.markdown(f"### Periodo Completo: {period_data.get('period', 'N/A')}")
            
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Documentos", f"{period_data.get('num_documents', 0):,}")
            col2.metric("FWCI Promedio", f"{period_data.get('fwci_avg', 0):.2f}")
            col3.metric("% Top 10%", f"{period_data.get('pct_top_10', 0):.1f}%")
            col4.metric("% Top 1%", f"{period_data.get('pct_top_1', 0):.1f}%")
            col5.metric("Percentil Prom.", f"{period_data.get('avg_percentile', 0):.1f}")
            
            # Open Access breakdown
            st.markdown("#### Distribución de Acceso Abierto")
            oa_data = {
                'Tipo': ['Gold', 'Green', 'Hybrid', 'Bronze', 'Closed'],
                'Porcentaje': [
                    period_data.get('pct_oa_gold', 0),
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
    st.dataframe(
        top_journals[['display_name', 'issn_l', 'works_count', 'cited_by_count', '2yr_mean_citedness', 'is_oa']],
        use_container_width=True,
        hide_index=True
    )
    
    if has_cached_metrics:
        # Load country metrics
        country_annual = load_cached_metrics('country', 'annual')
        country_period = load_cached_metrics('country', 'period')
        
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
                col5.metric("Percentil Prom.", f"{period_data.get('avg_percentile', 0):.1f}")
                # Data Tables Expander
                with st.expander("📊 Ver Tablas de Datos (Crudos y Suavizados)"):
                    tab1, tab2, tab3 = st.tabs(["Datos Crudos", "Suavizado (w=3)", "Suavizado (w=5)"])
                    
                    # RAW DATA
                    if os.path.exists(traj_raw_file):
                        raw_df = pd.read_parquet(traj_raw_file)
                        raw_subset = raw_df[raw_df['id'].isin([target_id, country_code, 'LATAM'])].sort_values(['id', 'year'])
                        # Format for display
                        cols_to_show = ['name', 'type', 'year', 'num_documents', 'fwci_avg', 'avg_percentile', 'pct_top_10', 'pct_top_1']
                        existing_cols = [c for c in cols_to_show if c in raw_subset.columns]
                        tab1.dataframe(raw_subset[existing_cols], use_container_width=True, hide_index=True)
                    else:
                        tab1.warning("Archivo de datos crudos no encontrado.")
                        
                    # SMOOTHED DATA (w=3)
                    if os.path.exists(traj_smooth_file):
                        smooth_df = pd.read_parquet(traj_smooth_file)
                        smooth_subset = smooth_df[smooth_df['id'].isin([target_id, country_code, 'LATAM'])].sort_values(['id', 'year'])
                        # Format
                        existing_cols_s = [c for c in cols_to_show if c in smooth_subset.columns]
                        tab2.dataframe(smooth_subset[existing_cols_s], use_container_width=True, hide_index=True)
                        tab2.caption("Nota: Datos suavizados (media móvil exponencial, window=3).")
                    else:
                        tab2.warning("Archivo de datos suavizados (w=3) no encontrado.")

                    # SMOOTHED DATA (w=5)
                    traj_smooth_w5_file = os.path.join(base_path, 'data', 'cache', 'trajectory_data_smoothed_w5.parquet')
                    if os.path.exists(traj_smooth_w5_file):
                        smooth_w5_df = pd.read_parquet(traj_smooth_w5_file)
                        smooth_w5_subset = smooth_w5_df[smooth_w5_df['id'].isin([target_id, country_code, 'LATAM'])].sort_values(['id', 'year'])
                        # Format
                        existing_cols_w5 = [c for c in cols_to_show if c in smooth_w5_subset.columns]
                        tab3.dataframe(smooth_w5_subset[existing_cols_w5], use_container_width=True, hide_index=True)
                        tab3.caption("Nota: Datos suavizados intensamente (media móvil exponencial, window=5).")
                    else:
                        tab3.warning("Archivo de datos suavizados (w=5) no encontrado.")
                
                # Open Access breakdown
                st.markdown("#### Distribución de Acceso Abierto")
                oa_data = {
                    'Tipo': ['Gold', 'Green', 'Hybrid', 'Bronze', 'Closed'],
                    'Porcentaje': [
                        period_data.get('pct_oa_gold', 0),
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
    
    # Header Info
    st.subheader(journal_data['display_name'])
    st.caption(f"ISSN: {journal_data['issn_l']} | URL: {journal_data['homepage_url']}")
    
    # Metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Documentos", journal_data['works_count'])
    m2.metric("Total Citas", journal_data['cited_by_count'])
    m3.metric("Impacto (2yr)", f"{journal_data['2yr_mean_citedness']:.2f}")
    m4.metric("Índice H", journal_data['h_index'])
    
    if has_cached_metrics:
        # Load journal metrics
        journal_annual = load_cached_metrics('journal', 'annual')
        journal_period = load_cached_metrics('journal', 'period')
        
        if journal_period is not None:
            journal_period_data = journal_period[journal_period['journal_id'] == journal_data['id']]
            
            if len(journal_period_data) > 0:
                st.markdown("---")
                st.subheader("Indicadores de Desempeño")
                
                period_data = journal_period_data.iloc[0]
                
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("Documentos", f"{period_data.get('num_documents', 0):,}")
                col2.metric("FWCI Promedio", f"{period_data.get('fwci_avg', 0):.2f}")
                col3.metric("% Top 10%", f"{period_data.get('pct_top_10', 0):.1f}%")
                col4.metric("% Top 1%", f"{period_data.get('pct_top_1', 0):.1f}%")
                col5.metric("Percentil Prom.", f"{period_data.get('avg_percentile', 0):.1f}")
                
                # Open Access breakdown
                st.markdown("#### Distribución de Acceso Abierto")
                oa_data = {
                    'Tipo': ['Gold', 'Green', 'Hybrid', 'Bronze', 'Closed'],
                    'Porcentaje': [
                        period_data.get('pct_oa_gold', 0),
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
    
    base_path = os.path.dirname(os.path.abspath(__file__))
    traj_coords_file = os.path.join(base_path, 'data', 'cache', 'trajectory_coordinates.parquet')
    traj_raw_file = os.path.join(base_path, 'data', 'cache', 'trajectory_data_raw.parquet')
    traj_smooth_file = os.path.join(base_path, 'data', 'cache', 'trajectory_data_smoothed.parquet')
    
    if os.path.exists(traj_coords_file):
        try:
            coords_df = pd.read_parquet(traj_coords_file)
            
            # IDs to show
            target_id = journal_data['id']
            country_code = journal_data.get('country_code', '')
            
            # Filter: Journal, Country, and LATAM
            mask = (coords_df['id'] == target_id) | (coords_df['id'] == country_code) | (coords_df['id'] == 'LATAM')
            subset_df = coords_df[mask].copy()
            
            if not subset_df.empty:
                fig_traj = go.Figure()
                
                # Colors/Names Mapping
                colors = {target_id: '#1f77b4', country_code: '#ff7f0e', 'LATAM': '#2ca02c'}
                names = {target_id: selected_journal_name, country_code: f'País: {country_code}', 'LATAM': 'Iberoamérica (Ref.)'}
                
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
                        raw_subset = raw_df[raw_df['id'].isin([target_id, country_code, 'LATAM'])].sort_values(['id', 'year'])
                        existing_cols = [c for c in cols_to_show if c in raw_subset.columns]
                        tab1.dataframe(raw_subset[existing_cols], use_container_width=True, hide_index=True)
                    else:
                        tab1.warning("Archivo de datos crudos no encontrado.")
                        
                    # SMOOTHED DATA (w=3)
                    if os.path.exists(traj_smooth_file):
                        smooth_df = pd.read_parquet(traj_smooth_file)
                        smooth_subset = smooth_df[smooth_df['id'].isin([target_id, country_code, 'LATAM'])].sort_values(['id', 'year'])
                        existing_cols_s = [c for c in cols_to_show if c in smooth_subset.columns]
                        tab2.dataframe(smooth_subset[existing_cols_s], use_container_width=True, hide_index=True)
                        tab2.caption("Media móvil exponencial (window=3, tau=1).")
                    else:
                        tab2.warning("Archivo w=3 no encontrado.")
                        
                    # SMOOTHED DATA (w=5)
                    traj_smooth_w5_file = os.path.join(base_path, 'data', 'cache', 'trajectory_data_smoothed_w5.parquet')
                    if os.path.exists(traj_smooth_w5_file):
                        smooth_w5_df = pd.read_parquet(traj_smooth_w5_file)
                        smooth_w5_subset = smooth_w5_df[smooth_w5_df['id'].isin([target_id, country_code, 'LATAM'])].sort_values(['id', 'year'])
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

# Footer
st.markdown("---")
st.markdown("Desarrollado con ❤️ para el análisis bibliométrico latinoamericano.")
