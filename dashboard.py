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

# Data Update Section
st.sidebar.subheader("Gestión de Datos")
if st.sidebar.button("Actualizar Datos (OpenAlex)"):
    with st.sidebar.status("Descargando datos...", expanded=True) as status:
        st.write("Conectando con OpenAlex...")
        count = update_data()
        if count > 0:
            status.update(label="¡Actualización completa!", state="complete", expanded=False)
            st.sidebar.success(f"Se actualizaron {count} revistas.")
            st.rerun()
        else:
            status.update(label="Error en actualización", state="error", expanded=False)
            st.sidebar.error("No se pudieron descargar datos.")

# Metrics Precalculation Section
if st.sidebar.button("Precalcular Indicadores"):
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    works_path = os.path.join(data_dir, 'latin_american_works.parquet')
    journals_path = os.path.join(data_dir, 'latin_american_journals.parquet')
    
    if not os.path.exists(works_path):
        st.sidebar.error("No hay datos de artículos disponibles.")
    else:
        with st.sidebar.status("Precalculando indicadores...", expanded=True) as status:
            st.write("Calculando métricas por año...")
            try:
                result = compute_and_cache_all_metrics(works_path, journals_path, force_recalculate=True)
                if result:
                    status.update(label="¡Precálculo completo!", state="complete", expanded=False)
                    st.sidebar.success(f"Métricas calculadas exitosamente.")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    status.update(label="Error en precálculo", state="error", expanded=False)
                    st.sidebar.error("No se pudieron calcular las métricas.")
            except Exception as e:
                status.update(label="Error en precálculo", state="error", expanded=False)
                st.sidebar.error(f"Error: {e}")

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

# Footer
st.markdown("---")
st.markdown("Desarrollado con ❤️ para el análisis bibliométrico latinoamericano.")
