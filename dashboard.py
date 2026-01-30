import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from data_collector import update_data
from data_processor import get_latam_kpis, get_country_stats, analyze_oa_vs_impact, load_data as collector_load_data
from performance_metrics import (load_works_data, calculate_journal_performance_metrics,
                                  calculate_country_performance_metrics, 
                                  calculate_latam_performance_metrics,
                                  get_topic_baselines, enrich_works_data)

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
            st.rerun() # Refresh app to reload data
        else:
            status.update(label="Error en actualización", state="error", expanded=False)
            st.sidebar.error("No se pudieron descargar datos.")

# --- Main Content ---
st.title("Sistema de Información Bibliométrica")

# Load Data
# Load Data with Cache
@st.cache_data
def load_and_enrich_data():
    # Load raw
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    journals_path = os.path.join(data_dir, 'latin_american_journals.parquet')
    works_path = os.path.join(data_dir, 'latin_american_works.parquet')
    
    df = collector_load_data(journals_path)
    works_df = load_works_data(works_path)
    
    # Enrich works if available
    if not works_df.empty:
        with st.spinner("Calculando indicadores avanzados (FWCI, Percentiles)..."):
            topic_baselines = get_topic_baselines(works_df)
            works_df = enrich_works_data(works_df, topic_baselines)
            
    return df, works_df

df, works_df = load_and_enrich_data()

if df.empty:
    st.warning("⚠️ No hay datos disponibles. Por favor, pulsa 'Actualizar Datos' en la barra lateral para comenzar.")
    st.stop()

# Check if works data exists
has_works_data = not works_df.empty

# Filter by Level
if level == "Region (Latinoamérica)":
    st.header("Panorama Regional")
    
    # KPIs
    kpis = get_latam_kpis(df)
    col1, col2, col3, col4 = st.columns(4)
    if kpis:
        col1.metric("Revistas Indexadas", kpis['total_journals'])
        col2.metric("Total Artículos", f"{kpis['total_articles']:,}")
        col3.metric("Citas Totales", f"{kpis['total_citations']:,}")
        col4.metric("% Acceso Abierto", f"{kpis['percent_oa']:.1f}%")
    
    # Charts
    st.markdown("### Producción por País")
    country_stats = get_country_stats(df)
    
    if not country_stats.empty:
        fig_bar = px.bar(
            country_stats.sort_values('num_journals', ascending=False),
            x='country_code', y='num_journals',
            title='Número de Revistas por País',
            labels={'country_code': 'País', 'num_journals': 'Cantidad de Revistas'},
            color='num_journals'
        )
        st.plotly_chart(fig_bar, use_container_width=True)
        
        st.markdown("### Impacto vs Acceso Abierto")
        fig_scatter = px.scatter(
            country_stats,
            x='percent_oa', y='avg_impact_factor',
            size='num_journals',
            color='country_code',
            hover_name='country_code',
            title='Relación entre % OA e Impacto Promedio (por País)',
            labels={'percent_oa': '% Acceso Abierto', 'avg_impact_factor': 'Impacto Promedio (2yr citations)'}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Performance Metrics (if works data available)
    if has_works_data:
        st.markdown("### Indicadores de Desempeño")
        perf_metrics = calculate_latam_performance_metrics(works_df, df)
        
        if perf_metrics:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Documentos", f"{perf_metrics['num_documents']:,}")
            col2.metric("% Artículos OA", f"{perf_metrics['pct_oa_articles']:.1f}%")
            col3.metric("FWCI Promedio", f"{perf_metrics['fwci_approx']:.2f}")
            col4.metric("% Top 10%", f"{perf_metrics['pct_top10']:.1f}%")
            
            col5, col6, col7, col8 = st.columns(4)
            col5.metric("Percentil Prom.", f"{perf_metrics['avg_percentile']:.1f}")
            col6.metric("% Scopus", f"{perf_metrics['pct_scopus']:.1f}%")
            col7.metric("% CORE", f"{perf_metrics['pct_core']:.1f}%")
            col8.metric("% DOAJ", f"{perf_metrics['pct_doaj']:.1f}%")
            
            # --- New Visualizations for Region ---
            st.divider()
            
            # 1. FWCI Distribution
            st.markdown("#### Distribución de Impacto Normalizado (FWCI)")
            # Filter outliers for better visualization
            filtered_fwci = works_df[works_df['fwci'] < 10] 
            fig_fwci = px.histogram(
                filtered_fwci, 
                x='fwci', 
                nbins=50,
                title="Distribución de FWCI (Artículos con FWCI < 10)",
                labels={'fwci': 'Field-Weighted Citation Impact'},
                color_discrete_sequence=['#636EFA']
            )
            fig_fwci.add_vline(x=1.0, line_dash="dash", line_color="red", annotation_text="Promedio Mundial (1.0)")
            st.plotly_chart(fig_fwci, use_container_width=True)
            
            # 2. Citation Percentiles
            st.markdown("#### Distribución de Percentiles de Citas")
            fig_pct = px.histogram(
                works_df,
                x='citation_percentile',
                nbins=20,
                title="Distribución de Percentiles (0 = Peor, 100 = Mejor)",
                labels={'citation_percentile': 'Percentil'},
                color_discrete_sequence=['#00CC96']
            )
            st.plotly_chart(fig_pct, use_container_width=True)

elif level == "País":
    st.header("Análisis por País")
    
    countries = sorted(df['country_code'].unique())
    selected_country = st.selectbox("Selecciona un País", countries)
    
    # Filter data
    country_df = df[df['country_code'] == selected_country]
    
    # KPIs for country
    kpis = get_latam_kpis(country_df)
    col1, col2, col3, col4 = st.columns(4)
    if kpis:
        col1.metric("Revistas", kpis['total_journals'])
        col2.metric("Artículos", f"{kpis['total_articles']:,}")
        col3.metric("Citas", f"{kpis['total_citations']:,}")
        col4.metric("% OA", f"{kpis['percent_oa']:.1f}%")
        
    # Top Journals Table
    st.markdown("### Top Revistas por Citas")
    top_journals = country_df.sort_values('cited_by_count', ascending=False).head(10)
    st.dataframe(
        top_journals[['display_name', 'issn_l', 'works_count', 'cited_by_count', '2yr_mean_citedness', 'is_oa']],
        use_container_width=True,
        hide_index=True
    )
    
    # Distribution of H-index
    st.markdown("### Distribución del Índice H")
    fig_hist = px.histogram(
        country_df, 
        x='h_index',
        nbins=20,
        title='Distribución de Índice H de las Revistas',
        labels={'h_index': 'Índice H'}
    )
    st.plotly_chart(fig_hist, use_container_width=True)

    # Journals Scatter: Impact vs OA
    st.markdown("### Impacto vs Acceso Abierto (Detalle de Revistas)")
    
    # Prepare data for plotting
    plot_df = country_df.copy()
    plot_df['Tipo de Acceso'] = plot_df['is_oa'].map({True: 'Acceso Abierto (OA)', False: 'Suscripción/Híbrido'})
    
    fig_journals_scatter = px.scatter(
        plot_df,
        x='works_count',
        y='2yr_mean_citedness',
        color='Tipo de Acceso',
        size='cited_by_count',
        hover_name='display_name',
        log_x=True, # Log scale usually better for document counts
        title='Relación: Volumen vs Impacto Promedio (Color por Acceso)',
        labels={
            'works_count': 'Cantidad de Documentos (Log)',
            '2yr_mean_citedness': 'Impacto Promedio (2yr)',
            'cited_by_count': 'Citas Totales'
        }
    )
    st.plotly_chart(fig_journals_scatter, use_container_width=True)
    
    # Performance Metrics (if works data available)
    if has_works_data:
        st.markdown("### Indicadores de Desempeño del País")
        perf_metrics = calculate_country_performance_metrics(works_df, df, selected_country)
        
        if perf_metrics and perf_metrics['num_documents'] > 0:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Documentos", f"{perf_metrics['num_documents']:,}")
            col2.metric("% Artículos OA", f"{perf_metrics['pct_oa_articles']:.1f}%")
            col3.metric("FWCI Promedio", f"{perf_metrics['fwci_approx']:.2f}")
            col4.metric("% Top 10%", f"{perf_metrics['pct_top10']:.1f}%")
            
            col5, col6, col7, col8 = st.columns(4)
            col5.metric("Percentil Prom.", f"{perf_metrics['avg_percentile']:.1f}")
            col6.metric("% Scopus", f"{perf_metrics['pct_scopus']:.1f}%")
            col7.metric("% CORE", f"{perf_metrics['pct_core']:.1f}%")
            col8.metric("% DOAJ", f"{perf_metrics['pct_doaj']:.1f}%")
            
            # --- New Visualizations for Country ---
            st.divider()
            
            # Get works for this country
            journal_ids = df[df['country_code'] == selected_country]['id'].tolist()
            country_works = works_df[works_df['journal_id'].isin(journal_ids)]
            
            c1, c2 = st.columns(2)
            
            with c1:
                st.markdown("#### Distribución de FWCI")
                filtered_fwci = country_works[country_works['fwci'] < 10]
                fig_fwci = px.histogram(
                    filtered_fwci, 
                    x='fwci', 
                    nbins=40,
                    title="Distribución de FWCI (Recortado < 10)",
                    labels={'fwci': 'FWCI'},
                    color_discrete_sequence=['#EF553B']
                )
                fig_fwci.add_vline(x=1.0, line_dash="dash", line_color="black")
                st.plotly_chart(fig_fwci, use_container_width=True)
                
            with c2:
                st.markdown("#### Distribución de Percentiles")
                fig_pct_box = px.box(
                    country_works,
                    y='citation_percentile',
                    title="Dispersión de Percentiles de Citas",
                    labels={'citation_percentile': 'Percentil Global'}
                )
                st.plotly_chart(fig_pct_box, use_container_width=True)

        else:
            st.info("No hay datos de artículos disponibles para este país.")

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
    
    # Performance Metrics (if works data available)
    if has_works_data:
        st.markdown("### Indicadores de Desempeño de la Revista")
        perf_metrics = calculate_journal_performance_metrics(works_df, journal_data['id'])
        
        if perf_metrics and perf_metrics['num_documents'] > 0:
            p1, p2, p3, p4 = st.columns(4)
            p1.metric("Total Documentos", f"{perf_metrics['num_documents']:,}")
            p2.metric("% Artículos OA", f"{perf_metrics['pct_oa_articles']:.1f}%")
            p3.metric("FWCI Aprox.", f"{perf_metrics['fwci_approx']:.2f}")
            p4.metric("% Top 10%", f"{perf_metrics['pct_top10']:.1f}%")
            
            p5, p6 = st.columns(2)
            p5.metric("Citas Promedio", f"{perf_metrics['avg_citations']:.1f}")
            p6.metric("Percentil Prom.", f"{perf_metrics['avg_percentile']:.1f}")
            
            # --- New Visualizations for Journal ---
            st.divider()
            
            # Get works for this journal
            journal_works = works_df[works_df['journal_id'] == journal_data['id']]
            
            tab1, tab2 = st.tabs(["📊 Distribución de Impacto", "🏆 Top Artículos"])
            
            with tab1:
                c1, c2 = st.columns(2)
                with c1:
                    fig_hist_perf = px.histogram(
                        journal_works,
                        x='citation_percentile',
                        nbins=20,
                        title='Distribución de Percentiles',
                        labels={'citation_percentile': 'Percentil Global'},
                        color_discrete_sequence=['#AB63FA']
                    )
                    st.plotly_chart(fig_hist_perf, use_container_width=True)
                
                with c2:
                    fig_fwci_box = px.box(
                        journal_works[journal_works['fwci'] < 20], # Remove extreme outliers
                        y='fwci',
                        title='Variabilidad de FWCI',
                        labels={'fwci': 'FWCI (Normalizado)'}
                    )
                    fig_fwci_box.add_hline(y=1.0, line_dash="dash", line_color="red")
                    st.plotly_chart(fig_fwci_box, use_container_width=True)

            with tab2:
                st.markdown("#### Artículos más citados")
                top_articles = journal_works.sort_values('cited_by_count', ascending=False).head(10)
                st.dataframe(
                    top_articles[['title', 'publication_year', 'cited_by_count', 'fwci', 'citation_percentile', 'primary_topic']],
                    use_container_width=True,
                    hide_index=True
                )

        else:
            st.info("No hay datos de artículos disponibles para esta revista.")
    
    st.divider()
    
    # Topics Sunburst
    st.markdown("### Jerarquía de Tópicos")
    topics = journal_data['topics']
    if topics is not None and len(topics) > 0:
        try:
            topics_data = []
            for topic in journal_data['topics']:
                # Handle cases where topic might be just a string (backward compatibility) or full dict
                if isinstance(topic, dict):
                    topics_data.append({
                        'domain': topic.get('domain', {}).get('display_name', 'Unknown'),
                        'field': topic.get('field', {}).get('display_name', 'Unknown'),
                        'subfield': topic.get('subfield', {}).get('display_name', 'Unknown'),
                        'topic': topic.get('display_name', 'Unknown'),
                        'count': topic.get('count', 1)
                    })
                else:
                    # Fallback for simple string list
                    topics_data.append({'domain': 'General', 'field': 'General', 'subfield': 'General', 'topic': str(topic), 'count': 1})
            
            if topics_data:
                topics_df = pd.DataFrame(topics_data)
                fig_sunburst = px.sunburst(
                    topics_df,
                    path=['domain', 'field', 'subfield', 'topic'],
                    values='count',
                    title='Distribución Temática'
                )
                st.plotly_chart(fig_sunburst, use_container_width=True)
                
                # Also show simple bar chart of top topics
                top_topics = topics_df.sort_values('count', ascending=False).head(10)
                fig_bar = px.bar(
                    top_topics,
                    x='count', y='topic',
                    orientation='h',
                    title='Top 10 Tópicos'
                )
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("Datos de tópicos insuficientes para graficar.")
                
        except Exception as e:
            st.error(f"Error generando gráfico de tópicos: {e}")
            st.write(journal_data['topics']) # Fallback display
    else:
        st.info("No hay información de tópicos disponible.")
            
    # Time Series (using counts_by_year)
    # counts_by_year is a list of dicts [{'year': 2024, 'works_count': 10, ...}, ...]
    st.markdown("### Evolución Temporal")
    counts = journal_data['counts_by_year']
    if counts is not None and len(counts) > 0:
        ts_df = pd.DataFrame(list(journal_data['counts_by_year']))
        ts_df = ts_df.sort_values('year')
        
        fig_line = px.line(
            ts_df, 
            x='year', 
            y=['works_count', 'cited_by_count'],
            markers=True,
            title='Evolución de Publicaciones y Citas',
            labels={'value': 'Cantidad', 'year': 'Año', 'variable': 'Métrica'}
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("No hay datos históricos disponibles.")

# Footer
st.markdown("---")
st.markdown("Desarrollado con ❤️ para el análisis bibliométrico latinoamericano.")
