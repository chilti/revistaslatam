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

# Footer
st.markdown("---")
st.markdown("Desarrollado por ...")
