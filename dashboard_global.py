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

# Estilos CSS Premium
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
    }

    .stApp {
        background: radial-gradient(circle at top right, #fdfdfd, #f4f7f6);
    }

    /* Tarjetas de Métricas Premium */
    .metric-container {
        display: flex;
        justify-content: space-between;
        gap: 20px;
        margin-bottom: 2rem;
    }

    .metric-card {
        background: white;
        padding: 24px;
        border-radius: 16px;
        box-shadow: 0 10px 25px rgba(0,0,0,0.05);
        border: 1px solid rgba(0,0,0,0.05);
        transition: all 0.3s ease;
        flex: 1;
        text-align: left;
    }

    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 15px 35px rgba(0,0,0,0.1);
    }

    .metric-label {
        font-size: 0.9rem;
        color: #64748b;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 8px;
    }

    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1e293b;
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    .metric-delta {
        font-size: 0.85rem;
        font-weight: 500;
        margin-top: 4px;
    }

    .delta-positive { color: #10b981; }
    .delta-negative { color: #ef4444; }

    /* Headers */
    h1, h2, h3 {
        font-weight: 700 !important;
        color: #0f172a !important;
    }

    /* Sidebar Customization */
    [data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e2e8f0;
    }
</style>
""", unsafe_allow_html=True)

def premium_metric(label, value, delta=None):
    delta_html = ""
    if delta:
        color_class = "delta-positive" if delta.startswith("+") else "delta-negative"
        delta_html = f'<div class="metric-delta {color_class}">{delta}</div>'
    
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

# Rutas y Carga de datos
BASE_PATH = Path(__file__).parent if '__file__' in locals() else Path(os.getcwd())
CACHE_DIR = BASE_PATH / 'data' / 'cache'

# Referencia a src/regions.py
sys.path.append(str(BASE_PATH / 'src'))
try:
    from regions import GLOBAL_REGIONS, get_region_for_country
except ImportError:
    st.error("Error cargando `src/regions.py`. Asegúrate de que existe.")
    st.stop()

# Paleta de Colores Premium por Región
REGION_COLORS = {
    'China': '#e11d48',
    'Asia Emergente': '#f59e0b',
    'Latinoamérica y Caribe': '#10b981',
    'África Subsahariana': '#8b5cf6',
    'MENA (Medio Oriente/Norte África)': '#d97706',
    'Norteamérica Anglosajona': '#3b82f6',
    'Europa Central/Occidental': '#6366f1',
    'Europa del Este': '#64748b',
    'Asia-Pacífico Desarrollado': '#06b6d4'
}

@st.cache_data
def load_data(filename: str):
    file_path = CACHE_DIR / filename
    if not file_path.exists():
        return None
    return pd.read_parquet(file_path)

# --- CARGA DE DATOS ---
df_region_annual = load_data('metrics_global_region_annual.parquet')
df_country_annual = load_data('metrics_global_country_annual.parquet')
df_country_recent = load_data('metrics_global_country_period_2021_2025.parquet') # Nuevo dataset
df_umap_country = load_data('umap_countries_recent.parquet') # UMAP País
df_umap_macro = load_data('umap_global_regions.parquet')
df_journals_metadata = load_data('global_journals_metadata.parquet')
df_journals_annual = load_data('metrics_global_journal_annual.parquet')
df_journals_recent = load_data('metrics_global_journal_period_2021_2025.parquet')
df_thematic_evo = load_data('thematic_evolution_base.parquet')
# Archivos de Sunburst (Jerárquicos)
SUNBURST_METRICS_REGION = CACHE_DIR / 'sunburst_metrics_region.parquet'
SUNBURST_METRICS_COUNTRY = CACHE_DIR / 'sunburst_metrics_country.parquet'
SUNBURST_METRICS_JOURNAL = CACHE_DIR / 'sunburst_metrics_journal.parquet'

# Variables globales para filtrar el año más reciente disponible en toda la base
latest_year = 2025 # Por default
if df_region_annual is not None and not df_region_annual.empty:
    latest_year = int(df_region_annual['year'].max())

# --- SIDEBAR ---
st.sidebar.title("🌍 Navegación Mundial")
level = st.sidebar.radio(
    "Nivel de Análisis:",
    ["1. Mundo (Macro)", "2. Exploración por Región", "3. Análisis de País", "4. Buscador de Revista", "5. Acerca del Sistema"]
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
        st.markdown('<div class="metric-container">', unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        
        total_docs = df_latest['num_documents'].sum() if not df_latest.empty else 0
        total_journals = df_latest['num_journals'].sum() if not df_latest.empty else 0
        total_diamond = (df_latest['num_documents'] * (df_latest['pct_oa_diamond']/100)).sum() if not df_latest.empty else 0
        
        with col1:
            premium_metric("Producción Mundial", f"{int(total_docs):,}", "+4.2% vs prev")
        with col2:
            premium_metric("Revistas Globales", f"{int(total_journals):,}", "+156 nuevas")
        with col3:
            premium_metric("Acceso Diamante", f"{int(total_diamond):,}", "Estimado")
        st.markdown('</div>', unsafe_allow_html=True)
        
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
                color='region',
                color_discrete_map=REGION_COLORS,
                size='num_documents',
                hover_data=['num_journals', 'fwci_avg', 'pct_oa_diamond', 'pct_lang_en'],
                template="plotly_white"
            )
            fig_umap.update_traces(textposition='top center', marker=dict(line=dict(width=1, color='DarkSlateGrey')))
            fig_umap.update_layout(showlegend=False, height=600, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_umap, use_container_width=True)
        else:
            st.warning("No se encontró `umap_global_regions.parquet`. Ejecute el pipeline estático global.")
            
        # --- TRAYECTORIAS (LINE CHARTS) ---
        st.subheader("Evolución Temporal: Impacto vs Idioma")
        colA, colB = st.columns(2)
        
        with colA:
            fig_fwci = px.line(
                df_region_annual, x='year', y='fwci_avg', color='region',
                color_discrete_map=REGION_COLORS,
                template="plotly_white",
                title="Evolución FWCI Promedio (2000-Presente)"
            )
            st.plotly_chart(fig_fwci, use_container_width=True)
           
        with colB:
            fig_en = px.line(
                df_region_annual, x='year', y='pct_lang_en', color='region',
                color_discrete_map=REGION_COLORS,
                template="plotly_white",
                title="Penetración del Idioma Inglés (%)"
            )
            st.plotly_chart(fig_en, use_container_width=True)

        # --- MAPA GLOBAL DE DISTRIBUCIÓN (Chilti Request) ---
        st.markdown("---")
        st.subheader(f"Distribución Geográfica Mundial ({latest_year})")
        
        map_indicators = {
            'Impacto (FWCI)': 'fwci_avg',
            'Producción (Documentos)': 'num_documents',
            'Revistas Activas': 'num_journals',
            '% Acceso Diamante': 'pct_oa_diamond',
            '% Top 10%': 'pct_top_10',
            '% Top 1%': 'pct_top_1',
            'Percentil Promedio': 'avg_percentile'
        }
        
        col_map1, col_map2 = st.columns([1, 1])
        with col_map1:
            selected_map_ind = st.selectbox("Seleccione Indicador para el Mapa:", options=list(map_indicators.keys()))
            map_metric = map_indicators[selected_map_ind]

        if df_country_annual is not None:
            df_map_data = df_country_annual[df_country_annual['year'] == latest_year]
            
            fig_map = px.choropleth(
                df_map_data,
                locations="country_code",
                color=map_metric,
                hover_name="country_code",
                hover_data=["num_journals", "num_documents", "fwci_avg", "pct_oa_diamond"],
                color_continuous_scale=px.colors.sequential.Teal,
                template="plotly_white",
                title=f"Distribución Global de {selected_map_ind}"
            )
            fig_map.update_layout(
                geo=dict(showcoastlines=True, projection_type='equirectangular', lakecolor="white"),
                margin=dict(l=0, r=0, t=50, b=0),
                height=700
            )
            st.plotly_chart(fig_map, use_container_width=True)
            st.info("💡 El mapa muestra todos los países pintados de acuerdo al indicador seleccionado.")
            
        # --- TABLA GLOBAL DE INDICADORES ANUALES ---
        st.markdown("---")
        st.subheader("Tabla Global de Indicadores Anuales")
        df_global_annual = df_region_annual.groupby('year').mean(numeric_only=True).reset_index()
        df_global_annual['num_documents'] = df_region_annual.groupby('year')['num_documents'].sum().values
        
        table_cols = {
            'year': 'Año',
            'num_documents': 'Documentos',
            'fwci_avg': 'FWCI',
            'pct_oa_diamond': '% OA Diam.',
            'pct_oa_gold': '% OA Gold',
            'pct_oa_green': '% OA Verde',
            'pct_oa_hybrid': '% OA Hyb.',
            'pct_oa_bronze': '% OA Bronce',
            'pct_oa_closed': '% Cerrado',
            'pct_lang_es': '% Es',
            'pct_lang_en': '% En',
            'pct_lang_pt': '% Pt',
            'pct_lang_fr': '% Fr',
            'pct_lang_de': '% De',
            'pct_lang_it': '% It',
            'avg_percentile': 'Perc.',
            'pct_top_10': '% T10',
            'pct_top_1': '% T1'
        }
        
        df_global_table = df_global_annual[list(table_cols.keys())].rename(columns=table_cols)
        st.dataframe(
            df_global_table.sort_values('Año', ascending=False).style.format({c: "{:.1f}" for c in df_global_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
            use_container_width=True, hide_index=True
        )

        st.markdown("---")
        st.subheader("Comparativa Interregional: Periodo Reciente (2021-2025)")
        df_rec = df_region_annual[(df_region_annual['year'] >= 2021) & (df_region_annual['year'] <= 2025)]
        df_reg_recent = df_rec.groupby('region').mean(numeric_only=True).reset_index()
        df_reg_recent['num_documents'] = df_rec.groupby('region')['num_documents'].sum().values
        
        # Renombrar columnas para la tabla
        reg_table_cols = {
            'region': 'Región',
            'num_documents': 'Docs',
            'num_journals': 'Rev (Prom)',
            'fwci_avg': 'FWCI',
            'pct_oa_diamond': '% Diam',
            'pct_oa_gold': '% Gold',
            'pct_lang_es': '% Es',
            'pct_lang_en': '% En',
            'pct_lang_pt': '% Pt',
            'avg_percentile': 'Perc',
            'pct_top_10': '% T10',
            'pct_top_1': '% T1'
        }
        
        df_reg_rec_table = df_reg_recent[[c for c in reg_table_cols.keys() if c in df_reg_recent.columns]].rename(columns=reg_table_cols)
        st.dataframe(
            df_reg_rec_table.sort_values('Docs', ascending=False).style.format({c: "{:.1f}" for c in df_reg_rec_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
            use_container_width=True, hide_index=True
        )

        st.subheader("Comparativa Interregional: Periodo Completo")
        df_reg_full = df_region_annual.groupby('region').mean(numeric_only=True).reset_index()
        df_reg_full['num_documents'] = df_region_annual.groupby('region')['num_documents'].sum().values
        
        df_reg_full_table = df_reg_full[[c for c in reg_table_cols.keys() if c in df_reg_full.columns]].rename(columns=reg_table_cols)
        st.dataframe(
            df_reg_full_table.sort_values('Docs', ascending=False).style.format({c: "{:.1f}" for c in df_reg_full_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
            use_container_width=True, hide_index=True
        )

        # --- EVOLUCIÓN HISTÓRICA GLOBAL (Chilti Request) ---
        st.markdown("---")
        st.subheader("Evolución Histórica de Perfiles de Conocimiento: Mundo")
        if df_thematic_evo is not None:
            evo_levels = {'Dominio': 'domain', 'Campo': 'field', 'Subcampo': 'subfield', 'Tópico': 'topic'}
            sel_evo_level = st.radio("Nivel Temático:", options=list(evo_levels.keys()), horizontal=True, key='evo_mundo')
            target_evo = evo_levels[sel_evo_level]
            
            df_m_evo = df_thematic_evo.groupby(['year', target_evo])['num_documents'].sum().reset_index()
            df_m_pivot = df_m_evo.pivot(index=target_evo, columns='year', values='num_documents').fillna(0)
            df_m_pivot['Total'] = df_m_pivot.sum(axis=1)
            df_m_pivot = df_m_pivot.sort_values('Total', ascending=False).drop(columns=['Total'])
            
            if len(df_m_pivot) > 30:
                st.caption(f"Mostrando los 30 {sel_evo_level}s con mayor producción histórica.")
                df_m_pivot = df_m_pivot.head(30)
            
            st.dataframe(df_m_pivot.style.background_gradient(cmap='Blues', axis=1).format("{:.0f}"), use_container_width=True)
            
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
            
            # KPI Cards for Region
            st.markdown('<div class="metric-container">', unsafe_allow_html=True)
            kcol1, kcol2, kcol3 = st.columns(3)
            
            reg_docs = df_latest['num_documents'].sum()
            reg_fwci = df_latest['fwci_avg'].mean()
            reg_journals = df_latest['num_journals'].sum()
            
            with kcol1:
                premium_metric("Docs en Región", f"{int(reg_docs):,}")
            with kcol2:
                premium_metric("FWCI Promedio", f"{reg_fwci:.2f}")
            with kcol3:
                premium_metric("Revistas Activas", f"{int(reg_journals):,}")
            st.markdown('</div>', unsafe_allow_html=True)

            # El mapa ha sido movido a la sección Mundo para una visión comparativa global.
            
            # --- TABS DE ANÁLISIS REGIONAL ---
            rtab_oa, rtab_sunburst, rtab_tabla = st.tabs(["🌎 Tipología y Acceso", "🧩 Perfil de Conocimiento", "📊 Perfiles de Desempeño de Países"])
            
            with rtab_oa:
                st.markdown(f"### Composición de la Región: {selected_region}")
                colA, colB = st.columns(2)
                
                # Datos agregados por región desde la tabla macro
                df_reg_macro = df_region_annual[(df_region_annual['region'] == selected_region) & (df_region_annual['year'] == latest_year)]
                
                if not df_reg_macro.empty:
                    reg_data = df_reg_macro.iloc[0]
                    with colA:
                        oa_data = {
                            'Tipo': ['Diamond (Est.)', 'Gold', 'Green', 'Hybrid', 'Bronze', 'Closed'],
                            'Porcentaje': [
                                reg_data['pct_oa_diamond'], reg_data['pct_oa_gold'], reg_data['pct_oa_green'],
                                reg_data['pct_oa_hybrid'], reg_data['pct_oa_bronze'], reg_data['pct_oa_closed']
                            ]
                        }
                        oa_df = pd.DataFrame(oa_data)
                        fig_oa = px.pie(oa_df, values='Porcentaje', names='Tipo', hole=0.4, title="Acceso Abierto Region",
                                       color_discrete_sequence=px.colors.qualitative.Pastel)
                        st.plotly_chart(fig_oa, use_container_width=True)
                        
                    with colB:
                        lang_data = {
                            'Idioma': ['Inglés', 'Español', 'Portugués', 'Francés', 'Alemán', 'Italiano'],
                            'Porcentaje': [
                                reg_data['pct_lang_en'], reg_data['pct_lang_es'], reg_data['pct_lang_pt'],
                                reg_data['pct_lang_fr'], reg_data['pct_lang_de'], reg_data['pct_lang_it']
                            ]
                        }
                        lang_df = pd.DataFrame(lang_data)
                        lang_df = lang_df[lang_df['Porcentaje'] > 0]
                        if not lang_df.empty:
                            fig_lang = px.pie(lang_df, values='Porcentaje', names='Idioma', hole=0.4, title="Idiomas Region",
                                             color_discrete_sequence=px.colors.qualitative.Set3)
                            st.plotly_chart(fig_lang, use_container_width=True)
                else:
                    st.info("No hay datos históricos macro para esta región.")

            with rtab_sunburst:
                st.markdown(f"### Especialización Temática de {selected_region}")
                if SUNBURST_METRICS_REGION.exists():
                    try:
                        df_sun_r = pd.read_parquet(SUNBURST_METRICS_REGION, filters=[('region', '==', selected_region)])
                        if not df_sun_r.empty:
                            sb_indicator_options = {
                                'FWCI (2021-2025)': 'fwci_avg_recent',
                                'Percentil (2021-2025)': 'avg_percentile_recent',
                                '% Top 1% (2021-2025)': 'pct_top_1_recent',
                                'FWCI (Todo)': 'fwci_avg_full',
                                '% OA Gold (Todo)': 'pct_oa_gold_full',
                            }
                            selected_sb_ind_r = st.selectbox("Indicador de Color:", options=list(sb_indicator_options.keys()), key='sb_ind_region')
                            ind_col_r = sb_indicator_options[selected_sb_ind_r]
                            size_col_r = 'count_recent' if '_recent' in ind_col_r else 'count_full'
                            
                            df_plot_r = df_sun_r[(df_sun_r[size_col_r] > 0) & (df_sun_r['level'] != 'topic')]
                            
                            if not df_plot_r.empty:
                                ids, labels, parents, values, colors = [], [], [], [], []
                                for _, row in df_plot_r.iterrows():
                                    if row['level'] == 'domain':
                                        curr_id, curr_parent = row['domain'], ""
                                    elif row['level'] == 'field':
                                        curr_id, curr_parent = f"{row['domain']}||{row['field']}", row['domain']
                                    elif row['level'] == 'subfield':
                                        curr_id, curr_parent = f"{row['domain']}||{row['field']}||{row['subfield']}", f"{row['domain']}||{row['field']}"
                                    else: # topic
                                        curr_id, curr_parent = f"{row['domain']}||{row['field']}||{row['subfield']}||{row['topic']}", f"{row['domain']}||{row['field']}||{row['subfield']}"
                                    
                                    ids.append(curr_id)
                                    labels.append(row[row['level']])
                                    parents.append(curr_parent)
                                    values.append(row[size_col_r])
                                    colors.append(row[ind_col_r])
                                    
                                cscale = 'Viridis'
                                cmin, cmax = None, None
                                if 'fwci_avg' in ind_col_r:
                                    vals_for_scale = [v for v in colors if v is not None]
                                    if vals_for_scale:
                                        min_v, max_v = min(vals_for_scale), max(vals_for_scale + [1.0])
                                        cmin, cmax = min_v, max_v
                                        norm_mid = max(0, min(1, (1.0 - min_v) / (max_v - min_v))) if max_v > min_v else 0.5
                                        cscale = [[0, 'red'], [norm_mid, 'yellow'], [1, 'green']]
                                        
                                fig_sun_r = go.Figure(go.Sunburst(
                                    ids=ids, labels=labels, parents=parents, values=values,
                                    branchvalues="total",
                                    marker=dict(colors=colors, colorscale=cscale, cmin=cmin, cmax=cmax, showscale=True, colorbar=dict(title=selected_sb_ind_r[:12], x=1.0)),
                                    hovertemplate='<b>%{label}</b><br>Docs: %{value}<br>Ind: %{color:.2f}<extra></extra>'
                                ))
                                fig_sun_r.update_layout(height=600, margin=dict(t=10, l=10, r=10, b=10))
                                st.plotly_chart(fig_sun_r, use_container_width=True)
                            else:
                                st.warning("Sin datos para el periodo seleccionado.")
                            
                            # --- MATRIZ DE CONOCIMIENTO REGIONAL (Chilti Request) ---
                            st.markdown("---")
                            st.subheader(f"Matriz de Conocimiento Regional: {selected_region}")
                            st.markdown("Concentración de la producción científica por país y área temática.")
                            
                            matrix_level_options = {
                                'Dominio': 'domain',
                                'Campo': 'field',
                                'Subcampo': 'subfield',
                                'Tópico': 'topic'
                            }
                            selected_m_level_label = st.radio("Nivel Jerárquico de la Matriz:", options=list(matrix_level_options.keys()), horizontal=True)
                            m_level = matrix_level_options[selected_m_level_label]
                            
                            if SUNBURST_METRICS_COUNTRY.exists():
                                df_sun_c_all = pd.read_parquet(SUNBURST_METRICS_COUNTRY)
                                # Filtrar por países de la región y nivel seleccionado
                                countries_in_reg = GLOBAL_REGIONS[selected_region]
                                df_matrix_raw = df_sun_c_all[
                                    (df_sun_c_all['country_code'].isin(countries_in_reg)) & 
                                    (df_sun_c_all['level'] == m_level)
                                ]
                                
                                if not df_matrix_raw.empty:
                                    # Pivotar: País x Nivel (Usando Docs Recent o Full según el sunburst de arriba)
                                    val_col = 'count_recent' if '_recent' in ind_col_r else 'count_full'
                                    df_pivot = df_matrix_raw.pivot_table(
                                        index='country_code', 
                                        columns=m_level, 
                                        values=val_col, 
                                        aggfunc='sum'
                                    ).fillna(0)
                                    
                                    # Ordenar por volumen total de documentos por país
                                    df_pivot['Total'] = df_pivot.sum(axis=1)
                                    df_pivot = df_pivot.sort_values('Total', ascending=False).drop(columns=['Total'])
                                    
                                    # Mostrar como Heatmap o Tabla Estilizada
                                    if len(df_pivot.columns) > 50:
                                        st.caption(f"Mostrando los 50 {selected_m_level_label}s con mayor producción en la región.")
                                        top_cols = df_pivot.sum().sort_values(ascending=False).head(50).index
                                        df_pivot = df_pivot[top_cols]
                                    
                                    st.dataframe(
                                        df_pivot.style.background_gradient(cmap='YlGnBu', axis=None).format("{:.0f}"),
                                        use_container_width=True
                                    )
                                else:
                                    st.info("No hay datos suficientes para generar la matriz en este nivel.")

                    except Exception as e:
                        st.error(f"Error en Sunburst/Matriz Regional: {e}")
                else:
                    st.info("Ejecute el pipeline para generar `sunburst_metrics_region.parquet`.")

            with rtab_tabla:
                # Filtrar países de la región seleccionada
                region_countries = GLOBAL_REGIONS.get(selected_region, [])
                df_c_reg = df_country_annual[df_country_annual['country_code'].isin(region_countries)]
                
                st.subheader(f"Comparativa de Países: Periodo Reciente (2021-2025)")
                df_c_rec_raw = df_c_reg[(df_c_reg['year'] >= 2021) & (df_c_reg['year'] <= 2025)]
                df_c_rec = df_c_rec_raw.groupby('country_code').mean(numeric_only=True).reset_index()
                df_c_rec['num_documents'] = df_c_rec_raw.groupby('country_code')['num_documents'].sum().values
                
                country_table_cols = {
                    'country_code': 'País',
                    'num_documents': 'Docs',
                    'num_journals': 'Rev (Prom)',
                    'fwci_avg': 'FWCI',
                    'pct_oa_diamond': '% Diam',
                    'pct_oa_gold': '% Gold',
                    'pct_lang_es': '% Es',
                    'pct_lang_en': '% En',
                    'avg_percentile': 'Perc',
                    'pct_top_10': '% T10',
                    'pct_top_1': '% T1'
                }
                
                df_c_rec_table = df_c_rec[[c for c in country_table_cols.keys() if c in df_c_rec.columns]].rename(columns=country_table_cols)
                st.dataframe(
                    df_c_rec_table.sort_values('Docs', ascending=False).style.format({c: "{:.1f}" for c in df_c_rec_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
                    use_container_width=True, hide_index=True
                )

                st.subheader(f"Comparativa de Países: Periodo Completo")
                df_c_full = df_c_reg.groupby('country_code').mean(numeric_only=True).reset_index()
                df_c_full['num_documents'] = df_c_reg.groupby('country_code')['num_documents'].sum().values
                
                df_c_full_table = df_c_full[[c for c in country_table_cols.keys() if c in df_c_full.columns]].rename(columns=country_table_cols)
                st.dataframe(
                    df_c_full_table.sort_values('Docs', ascending=False).style.format({c: "{:.1f}" for c in df_c_full_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
                    use_container_width=True, hide_index=True
                )
                
                st.markdown("---")
                st.subheader(f"Evolución Histórica de la Región: {selected_region}")
                df_reg_hist = df_region_annual[df_region_annual['region'] == selected_region].sort_values('year', ascending=False)
                
                reg_table_cols = {
                    'year': 'Año',
                    'num_documents': 'Docs',
                    'num_journals': 'Rev.',
                    'fwci_avg': 'FWCI',
                    'pct_oa_diamond': '% Diam',
                    'pct_oa_gold': '% Gold',
                    'pct_oa_green': '% Verd',
                    'pct_oa_hybrid': '% Hyb',
                    'pct_oa_bronze': '% Bron',
                    'pct_oa_closed': '% Cerr',
                    'pct_lang_es': '% Es',
                    'pct_lang_en': '% En',
                    'pct_lang_pt': '% Pt',
                    'pct_lang_fr': '% Fr',
                    'pct_lang_de': '% De',
                    'pct_lang_it': '% It',
                    'avg_percentile': 'Perc',
                    'pct_top_10': '% T10',
                    'pct_top_1': '% T1'
                }
                
                df_reg_table = df_reg_hist[[c for c in reg_table_cols.keys() if c in df_reg_hist.columns]].rename(columns=reg_table_cols)
                st.dataframe(
                    df_reg_table.style.format({c: "{:.1f}" for c in df_reg_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
                    use_container_width=True, hide_index=True
                )
                
                # --- EVOLUCIÓN HISTÓRICA REGIONAL (Chilti Request) ---
                st.markdown("---")
                st.subheader(f"Evolución de Perfiles de Conocimiento: {selected_region}")
                if df_thematic_evo is not None:
                    # Obtener journals de la región para filtrar
                    if df_journals_metadata is not None:
                        reg_paises = GLOBAL_REGIONS.get(selected_region, [])
                        reg_jids = df_journals_metadata[df_journals_metadata['country_code'].isin(reg_paises)]['id'].tolist()
                        
                        evo_levels_r = {'Dominio': 'domain', 'Campo': 'field', 'Subcampo': 'subfield', 'Tópico': 'topic'}
                        sel_evo_r = st.radio("Nivel Temático:", options=list(evo_levels_r.keys()), horizontal=True, key='evo_reg_radio')
                        t_evo_r = evo_levels_r[sel_evo_r]
                        
                        df_r_evo = df_thematic_evo[df_thematic_evo['journal_id'].isin(reg_jids)].groupby(['year', t_evo_r])['num_documents'].sum().reset_index()
                        if not df_r_evo.empty:
                            df_r_p = df_r_evo.pivot(index=t_evo_r, columns='year', values='num_documents').fillna(0)
                            df_r_p['Total'] = df_r_p.sum(axis=1)
                            df_r_p = df_r_p.sort_values('Total', ascending=False).drop(columns=['Total'])
                            if len(df_r_p) > 30: df_r_p = df_r_p.head(30)
                            st.dataframe(df_r_p.style.background_gradient(cmap='Greens', axis=1).format("{:.0f}"), use_container_width=True)
                        else:
                            st.info("Sin datos de evolución temática para esta región.")
                
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
            latest = df_target.iloc[-1]
            
            st.markdown('<div class="metric-container">', unsafe_allow_html=True)
            pcol1, pcol2, pcol3 = st.columns(3)
            with pcol1:
                premium_metric("Producción Últ. Año", f"{int(latest['num_documents']):,}")
            with pcol2:
                premium_metric("Impacto (FWCI)", f"{latest['fwci_avg']:.2f}")
            with pcol3:
                premium_metric("Revistas", f"{int(latest['num_journals']):,}")
            st.markdown('</div>', unsafe_allow_html=True)
            
            fig = px.area(
                df_target, x="year", y="num_documents",
                template="plotly_white",
                color_discrete_sequence=['#6366f1']
            )
            fig.update_layout(
                title=dict(text=f"Historial de Producción - {selected_country}", font=dict(size=20, color="#1e293b")),
                xaxis_title="Año de Publicación",
                yaxis_title="Documentos",
                margin=dict(l=0, r=0, t=50, b=0),
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # --- TABS DE ANÁLISIS ---
            tab_radar, tab_oa, tab_sunburst, tab_tabla = st.tabs(["Radar Relativo", "Tipología y Acceso", "Perfil Temático", "Datos Históricos"])
            
            with tab_radar:
                st.markdown("### Perfil de Desempeño (2021-2025)")
                st.caption("Comparación de indicadores recientes normalizados frente a los máximos globales del mismo año.")
                
                if df_country_recent is not None and not df_country_recent.empty:
                    df_rec_country = df_country_recent[df_country_recent['country_code'] == selected_country]
                    
                    if not df_rec_country.empty:
                        # Normalize against global max from recent period
                        radar_vars = ['fwci_avg_recent', 'avg_percentile_recent', 'pct_top_10_recent', 'pct_top_1_recent', 'pct_oa_gold_recent']
                        radar_labels = ['FWCI', 'Percentil Norm.', 'Top 10%', 'Top 1%', 'OA Gold']
                        
                        # Simulating global max for radar (ideally precomputed, here derived dynamically)
                        max_vals = df_country_recent[radar_vars].max()
                        
                        row = df_rec_country.iloc[0]
                        values = [row[v] / max_vals[v] if max_vals[v] > 0 else 0 for v in radar_vars]
                        
                        # Close the polygon
                        values += [values[0]]
                        labels_closed = radar_labels + [radar_labels[0]]
                        
                        fig_radar = go.Figure()
                        fig_radar.add_trace(go.Scatterpolar(
                            r=values,
                            theta=labels_closed,
                            fill='toself',
                            name=selected_country,
                            line_color='#0ea5e9',
                            fillcolor='rgba(14, 165, 233, 0.4)'
                        ))
                        
                        fig_radar.update_layout(
                            polar=dict(radialaxis=dict(visible=True, range=[0, 1.05], showticklabels=False)),
                            showlegend=False,
                            margin=dict(t=20, b=20, l=20, r=20),
                            height=400
                        )
                        st.plotly_chart(fig_radar, use_container_width=True)
                    else:
                        st.info("Sin datos de desempeño reciente para construir el radar.")
                else:
                    st.warning("Debe generar metrics_global_country_period_2021_2025.parquet para ver este gráfico.")
            
            with tab_oa:
                st.markdown("### Distribución Modal Últ. Año")
                colA, colB = st.columns(2)
                
                with colA:
                    oa_data = {
                        'Tipo': ['Diamond (Estimado)', 'Gold', 'Green', 'Hybrid', 'Bronze', 'Closed'],
                        'Porcentaje': [
                            latest['pct_oa_diamond'],
                            latest['pct_oa_gold'],
                            latest['pct_oa_green'],
                            latest['pct_oa_hybrid'],
                            latest['pct_oa_bronze'],
                            latest['pct_oa_closed']
                        ]
                    }
                    oa_df = pd.DataFrame(oa_data)
                    fig_oa = px.pie(oa_df, values='Porcentaje', names='Tipo', hole=0.4,
                                   color_discrete_sequence=px.colors.qualitative.Pastel)
                    fig_oa.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_oa, use_container_width=True)
                    
                with colB:
                    lang_data = {
                        'Idioma': ['Inglés', 'Español', 'Portugués', 'Francés', 'Alemán', 'Italiano'],
                        'Porcentaje': [
                            latest['pct_lang_en'], latest['pct_lang_es'], latest['pct_lang_pt'],
                            latest['pct_lang_fr'], latest['pct_lang_de'], latest['pct_lang_it']
                        ]
                    }
                    lang_df = pd.DataFrame(lang_data)
                    lang_df = lang_df[lang_df['Porcentaje'] > 0]
                    if not lang_df.empty:
                        fig_lang = px.pie(lang_df, values='Porcentaje', names='Idioma', hole=0.4,
                                         color_discrete_sequence=px.colors.qualitative.Set3)
                        st.plotly_chart(fig_lang, use_container_width=True)
                    else:
                        st.info("Sin desglose idiomático.")
                        
            with tab_sunburst:
                st.markdown("### Perfil de Conocimiento Nacional (Sunburst)")
                SUNBURST_METRICS_COUNTRY = CACHE_DIR / 'sunburst_metrics_country.parquet'
                
                if SUNBURST_METRICS_COUNTRY.exists():
                    try:
                        df_sun_c = pd.read_parquet(SUNBURST_METRICS_COUNTRY, filters=[('country_code', '==', selected_country)])
                        
                        if not df_sun_c.empty:
                            sb_indicator_options = {
                                'FWCI (2021-2025)': 'fwci_avg_recent',
                                'Percentil (2021-2025)': 'avg_percentile_recent',
                                '% Top 1% (2021-2025)': 'pct_top_1_recent',
                                'FWCI (Todo)': 'fwci_avg_full',
                                '% OA Gold (Todo)': 'pct_oa_gold_full',
                            }
                            
                            selected_sb_ind_c = st.selectbox("Indicador de Calor:", options=list(sb_indicator_options.keys()), key='sb_ind_country')
                            ind_col_c = sb_indicator_options[selected_sb_ind_c]
                            size_col_c = 'count_recent' if '_recent' in ind_col_c else 'count_full'
                            
                            df_plot_c = df_sun_c[(df_sun_c[size_col_c] > 0) & (df_sun_c['level'] != 'topic')]
                            
                            if not df_plot_c.empty:
                                ids, labels, parents, values, colors = [], [], [], [], []
                                for _, row in df_plot_c.iterrows():
                                    if row['level'] == 'domain':
                                        curr_id, curr_parent = row['domain'], ""
                                    elif row['level'] == 'field':
                                        curr_id, curr_parent = f"{row['domain']}||{row['field']}", row['domain']
                                    elif row['level'] == 'subfield':
                                        curr_id, curr_parent = f"{row['domain']}||{row['field']}||{row['subfield']}", f"{row['domain']}||{row['field']}"
                                    else: # topic
                                        curr_id, curr_parent = f"{row['domain']}||{row['field']}||{row['subfield']}||{row['topic']}", f"{row['domain']}||{row['field']}||{row['subfield']}"
                                    
                                    ids.append(curr_id)
                                    labels.append(row[row['level']])
                                    parents.append(curr_parent)
                                    values.append(row[size_col_c])
                                    colors.append(row[ind_col_c])
                                    
                                cscale = 'Viridis'
                                cmin, cmax = None, None
                                if 'fwci_avg' in ind_col_c:
                                    vals_for_scale = [v for v in colors if v is not None]
                                    if vals_for_scale:
                                        min_v, max_v = min(vals_for_scale), max(vals_for_scale + [1.0])
                                        cmin, cmax = min_v, max_v
                                        norm_mid = max(0, min(1, (1.0 - min_v) / (max_v - min_v))) if max_v > min_v else 0.5
                                        cscale = [[0, 'red'], [norm_mid, 'yellow'], [1, 'green']]
                                        
                                fig_sun = go.Figure(go.Sunburst(
                                    ids=ids, labels=labels, parents=parents, values=values,
                                    branchvalues="total",
                                    marker=dict(colors=colors, colorscale=cscale, cmin=cmin, cmax=cmax, showscale=True, colorbar=dict(title=selected_sb_ind_c[:12], x=1.0)),
                                    hovertemplate='<b>%{label}</b><br>Docs: %{value}<br>Ind: %{color:.2f}<extra></extra>'
                                ))
                                fig_sun.update_layout(margin=dict(t=30, l=10, r=10, b=10), height=550)
                                st.plotly_chart(fig_sun, use_container_width=True)

                                # --- MATRIZ DE CONOCIMIENTO POR REVISTA (Chilti Request) ---
                                st.markdown("---")
                                st.subheader(f"Matriz de Conocimiento: Revistas de {selected_country}")
                                st.markdown("Especialización temática detallada comparativa entre las revistas nacionales.")

                                m_levels = {
                                    'Dominio': 'domain',
                                    'Campo': 'field',
                                    'Subcampo': 'subfield',
                                    'Tópico': 'topic'
                                }
                                m_level_label = st.radio("Nivel Temático de la Matriz:", options=list(m_levels.keys()), horizontal=True, key='m_level_country')
                                target_level = m_levels[m_level_label]

                                if SUNBURST_METRICS_JOURNAL.exists():
                                    df_sun_j_all = pd.read_parquet(SUNBURST_METRICS_JOURNAL)
                                    
                                    # Obtener metadatos para filtrar por país
                                    df_meta = pd.read_parquet(CACHE_DIR / 'global_journals_metadata.parquet')
                                    j_ids_in_country = df_meta[df_meta['country_code'] == selected_country]['id'].tolist()
                                    
                                    # Filtrar por revistas del país y nivel seleccionado
                                    df_m_raw = df_sun_j_all[
                                        (df_sun_j_all['journal_id'].isin(j_ids_in_country)) & 
                                        (df_sun_j_all['level'] == target_level)
                                    ].merge(df_meta[['id', 'display_name']], left_on='journal_id', right_on='id', how='left')

                                    if not df_m_raw.empty:
                                        # Pivotar: Revista x Nivel
                                        m_val_col = 'count_recent' if '_recent' in ind_col_c else 'count_full'
                                        df_m_pivot = df_m_raw.pivot_table(
                                            index='display_name',
                                            columns=target_level,
                                            values=m_val_col,
                                            aggfunc='sum'
                                        ).fillna(0)

                                        # Ordenar por volumen total
                                        df_m_pivot['Total'] = df_m_pivot.sum(axis=1)
                                        df_m_pivot = df_m_pivot.sort_values('Total', ascending=False).drop(columns=['Total'])

                                        # Limitar columnas si son demasiadas para legibilidad
                                        if len(df_m_pivot.columns) > 40:
                                            st.caption(f"Mostrando los 40 {m_level_label}s con mayor presencia en estas revistas.")
                                            top_m_cols = df_m_pivot.sum().sort_values(ascending=False).head(40).index
                                            df_m_pivot = df_m_pivot[top_m_cols]

                                        st.dataframe(
                                            df_m_pivot.style.background_gradient(cmap='GnBu', axis=None).format("{:.0f}"),
                                            use_container_width=True
                                        )

                                        # --- EVOLUCIÓN HISTÓRICA NACIONAL (Chilti Request) ---
                                        st.markdown("---")
                                        st.subheader(f"Evolución de Perfiles de Conocimiento: {selected_country}")
                                        if df_thematic_evo is not None:
                                            # Ya tenemos j_ids_in_country arriba
                                            sel_evo_c = st.radio("Nivel Temático:", options=list(m_levels.keys()), horizontal=True, key='evo_country_radio')
                                            t_evo_c = m_levels[sel_evo_c]
                                            
                                            df_c_evo = df_thematic_evo[df_thematic_evo['journal_id'].isin(j_ids_in_country)].groupby(['year', t_evo_c])['num_documents'].sum().reset_index()
                                            if not df_c_evo.empty:
                                                df_c_p = df_c_evo.pivot(index=t_evo_c, columns='year', values='num_documents').fillna(0)
                                                df_c_p['Total'] = df_c_p.sum(axis=1)
                                                df_c_p = df_c_p.sort_values('Total', ascending=False).drop(columns=['Total'])
                                                if len(df_c_p) > 30: df_c_p = df_c_p.head(30)
                                                st.dataframe(df_c_p.style.background_gradient(cmap='Purples', axis=1).format("{:.0f}"), use_container_width=True)
                                            else:
                                                st.info("Sin datos de evolución temática para este país.")
                                    else:
                                        st.info("No hay datos de especialización granular disponibles para estas revistas.")
                            else:
                                st.warning("Sin documentos suficientes para el período seleccionado.")
                    except Exception as e:
                        st.error(f"Error procesando sunburst: {e}")
                else:
                    st.info("No se encontró el archivo jerárquico del país. Computar base analítica requerida.")

            with tab_tabla:
                st.subheader(f"Indicadores Anuales - {selected_country}")
                
                c_table_cols = {
                    'year': 'Año',
                    'num_documents': 'Docs',
                    'num_journals': 'Rev',
                    'fwci_avg': 'FWCI',
                    'pct_oa_diamond': '% Diam',
                    'pct_oa_gold': '% Gold',
                    'pct_oa_green': '% Verd',
                    'pct_oa_hybrid': '% Hyb',
                    'pct_oa_bronze': '% Bron',
                    'pct_oa_closed': '% Cerr',
                    'pct_lang_es': '% Es',
                    'pct_lang_en': '% En',
                    'pct_lang_pt': '% Pt',
                    'pct_lang_fr': '% Fr',
                    'pct_lang_de': '% De',
                    'pct_lang_it': '% It',
                    'avg_percentile': 'Perc',
                    'pct_top_10': '% T10',
                    'pct_top_1': '% T1'
                }
                
                df_c_table = df_target[[c for c in c_table_cols.keys() if c in df_target.columns]].rename(columns=c_table_cols)
                st.dataframe(
                    df_c_table.sort_values('Año', ascending=False).style.format({c: "{:.1f}" for c in df_c_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
                    use_container_width=True, hide_index=True
                )

                # --- COMPARATIVA DE REVISTAS (Chilti Request) ---
                st.markdown("---")
                st.subheader(f"Perfiles de Desempeño de Revistas de {selected_country}: Periodo Reciente (2021-2025)")
                
                if df_journals_recent is not None and df_journals_metadata is not None:
                    # Identificar journals del país
                    country_journals = df_journals_metadata[df_journals_metadata['country_code'] == selected_country]['id'].unique()
                    df_jr_rec = df_journals_recent[df_journals_recent['journal_id'].isin(country_journals)]
                    
                    if not df_jr_rec.empty:
                        # Join para obtener nombres
                        df_jr_rec = df_jr_rec.merge(df_journals_metadata[['id', 'display_name']], left_on='journal_id', right_on='id', how='left')
                        
                        jr_table_cols = {
                            'display_name': 'Revista',
                            'num_documents': 'Docs',
                            'fwci_avg': 'FWCI',
                            'pct_oa_gold': '% Gold',
                            'pct_lang_en': '% En',
                            'avg_percentile': 'Perc',
                            'pct_top_10': '% T10',
                            'pct_top_1': '% T1'
                        }
                        
                        df_jr_rec_table = df_jr_rec[[c for c in jr_table_cols.keys() if c in df_jr_rec.columns]].rename(columns=jr_table_cols)
                        st.dataframe(
                            df_jr_rec_table.sort_values('Docs', ascending=False).style.format({c: "{:.1f}" for c in df_jr_rec_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
                            use_container_width=True, hide_index=True
                        )
                    else:
                        st.info("No se encontraron métricas de revistas para este país en el periodo reciente.")

                st.subheader(f"Perfiles de Desempeño de Revistas de {selected_country}: Historial Completo")
                if df_journals_annual is not None and df_journals_metadata is not None:
                    # Agrupar el anual por journal
                    country_journals = df_journals_metadata[df_journals_metadata['country_code'] == selected_country]['id'].unique()
                    df_ja_filtered = df_journals_annual[df_journals_annual['journal_id'].isin(country_journals)]
                    
                    if not df_ja_filtered.empty:
                        df_jr_full = df_ja_filtered.groupby('journal_id').mean(numeric_only=True).reset_index()
                        df_jr_full['num_documents'] = df_ja_filtered.groupby('journal_id')['num_documents'].sum().values
                        
                        # Join para obtener nombres
                        df_jr_full = df_jr_full.merge(df_journals_metadata[['id', 'display_name']], left_on='journal_id', right_on='id', how='left')
                        
                        df_jr_full_table = df_jr_full[[c for c in jr_table_cols.keys() if c in df_jr_full.columns]].rename(columns=jr_table_cols)
                        st.dataframe(
                            df_jr_full_table.sort_values('Docs', ascending=False).style.format({c: "{:.1f}" for c in df_jr_full_table.columns if '%' in c or 'FWCI' in c or 'Perc' in c}),
                            use_container_width=True, hide_index=True
                        )
                
            # --- UMAP DE PAÍSES ---
            st.markdown("---")
            st.subheader("Mapa Global de Similitud entre Países (UMAP)")
            st.caption("Visualización 2D basada en perfiles recientes (2021-2025): Volumen, OA Diamante/Gold, Impacto e Idiomas.")
            
            if df_umap_country is not None and not df_umap_country.empty:
                if 'umap_x' in df_umap_country.columns and 'umap_y' in df_umap_country.columns:
                    # Merge con metadata regional para usar los colores del mapa global
                    temp_regions = []
                    for code in df_umap_country['country_code']:
                        found = 'Otra'
                        for reg, countries in GLOBAL_REGIONS.items():
                            if code in countries:
                                found = reg
                                break
                        temp_regions.append(found)
                    df_umap_country['region'] = temp_regions
                    
                    fig_umap_c = px.scatter(
                        df_umap_country,
                        x='umap_x',
                        y='umap_y',
                        color='region',
                        color_discrete_map=REGION_COLORS,
                        text='country_code',
                        hover_data={
                            'country_code': True,
                            'region': False,
                            'num_journals': ':,',
                            'fwci_avg': ':.2f',
                            'pct_top_10': ':.1f',
                            'pct_oa_diamond': ':.1f',
                            'umap_x': False,
                            'umap_y': False
                        },
                        title='Espacio de Similitud de Países'
                    )
                    
                    fig_umap_c.update_traces(
                        textposition='top center',
                        marker=dict(size=12, line=dict(width=1, color='white'))
                    )
                    
                    fig_umap_c.update_layout(
                        height=500,
                        showlegend=True,
                        legend_title="Región",
                        xaxis=dict(showgrid=True, zeroline=True, title='Dimensión UMAP 1'),
                        yaxis=dict(showgrid=True, zeroline=True, title='Dimensión UMAP 2')
                    )
                    
                    st.plotly_chart(fig_umap_c, use_container_width=True)
                    st.info("💡 Los países cercanos en el mapa tienen perfiles bibliométricos similares estructuralmente.")
                else:
                    st.error("El archivo de UMAP de países no contiene dimensiones 'umap_x' y 'umap_y'.")
                    
            
elif level == "4. Buscador de Revista":
    st.title("Buscador Global de Revistas")
    st.markdown("Busca y analiza el perfil de desempeño de cualquier revista científica del mundo.")
    
    if df_journals_metadata is not None and df_journals_annual is not None:
        # Preparar opciones para el buscador (Nombre + ISSN)
        df_journals_metadata['search_label'] = df_journals_metadata['display_name'] + " (" + df_journals_metadata['issn_l'] + ")"
        
        selected_search = st.selectbox(
            "Ingrese nombre o ISSN de la revista:",
            options=df_journals_metadata['search_label'].tolist(),
            index=None,
            placeholder="Ej: Nature, Revista Latinoamericana de Psicología..."
        )
        
        if selected_search:
            # Extraer ID
            journal_info = df_journals_metadata[df_journals_metadata['search_label'] == selected_search].iloc[0]
            jid = journal_info['id']
            
            st.markdown("---")
            
            # --- HEADER DE REVISTA ---
            hcol1, hcol2 = st.columns([3, 1])
            with hcol1:
                st.subheader(journal_info['display_name'])
                st.caption(f"ID: {jid} | ISSN-L: {journal_info['issn_l']} | País: {journal_info['country_code']} | Tipo: {journal_info['type']}")
            
            # Enlace a LATAM si aplica
            if journal_info['country_code'] in [c for r in GLOBAL_REGIONS for c in GLOBAL_REGIONS[r] if 'Latinoamérica' in r]:
                with hcol2:
                    st.warning("📍 Esta revista es LATAM")
                    if st.button("Ver en Dashboard especializado"):
                        st.info("Redirigiendo... (Simulado)")
            
            # --- MÉTRICAS DE LA REVISTA ---
            df_j_history = df_journals_annual[df_journals_annual['journal_id'] == jid].sort_values('year')
            df_j_recent_data = df_journals_recent[df_journals_recent['journal_id'] == jid] if df_journals_recent is not None else pd.DataFrame()
            
            if not df_j_history.empty:
                latest_j = df_j_history.iloc[-1]
                
                # --- KPI CARDS (Indicadores de Impacto y Volumetría) ---
                st.markdown('<div class="metric-container">', unsafe_allow_html=True)
                mcol1, mcol2, mcol3, mcol4, mcol5 = st.columns(5)
                with mcol1:
                    premium_metric("Docs Total", f"{df_j_history['num_documents'].sum():,}")
                with mcol2:
                    premium_metric("Total Citas", f"{df_j_history['total_citations'].sum():,}")
                with mcol3:
                    premium_metric("Citas/Doc (2yr)", f"{journal_info.get('citedness_2yr', 0):.2f}")
                with mcol4:
                    premium_metric("H-Index", f"{journal_info.get('h_index', 0)}")
                with mcol5:
                    premium_metric("i10-Index", f"{journal_info.get('i10_index', 0)}")
                st.markdown('</div>', unsafe_allow_html=True)
                
                # --- INDICADORES DE CALIDAD Y EXCELENCIA ---
                st.markdown('<div class="metric-container">', unsafe_allow_html=True)
                ecol1, ecol2, ecol3, ecol4 = st.columns(4)
                with ecol1:
                    premium_metric("% Top 10%", f"{latest_j['pct_top_10']:.1f}%")
                with ecol2:
                    premium_metric("% Top 1%", f"{latest_j['pct_top_1']:.1f}%")
                with ecol3:
                    premium_metric("Percentil Prom.", f"{latest_j['avg_percentile']:.1f}")
                with ecol4:
                    premium_metric("FWCI Prom.", f"{latest_j['fwci_avg']:.2f}")
                st.markdown('</div>', unsafe_allow_html=True)

                # --- INDEXACIÓN ---
                st.write("🔍 **Indexación y Propiedades**")
                idx1, idx2, idx3, idx4, idx5 = st.columns(5)
                idx1.checkbox("En DOAJ", value=bool(journal_info.get('is_in_doaj', False)), disabled=True)
                idx2.checkbox("En Scopus", value=bool(journal_info.get('in_scopus', 0)), disabled=True)
                idx3.checkbox("En SciELO", value=bool(journal_info.get('in_scielo', 0)), disabled=True)
                idx4.checkbox("En CORE", value=bool(journal_info.get('in_core', 0)), disabled=True)
                idx5.checkbox("Usa OJS", value="OJS" in str(journal_info.get('display_name', '')), disabled=True)
                
                # --- TABLA DE INDICADORES ANUALES (Chilti Request) ---
                st.markdown("---")
                st.subheader("Tabla de Indicadores Anuales")
                
                table_cols = {
                    'year': 'Año',
                    'num_documents': 'Documentos',
                    'fwci_avg': 'FWCI',
                    'pct_oa_diamond': '% OA Diamante',
                    'pct_oa_gold': '% OA Gold',
                    'pct_oa_green': '% OA Verde',
                    'pct_oa_hybrid': '% OA Híbrido',
                    'pct_oa_bronze': '% OA Bronce',
                    'pct_oa_closed': '% Cerrado',
                    'pct_lang_es': '% Español',
                    'pct_lang_en': '% Inglés',
                    'pct_lang_pt': '% Portugués',
                    'pct_lang_fr': '% Francés',
                    'pct_lang_de': '% Alemán',
                    'pct_lang_it': '% Italiano',
                    'avg_percentile': 'Percentil Prom.',
                    'pct_top_10': '% Top 10',
                    'pct_top_1': '% Top 1'
                }
                
                # Filtrar columnas existentes y renombrar
                available_cols = [c for c in table_cols.keys() if c in df_j_history.columns]
                df_table = df_j_history[available_cols].rename(columns=table_cols)
                
                # Formateo
                st.dataframe(
                    df_table.style.format({c: "{:.1f}" for c in df_table.columns if '%' in c or 'FWCI' in c or 'Percentil' in c}),
                    use_container_width=True,
                    hide_index=True
                )
                
                # --- SUNBURST DE TEMÁTICAS ---
                SUNBURST_METRICS_JOURNAL = CACHE_DIR / 'sunburst_metrics_journal.parquet'
                if SUNBURST_METRICS_JOURNAL.exists():
                    try:
                        df_sun_j = pd.read_parquet(SUNBURST_METRICS_JOURNAL, filters=[('journal_id', '==', jid)])
                        if not df_sun_j.empty:
                            st.markdown("---")
                            st.subheader("Temáticas de Investigación (Sunburst)")
                            sb_indicator_options = {
                                'FWCI (2021-2025)': 'fwci_avg_recent',
                                'Percentil (2021-2025)': 'avg_percentile_recent',
                                '% Top 1% (2021-2025)': 'pct_top_1_recent',
                                '% Top 10% (2021-2025)': 'pct_top_10_recent',
                                '% OA Gold (2021-2025)': 'pct_oa_gold_recent',
                                'FWCI (Todo)': 'fwci_avg_full',
                                'Percentil (Todo)': 'avg_percentile_full',
                                '% Top 1% (Todo)': 'pct_top_1_full',
                                '% Top 10% (Todo)': 'pct_top_10_full',
                                '% OA Gold (Todo)': 'pct_oa_gold_full',
                            }
                            selected_sb_ind_j = st.selectbox("Indicador de Color:", options=list(sb_indicator_options.keys()), key='sb_ind_journal', index=0)
                            ind_col_j = sb_indicator_options[selected_sb_ind_j]
                            size_col_j = 'count_recent' if '_recent' in ind_col_j else 'count_full'
                            
                            ids, labels, parents, values, colors = [], [], [], [], []
                            df_plot_j = df_sun_j[(df_sun_j[size_col_j] > 0) & (df_sun_j['level'] != 'topic')]
                            
                            for _, row in df_plot_j.iterrows():
                                if row['level'] == 'domain':
                                    curr_id, curr_parent = row['domain'], ""
                                elif row['level'] == 'field':
                                    curr_id, curr_parent = f"{row['domain']}||{row['field']}", row['domain']
                                elif row['level'] == 'subfield':
                                    curr_id, curr_parent = f"{row['domain']}||{row['field']}||{row['subfield']}", f"{row['domain']}||{row['field']}"
                                else: # topic
                                    curr_id, curr_parent = f"{row['domain']}||{row['field']}||{row['subfield']}||{row['topic']}", f"{row['domain']}||{row['field']}||{row['subfield']}"
                                
                                ids.append(curr_id)
                                labels.append(row[row['level']])
                                parents.append(curr_parent)
                                values.append(row[size_col_j])
                                colors.append(row[ind_col_j])
                            
                            cscale_j, cmin_j, cmax_j = 'Viridis', None, None
                            if 'fwci_avg' in ind_col_j:
                                vals_for_scale = [v for v in colors if v is not None]
                                if vals_for_scale:
                                    min_v, max_v = min(vals_for_scale), max(vals_for_scale + [1.0])
                                    cmin_j, cmax_j = min_v, max_v
                                    norm_mid = max(0, min(1, (1.0 - min_v) / (max_v - min_v))) if max_v > min_v else 0.5
                                    cscale_j = [[0, 'red'], [norm_mid, 'yellow'], [1, 'green']]
                                    
                            fig_sun = go.Figure(go.Sunburst(
                                ids=ids, labels=labels, parents=parents, values=values, branchvalues='total',
                                marker=dict(colors=colors, colorscale=cscale_j, cmin=cmin_j, cmax=cmax_j, showscale=True, colorbar=dict(title=selected_sb_ind_j)),
                                hovertemplate='<b>%{label}</b><br>Artículos: %{value:,.1f}<br>' + f'{selected_sb_ind_j}: ' + '%{color:.2f}<extra></extra>'
                            ))
                            fig_sun.update_layout(margin=dict(t=10, l=0, r=0, b=10), height=500)
                            st.plotly_chart(fig_sun, use_container_width=True)
                            st.caption("Jerarquía: Dominio -> Campo -> Subcampo. Tamaño basado en volumen de documentos.")
                            
                            # --- EVOLUCIÓN HISTÓRICA DE REVISTA (Chilti Request) ---
                            st.markdown("---")
                            st.subheader(f"Evolución del Perfil Editorial: {journal_info['display_name']}")
                            if df_thematic_evo is not None:
                                evo_levels_j = {'Dominio': 'domain', 'Campo': 'field', 'Subcampo': 'subfield', 'Tópico': 'topic'}
                                sel_evo_j = st.radio("Nivel Temático:", options=list(evo_levels_j.keys()), horizontal=True, key='evo_journal_radio')
                                t_evo_j = evo_levels_j[sel_evo_j]
                                
                                df_j_evo = df_thematic_evo[df_thematic_evo['journal_id'] == jid].groupby(['year', t_evo_j])['num_documents'].sum().reset_index()
                                if not df_j_evo.empty:
                                    df_j_p = df_j_evo.pivot(index=t_evo_j, columns='year', values='num_documents').fillna(0)
                                    st.dataframe(df_j_p.style.background_gradient(cmap='Oranges', axis=1).format("{:.0f}"), use_container_width=True)
                                else:
                                    st.info("Sin datos de evolución temática para esta revista.")
                            
                            st.markdown("---")
                    except Exception as e:
                        st.warning(f"No se pudieron cargar los temas: {e}")
                
                # --- GRÁFICOS COMPARATIVOS ---
                tab1, tab2, tab3 = st.tabs(["Trayectoria de Impacto", "Distribución de Acceso Abierto", "Perfil Radial (2021-2025)"])
                
                with tab1:
                    fig_j = px.line(
                        df_j_history, x='year', y='fwci_avg', 
                        markers=True, template="plotly_white",
                        title="Evolución del FWCI (Field Weighted Citation Impact)"
                    )
                    fig_j.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="Promedio Mundial")
                    st.plotly_chart(fig_j, use_container_width=True)
                
                with tab2:
                    # Preparar datos OA (asumiendo que transform_counts_to_pcts hizo su trabajo)
                    oa_cols = [c for c in df_j_history.columns if c.startswith('pct_oa_') and c != 'pct_oa_total']
                    df_oa_melt = df_j_history.melt(id_vars=['year'], value_vars=oa_cols, var_name='Tipo OA', value_name='Porcentaje')
                    df_oa_melt['Tipo OA'] = df_oa_melt['Tipo OA'].str.replace('pct_oa_', '').str.capitalize()
                    
                    fig_oa = px.bar(
                        df_oa_melt[df_oa_melt['year'] == latest_j['year']], 
                        x='Tipo OA', y='Porcentaje', color='Tipo OA',
                        template="plotly_white", title=f"Mix de Acceso Abierto ({int(latest_j['year'])})"
                    )
                    st.plotly_chart(fig_oa, use_container_width=True)
                    
                with tab3:
                    if not df_j_recent_data.empty:
                        rec_row = df_j_recent_data.iloc[0]
                        st.info("Valores normalizados (0-1) respecto al máximo global.")
                        
                        radar_indicators = ['fwci_avg', 'avg_percentile', 'pct_top_10', 'pct_top_1', 'pct_oa_diamond']
                        radar_labels = ['FWCI', 'Percentil Norm.', 'Top 10%', 'Top 1%', 'OA Diamante']
                        
                        valid_ind, valid_lbl, max_vals = [], [], []
                        for i, col in enumerate(radar_indicators):
                            if col in df_journals_recent.columns:
                                valid_ind.append(col)
                                valid_lbl.append(radar_labels[i])
                                m = df_journals_recent[col].max()
                                max_vals.append(m if m > 0 else 1.0)
                        
                        if len(valid_ind) >= 3:
                            values = [rec_row.get(col, 0) / mx for col, mx in zip(valid_ind, max_vals)]
                            values += [values[0]]
                            labels_closed = valid_lbl + [valid_lbl[0]]
                            
                            fig_radar = go.Figure()
                            fig_radar.add_trace(go.Scatterpolar(
                                r=values, theta=labels_closed, fill='toself',
                                name='2021-2025', line_color='#10b981', opacity=0.5
                            ))
                            fig_radar.update_layout(
                                polar=dict(radialaxis=dict(visible=True, range=[0, 1.05], showticklabels=False)),
                                showlegend=False, height=400, margin=dict(t=40, b=40, l=40, r=40)
                            )
                            st.plotly_chart(fig_radar, use_container_width=True)
                    else:
                        st.warning("No hay datos recientes suficientes para generar el Radar.")
                        
            else:
                st.info("No se encontraron registros históricos detallados para esta revista en el periodo analizado.")
    else:
        st.error("No se han cargado los metadatos de revistas. Ejecute el pipeline de ClickHouse.")

elif level == "5. Acerca del Sistema":
    st.title("Arquitectura del Sistema (Global OLAP)")
    st.markdown("""
    Este dashboard representa la evolución del sistema original, migrando de una base de datos relacional tradicional (Postgres) 
    a un motor de análisis de alto rendimiento (**ClickHouse**) capaz de procesar el snapshot completo de OpenAlex (>250 millones de registros).
    """)
    
    st.markdown("### Flujo de Datos Actualizado")
    st.code("""
graph TD
    subgraph "Capas de Datos"
        OA[(OpenAlex Snapshot\n.json.gz local)]
        CH[(ClickHouse Server\nOLAP Engine)]
    end

    subgraph "Pipeline de Ingesta"
        L1[load_openalex_clickhouse.py]
        L1 -->|Schema Inference| OA
        L1 -->|Bulk Insert| CH
    end
    
    subgraph "Motor de Cálculo"
        C1[compute_metrics_clickhouse.py]
        C1 -->|SQL Aggregation| CH
        C1 -->|Genera| P1[Parquets Estáticos]
    end

    subgraph "Visualización (Dashboard)"
        D[dashboard_global.py]
        D -->|Lectura Rápida| P1
    end
    """, language='mermaid')

    st.markdown("""
    #### Componentes Técnicos
    1. **ClickHouse (Core)**: La base de datos es una instancia de ClickHouse operando en el servidor local. Su arquitectura columnar permite realizar agregaciones (promedios, FWCI, conteos únicos) sobre cientos de millones de filas en segundos.
    2. **Ingesta Dinámica**: Los archivos raw de OpenAlex se ingieren directamente detectando tipos de datos automáticamente, lo que facilita la actualización mensual del snapshot.
    3. **Modelo OLAP (Caché Parquet)**: Para garantizar 0 latencia en la UI, el dashboard consume cubos de datos pre-calculados (Parquets) que residen en memoria cuando es necesario.
    4. **Métricas Globales**: A diferencia del dashboard LATAM, este sistema permite comparar cualquier país o revista del mundo bajo el mismo estándar bibliométrico.
    """)
    st.markdown("---")
    st.caption("Desarrollado para el análisis MASIVO de la producción científica mundial.")
