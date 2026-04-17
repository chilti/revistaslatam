import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import json
import time
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Añadir src al path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))
from regions import GLOBAL_REGIONS, get_region_for_country, get_all_country_codes

# ClickHouse connection params
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

# Configuración de página
st.set_page_config(
    page_title="Dashboard de Temas Global (OpenAlex)",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- ESTILOS CSS PREMIUM (Reutilizados de dashboard.py) ---
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
        gap: 15px;
        margin-bottom: 1.5rem;
    }

    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
        border: 1px solid rgba(0,0,0,0.05);
        transition: all 0.3s ease;
        flex: 1;
        text-align: left;
    }

    .metric-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.08);
    }

    .metric-label {
        font-size: 0.8rem;
        color: #64748b;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 5px;
    }

    .metric-value {
        font-size: 1.6rem;
        font-weight: 700;
        color: #1e293b;
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

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
        color_class = "delta-positive" if str(delta).startswith("+") else "delta-negative"
        delta_html = f'<div class="metric-delta {color_class}" style="font-size: 0.8rem; font-weight: 500; margin-top: 4px;">{delta}</div>'
    
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

# --- RUTAS ---
BASE_PATH = Path(__file__).parent
DATA_DIR = BASE_PATH / 'data'
CACHE_TEMAS_DIR = DATA_DIR / 'cache_temas'
CACHE_TEMAS_DIR.mkdir(parents=True, exist_ok=True)

# --- CLICKHOUSE CLIENT SETUP ---
try:
    import clickhouse_connect
except ImportError:
    st.error("Por favor instala `clickhouse-connect`")
    st.stop()

def get_ch_client():
    try:
        return clickhouse_connect.get_client(
            host=os.environ.get('CH_HOST', 'localhost'),
            port=int(os.environ.get('CH_PORT', 8123)),
            username=os.environ.get('CH_USER', 'default'),
            password=os.environ.get('CH_PASSWORD', ''),
            database=os.environ.get('CH_DATABASE', 'rag')
        )
    except Exception as e:
        st.error(f"Error conectando a ClickHouse: {e}")
        return None

# --- SIDEBAR: JERARQUÍA ---
st.sidebar.title("🧬 Análisis de Temas")
st.sidebar.markdown("---")

@st.cache_data
def get_hierarchy():
    """Obtiene la jerarquía Domain > Field > Subfield, usando caché local o ClickHouse."""
    hierarchy_path = DATA_DIR / 'cache' / 'topic_hierarchy.parquet'
    
    # 1. Intentar cargar desde el archivo específico de jerarquía
    if hierarchy_path.exists():
        return pd.read_parquet(hierarchy_path)
    
    # 2. Intentar extraer de sunburst_metrics_latam (si existe)
    sunburst_path = DATA_DIR / 'cache' / 'sunburst_metrics_latam.parquet'
    df_hier = None
    
    if sunburst_path.exists():
        df_hier = pd.read_parquet(sunburst_path)
        df_hier = df_hier[df_hier['level'].isin(['domain', 'field', 'subfield'])]
        df_hier = df_hier[['domain', 'field', 'subfield']].drop_duplicates()
        df_hier = df_hier.replace('ALL', np.nan)
    else:
        # 3. Consultar ClickHouse como último recurso
        client = get_ch_client()
        if not client: return None
        
        query = """
        SELECT DISTINCT
            domain,
            field,
            subfield
        FROM works
        WHERE domain != ''
        """
        try:
            df_hier = client.query_df(query)
        except Exception as e:
            st.error(f"Error cargando jerarquía de ClickHouse: {e}")
            return None

    # Guardar para la próxima vez si logramos obtenerla
    if df_hier is not None and not df_hier.empty:
        df_hier.to_parquet(hierarchy_path, index=False)
        return df_hier
    
    return None

df_hier = get_hierarchy()

if df_hier is not None:
    domains = sorted(df_hier['domain'].dropna().unique())
    selected_domain = st.sidebar.selectbox("1. Dominio", domains, index=domains.index('Health Sciences') if 'Health Sciences' in domains else 0)
    
    fields = sorted(df_hier[df_hier['domain'] == selected_domain]['field'].dropna().unique())
    selected_field = st.sidebar.selectbox("2. Campo", fields, index=fields.index('Medicine') if 'Medicine' in fields else 0)
    
    subfields = sorted(df_hier[df_hier['field'] == selected_field]['subfield'].dropna().unique())
    default_sub = "Pulmonary and Respiratory Medicine"
    selected_subfield = st.sidebar.selectbox("3. Subcampo", subfields, index=subfields.index(default_sub) if default_sub in subfields else 0)
else:
    st.sidebar.error("No se pudo cargar la jerarquía de temas.")
    st.stop()

st.sidebar.markdown("---")

# Period Selection
period_mode = st.sidebar.radio("Periodo de Análisis", ["Últimos 5 años (2021-2025)", "Periodo Completo"], index=0)

st.sidebar.markdown("---")
st.sidebar.caption("Datos mundiales basados en OpenAlex via ClickHouse")

# --- MAIN CONTENT ---
st.title(f"Tema: {selected_subfield}")
st.markdown(f"**Dominio:** {selected_domain} | **Campo:** {selected_field}")
st.markdown("---")

def compute_subfield_data(subfield):
    """Ejecuta consulta OLAP en ClickHouse. Intenta usar la tabla SummingMergeTree primero."""
    client = get_ch_client()
    if not client: return False
    
    # Intento 1: Usar tabla de agregación optimizada (si existe y tiene datos)
    query_fast = f"""
    SELECT 
        year, 
        country_code, 
        source_id as journal_id, 
        topic,
        sum(doc_count) as doc_count,
        sum(fwci_sum) as fwci_sum,
        sum(percentile_sum) as percentile_sum,
        sum(top_10_sum) as top_10_sum,
        sum(top_1_sum) as top_1_sum,
        sum(gold_count) as gold_count,
        sum(diamond_count) as diamond_count,
        sum(green_count) as green_count,
        sum(hybrid_count) as hybrid_count,
        sum(bronze_count) as bronze_count,
        sum(closed_count) as closed_count,
        sum(lang_en) as lang_en,
        sum(lang_es) as lang_es,
        sum(lang_pt) as lang_pt
    FROM summing_subfield_metrics
    WHERE subfield = '{subfield}' AND year >= 2000
    GROUP BY year, country_code, journal_id, topic
    """
    
    # Intento 2: Fallback a tabla 'works' con columnas materializadas (Deduplicado)
    query_fallback = f"""
    SELECT 
        year, 
        country_code, 
        journal_id, 
        topic,
        count() as doc_count,
        sum(fwci) as fwci_sum,
        sum(percentile) as percentile_sum,
        sum(toUInt64(is_top_10)) as top_10_sum,
        sum(toUInt64(is_top_1)) as top_1_sum,
        sum(toUInt64(oa_status='gold')) as gold_count,
        sum(toUInt64(oa_status='diamond')) as diamond_count,
        sum(toUInt64(oa_status='green')) as green_count,
        sum(toUInt64(oa_status='hybrid')) as hybrid_count,
        sum(toUInt64(oa_status='bronze')) as bronze_count,
        sum(toUInt64(oa_status='closed')) as closed_count,
        sum(toUInt64(language='en')) as lang_en,
        sum(toUInt64(language='es')) as lang_es,
        sum(toUInt64(language='pt')) as lang_pt
    FROM (
        SELECT 
            id,
            argMax(publication_year, updated_date) as year,
            argMax(subfield, updated_date) as subfield,
            argMax(topic, updated_date) as topic,
            argMax(source_id, updated_date) as journal_id,
            argMax(language, updated_date) as language,
            argMax(oa_status, updated_date) as oa_status,
            argMax(fwci, updated_date) as fwci,
            argMax(percentile, updated_date) as percentile,
            argMax(is_top_10, updated_date) as is_top_10,
            argMax(is_top_1, updated_date) as is_top_1,
            argMax(country_code, updated_date) as country_code
        FROM works
        GROUP BY id
    )
    WHERE subfield = '{subfield}' AND year >= 2000
    GROUP BY year, country_code, journal_id, topic
    """
    
    try:
        # Intentar query rápida
        df = client.query_df(query_fast)
        
        # Si está vacía, usar fallback
        if df.empty:
            with st.status("Consultando tabla maestra (esto tardará un poco más)..."):
                df = client.query_df(query_fallback)
        
        if df.empty:
            return False
            
        # Post-procesamiento
        df['region'] = df['country_code'].apply(get_region_for_country)
        cache_path = CACHE_TEMAS_DIR / f"{subfield.replace(' ', '_').lower()}.parquet"
        df.to_parquet(cache_path, index=False)
        return True
    except Exception as e:
        st.error(f"Error en ClickHouse: {e}")
        return False

# Placeholder for columns and calculation logic
if 'selected_subfield' not in st.session_state or st.session_state.selected_subfield != selected_subfield:
    st.session_state.selected_subfield = selected_subfield
    # Check cache
    cache_path = CACHE_TEMAS_DIR / f"{selected_subfield.replace(' ', '_').lower()}.parquet"
    st.session_state.has_cache = cache_path.exists()

if not st.session_state.has_cache:
    st.warning(f"⚠️ Los datos para '{selected_subfield}' no están calculados.")
    
    # Inicializar estado de cálculo si no existe
    if 'calculating' not in st.session_state:
        st.session_state.calculating = False
    
    if st.button("🚀 Lanzar Cálculo en ClickHouse", disabled=st.session_state.calculating):
        st.session_state.calculating = True
        st.rerun()

# Si se activó el cálculo, ejecutarlo
if not st.session_state.has_cache and st.session_state.get('calculating'):
    with st.spinner("Calculando métricas globales..."):
        success = compute_subfield_data(selected_subfield)
        st.session_state.calculating = False # Resetear estado
        if success:
            st.success("¡Cálculo finalizado!")
            st.session_state.has_cache = True
            st.rerun()
        else:
            st.error("No se encontraron datos o hubo un error.")
            # Permitir reintentar
            st.session_state.calculating = False
    st.stop()

# Load Data
@st.cache_data
def load_subfield_data(subfield):
    cache_path = CACHE_TEMAS_DIR / f"{subfield.replace(' ', '_').lower()}.parquet"
    if cache_path.exists():
        return pd.read_parquet(cache_path)
    return None

df_data = load_subfield_data(selected_subfield)

if df_data is None:
    st.error("Error al cargar los datos.")
    st.stop()

# --- DATA AGGREGATION LOGIC ---
def get_entity_metrics(df, entity_name, period="Últimos 5 años (2021-2025)"):
    """Filtra y agrega métricas para una entidad y periodo específico."""
    if df is None: return None
    dff = df.copy()
    
    # 1. Filtrar Entidad
    if entity_name == "Mundo":
        pass
    elif entity_name == "México":
        dff = dff[dff['country_code'] == 'MX']
    else:
        dff = dff[dff['region'] == entity_name]
        
    if dff.empty: return None

    # 2. Filtrar Periodo
    if period == "Últimos 5 años (2021-2025)":
        dff_period = dff[(dff['year'] >= 2021) & (dff['year'] <= 2025)]
    else:
        dff_period = dff.copy()

    # 3. Calcular Indicadores del Periodo
    metrics = {}
    if not dff_period.empty:
        total_docs = dff_period['doc_count'].sum()
        metrics['docs'] = int(total_docs)
        metrics['fwci'] = dff_period['fwci_sum'].sum() / total_docs if total_docs > 0 else 0
        metrics['percentile'] = (dff_period['percentile_sum'].sum() / total_docs) * 100 if total_docs > 0 else 0
        metrics['top_10'] = (dff_period['top_10_sum'].sum() / total_docs) * 100 if total_docs > 0 else 0
        metrics['top_1'] = (dff_period['top_1_sum'].sum() / total_docs) * 100 if total_docs > 0 else 0
        
        # OA
        for t in ['gold', 'diamond', 'green', 'hybrid', 'bronze', 'closed']:
            metrics[f'pct_oa_{t}'] = (dff_period[f'{t}_count'].sum() / total_docs) * 100 if total_docs > 0 else 0
            
        # Languages
        for l in ['en', 'es', 'pt']:
            metrics[f'pct_lang_{l}'] = (dff_period[f'lang_{l}'].sum() / total_docs) * 100 if total_docs > 0 else 0
    else:
        metrics = None

    # 4. Tendencias Anuales (Siempre todo el histórico para el gráfico)
    trends = dff.groupby('year').agg({
        'doc_count': 'sum',
        'fwci_sum': 'sum',
        'top_10_sum': 'sum'
    }).reset_index()
    trends['fwci'] = trends['fwci_sum'] / trends['doc_count']
    trends['pct_top_10'] = (trends['top_10_sum'] / trends['doc_count']) * 100
    
    # 5. Tópicos y Revistas
    # (Usando el periodo seleccionado para el sunburst y tablas)
    if not dff_period.empty:
        top_topics = dff_period.groupby('topic')['doc_count'].sum().sort_values(ascending=False).head(20)
        top_journals = dff_period.groupby('journal_id')['doc_count'].sum().sort_values(ascending=False).head(10)
    else:
        top_topics, top_journals = pd.Series(), pd.Series()

    return {
        'metrics': metrics,
        'trends': trends,
        'top_topics': top_topics,
        'top_journals': top_journals
    }

def render_entity_column(entity_name, df_all, period_label):
    if df_all is None:
        st.error(f"Datos no disponibles para {entity_name}")
        return

    data = get_entity_metrics(df_all, entity_name, period_label)
    
    if not data or not data['metrics']:
        st.warning(f"No hay suficientes datos para {entity_name} en este periodo.")
        return

    m = data['metrics']
    
    # KPIs
    st.markdown('<div class="metric-container">', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        premium_metric("Documentos", f"{m['docs']:,}")
        premium_metric("FWCI Promedio", f"{m['fwci']:.2f}")
    with c2:
        premium_metric("% Top 10%", f"{m['top_10']:.1f}%")
        premium_metric("Percentil (Norm)", f"{m['percentile']:.1f}")
    st.markdown('</div>', unsafe_allow_html=True)

    # Evolution Charts
    tab_prod, tab_impact = st.tabs(["📈 Producción", "💥 Impacto (FWCI)"])
    
    with tab_prod:
        fig_evol = px.line(data['trends'], x='year', y='doc_count', 
                          title=f"Evolución Anual: {entity_name}",
                          labels={'doc_count': 'Documentos', 'year': 'Año'},
                          markers=True, template="plotly_white")
        fig_evol.update_traces(line_color='#3b82f6', fill='tozeroy')
        st.plotly_chart(fig_evol, use_container_width=True)

    with tab_impact:
        fig_fwci = px.line(data['trends'], x='year', y='fwci', 
                          title=f"Evolución FWCI: {entity_name}",
                          labels={'fwci': 'FWCI', 'year': 'Año'},
                          markers=True, template="plotly_white")
        fig_fwci.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="Media Mundial")
        st.plotly_chart(fig_fwci, use_container_width=True)

    # Diversidad Temática (Topics)
    with st.expander("🧩 Desglose de Tópicos Internos", expanded=True):
        if not data['top_topics'].empty:
            fig_topics = px.pie(values=data['top_topics'].values, names=data['top_topics'].index,
                               hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_topics.update_layout(showlegend=False, height=350, margin=dict(l=0,r=0,t=0,b=0))
            st.plotly_chart(fig_topics, use_container_width=True)
        else:
            st.info("Sin datos de tópicos.")

    # OA & Language
    col_oa, col_lang = st.columns(2)
    with col_oa:
        st.markdown("**Acceso Abierto**")
        oa_data = pd.DataFrame({
            'Tipo': ['Diamond', 'Gold', 'Green', 'Hybrid', 'Bronze', 'Closed'],
            'Valor': [m['pct_oa_diamond'], m['pct_oa_gold'], m['pct_oa_green'], 
                     m['pct_oa_hybrid'], m['pct_oa_bronze'], m['pct_oa_closed']]
        })
        fig_oa = px.bar(oa_data[oa_data['Valor']>0], x='Tipo', y='Valor', color='Tipo', 
                       color_discrete_sequence=px.colors.qualitative.Set3)
        fig_oa.update_layout(showlegend=False, height=300, xaxis_title=None, yaxis_title="%")
        st.plotly_chart(fig_oa, use_container_width=True)

    with col_lang:
        st.markdown("**Idiomas (Predominantes)**")
        l_data = pd.DataFrame({
            'Idioma': ['EN', 'ES', 'PT'],
            'Pct': [m['pct_lang_en'], m['pct_lang_es'], m['pct_lang_pt']]
        })
        fig_l = px.pie(l_data[l_data['Pct']>0], values='Pct', names='Idioma',
                      color_discrete_sequence=['#3b82f6', '#10b981', '#f59e0b'])
        fig_l.update_layout(showlegend=True, height=300, margin=dict(l=0,r=0,t=0,b=0))
        st.plotly_chart(fig_l, use_container_width=True)

    # Top Journals Table
    with st.expander("📚 Top 10 Revistas Líderes", expanded=False):
        if not data['top_journals'].empty:
            df_j = data['top_journals'].reset_index()
            df_j.columns = ['ID Revista', 'Artículos']
            st.table(df_j)
        else:
            st.info("Sin datos de revistas.")

# --- COMPARISON LAYOUT ---
if df_data is not None:
    col_A, col_B = st.columns(2)

    entities = ["Mundo", "México"] + sorted(list(GLOBAL_REGIONS.keys()))

    with col_A:
        ent1 = st.selectbox("Entidad de Comparativa A", entities, index=0)
        st.markdown(f"### 🌏 {ent1}")
        render_entity_column(ent1, df_data, period_mode)

    with col_B:
        ent2 = st.selectbox("Entidad de Comparativa B", entities, index=entities.index("Latinoamérica y Caribe") if "Latinoamérica y Caribe" in entities else 1)
        st.markdown(f"### 📍 {ent2}")
        render_entity_column(ent2, df_data, period_mode)
else:
    st.info("Por favor, selecciona un tema y lanza el cálculo si es necesario.")
