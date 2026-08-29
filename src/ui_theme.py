"""
ui_theme.py - Bespoke Editorial & Modern UI Design System for Revistas LATAM
Transforms default Streamlit controls into a sober, distinctive, and high-end analytical platform.
"""
import streamlit as st

def get_theme_css(theme_name="☀️ Claro (Blanco)"):
    """
    Returns custom CSS rules for typography, segmented controls, tabs,
    cards, inputs, and buttons based on the selected theme.
    """
    
    if theme_name == "🌙 Oscuro (Dark)":
        bg_gradient = "radial-gradient(circle at 10% 20%, #0f172a 0%, #020617 90%)"
        sidebar_bg = "#090d16"
        sidebar_border = "#1e293b"
        card_bg = "rgba(15, 23, 42, 0.85)"
        card_border = "rgba(51, 65, 85, 0.6)"
        card_hover_border = "rgba(56, 189, 248, 0.4)"
        text_primary = "#f8fafc"
        text_secondary = "#94a3b8"
        accent_color = "#38bdf8"
        accent_gradient = "linear-gradient(135deg, #38bdf8 0%, #818cf8 100%)"
        tab_bg = "rgba(30, 41, 59, 0.5)"
        tab_active_bg = "rgba(56, 189, 248, 0.15)"
        tab_active_text = "#38bdf8"
        input_bg = "#0f172a"
        input_border = "#334155"
        btn_bg = "#1e293b"
        btn_border = "#334155"
        btn_text = "#f8fafc"
        btn_hover_bg = "#2563eb"
        plotly_template = "plotly_dark"
        
    elif theme_name == "🌌 Azul Noche (Navy)":
        bg_gradient = "radial-gradient(circle at 10% 20%, #0b192f 0%, #030712 90%)"
        sidebar_bg = "#071224"
        sidebar_border = "#1e3a8a"
        card_bg = "rgba(11, 25, 47, 0.85)"
        card_border = "rgba(30, 58, 138, 0.5)"
        card_hover_border = "rgba(14, 165, 233, 0.5)"
        text_primary = "#f0fdf4"
        text_secondary = "#7dd3fc"
        accent_color = "#0ea5e9"
        accent_gradient = "linear-gradient(135deg, #0ea5e9 0%, #38bdf8 100%)"
        tab_bg = "rgba(15, 39, 74, 0.5)"
        tab_active_bg = "rgba(14, 165, 233, 0.2)"
        tab_active_text = "#38bdf8"
        input_bg = "#071731"
        input_border = "#1e3a8a"
        btn_bg = "#0f274a"
        btn_border = "#1e3a8a"
        btn_text = "#e0f2fe"
        btn_hover_bg = "#0284c7"
        plotly_template = "plotly_dark"
        
    else:
        # Claro (Nordic Slate & Sapphire)
        bg_gradient = "linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%)"
        sidebar_bg = "#ffffff"
        sidebar_border = "#e2e8f0"
        card_bg = "#ffffff"
        card_border = "rgba(226, 232, 240, 0.85)"
        card_hover_border = "rgba(2, 132, 199, 0.35)"
        text_primary = "#0f172a"
        text_secondary = "#64748b"
        accent_color = "#0284c7"
        accent_gradient = "linear-gradient(135deg, #0284c7 0%, #2563eb 100%)"
        tab_bg = "#f1f5f9"
        tab_active_bg = "#ffffff"
        tab_active_text = "#0284c7"
        input_bg = "#ffffff"
        input_border = "#cbd5e1"
        btn_bg = "#ffffff"
        btn_border = "#cbd5e1"
        btn_text = "#0f172a"
        btn_hover_bg = "#0284c7"
        plotly_template = "plotly_white"

    css = f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=Outfit:wght@400;600;700&display=swap');
        
        /* 1. Global Typography & Canvas */
        html, body, [class*="css"], .stMarkdown, .stText, .stCaption {{
            font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif !important;
            -webkit-font-smoothing: antialiased;
            letter-spacing: -0.01em;
        }}
        
        .stApp {{
            background: {bg_gradient} !important;
            color: {text_primary} !important;
        }}
        
        /* 2. Sleek Custom Scrollbar */
        ::-webkit-scrollbar {{
            width: 7px;
            height: 7px;
        }}
        ::-webkit-scrollbar-track {{
            background: transparent;
        }}
        ::-webkit-scrollbar-thumb {{
            background: rgba(148, 163, 184, 0.3);
            border-radius: 8px;
        }}
        ::-webkit-scrollbar-thumb:hover {{
            background: rgba(148, 163, 184, 0.6);
        }}
        
        /* 3. Sidebar Elevation & Branding */
        [data-testid="stSidebar"] {{
            background-color: {sidebar_bg} !important;
            border-right: 1px solid {sidebar_border} !important;
            box-shadow: 2px 0 20px rgba(0, 0, 0, 0.02);
        }}
        
        .sidebar-brand-badge {{
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 12px 14px;
            background: rgba(2, 132, 199, 0.06);
            border: 1px solid rgba(2, 132, 199, 0.18);
            border-radius: 10px;
            margin-bottom: 15px;
        }}
        
        /* 4. Segmented Controls for Radio Buttons (Horizontal) */
        div[data-testid="stRadio"] > label {{
            font-size: 0.85rem !important;
            font-weight: 600 !important;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            color: {text_secondary} !important;
            margin-bottom: 6px;
        }}
        
        div[data-testid="stRadio"] div[role="radiogroup"] {{
            display: flex;
            background: {tab_bg};
            padding: 4px;
            border-radius: 10px;
            border: 1px solid {sidebar_border};
            gap: 4px;
        }}
        
        div[data-testid="stRadio"] div[role="radiogroup"] > label {{
            flex: 1;
            text-align: center;
            background: transparent;
            padding: 6px 12px;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
            margin: 0 !important;
            border: 1px solid transparent;
        }}
        
        div[data-testid="stRadio"] div[role="radiogroup"] > label:hover {{
            background: rgba(255, 255, 255, 0.3);
        }}
        
        div[data-testid="stRadio"] div[role="radiogroup"] > label > div:first-child {{
            display: none !important; /* Hide circular radio dot */
        }}
        
        div[data-testid="stRadio"] div[role="radiogroup"] > label > div:last-child {{
            color: {text_primary};
            font-size: 0.88rem;
            font-weight: 500;
        }}
        
        /* 5. Modern Pill Tabs (st.tabs) */
        div[data-testid="stTabs"] {{
            border-bottom: none !important;
        }}
        
        div[data-testid="stTabs"] div[role="tablist"] {{
            gap: 8px;
            background: {tab_bg};
            padding: 5px;
            border-radius: 12px;
            border: 1px solid {sidebar_border};
            display: inline-flex;
            margin-bottom: 16px;
        }}
        
        div[data-testid="stTabs"] button[role="tab"] {{
            border: 1px solid transparent !important;
            background: transparent !important;
            color: {text_secondary} !important;
            padding: 8px 16px !important;
            border-radius: 8px !important;
            font-size: 0.90rem !important;
            font-weight: 600 !important;
            transition: all 0.2s ease !important;
        }}
        
        div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {{
            background: {tab_active_bg} !important;
            color: {tab_active_text} !important;
            border: 1px solid {card_border} !important;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04) !important;
        }}
        
        /* 6. Refined Inputs, Selects & Multiselects */
        div[data-baseweb="select"] > div {{
            background-color: {input_bg} !important;
            border: 1px solid {input_border} !important;
            border-radius: 8px !important;
            transition: all 0.2s ease;
            box-shadow: 0 1px 2px rgba(0,0,0,0.03);
        }}
        
        div[data-baseweb="select"] > div:hover {{
            border-color: {accent_color} !important;
        }}
        
        div[data-baseweb="select"] > div:focus-within {{
            border-color: {accent_color} !important;
            box-shadow: 0 0 0 3px rgba(2, 132, 199, 0.15) !important;
        }}
        
        /* Multiselect Tag Chips */
        span[data-baseweb="tag"] {{
            background: rgba(2, 132, 199, 0.1) !important;
            border: 1px solid rgba(2, 132, 199, 0.2) !important;
            border-radius: 6px !important;
            color: {text_primary} !important;
            font-size: 0.82rem !important;
            font-weight: 500;
        }}
        
        /* 7. Tactical Buttons */
        div.stButton > button, div.stDownloadButton > button {{
            background: {btn_bg} !important;
            color: {btn_text} !important;
            border: 1px solid {btn_border} !important;
            border-radius: 8px !important;
            font-size: 0.88rem !important;
            font-weight: 600 !important;
            padding: 6px 14px !important;
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04) !important;
        }}
        
        div.stButton > button:hover, div.stDownloadButton > button:hover {{
            border-color: {btn_hover_bg} !important;
            color: {btn_hover_bg} !important;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(2, 132, 199, 0.12) !important;
        }}
        
        div.stButton > button:active, div.stDownloadButton > button:active {{
            transform: translateY(0);
        }}
        
        /* 8. KPI Metric Cards */
        .metric-card {{
            background: {card_bg};
            border: 1px solid {card_border};
            border-radius: 14px;
            padding: 20px 22px;
            box-shadow: 0 4px 20px -2px rgba(0,0,0,0.04);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }}
        
        .metric-card:hover {{
            transform: translateY(-3px);
            border-color: {card_hover_border};
            box-shadow: 0 12px 28px -4px rgba(0,0,0,0.08);
        }}
        
        .metric-label {{
            font-size: 0.82rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: {text_secondary};
            margin-bottom: 6px;
        }}
        
        .metric-value {{
            font-family: 'Outfit', sans-serif;
            font-size: 1.95rem;
            font-weight: 700;
            color: {text_primary};
            line-height: 1.15;
        }}
        
        .metric-badge {{
            display: inline-flex;
            align-items: center;
            padding: 2px 8px;
            border-radius: 6px;
            font-size: 0.78rem;
            font-weight: 600;
            margin-top: 6px;
        }}
        
        .badge-positive {{
            background: rgba(16, 185, 129, 0.12);
            color: #10b981;
            border: 1px solid rgba(16, 185, 129, 0.25);
        }}
        
        .badge-negative {{
            background: rgba(239, 68, 68, 0.12);
            color: #ef4444;
            border: 1px solid rgba(239, 68, 68, 0.25);
        }}
        
        /* 9. Clean Callout Boxes */
        div[data-testid="stAlert"] {{
            border-radius: 10px !important;
            border: 1px solid {sidebar_border} !important;
            border-left: 4px solid {accent_color} !important;
            background: {card_bg} !important;
            color: {text_primary} !important;
        }}
        
        /* 10. Clean Dataframe Wrappers */
        [data-testid="stDataFrame"] {{
            border: 1px solid {sidebar_border};
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.02);
        }}
        
        /* Section Header Micro-Accents */
        h1, h2, h3 {{
            font-family: 'Outfit', sans-serif !important;
            font-weight: 700 !important;
            letter-spacing: -0.02em !important;
            color: {text_primary} !important;
        }}
        
        h2::before {{
            content: "";
            display: inline-block;
            width: 4px;
            height: 18px;
            background: {accent_gradient};
            border-radius: 2px;
            margin-right: 8px;
            vertical-align: middle;
        }}
    </style>
    """
    return css, plotly_template

def render_premium_metric(label, value, delta=None):
    """
    Renders a bespoke analytical metric card with clean styling and optional delta badge.
    """
    delta_html = ""
    if delta is not None:
        is_pos = str(delta).startswith("+") or (isinstance(delta, (int, float)) and delta > 0)
        badge_cls = "badge-positive" if is_pos else "badge-negative"
        delta_str = str(delta) if isinstance(delta, str) else f"{delta:+,g}"
        delta_html = f'<div class="metric-badge {badge_cls}">{delta_str}</div>'
    
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

def render_sidebar_header():
    """
    Renders an elevated institutional header in the sidebar.
    """
    st.sidebar.markdown("""
    <div class="sidebar-brand-badge">
        <span style="font-size: 22px;">📚</span>
        <div>
            <div style="font-weight: 700; font-size: 14.5px; line-height: 1.2;">Bibliometría LATAM</div>
            <div style="font-size: 11px; color: #64748b;">Plataforma de Inteligencia Cienciométrica</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
