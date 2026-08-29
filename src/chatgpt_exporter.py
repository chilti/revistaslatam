"""
chatgpt_exporter.py - Custom GPT 'Revistas Latam GPT' Exporter & Multi-Select Export Drawer
===========================================================================================
Integrates directly with Custom GPT (e.g. https://chatgpt.com/g/g-XXXXX-revistas-latam)
Supports:
1. Direct Custom GPT URL Intent (`https://chatgpt.com/g/.../?q=...`)
2. Continuous Thread Continuation Formatter for ongoing chats.
3. Real-Time Dynamic Character Budget Meter (Safe limit: ~5,500 chars).
4. Multi-Select Export Drawer and Cumulative Study Dossier.
"""
import os
import urllib.parse
import json
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

URL_SAFE_CHAR_LIMIT = 5500


def get_custom_gpt_base_url():
    """
    Returns the configured Custom GPT URL from .env or session state.
    Defaults to generic chatgpt.com if no Custom GPT is specified.
    """
    custom_url = os.getenv("CHATGPT_CUSTOM_GPT_URL", "").strip()
    if custom_url and custom_url.startswith("http"):
        return custom_url.rstrip("/")
    return "https://chatgpt.com"


def _df_to_markdown(df: pd.DataFrame, index: bool = False) -> str:
    """Converts DataFrame to markdown table, with fallback if tabulate is not available."""
    try:
        return df.to_markdown(index=index)
    except (ImportError, Exception):
        if index:
            headers = [str(df.index.name or '')] + [str(col) for col in df.columns]
            rows = [[str(idx)] + [str(val) for val in row] for idx, row in zip(df.index, df.values)]
        else:
            headers = [str(col) for col in df.columns]
            rows = [[str(val) for val in row] for row in df.values]
        header_line = "| " + " | ".join(headers) + " |"
        sep_line = "| " + " | ".join(["---"] * len(headers)) + " |"
        row_lines = ["| " + " | ".join(row) + " |" for row in rows]
        return "\n".join([header_line, sep_line] + row_lines)


# ============================================================================
# Page Data Registry
# ============================================================================
def init_page_registry():
    st.session_state.page_exportables = {}
    st.session_state.plotly_chart_render_idx = 0


def register_exportable(item_id, title, data_payload, context="", category="Métricas"):
    if "page_exportables" not in st.session_state:
        st.session_state.page_exportables = {}
        
    if isinstance(data_payload, pd.DataFrame):
        df_sub = data_payload.head(12)
        text_repr = _df_to_markdown(df_sub, index=False)
        if len(data_payload) > 12:
            text_repr += f"\n*(Muestra de 12 filas de {len(data_payload):,} registros)*"
    elif isinstance(data_payload, dict):
        lines = [f"- **{k}**: {v}" for k, v in data_payload.items()]
        text_repr = "\n".join(lines)
    else:
        text_repr = str(data_payload)
        
    char_count = len(text_repr)
    
    st.session_state.page_exportables[item_id] = {
        "id": item_id,
        "title": title,
        "category": category,
        "context": context,
        "data_text": text_repr,
        "raw_data": data_payload,
        "char_count": char_count
    }


# ============================================================================
# Prompt Formatting & URL Generation for Custom GPT
# ============================================================================
def format_chatgpt_prompt(title, data_summary, context_desc="", is_continuation=False):
    header = "[NUEVO BLOQUE DE DATOS: ESTUDIO REVISTAS LATAM]" if is_continuation else "Analiza el siguiente paquete de indicadores de RevistasLATAM (OpenAlex):"
    
    prompt = f"""{header}

📌 OBJETO DE ANÁLISIS:
{title}

{f"📝 CONTEXTO:\n{context_desc}\n" if context_desc else ""}
📊 DATOS E INDICADORES:
{data_summary}

🎯 OBJETIVOS:
1. Integra estos datos al análisis global del ecosistema de revistas latinoamericanas.
2. Identifica fortalezas, patrones disciplinares y asimetrías de citación (FWCI / Percentiles) y Acceso Abierto (Diamante vs Oro).
3. Redacta una síntesis analítica académica para incorporar al reporte 'Revistas Latam'.
"""
    return prompt.strip()


def get_chatgpt_url(prompt):
    base_url = get_custom_gpt_base_url()
    encoded = urllib.parse.quote(prompt)
    separator = "?" if "?" not in base_url else "&"
    return f"{base_url}{separator}q={encoded}"


def add_to_study_dossier(section_title, data_dict_or_df, narrative=""):
    if "revistas_latam_dossier" not in st.session_state:
        st.session_state.revistas_latam_dossier = []
        
    entry = {
        "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "title": section_title,
        "narrative": narrative,
        "data": data_dict_or_df
    }
    st.session_state.revistas_latam_dossier.append(entry)


# ============================================================================
# Floating Multi-Select Export Drawer (Bandeja Multiselección Inteligente)
# ============================================================================
def render_export_drawer():
    items = st.session_state.get("page_exportables", {})
    if not items:
        return

    custom_gpt_url = get_custom_gpt_base_url()
    is_custom = "/g/" in custom_gpt_url
    gpt_label = "Revistas Latam GPT" if is_custom else "ChatGPT"

    with st.expander(f"🤖 Bandeja de Exportación a {gpt_label} ({len(items)} fuentes disponibles en esta vista)", expanded=False):
        st.caption(f"Selecciona qué métricas y gráficas deseas empaquetar juntas para analizar con **{gpt_label}** o compilar en tu reporte:")

        # Multi-select checkboxes
        selected_ids = []
        total_chars = 0
        
        cols = st.columns(min(3, max(1, len(items))))
        for idx, (k, item) in enumerate(items.items()):
            col_target = cols[idx % len(cols)]
            with col_target:
                chk_key = f"chk_export_{k}"
                is_checked = st.checkbox(
                    f"{item['title']} (`~{item['char_count']} chars`)",
                    value=st.session_state.get(chk_key, idx < 2),
                    key=chk_key,
                    help=item.get("context", "")
                )
                if is_checked:
                    selected_ids.append(k)
                    total_chars += item["char_count"]

        st.markdown("---")

        # Real-time Budget Calculation
        prompt_overhead = 400
        estimated_total = total_chars + prompt_overhead
        pct_used = min(1.0, estimated_total / URL_SAFE_CHAR_LIMIT)

        b_col1, b_col2 = st.columns([3, 1])
        with b_col1:
            st.progress(pct_used)
            if estimated_total <= URL_SAFE_CHAR_LIMIT:
                st.caption(f"🟢 **Capacidad URL**: `{estimated_total:,} / {URL_SAFE_CHAR_LIMIT:,} caracteres` ({int(pct_used*100)}% usado) — *Óptimo para abrir directo en {gpt_label}*")
            else:
                over_chars = estimated_total - URL_SAFE_CHAR_LIMIT
                st.caption(f"🔴 **Capacidad URL**: `{estimated_total:,} / {URL_SAFE_CHAR_LIMIT:,} caracteres` (+{over_chars:,} chars) — *Usa 'Copiar Prompt' o 'Descargar' para chats existentes*")

        with b_col2:
            st.markdown(f"**{len(selected_ids)} de {len(items)}** seleccionados")

        if not selected_ids:
            st.warning("Selecciona al menos una casilla para generar el paquete.")
            return

        # Build bundled prompt
        bundled_sections = []
        for sid in selected_ids:
            it = items[sid]
            bundled_sections.append(f"### {it['title']}\n{it.get('context', '')}\n\n{it['data_text']}")

        combined_data_text = "\n\n---\n\n".join(bundled_sections)
        final_prompt = format_chatgpt_prompt("Paquete Multivariable LATAM", combined_data_text, "Conjunto de datos seleccionados en RevistasLATAM.")
        direct_url = get_chatgpt_url(final_prompt)

        # Action Buttons
        btn_c1, btn_c2, btn_c3 = st.columns([1.5, 1.5, 1.2])
        
        with btn_c1:
            if estimated_total <= URL_SAFE_CHAR_LIMIT:
                st.markdown(
                    f"""
                    <a href="{direct_url}" target="_blank" style="text-decoration:none;">
                        <div style="
                            display: block; width: 100%;
                            background: linear-gradient(135deg, #10a37f 0%, #0d8a6a 100%);
                            color: white; font-size: 12px; font-weight: 700;
                            padding: 8px 12px; border-radius: 6px; border: 1px solid #0a6c53;
                            box-shadow: 0 2px 6px rgba(0,0,0,0.12); text-align: center;
                        ">
                            🚀 Abrir en {gpt_label} ↗
                        </div>
                    </a>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.button("⚠️ Muy grande para URL directa (Usa Descargar)", disabled=True, use_container_width=True)

        with btn_c2:
            if st.button("📥 Guardar Selección en Reporte 'Revistas Latam'", key="btn_save_selection_dossier", use_container_width=True):
                for sid in selected_ids:
                    it = items[sid]
                    add_to_study_dossier(it['title'], it['raw_data'], it.get('context', ''))
                st.toast(f"✅ Se agregaron {len(selected_ids)} paquetes al Reporte Global", icon="📚")

        with btn_c3:
            st.download_button(
                "💾 Descargar Prompt .md",
                data=final_prompt,
                file_name="paquete_revistas_latam.md",
                mime="text/markdown",
                use_container_width=True
            )


# ============================================================================
# Plotly Figure Data Extractor & Unified Renderer
# ============================================================================
def extract_plotly_data_summary(fig):
    """Extracts a tabular or structured summary from a Plotly Figure."""
    if not hasattr(fig, 'data') or not fig.data:
        title = ""
        if hasattr(fig, 'layout') and hasattr(fig.layout, 'title') and getattr(fig.layout.title, 'text', None):
            title = fig.layout.title.text or ""
        return {"Visualización": title or "Gráfica Plotly"}
    
    records = []
    try:
        for t in fig.data:
            trace_type = getattr(t, 'type', 'scatter')
            trace_name = getattr(t, 'name', '') or ''
            
            if trace_type in ('scatter', 'scattergl', 'bar', 'line', 'histogram'):
                x_vals = getattr(t, 'x', None)
                y_vals = getattr(t, 'y', None)
                if x_vals is not None and y_vals is not None:
                    for x, y in zip(x_vals, y_vals):
                        rec = {'Serie': trace_name, 'Eje X': x, 'Valor': y} if trace_name else {'Eje X': x, 'Valor': y}
                        records.append(rec)
            elif trace_type == 'scatterpolar':
                r_vals = getattr(t, 'r', None)
                theta_vals = getattr(t, 'theta', None)
                if r_vals is not None and theta_vals is not None:
                    for th, r in zip(theta_vals, r_vals):
                        rec = {'Serie': trace_name, 'Métrica': th, 'Valor': r} if trace_name else {'Métrica': th, 'Valor': r}
                        records.append(rec)
            elif trace_type == 'pie':
                labels = getattr(t, 'labels', None)
                values = getattr(t, 'values', None)
                if labels is not None and values is not None:
                    for l, v in zip(labels, values):
                        records.append({'Categoría': l, 'Valor': v})
            elif trace_type == 'sunburst':
                labels = getattr(t, 'labels', None)
                values = getattr(t, 'values', None)
                parents = getattr(t, 'parents', None)
                if labels is not None and values is not None:
                    for l, p, v in zip(labels, parents or ['']*len(labels), values):
                        records.append({'Categoría': l, 'Jerarquía (Padre)': p, 'Valor': v})
            elif trace_type == 'choropleth':
                locations = getattr(t, 'locations', None)
                z = getattr(t, 'z', None)
                if locations is not None and z is not None:
                    for loc, val in zip(locations, z):
                        records.append({'Código / Ubicación': loc, 'Valor': val})
            elif trace_type == 'sankey':
                node_labels = getattr(t.node, 'label', None)
                link_source = getattr(t.link, 'source', None)
                link_target = getattr(t.link, 'target', None)
                link_value = getattr(t.link, 'value', None)
                if node_labels is not None and link_source is not None and link_target is not None and link_value is not None:
                    for s, tg, v in zip(link_source, link_target, link_value):
                        src_name = node_labels[s] if s < len(node_labels) else s
                        tgt_name = node_labels[tg] if tg < len(node_labels) else tg
                        records.append({'Origen': src_name, 'Destino': tgt_name, 'Flujo': v})
                        
        if records:
            df_res = pd.DataFrame(records)
            if len(df_res) > 50:
                return df_res.head(50)
            return df_res
    except Exception:
        pass
    
    title = ""
    if hasattr(fig, 'layout') and hasattr(fig.layout, 'title') and getattr(fig.layout.title, 'text', None):
        title = fig.layout.title.text or ""
    return {"Visualización": title or "Gráfica Plotly", "Trazas": len(fig.data)}


def render_plotly_chart(
    fig,
    title=None,
    data_payload=None,
    context="",
    category="Gráficas",
    key=None,
    use_container_width=True,
    show_report_button=True,
    button_label="📥 Guardar en Reporte",
    **kwargs
):
    """
    Renders a Plotly figure and adds a sleek button to save its data to the Study Dossier Report.
    """
    st.plotly_chart(fig, use_container_width=use_container_width, **kwargs)
    
    if not show_report_button:
        return
        
    if not title:
        if hasattr(fig, 'layout') and hasattr(fig.layout, 'title') and getattr(fig.layout.title, 'text', None):
            title = fig.layout.title.text
        else:
            title = "Gráfica"
            
    if data_payload is None:
        data_payload = extract_plotly_data_summary(fig)
        
    if "plotly_chart_render_idx" not in st.session_state:
        st.session_state.plotly_chart_render_idx = 0
    st.session_state.plotly_chart_render_idx += 1
    
    if not key:
        widget_key = f"fig_rep_{abs(hash(str(title))) % 100000}_{st.session_state.plotly_chart_render_idx}"
    else:
        widget_key = f"fig_rep_{key}_{st.session_state.plotly_chart_render_idx}"
    
    register_exportable(widget_key, title, data_payload, context=context, category=category)
    
    if st.button(button_label, key=f"btn_dossier_{widget_key}", help=f"Guardar los datos de '{title}' en el Reporte 'Revistas Latam'", use_container_width=False):
        add_to_study_dossier(title, data_payload, context)
        st.toast(f"✅ Agregado al reporte: {title}", icon="📚")


# ============================================================================
# Compact Single Button / Popover
# ============================================================================
def render_chatgpt_button(title, data_payload, context="", key=None, button_label="Enviar a ChatGPT"):
    widget_key = key or f"gpt_exp_{abs(hash(title)) % 100000}"
    register_exportable(widget_key, title, data_payload, context=context)

    custom_gpt_url = get_custom_gpt_base_url()
    is_custom = "/g/" in custom_gpt_url
    gpt_label = "Revistas Latam GPT" if is_custom else "ChatGPT"

    if isinstance(data_payload, pd.DataFrame):
        data_text = _df_to_markdown(data_payload.head(15), index=False)
    elif isinstance(data_payload, dict):
        data_text = "\n".join([f"- **{k}**: {v}" for k, v in data_payload.items()])
    else:
        data_text = str(data_payload)

    prompt = format_chatgpt_prompt(title, data_text, context)
    chatgpt_url = get_chatgpt_url(prompt)

    with st.popover(f"🤖 {button_label}"):
        st.markdown(f"**{title}**")
        st.caption(f"Destino: `{gpt_label}` • Tamaño: `{len(prompt):,} chars`")
        
        st.markdown(
            f"""
            <a href="{chatgpt_url}" target="_blank" style="text-decoration:none;">
                <div style="
                    display: block; width: 100%;
                    background: linear-gradient(135deg, #10a37f 0%, #0d8a6a 100%);
                    color: white; font-size: 11.5px; font-weight: 600;
                    padding: 6px 10px; border-radius: 5px; border: 1px solid #0a6c53;
                    text-align: center; margin-bottom: 6px;
                ">
                    🚀 Abrir en {gpt_label} ↗
                </div>
            </a>
            """,
            unsafe_allow_html=True
        )
        if st.button("📥 Guardar en Reporte 'Revistas Latam'", key=f"btn_pop_dossier_{widget_key}", use_container_width=True):
            add_to_study_dossier(title, data_payload, context)
            st.toast(f"✅ Agregado al reporte: {title}", icon="📚")


# ============================================================================
# Sidebar Study Dossier Manager
# ============================================================================
def render_study_dossier_sidebar():
    dossier = st.session_state.get("revistas_latam_dossier", [])
    custom_gpt_url = get_custom_gpt_base_url()
    is_custom = "/g/" in custom_gpt_url
    gpt_label = "Revistas Latam GPT" if is_custom else "ChatGPT"
    
    with st.sidebar.expander(f"📚 Reporte 'Revistas Latam' ({len(dossier)} datos)", expanded=False):
        if not dossier:
            st.caption(f"Aún no has acumulado datos. Usa **'📥 Guardar en Reporte'** o la **Bandeja de Exportación** para compilar tu estudio.")
            return

        st.markdown("**Paquetes acumulados:**")
        for i, item in enumerate(dossier):
            st.markdown(f"{i+1}. **{item['title']}** `({item['timestamp'].split()[1]})`")
            
        st.markdown("---")
        
        compiled_sections = []
        for i, item in enumerate(dossier):
            d_str = ""
            if isinstance(item['data'], pd.DataFrame):
                d_str = _df_to_markdown(item['data'].head(10), index=False)
            elif isinstance(item['data'], dict):
                d_str = "\n".join([f"- **{k}**: {v}" for k, v in item['data'].items()])
            else:
                d_str = str(item['data'])
                
            compiled_sections.append(f"### Módulo {i+1}: {item['title']}\n{item.get('narrative', '')}\n\n{d_str}")
            
        full_dossier_text = "\n\n---\n\n".join(compiled_sections)
        
        master_prompt = f"""Actúa como un cienciómetro principal y redactor de políticas científicas.
Dossier completo acumulado en RevistasLATAM para el reporte 'Revistas Latam':

====================================================================
DOSSIER COMPLETO DE DATOS (REVISTAS LATINOAMERICANAS):
====================================================================
{full_dossier_text}

====================================================================
TAREA:
1. Genera un REPORTE EJECUTIVO Y ESTUDIO CIENCIOMÉTRICO INTEGRAL con título formal, resumen ejecutivo, análisis de hallazgos transversales (patrones regionales, disciplinas, modelos de acceso abierto Diamante/Oro, citas y visibilidad global).
2. Redacta conclusiones estratégicas y recomendaciones para editores e instituciones latinoamericanas.
"""
        master_chatgpt_url = get_chatgpt_url(master_prompt)
        
        if len(master_prompt) <= URL_SAFE_CHAR_LIMIT:
            st.markdown(
                f"""
                <a href="{master_chatgpt_url}" target="_blank" style="text-decoration:none;">
                    <div style="
                        display: block; width: 100%;
                        background: linear-gradient(135deg, #10a37f 0%, #0d8a6a 100%);
                        color: white; font-size: 12px; font-weight: 700;
                        padding: 8px 12px; border-radius: 6px; border: 1px solid #0a6c53;
                        box-shadow: 0 4px 10px rgba(0,0,0,0.15); text-align: center; margin-bottom: 8px;
                    ">
                        🚀 Enviar Reporte a {gpt_label} ↗
                    </div>
                </a>
                """,
                unsafe_allow_html=True
            )
        else:
            st.info(f"ℹ️ Reporte grande ({len(master_prompt):,} caracteres). Descárgalo a continuación para adjuntarlo a {gpt_label}:")
        
        c_d1, c_d2 = st.columns(2)
        with c_d1:
            st.download_button(
                "💾 Descargar .md",
                data=master_prompt,
                file_name="Reporte_Revistas_Latam.md",
                mime="text/markdown",
                use_container_width=True
            )
        with c_d2:
            if st.button("🗑️ Limpiar", use_container_width=True):
                st.session_state.revistas_latam_dossier = []
                st.rerun()
