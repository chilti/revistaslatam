# webgl_map.py - High-Performance WebGL Point Cloud Visualizer for Scientific Article Landscapes and Journals
import json
import numpy as np
import pandas as pd

def get_turbo_rgb(val_norm):
    t = max(0.0, min(1.0, float(val_norm)))
    r = int(np.clip(34.61 + t * (1172.33 + t * (-10793.56 + t * (33300.12 + t * (-38394.49 + t * 14825.25)))), 0, 255))
    g = int(np.clip(23.31 + t * (557.33 + t * (1225.33 + t * (-3574.96 + t * (1073.77 + t * 707.56)))), 0, 255))
    b = int(np.clip(27.2 + t * (3211.1 + t * (-15327.97 + t * (27814.0 + t * (-22569.18 + t * 6838.66)))), 0, 255))
    return f"rgb({r},{g},{b})"

COMMUNITY_PALETTE = [
    "#0284c7", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
    "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1",
    "#14b8a6", "#e11d48", "#a855f7", "#38bdf8", "#22c55e"
]

def _get_theme_config(theme_name="☀️ Claro (Blanco)"):
    """
    Returns visual styling and WebGL clearColor parameters based on the selected theme.
    """
    if theme_name == "🌙 Oscuro (Dark)":
        return {
            'bg_color': '#0f172a',
            'border_color': '#334155',
            'gl_r': '0.059', 'gl_g': '0.090', 'gl_b': '0.165',
            'hud_bg': 'rgba(15, 23, 42, 0.94)',
            'hud_text': '#f8fafc',
            'hud_border': '#334155',
            'hud_subtext': '#94a3b8',
            'btn_bg': '#1e293b',
            'btn_text': '#f8fafc',
            'btn_border': '#334155',
            'btn_hover_bg': '#38bdf8',
            'btn_hover_text': '#0f172a',
            'tooltip_bg': 'rgba(15, 23, 42, 0.98)',
            'tooltip_border': '#334155',
            'tooltip_text': '#f8fafc',
            'tooltip_title': '#38bdf8',
            'tooltip_sub': '#94a3b8',
            'tooltip_shadow': '0 16px 36px rgba(0,0,0,0.7)',
            'badge_bg': '#1e293b',
            'badge_border': '#334155',
            'badge_text': '#cbd5e1',
            'accent_color': '#38bdf8'
        }
    elif theme_name == "🌌 Azul Noche (Navy)":
        return {
            'bg_color': '#071731',
            'border_color': '#1e3a8a',
            'gl_r': '0.027', 'gl_g': '0.090', 'gl_b': '0.192',
            'hud_bg': 'rgba(7, 23, 49, 0.94)',
            'hud_text': '#e0f2fe',
            'hud_border': '#1e3a8a',
            'hud_subtext': '#7dd3fc',
            'btn_bg': '#0f274a',
            'btn_text': '#e0f2fe',
            'btn_border': '#1e3a8a',
            'btn_hover_bg': '#0ea5e9',
            'btn_hover_text': '#ffffff',
            'tooltip_bg': 'rgba(7, 23, 49, 0.98)',
            'tooltip_border': '#1e3a8a',
            'tooltip_text': '#e0f2fe',
            'tooltip_title': '#38bdf8',
            'tooltip_sub': '#7dd3fc',
            'tooltip_shadow': '0 16px 36px rgba(0,0,0,0.8)',
            'badge_bg': '#0f274a',
            'badge_border': '#1e3a8a',
            'badge_text': '#7dd3fc',
            'accent_color': '#0ea5e9'
        }
    else:
        # Default: ☀️ Claro (Blanco)
        return {
            'bg_color': '#ffffff',
            'border_color': '#e2e8f0',
            'gl_r': '1.0', 'gl_g': '1.0', 'gl_b': '1.0',
            'hud_bg': 'rgba(255, 255, 255, 0.95)',
            'hud_text': '#1e293b',
            'hud_border': '#cbd5e1',
            'hud_subtext': '#64748b',
            'btn_bg': '#f8fafc',
            'btn_text': '#0f172a',
            'btn_border': '#cbd5e1',
            'btn_hover_bg': '#0284c7',
            'btn_hover_text': '#ffffff',
            'tooltip_bg': 'rgba(255, 255, 255, 0.98)',
            'tooltip_border': '#cbd5e1',
            'tooltip_text': '#0f172a',
            'tooltip_title': '#0284c7',
            'tooltip_sub': '#475569',
            'tooltip_shadow': '0 16px 36px rgba(0,0,0,0.12), 0 0 20px rgba(2, 132, 199, 0.08)',
            'badge_bg': '#f8fafc',
            'badge_border': '#e2e8f0',
            'badge_text': '#475569',
            'accent_color': '#0284c7'
        }

def generate_webgl_landscape_html(df_articles, color_mode="year", size_mode="citations", height=720, theme_name="☀️ Claro (Blanco)"):
    if df_articles is None or len(df_articles) == 0:
        return "<div style='color:#64748b; padding:20px;'>No hay datos para renderizar en WebGL.</div>"

    df = df_articles.copy()
    cfg = _get_theme_config(theme_name)

    df['umap_x'] = pd.to_numeric(df['umap_x'], errors='coerce').fillna(0.0)
    df['umap_y'] = pd.to_numeric(df['umap_y'], errors='coerce').fillna(0.0)

    min_x, max_x = float(df['umap_x'].min()), float(df['umap_x'].max())
    min_y, max_y = float(df['umap_y'].min()), float(df['umap_y'].max())
    span_x = max_x - min_x if max_x > min_x else 1.0
    span_y = max_y - min_y if max_y > min_y else 1.0

    norm_x = (((df['umap_x'] - min_x) / span_x) * 1.8 - 0.9).round(4).tolist()
    norm_y = (((df['umap_y'] - min_y) / span_y) * 1.8 - 0.9).round(4).tolist()

    if size_mode == "citations" and 'cited_by_count' in df.columns:
        raw_s = pd.to_numeric(df['cited_by_count'], errors='coerce').fillna(0).clip(lower=0)
    elif size_mode == "fwci" and 'fwci' in df.columns:
        raw_s = pd.to_numeric(df['fwci'], errors='coerce').fillna(0.1).clip(lower=0.01)
    else:
        raw_s = pd.Series(np.ones(len(df)))

    p98 = raw_s.quantile(0.98) if len(raw_s) > 10 else raw_s.max()
    p98 = max(float(p98), 0.1)
    sizes = (4.0 + 9.0 * np.sqrt((raw_s / p98).clip(0.0, 1.0))).round(2).tolist()

    colors_rgb = []
    
    if color_mode == "year" and 'publication_year' in df.columns:
        y_min = float(df['publication_year'].min()) if df['publication_year'].min() > 1900 else 1980.0
        y_max = float(df['publication_year'].max()) if df['publication_year'].max() > 1900 else 2026.0
        y_span = y_max - y_min if y_max > y_min else 1.0
        for y in df['publication_year'].fillna(y_min):
            val_norm = (float(y) - y_min) / y_span
            t = max(0.0, min(1.0, val_norm))
            r = np.clip(34.61 + t * (1172.33 + t * (-10793.56 + t * (33300.12 + t * (-38394.49 + t * 14825.25)))), 0, 255) / 255.0
            g = np.clip(23.31 + t * (557.33 + t * (1225.33 + t * (-3574.96 + t * (1073.77 + t * 707.56)))), 0, 255) / 255.0
            b = np.clip(27.2 + t * (3211.1 + t * (-15327.97 + t * (27814.0 + t * (-22569.18 + t * 6838.66)))), 0, 255) / 255.0
            colors_rgb.extend([round(r, 3), round(g, 3), round(b, 3), 0.88])
    elif color_mode == "community" and 'cluster_id' in df.columns:
        for cid in df['cluster_id'].fillna(0):
            hex_c = COMMUNITY_PALETTE[int(cid) % len(COMMUNITY_PALETTE)]
            r = int(hex_c[1:3], 16) / 255.0
            g = int(hex_c[3:5], 16) / 255.0
            b = int(hex_c[5:7], 16) / 255.0
            colors_rgb.extend([round(r, 3), round(g, 3), round(b, 3), 0.88])
    elif color_mode == "fwci" and 'fwci' in df.columns:
        for f in df['fwci'].fillna(0.5):
            val_norm = min(float(f) / 3.0, 1.0)
            r = round(val_norm * 0.9, 3)
            g = round(np.sqrt(val_norm) * 0.85 + 0.15, 3)
            b = round((1.0 - val_norm) * 0.8 + 0.2, 3)
            colors_rgb.extend([r, g, b, 0.88])
    else:
        for _ in range(len(df)):
            colors_rgb.extend([0.02, 0.52, 0.78, 0.88])

    titles = df['title'].fillna('Sin título').astype(str).str.slice(0, 160).tolist()
    journals = df['journal_name'].fillna('Revista').astype(str).tolist() if 'journal_name' in df.columns else [''] * len(df)
    countries = df['country_code'].fillna('').astype(str).tolist() if 'country_code' in df.columns else [''] * len(df)
    years = df['publication_year'].fillna(0).astype(int).tolist() if 'publication_year' in df.columns else [0] * len(df)
    citations = df['cited_by_count'].fillna(0).astype(int).tolist() if 'cited_by_count' in df.columns else [0] * len(df)
    fwcis = df['fwci'].fillna(0.0).round(2).tolist() if 'fwci' in df.columns else [0.0] * len(df)
    comms = df['community_name'].fillna('General').astype(str).tolist() if 'community_name' in df.columns else [''] * len(df)
    oa = df['oa_status'].fillna('closed').astype(str).tolist() if 'oa_status' in df.columns else [''] * len(df)
    
    authors_list = df['authors'].fillna('Autores no disponibles').astype(str).tolist() if 'authors' in df.columns else ['Autores no disponibles'] * len(df)
    openalex_urls = df['id'].fillna('').astype(str).tolist() if 'id' in df.columns else ['https://openalex.org'] * len(df)

    payload = {
        'x': norm_x,
        'y': norm_y,
        'sizes': sizes,
        'colors': colors_rgb,
        'titles': titles,
        'authors': authors_list,
        'journals': journals,
        'countries': countries,
        'years': years,
        'citations': citations,
        'fwci': fwcis,
        'communities': comms,
        'oa': oa,
        'urls': openalex_urls,
        'total': len(df)
    }

    payload_json = json.dumps(payload, ensure_ascii=False)

    html_code = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
        html, body {{ width: 100%; height: 100%; overflow: hidden; background: {cfg['bg_color']}; color: {cfg['hud_text']}; }}
        #webgl-container {{ position: relative; width: 100%; height: {height}px; background: {cfg['bg_color']}; border: 1px solid {cfg['border_color']}; border-radius: 10px; overflow: hidden; }}
        canvas {{ width: 100%; height: 100%; display: block; cursor: crosshair; background: {cfg['bg_color']}; }}
        
        .hud-controls {{
            position: absolute; top: 14px; left: 16px; z-index: 10;
            display: flex; gap: 8px; align-items: center;
            background: {cfg['hud_bg']}; backdrop-filter: blur(10px);
            padding: 6px 14px; border-radius: 8px; border: 1px solid {cfg['hud_border']};
            box-shadow: 0 4px 16px rgba(0,0,0,0.08); font-size: 12px; color: {cfg['hud_text']};
        }}
        .hud-btn {{
            background: {cfg['btn_bg']}; border: 1px solid {cfg['btn_border']}; color: {cfg['btn_text']};
            padding: 4px 10px; border-radius: 5px; font-size: 11px; font-weight: 600; cursor: pointer;
            transition: all 0.2s;
        }}
        .hud-btn:hover {{ background: {cfg['btn_hover_bg']}; border-color: {cfg['btn_hover_bg']}; color: {cfg['btn_hover_text']}; }}
        
        #tooltip {{
            position: absolute; display: none; pointer-events: none; z-index: 100;
            background: {cfg['tooltip_bg']}; backdrop-filter: blur(12px);
            border: 1px solid {cfg['tooltip_border']}; border-radius: 10px;
            padding: 12px 16px; color: {cfg['tooltip_text']}; max-width: 390px; font-size: 12px;
            box-shadow: {cfg['tooltip_shadow']};
        }}
        #tooltip .t-title {{ font-size: 13px; font-weight: 700; color: {cfg['tooltip_title']}; margin-bottom: 6px; line-height: 1.35; }}
        #tooltip .t-authors {{ font-size: 11.5px; color: {cfg['tooltip_sub']}; margin-bottom: 6px; }}
        #tooltip .t-meta {{ display: flex; flex-wrap: wrap; gap: 8px; font-size: 11px; color: {cfg['tooltip_sub']}; margin-bottom: 6px; }}
        #tooltip .t-badge {{ background: {cfg['badge_bg']}; padding: 2px 6px; border-radius: 4px; border: 1px solid {cfg['badge_border']}; color: {cfg['badge_text']}; }}
        #tooltip .t-action {{ font-size: 10.5px; color: #f59e0b; margin-top: 6px; padding-top: 6px; border-top: 1px solid {cfg['border_color']}; }}
    </style>
</head>
<body>
    <div id="webgl-container">
        <div class="hud-controls">
            <span>⚡ WebGL Engine: <strong style="color:{cfg['accent_color']};">{len(df):,} artículos</strong></span>
            <button class="hud-btn" id="btn-recenter">⌖ Recentrar</button>
            <span style="font-size:11px; color:{cfg['hud_subtext']};">| Rueda: Zoom • Arrastrar: Pan • <strong style="color:#f59e0b;">🖱️ Clic Derecho: Abrir en OpenAlex ↗</strong></span>
        </div>
        <canvas id="glcanvas"></canvas>
        <div id="tooltip"></div>
    </div>

    <script>
        const DATA = {payload_json};
        const container = document.getElementById('webgl-container');
        const canvas = document.getElementById('glcanvas');
        const tooltip = document.getElementById('tooltip');
        const btnRecenter = document.getElementById('btn-recenter');

        const gl = canvas.getContext('webgl', {{ antialias: true, alpha: false }});
        if (!gl) {{
            container.innerHTML = '<div style="color:red; padding:20px;">WebGL no disponible en el navegador.</div>';
        }}

        function resizeCanvas() {{
            const width = container.clientWidth;
            const height = container.clientHeight;
            if (canvas.width !== width || canvas.height !== height) {{
                canvas.width = width;
                canvas.height = height;
                gl.viewport(0, 0, width, height);
            }}
        }}
        window.addEventListener('resize', resizeCanvas);
        resizeCanvas();

        const vsSource = `
            attribute vec2 a_position;
            attribute vec4 a_color;
            attribute float a_size;
            uniform vec2 u_resolution;
            uniform vec2 u_translation;
            uniform float u_zoom;
            varying vec4 v_color;
            void main() {{
                vec2 pos = (a_position * u_zoom) + u_translation;
                float aspect = u_resolution.x / u_resolution.y;
                vec2 clipSpace = vec2(pos.x / aspect, pos.y);
                gl_Position = vec4(clipSpace, 0.0, 1.0);
                gl_PointSize = clamp(a_size * sqrt(u_zoom), 2.5, 28.0);
                v_color = a_color;
            }}
        `;

        const fsSource = `
            precision mediump float;
            varying vec4 v_color;
            void main() {{
                vec2 coord = gl_PointCoord - vec2(0.5);
                float dist = length(coord);
                if (dist > 0.5) {{
                    discard;
                }}
                float alpha = smoothstep(0.5, 0.40, dist);
                float ring = smoothstep(0.5, 0.46, dist);
                vec3 col = mix(v_color.rgb, v_color.rgb * 0.85, ring * 0.3);
                gl_FragColor = vec4(col, v_color.a * alpha);
            }}
        `;

        function createShader(gl, type, source) {{
            const shader = gl.createShader(type);
            gl.shaderSource(shader, source);
            gl.compileShader(shader);
            if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {{
                console.error(gl.getShaderInfoLog(shader));
                gl.deleteShader(shader);
                return null;
            }}
            return shader;
        }}

        const program = gl.createProgram();
        gl.attachShader(program, createShader(gl, gl.VERTEX_SHADER, vsSource));
        gl.attachShader(program, createShader(gl, gl.FRAGMENT_SHADER, fsSource));
        gl.linkProgram(program);
        gl.useProgram(program);

        gl.enable(gl.BLEND);
        gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);

        const aPos = gl.getAttribLocation(program, 'a_position');
        const aColor = gl.getAttribLocation(program, 'a_color');
        const aSize = gl.getAttribLocation(program, 'a_size');

        const uRes = gl.getUniformLocation(program, 'u_resolution');
        const uTrans = gl.getUniformLocation(program, 'u_translation');
        const uZoom = gl.getUniformLocation(program, 'u_zoom');

        const posBuffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, posBuffer);
        const posData = new Float32Array(DATA.total * 2);
        for (let i = 0; i < DATA.total; i++) {{
            posData[i * 2] = DATA.x[i];
            posData[i * 2 + 1] = DATA.y[i];
        }}
        gl.bufferData(gl.ARRAY_BUFFER, posData, gl.STATIC_DRAW);

        const colorBuffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, colorBuffer);
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(DATA.colors), gl.STATIC_DRAW);

        const sizeBuffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, sizeBuffer);
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(DATA.sizes), gl.STATIC_DRAW);

        let zoom = 1.0;
        let transX = 0.0;
        let transY = 0.0;
        let isDragging = false;
        let startX = 0, startY = 0;
        let hoveredIdx = -1;

        function render() {{
            resizeCanvas();
            gl.clearColor({cfg['gl_r']}, {cfg['gl_g']}, {cfg['gl_b']}, 1.0);
            gl.clear(gl.COLOR_BUFFER_BIT);

            gl.uniform2f(uRes, canvas.width, canvas.height);
            gl.uniform2f(uTrans, transX, transY);
            gl.uniform1f(uZoom, zoom);

            gl.bindBuffer(gl.ARRAY_BUFFER, posBuffer);
            gl.enableVertexAttribArray(aPos);
            gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0);

            gl.bindBuffer(gl.ARRAY_BUFFER, colorBuffer);
            gl.enableVertexAttribArray(aColor);
            gl.vertexAttribPointer(aColor, 4, gl.FLOAT, false, 0, 0);

            gl.bindBuffer(gl.ARRAY_BUFFER, sizeBuffer);
            gl.enableVertexAttribArray(aSize);
            gl.vertexAttribPointer(aSize, 1, gl.FLOAT, false, 0, 0);

            gl.drawArrays(gl.POINTS, 0, DATA.total);
        }}

        render();

        btnRecenter.addEventListener('click', () => {{
            zoom = 1.0; transX = 0.0; transY = 0.0;
            render();
        }});

        canvas.addEventListener('mousedown', (e) => {{
            if (e.button === 0) {{
                isDragging = true;
                startX = e.clientX;
                startY = e.clientY;
            }}
        }});

        window.addEventListener('mouseup', () => {{ isDragging = false; }});

        window.addEventListener('mousemove', (e) => {{
            if (isDragging) {{
                const dx = (e.clientX - startX) / (canvas.width / 2);
                const dy = -(e.clientY - startY) / (canvas.height / 2);
                transX += dx;
                transY += dy;
                startX = e.clientX;
                startY = e.clientY;
                render();
            }} else {{
                // Tooltip hit-test
                const rect = canvas.getBoundingClientRect();
                const mouseX = e.clientX - rect.left;
                const mouseY = e.clientY - rect.top;

                const aspect = canvas.width / canvas.height;
                const clipX = (mouseX / canvas.width) * 2.0 - 1.0;
                const clipY = -((mouseY / canvas.height) * 2.0 - 1.0);

                const dataX = ((clipX * aspect) - transX) / zoom;
                const dataY = (clipY - transY) / zoom;

                let closest = -1;
                let minDist = 0.045 / zoom;

                for (let i = 0; i < DATA.total; i++) {{
                    const dx = DATA.x[i] - dataX;
                    const dy = DATA.y[i] - dataY;
                    const d = Math.sqrt(dx * dx + dy * dy);
                    if (d < minDist) {{
                        minDist = d;
                        closest = i;
                    }}
                }}

                hoveredIdx = closest;

                if (closest >= 0) {{
                    tooltip.style.display = 'block';
                    let tx = mouseX + 18;
                    let ty = mouseY + 18;
                    if (tx + 360 > container.clientWidth) tx = mouseX - 360;
                    if (ty + 180 > container.clientHeight) ty = mouseY - 180;
                    tooltip.style.left = tx + 'px';
                    tooltip.style.top = ty + 'px';

                    tooltip.innerHTML = `
                        <div class="t-title">${{DATA.titles[closest]}}</div>
                        <div class="t-authors">${{DATA.authors[closest]}}</div>
                        <div class="t-meta">
                            <span class="t-badge">📖 ${{DATA.journals[closest]}}</span>
                            <span class="t-badge">📅 ${{DATA.years[closest]}}</span>
                            <span class="t-badge">📍 ${{DATA.countries[closest]}}</span>
                            <span class="t-badge">🏷️ ${{DATA.communities[closest]}}</span>
                            <span class="t-badge">✨ Citas: ${{DATA.citations[closest]}}</span>
                            <span class="t-badge">⚡ FWCI: ${{DATA.fwci[closest]}}</span>
                            <span class="t-badge">🔓 ${{DATA.oa[closest]}}</span>
                        </div>
                        <div class="t-action">🖱️ Clic derecho para abrir en OpenAlex ↗</div>
                    `;
                }} else {{
                    tooltip.style.display = 'none';
                }}
            }}
        }});

        canvas.addEventListener('wheel', (e) => {{
            e.preventDefault();
            const factor = e.deltaY < 0 ? 1.15 : 0.87;
            const newZoom = Math.max(0.5, Math.min(zoom * factor, 60.0));

            const rect = canvas.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            const mouseY = e.clientY - rect.top;

            const aspect = canvas.width / canvas.height;
            const clipX = (mouseX / canvas.width) * 2.0 - 1.0;
            const clipY = -((mouseY / canvas.height) * 2.0 - 1.0);

            transX = clipX * aspect - (clipX * aspect - transX) * (newZoom / zoom);
            transY = clipY - (clipY - transY) * (newZoom / zoom);
            zoom = newZoom;

            render();
        }}, {{ passive: false }});

        canvas.addEventListener('contextmenu', (e) => {{
            e.preventDefault();
            if (hoveredIdx >= 0 && DATA.urls[hoveredIdx]) {{
                window.open(DATA.urls[hoveredIdx], '_blank');
            }}
        }});
    </script>
</body>
</html>"""
    return html_code


def generate_webgl_journals_html(df_journals, color_var="Comunidad Temática", size_metric="Total Artículos (works_count)", height=720, theme_name="☀️ Claro (Blanco)"):
    """
    Renders high-performance WebGL 2D scatter plot for scientific journals in UMAP space.
    """
    if df_journals is None or len(df_journals) == 0:
        return "<div style='color:#64748b; padding:20px;'>No hay datos de revistas para renderizar en WebGL.</div>"

    df = df_journals.copy()
    cfg = _get_theme_config(theme_name)

    df['umap_x'] = pd.to_numeric(df['umap_x'], errors='coerce').fillna(0.0)
    df['umap_y'] = pd.to_numeric(df['umap_y'], errors='coerce').fillna(0.0)

    min_x, max_x = float(df['umap_x'].min()), float(df['umap_x'].max())
    min_y, max_y = float(df['umap_y'].min()), float(df['umap_y'].max())
    span_x = max_x - min_x if max_x > min_x else 1.0
    span_y = max_y - min_y if max_y > min_y else 1.0

    norm_x = (((df['umap_x'] - min_x) / span_x) * 1.8 - 0.9).round(4).tolist()
    norm_y = (((df['umap_y'] - min_y) / span_y) * 1.8 - 0.9).round(4).tolist()

    # Sizes
    if size_metric.startswith("Total Artículos") and 'works_count' in df.columns:
        raw_s = pd.to_numeric(df['works_count'], errors='coerce').fillna(1).clip(lower=1)
    elif size_metric.startswith("Citas Totales") and 'cited_by_count' in df.columns:
        raw_s = pd.to_numeric(df['cited_by_count'], errors='coerce').fillna(0).clip(lower=0)
    elif size_metric.startswith("FWCI") and 'fwci_avg' in df.columns:
        raw_s = pd.to_numeric(df['fwci_avg'], errors='coerce').fillna(0.1).clip(lower=0.01)
    elif size_metric.startswith("Índice H") and 'h_index' in df.columns:
        raw_s = pd.to_numeric(df['h_index'], errors='coerce').fillna(1).clip(lower=1)
    else:
        raw_s = pd.Series(np.ones(len(df)))

    p98 = raw_s.quantile(0.98) if len(raw_s) > 10 else raw_s.max()
    p98 = max(float(p98), 0.1)
    sizes = (6.0 + 16.0 * np.sqrt((raw_s / p98).clip(0.0, 1.0))).round(2).tolist()

    # Colors
    colors_rgb = []
    
    if color_var == 'Comunidad Temática' and 'community_name' in df.columns:
        unique_comms = sorted(df['community_name'].dropna().unique().tolist())
        comm_to_idx = {c: i for i, c in enumerate(unique_comms)}
        for c_val in df['community_name'].fillna('General'):
            c_idx = comm_to_idx.get(c_val, 0)
            hex_c = COMMUNITY_PALETTE[c_idx % len(COMMUNITY_PALETTE)]
            r = int(hex_c[1:3], 16) / 255.0
            g = int(hex_c[3:5], 16) / 255.0
            b = int(hex_c[5:7], 16) / 255.0
            colors_rgb.extend([round(r, 3), round(g, 3), round(b, 3), 0.90])
    elif color_var == 'FWCI Promedio' and 'fwci_avg' in df.columns:
        for f in pd.to_numeric(df['fwci_avg'], errors='coerce').fillna(0.5):
            val_norm = max(0.0, min(float(f) / 2.5, 1.0))
            t = val_norm
            r = round(np.clip(0.267 + t * (0.01 - 0.267 + t * (0.99 - 0.01)), 0.0, 1.0), 3)
            g = round(np.clip(0.004 + t * (0.55 - 0.004 + t * (0.90 - 0.55)), 0.0, 1.0), 3)
            b = round(np.clip(0.329 + t * (0.55 - 0.329 + t * (0.14 - 0.55)), 0.0, 1.0), 3)
            colors_rgb.extend([r, g, b, 0.90])
    elif color_var == 'Índice H' and 'h_index' in df.columns:
        h_max = max(float(df['h_index'].quantile(0.98)), 10.0)
        for h in pd.to_numeric(df['h_index'], errors='coerce').fillna(0):
            val_norm = max(0.0, min(float(h) / h_max, 1.0))
            t = val_norm
            r = np.clip(34.61 + t * (1172.33 + t * (-10793.56 + t * (33300.12 + t * (-38394.49 + t * 14825.25)))), 0, 255) / 255.0
            g = np.clip(23.31 + t * (557.33 + t * (1225.33 + t * (-3574.96 + t * (1073.77 + t * 707.56)))), 0, 255) / 255.0
            b = np.clip(27.2 + t * (3211.1 + t * (-15327.97 + t * (27814.0 + t * (-22569.18 + t * 6838.66)))), 0, 255) / 255.0
            colors_rgb.extend([round(r, 3), round(g, 3), round(b, 3), 0.90])
    elif color_var == 'PageRank Citas' and 'pagerank' in df.columns:
        pr_max = max(float(df['pagerank'].quantile(0.98)), 0.001)
        for pr in pd.to_numeric(df['pagerank'], errors='coerce').fillna(0):
            val_norm = max(0.0, min(float(pr) / pr_max, 1.0))
            r = round(0.1 + 0.8 * val_norm, 3)
            g = round(0.4 + 0.5 * np.sqrt(val_norm), 3)
            b = round(0.9 * (1.0 - val_norm) + 0.1, 3)
            colors_rgb.extend([r, g, b, 0.90])
    elif color_var == '% OA Diamante' and 'pct_oa_diamond' in df.columns:
        for d in pd.to_numeric(df['pct_oa_diamond'], errors='coerce').fillna(0):
            val_norm = max(0.0, min(float(d) / 100.0, 1.0))
            r = round(0.1 + 0.1 * (1.0 - val_norm), 3)
            g = round(0.4 + 0.55 * val_norm, 3)
            b = round(0.4 + 0.45 * val_norm, 3)
            colors_rgb.extend([r, g, b, 0.90])
    elif color_var == 'País' and 'country_code' in df.columns:
        unique_countries = sorted([c for c in df['country_code'].dropna().unique().tolist() if c])
        c_to_idx = {c: i for i, c in enumerate(unique_countries)}
        for c_val in df['country_code'].fillna(''):
            c_idx = c_to_idx.get(c_val, 0)
            hex_c = COMMUNITY_PALETTE[c_idx % len(COMMUNITY_PALETTE)]
            r = int(hex_c[1:3], 16) / 255.0
            g = int(hex_c[3:5], 16) / 255.0
            b = int(hex_c[5:7], 16) / 255.0
            colors_rgb.extend([round(r, 3), round(g, 3), round(b, 3), 0.90])
    else:
        for _ in range(len(df)):
            colors_rgb.extend([0.02, 0.52, 0.78, 0.90])

    titles = df['display_name'].fillna('Revista').astype(str).str.slice(0, 150).tolist()
    publishers = df['publisher'].fillna('No especificado').astype(str).str.slice(0, 80).tolist() if 'publisher' in df.columns else [''] * len(df)
    countries = df['country_code'].fillna('').astype(str).tolist() if 'country_code' in df.columns else [''] * len(df)
    works = df['works_count'].fillna(0).astype(int).tolist() if 'works_count' in df.columns else [0] * len(df)
    citations = df['cited_by_count'].fillna(0).astype(int).tolist() if 'cited_by_count' in df.columns else [0] * len(df)
    fwcis = df['fwci_avg'].fillna(0.0).round(2).tolist() if 'fwci_avg' in df.columns else [0.0] * len(df)
    h_indices = df['h_index'].fillna(0).astype(int).tolist() if 'h_index' in df.columns else [0] * len(df)
    pageranks = df['pagerank'].fillna(0.0).round(4).tolist() if 'pagerank' in df.columns else [0.0] * len(df)
    pct_diamonds = df['pct_oa_diamond'].fillna(0.0).round(1).tolist() if 'pct_oa_diamond' in df.columns else [0.0] * len(df)
    comms = df['community_name'].fillna('General').astype(str).tolist() if 'community_name' in df.columns else [''] * len(df)
    
    doaj_list = ["Sí" if v in [True, 1, 'True', 'true'] else "No" for v in df.get('is_in_doaj', [False]*len(df))]
    scielo_list = ["Sí" if v in [True, 1, 'True', 'true'] else "No" for v in df.get('is_in_scielo', [False]*len(df))]
    scopus_list = ["Sí" if v in [True, 1, 'True', 'true'] else "No" for v in df.get('is_scopus', [False]*len(df))]
    
    openalex_urls = df['id'].fillna('').astype(str).tolist() if 'id' in df.columns else ['https://openalex.org'] * len(df)

    payload = {
        'x': norm_x,
        'y': norm_y,
        'sizes': sizes,
        'colors': colors_rgb,
        'titles': titles,
        'publishers': publishers,
        'countries': countries,
        'works': works,
        'citations': citations,
        'fwci': fwcis,
        'h_index': h_indices,
        'pagerank': pageranks,
        'pct_diamond': pct_diamonds,
        'communities': comms,
        'doaj': doaj_list,
        'scielo': scielo_list,
        'scopus': scopus_list,
        'urls': openalex_urls,
        'total': len(df)
    }

    payload_json = json.dumps(payload, ensure_ascii=False)

    html_code = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
        html, body {{ width: 100%; height: 100%; overflow: hidden; background: {cfg['bg_color']}; color: {cfg['hud_text']}; }}
        #webgl-container {{ position: relative; width: 100%; height: {height}px; background: {cfg['bg_color']}; border: 1px solid {cfg['border_color']}; border-radius: 10px; overflow: hidden; }}
        canvas {{ width: 100%; height: 100%; display: block; cursor: crosshair; background: {cfg['bg_color']}; }}
        
        .hud-controls {{
            position: absolute; top: 14px; left: 16px; z-index: 10;
            display: flex; gap: 8px; align-items: center;
            background: {cfg['hud_bg']}; backdrop-filter: blur(10px);
            padding: 6px 14px; border-radius: 8px; border: 1px solid {cfg['hud_border']};
            box-shadow: 0 4px 16px rgba(0,0,0,0.08); font-size: 12px; color: {cfg['hud_text']};
        }}
        .hud-btn {{
            background: {cfg['btn_bg']}; border: 1px solid {cfg['btn_border']}; color: {cfg['btn_text']};
            padding: 4px 10px; border-radius: 5px; font-size: 11px; font-weight: 600; cursor: pointer;
            transition: all 0.2s;
        }}
        .hud-btn:hover {{ background: {cfg['btn_hover_bg']}; border-color: {cfg['btn_hover_bg']}; color: {cfg['btn_hover_text']}; }}
        
        #tooltip {{
            position: absolute; display: none; pointer-events: none; z-index: 100;
            background: {cfg['tooltip_bg']}; backdrop-filter: blur(12px);
            border: 1px solid {cfg['tooltip_border']}; border-radius: 10px;
            padding: 12px 16px; color: {cfg['tooltip_text']}; max-width: 420px; font-size: 12px;
            box-shadow: {cfg['tooltip_shadow']};
        }}
        #tooltip .t-title {{ font-size: 13.5px; font-weight: 700; color: {cfg['tooltip_title']}; margin-bottom: 5px; line-height: 1.35; }}
        #tooltip .t-publisher {{ font-size: 11.5px; color: {cfg['tooltip_sub']}; margin-bottom: 6px; }}
        #tooltip .t-meta {{ display: flex; flex-wrap: wrap; gap: 6px; font-size: 11px; color: {cfg['tooltip_sub']}; margin-bottom: 5px; }}
        #tooltip .t-badge {{ background: {cfg['badge_bg']}; padding: 2px 6px; border-radius: 4px; border: 1px solid {cfg['badge_border']}; color: {cfg['badge_text']}; }}
        #tooltip .t-badge-idx {{ background: rgba(16, 185, 129, 0.12); color: #10b981; font-weight: 600; padding: 2px 6px; border-radius: 4px; border: 1px solid rgba(16, 185, 129, 0.25); }}
        #tooltip .t-action {{ font-size: 10.5px; color: #f59e0b; margin-top: 6px; padding-top: 6px; border-top: 1px solid {cfg['border_color']}; }}
    </style>
</head>
<body>
    <div id="webgl-container">
        <div class="hud-controls">
            <span>⚡ WebGL Engine: <strong style="color:{cfg['accent_color']};">{len(df):,} revistas</strong></span>
            <button class="hud-btn" id="btn-recenter">⌖ Recentrar</button>
            <span style="font-size:11px; color:{cfg['hud_subtext']};">| Rueda: Zoom • Arrastrar: Pan • <strong style="color:#f59e0b;">🖱️ Clic Derecho: Abrir en OpenAlex ↗</strong></span>
        </div>
        <canvas id="glcanvas"></canvas>
        <div id="tooltip"></div>
    </div>

    <script>
        const DATA = {payload_json};
        const container = document.getElementById('webgl-container');
        const canvas = document.getElementById('glcanvas');
        const tooltip = document.getElementById('tooltip');
        const btnRecenter = document.getElementById('btn-recenter');

        const gl = canvas.getContext('webgl', {{ antialias: true, alpha: false }});
        if (!gl) {{
            container.innerHTML = '<div style="color:red; padding:20px;">WebGL no disponible en el navegador.</div>';
        }}

        function resizeCanvas() {{
            const width = container.clientWidth;
            const height = container.clientHeight;
            if (canvas.width !== width || canvas.height !== height) {{
                canvas.width = width;
                canvas.height = height;
                gl.viewport(0, 0, width, height);
            }}
        }}
        window.addEventListener('resize', resizeCanvas);
        resizeCanvas();

        const vsSource = `
            attribute vec2 a_position;
            attribute vec4 a_color;
            attribute float a_size;
            uniform vec2 u_resolution;
            uniform vec2 u_translation;
            uniform float u_zoom;
            varying vec4 v_color;
            void main() {{
                vec2 pos = (a_position * u_zoom) + u_translation;
                float aspect = u_resolution.x / u_resolution.y;
                vec2 clipSpace = vec2(pos.x / aspect, pos.y);
                gl_Position = vec4(clipSpace, 0.0, 1.0);
                gl_PointSize = clamp(a_size * sqrt(u_zoom), 3.0, 36.0);
                v_color = a_color;
            }}
        `;

        const fsSource = `
            precision mediump float;
            varying vec4 v_color;
            void main() {{
                vec2 coord = gl_PointCoord - vec2(0.5);
                float dist = length(coord);
                if (dist > 0.5) {{
                    discard;
                }}
                float alpha = smoothstep(0.5, 0.40, dist);
                float ring = smoothstep(0.5, 0.46, dist);
                vec3 col = mix(v_color.rgb, v_color.rgb * 0.85, ring * 0.3);
                gl_FragColor = vec4(col, v_color.a * alpha);
            }}
        `;

        function createShader(gl, type, source) {{
            const shader = gl.createShader(type);
            gl.shaderSource(shader, source);
            gl.compileShader(shader);
            if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {{
                console.error(gl.getShaderInfoLog(shader));
                gl.deleteShader(shader);
                return null;
            }}
            return shader;
        }}

        const program = gl.createProgram();
        gl.attachShader(program, createShader(gl, gl.VERTEX_SHADER, vsSource));
        gl.attachShader(program, createShader(gl, gl.FRAGMENT_SHADER, fsSource));
        gl.linkProgram(program);
        gl.useProgram(program);

        gl.enable(gl.BLEND);
        gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);

        const aPos = gl.getAttribLocation(program, 'a_position');
        const aColor = gl.getAttribLocation(program, 'a_color');
        const aSize = gl.getAttribLocation(program, 'a_size');

        const uRes = gl.getUniformLocation(program, 'u_resolution');
        const uTrans = gl.getUniformLocation(program, 'u_translation');
        const uZoom = gl.getUniformLocation(program, 'u_zoom');

        const posBuffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, posBuffer);
        const posData = new Float32Array(DATA.total * 2);
        for (let i = 0; i < DATA.total; i++) {{
            posData[i * 2] = DATA.x[i];
            posData[i * 2 + 1] = DATA.y[i];
        }}
        gl.bufferData(gl.ARRAY_BUFFER, posData, gl.STATIC_DRAW);

        const colorBuffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, colorBuffer);
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(DATA.colors), gl.STATIC_DRAW);

        const sizeBuffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, sizeBuffer);
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(DATA.sizes), gl.STATIC_DRAW);

        let zoom = 1.0;
        let transX = 0.0;
        let transY = 0.0;
        let isDragging = false;
        let startX = 0, startY = 0;
        let hoveredIdx = -1;

        function render() {{
            resizeCanvas();
            gl.clearColor({cfg['gl_r']}, {cfg['gl_g']}, {cfg['gl_b']}, 1.0);
            gl.clear(gl.COLOR_BUFFER_BIT);

            gl.uniform2f(uRes, canvas.width, canvas.height);
            gl.uniform2f(uTrans, transX, transY);
            gl.uniform1f(uZoom, zoom);

            gl.bindBuffer(gl.ARRAY_BUFFER, posBuffer);
            gl.enableVertexAttribArray(aPos);
            gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0);

            gl.bindBuffer(gl.ARRAY_BUFFER, colorBuffer);
            gl.enableVertexAttribArray(aColor);
            gl.vertexAttribPointer(aColor, 4, gl.FLOAT, false, 0, 0);

            gl.bindBuffer(gl.ARRAY_BUFFER, sizeBuffer);
            gl.enableVertexAttribArray(aSize);
            gl.vertexAttribPointer(aSize, 1, gl.FLOAT, false, 0, 0);

            gl.drawArrays(gl.POINTS, 0, DATA.total);
        }}

        render();

        btnRecenter.addEventListener('click', () => {{
            zoom = 1.0; transX = 0.0; transY = 0.0;
            render();
        }});

        canvas.addEventListener('mousedown', (e) => {{
            if (e.button === 0) {{
                isDragging = true;
                startX = e.clientX;
                startY = e.clientY;
            }}
        }});

        window.addEventListener('mouseup', () => {{ isDragging = false; }});

        window.addEventListener('mousemove', (e) => {{
            if (isDragging) {{
                const dx = (e.clientX - startX) / (canvas.width / 2);
                const dy = -(e.clientY - startY) / (canvas.height / 2);
                transX += dx;
                transY += dy;
                startX = e.clientX;
                startY = e.clientY;
                render();
            }} else {{
                // Tooltip hit-test
                const rect = canvas.getBoundingClientRect();
                const mouseX = e.clientX - rect.left;
                const mouseY = e.clientY - rect.top;

                const aspect = canvas.width / canvas.height;
                const clipX = (mouseX / canvas.width) * 2.0 - 1.0;
                const clipY = -((mouseY / canvas.height) * 2.0 - 1.0);

                const dataX = ((clipX * aspect) - transX) / zoom;
                const dataY = (clipY - transY) / zoom;

                let closest = -1;
                let minDist = 0.05 / zoom;

                for (let i = 0; i < DATA.total; i++) {{
                    const dx = DATA.x[i] - dataX;
                    const dy = DATA.y[i] - dataY;
                    const d = Math.sqrt(dx * dx + dy * dy);
                    if (d < minDist) {{
                        minDist = d;
                        closest = i;
                    }}
                }}

                hoveredIdx = closest;

                if (closest >= 0) {{
                    tooltip.style.display = 'block';
                    let tx = mouseX + 18;
                    let ty = mouseY + 18;
                    if (tx + 400 > container.clientWidth) tx = mouseX - 400;
                    if (ty + 200 > container.clientHeight) ty = mouseY - 200;
                    tooltip.style.left = tx + 'px';
                    tooltip.style.top = ty + 'px';

                    const doajBadge = DATA.doaj[closest] === "Sí" ? '<span class="t-badge-idx">✓ DOAJ</span>' : '';
                    const scieloBadge = DATA.scielo[closest] === "Sí" ? '<span class="t-badge-idx">✓ SciELO</span>' : '';
                    const scopusBadge = DATA.scopus[closest] === "Sí" ? '<span class="t-badge-idx">✓ Scopus</span>' : '';

                    tooltip.innerHTML = `
                        <div class="t-title">${{DATA.titles[closest]}}</div>
                        <div class="t-publisher">🏛️ ${{DATA.publishers[closest]}} (${{DATA.countries[closest]}})</div>
                        <div class="t-meta">
                            <span class="t-badge">🏷️ ${{DATA.communities[closest]}}</span>
                            <span class="t-badge">📄 Artículos: ${{DATA.works[closest].toLocaleString()}}</span>
                            <span class="t-badge">✨ Citas: ${{DATA.citations[closest].toLocaleString()}}</span>
                            <span class="t-badge">⚡ FWCI: ${{DATA.fwci[closest]}}</span>
                            <span class="t-badge">🎖️ H-Index: ${{DATA.h_index[closest]}}</span>
                            <span class="t-badge">💎 OA Diamante: ${{DATA.pct_diamond[closest]}}%</span>
                            ${{doajBadge}}
                            ${{scieloBadge}}
                            ${{scopusBadge}}
                        </div>
                        <div class="t-action">🖱️ Clic derecho para abrir en OpenAlex ↗</div>
                    `;
                }} else {{
                    tooltip.style.display = 'none';
                }}
            }}
        }});

        canvas.addEventListener('wheel', (e) => {{
            e.preventDefault();
            const factor = e.deltaY < 0 ? 1.15 : 0.87;
            const newZoom = Math.max(0.5, Math.min(zoom * factor, 60.0));

            const rect = canvas.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            const mouseY = e.clientY - rect.top;

            const aspect = canvas.width / canvas.height;
            const clipX = (mouseX / canvas.width) * 2.0 - 1.0;
            const clipY = -((mouseY / canvas.height) * 2.0 - 1.0);

            transX = clipX * aspect - (clipX * aspect - transX) * (newZoom / zoom);
            transY = clipY - (clipY - transY) * (newZoom / zoom);
            zoom = newZoom;

            render();
        }}, {{ passive: false }});

        canvas.addEventListener('contextmenu', (e) => {{
            e.preventDefault();
            if (hoveredIdx >= 0 && DATA.urls[hoveredIdx]) {{
                window.open(DATA.urls[hoveredIdx], '_blank');
            }}
        }});
    </script>
</body>
</html>"""
    return html_code
