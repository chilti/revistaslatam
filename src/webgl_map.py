# webgl_map.py - High-Performance WebGL Point Cloud Visualizer for Scientific Article Landscapes
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
    "#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
    "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1",
    "#14b8a6", "#e11d48", "#a855f7", "#0ea5e9", "#22c55e"
]

def generate_webgl_landscape_html(df_articles, color_mode="year", size_mode="citations", height=720):
    if df_articles is None or len(df_articles) == 0:
        return "<div style='color:#64748b; padding:20px;'>No hay datos para renderizar en WebGL.</div>"

    df = df_articles.copy()

    df['umap_x'] = pd.to_numeric(df['umap_x'], errors='coerce').fillna(0.0)
    df['umap_y'] = pd.to_numeric(df['umap_y'], errors='coerce').fillna(0.0)

    min_x, max_x = df['umap_x'].min(), df['umap_x'].max()
    min_y, max_y = df['umap_y'].min(), df['umap_y'].max()
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
            colors_rgb.extend([0.22, 0.74, 0.97, 0.88])

    titles = df['title'].fillna('Sin título').astype(str).str.slice(0, 160).tolist()
    journals = df['journal_name'].fillna('Revista').astype(str).tolist() if 'journal_name' in df.columns else [''] * len(df)
    countries = df['country_code'].fillna('').astype(str).tolist() if 'country_code' in df.columns else [''] * len(df)
    years = df['publication_year'].fillna(0).astype(int).tolist() if 'publication_year' in df.columns else [0] * len(df)
    citations = df['cited_by_count'].fillna(0).astype(int).tolist() if 'cited_by_count' in df.columns else [0] * len(df)
    fwcis = df['fwci'].fillna(0.0).round(2).tolist() if 'fwci' in df.columns else [0.0] * len(df)
    comms = df['community_name'].fillna('General').astype(str).tolist() if 'community_name' in df.columns else [''] * len(df)
    oa = df['oa_status'].fillna('closed').astype(str).tolist() if 'oa_status' in df.columns else [''] * len(df)
    
    if 'authors' in df.columns:
        authors_list = df['authors'].fillna('Autores no disponibles').astype(str).tolist()
    else:
        authors_list = ['Autores no disponibles'] * len(df)

    if 'id' in df.columns:
        openalex_urls = df['id'].fillna('').astype(str).tolist()
    else:
        openalex_urls = ['https://openalex.org'] * len(df)

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

    html_code = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        html, body { width: 100%; height: 100%; overflow: hidden; background: #ffffff; color: #1e293b; }
        #webgl-container { position: relative; width: 100%; height: """ + str(height) + """px; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; }
        canvas { width: 100%; height: 100%; display: block; cursor: crosshair; }
        
        .hud-controls {
            position: absolute; top: 14px; left: 16px; z-index: 10;
            display: flex; gap: 8px; align-items: center;
            background: rgba(255, 255, 255, 0.95); backdrop-filter: blur(10px);
            padding: 6px 14px; border-radius: 8px; border: 1px solid #cbd5e1;
            box-shadow: 0 8px 32px rgba(0,0,0,0.4); font-size: 12px; color: #475569;
        }
        .hud-btn {
            background: #f8fafc; border: 1px solid #cbd5e1; color: #0f172a;
            padding: 4px 10px; border-radius: 5px; font-size: 11px; font-weight: 600; cursor: pointer;
            transition: all 0.2s;
        }
        .hud-btn:hover { background: #3b82f6; border-color: #3b82f6; color: white; }
        
        #tooltip {
            position: absolute; display: none; pointer-events: none; z-index: 100;
            background: rgba(255, 255, 255, 0.98); backdrop-filter: blur(12px);
            border: 1px solid #cbd5e1; border-radius: 10px;
            padding: 12px 16px; color: #0f172a; max-width: 390px; font-size: 12px;
            box-shadow: 0 16px 36px rgba(0,0,0,0.6), 0 0 20px rgba(56, 189, 248, 0.15);
        }
        #tooltip .t-title { font-size: 13px; font-weight: 700; color: #0284c7; margin-bottom: 6px; line-height: 1.35; }
        #tooltip .t-authors { font-size: 11.5px; color: #475569; margin-bottom: 6px; }
        #tooltip .t-meta { display: flex; flex-wrap: wrap; gap: 8px; font-size: 11px; color: #475569; margin-bottom: 6px; }
        #tooltip .t-badge { background: #f8fafc; padding: 2px 6px; border-radius: 4px; border: 1px solid #e2e8f0; }
        #tooltip .t-action { font-size: 10.5px; color: #b45309; margin-top: 6px; padding-top: 6px; border-top: 1px solid rgba(255,255,255,0.1); }
    </style>
</head>
<body>
    <div id="webgl-container">
        <div class="hud-controls">
            <span>⚡ WebGL Engine: <strong style="color:#0284c7;">""" + f"{len(df):,}" + """ artículos</strong></span>
            <button class="hud-btn" id="btn-recenter">⌖ Recentrar</button>
            <span style="font-size:11px; color:#94a3b8;">| Rueda: Zoom • Arrastrar: Pan • <strong style="color:#b45309;">🖱️ Clic Derecho: Abrir en OpenAlex ↗</strong></span>
        </div>
        <canvas id="glcanvas"></canvas>
        <div id="tooltip"></div>
    </div>

    <script>
        const DATA = """ + payload_json + """;
        const container = document.getElementById('webgl-container');
        const canvas = document.getElementById('glcanvas');
        const tooltip = document.getElementById('tooltip');
        const btnRecenter = document.getElementById('btn-recenter');

        const gl = canvas.getContext('webgl', { antialias: true, alpha: false });
        if (!gl) {
            container.innerHTML = '<div style="color:red; padding:20px;">WebGL no disponible en el navegador.</div>';
        }

        function resizeCanvas() {
            const width = container.clientWidth;
            const height = container.clientHeight;
            if (canvas.width !== width || canvas.height !== height) {
                canvas.width = width;
                canvas.height = height;
                gl.viewport(0, 0, width, height);
            }
        }
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
            void main() {
                vec2 pos = (a_position * u_zoom) + u_translation;
                float aspect = u_resolution.x / u_resolution.y;
                vec2 clipSpace = vec2(pos.x / aspect, pos.y);
                gl_Position = vec4(clipSpace, 0.0, 1.0);
                gl_PointSize = clamp(a_size * sqrt(u_zoom), 2.5, 28.0);
                v_color = a_color;
            }
        `;

        const fsSource = `
            precision mediump float;
            varying vec4 v_color;
            void main() {
                vec2 coord = gl_PointCoord - vec2(0.5);
                float dist = length(coord);
                if (dist > 0.5) {
                    discard;
                }
                float alpha = smoothstep(0.5, 0.38, dist);
                gl_FragColor = vec4(v_color.rgb, v_color.a * alpha);
            }
        `;

        function createShader(gl, type, source) {
            const shader = gl.createShader(type);
            gl.shaderSource(shader, source);
            gl.compileShader(shader);
            if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
                console.error(gl.getShaderInfoLog(shader));
                gl.deleteShader(shader);
                return null;
            }
            return shader;
        }

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
        for (let i = 0; i < DATA.total; i++) {
            posData[i * 2] = DATA.x[i];
            posData[i * 2 + 1] = DATA.y[i];
        }
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

        function render() {
            resizeCanvas();
            gl.clearColor(1.0, 1.0, 1.0, 1.0);
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
        }

        render();

        btnRecenter.addEventListener('click', () => {
            zoom = 1.0; transX = 0.0; transY = 0.0;
            render();
        });

        canvas.addEventListener('mousedown', (e) => {
            if (e.button === 0) {
                isDragging = true;
                startX = e.clientX;
                startY = e.clientY;
            }
        });

        window.addEventListener('mouseup', () => { isDragging = false; });

        window.addEventListener('mousemove', (e) => {
            if (isDragging) {
                const dx = (e.clientX - startX) / (canvas.width / 2);
                const dy = -(e.clientY - startY) / (canvas.height / 2);
                transX += dx;
                transY += dy;
                startX = e.clientX;
                startY = e.clientY;
                render();
            }
        });

        canvas.addEventListener('wheel', (e) => {
            e.preventDefault();
            const factor = e.deltaY < 0 ? 1.15 : 0.87;
            zoom = Math.max(0.3, Math.min(zoom * factor, 45.0));
            render();
            checkHover(e);
        }, { passive: false });

        function getScreenCoords(normX, normY) {
            const aspect = canvas.width / canvas.height;
            const clipX = (normX * zoom + transX) / aspect;
            const clipY = normY * zoom + transY;
            const screenX = (clipX + 1.0) * 0.5 * canvas.width;
            const screenY = (1.0 - (clipY + 1.0) * 0.5) * canvas.height;
            return { x: screenX, y: screenY };
        }

        function checkHover(e) {
            const rect = canvas.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            const mouseY = e.clientY - rect.top;

            let closestIdx = -1;
            let minDist = 20.0;

            for (let i = 0; i < DATA.total; i++) {
                const s = getScreenCoords(DATA.x[i], DATA.y[i]);
                const d = Math.hypot(mouseX - s.x, mouseY - s.y);
                if (d < minDist) {
                    minDist = d;
                    closestIdx = i;
                }
            }

            if (closestIdx !== -1) {
                hoveredIdx = closestIdx;
                const authStr = DATA.authors[closestIdx] || 'Autores no disponibles';
                tooltip.innerHTML = `
                    <div class="t-title">` + DATA.titles[closestIdx] + `</div>
                    <div class="t-authors">👥 <strong>Autores:</strong> ` + authStr + `</div>
                    <div class="t-meta">
                        <span class="t-badge">📖 ` + DATA.journals[closestIdx] + ` (` + DATA.countries[closestIdx] + `)</span>
                        <span class="t-badge">📅 ` + DATA.years[closestIdx] + `</span>
                        <span class="t-badge">📊 ` + DATA.citations[closestIdx] + ` citas</span>
                        <span class="t-badge">⚡ FWCI: ` + DATA.fwci[closestIdx] + `</span>
                    </div>
                    <div class="t-meta">
                        <span class="t-badge">🏷️ ` + DATA.communities[closestIdx] + `</span>
                        <span class="t-badge">🌐 OA: ` + DATA.oa[closestIdx] + `</span>
                    </div>
                    <div class="t-action">🖱️ <strong>Clic Derecho:</strong> Abrir artículo en OpenAlex ↗</div>
                `;
                tooltip.style.display = 'block';
                
                let tx = e.clientX - rect.left + 15;
                let ty = e.clientY - rect.top + 15;
                if (tx + 400 > canvas.width) tx -= 420;
                if (ty + 210 > canvas.height) ty -= 220;
                tooltip.style.left = tx + 'px';
                tooltip.style.top = ty + 'px';
            } else {
                hoveredIdx = -1;
                tooltip.style.display = 'none';
            }
        }

        canvas.addEventListener('mousemove', checkHover);
        canvas.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });

        canvas.addEventListener('contextmenu', (e) => {
            e.preventDefault();
            if (hoveredIdx !== -1) {
                const url = DATA.urls[hoveredIdx];
                if (url) {
                    const targetUrl = url.startsWith('http') ? url : 'https://openalex.org/' + url;
                    window.open(targetUrl, '_blank');
                }
            }
        });
    </script>
</body>
</html>"""
    return html_code


def generate_webgl_journals_html(df_journals, color_var="Comunidad Temática", size_metric="Total Artículos (works_count)", height=720):
    """
    Renders high-performance WebGL 2D scatter plot for scientific journals in UMAP space.
    """
    if df_journals is None or len(df_journals) == 0:
        return "<div style='color:#64748b; padding:20px;'>No hay datos de revistas para renderizar en WebGL.</div>"

    df = df_journals.copy()

    df['umap_x'] = pd.to_numeric(df['umap_x'], errors='coerce').fillna(0.0)
    df['umap_y'] = pd.to_numeric(df['umap_y'], errors='coerce').fillna(0.0)

    min_x, max_x = df['umap_x'].min(), df['umap_x'].max()
    min_y, max_y = df['umap_y'].min(), df['umap_y'].max()
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
            colors_rgb.extend([0.15, 0.65, 0.90, 0.90])

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

    if 'id' in df.columns:
        openalex_urls = df['id'].fillna('').astype(str).tolist()
    else:
        openalex_urls = ['https://openalex.org'] * len(df)

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

    html_code = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        html, body { width: 100%; height: 100%; overflow: hidden; background: #ffffff; color: #1e293b; }
        #webgl-container { position: relative; width: 100%; height: """ + str(height) + """px; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; }
        canvas { width: 100%; height: 100%; display: block; cursor: crosshair; }
        
        .hud-controls {
            position: absolute; top: 14px; left: 16px; z-index: 10;
            display: flex; gap: 8px; align-items: center;
            background: rgba(255, 255, 255, 0.95); backdrop-filter: blur(10px);
            padding: 6px 14px; border-radius: 8px; border: 1px solid #cbd5e1;
            box-shadow: 0 8px 32px rgba(0,0,0,0.4); font-size: 12px; color: #475569;
        }
        .hud-btn {
            background: #f8fafc; border: 1px solid #cbd5e1; color: #0f172a;
            padding: 4px 10px; border-radius: 5px; font-size: 11px; font-weight: 600; cursor: pointer;
            transition: all 0.2s;
        }
        .hud-btn:hover { background: #3b82f6; border-color: #3b82f6; color: white; }
        
        #tooltip {
            position: absolute; display: none; pointer-events: none; z-index: 100;
            background: rgba(255, 255, 255, 0.98); backdrop-filter: blur(12px);
            border: 1px solid #cbd5e1; border-radius: 10px;
            padding: 12px 16px; color: #0f172a; max-width: 420px; font-size: 12px;
            box-shadow: 0 16px 36px rgba(0,0,0,0.6), 0 0 20px rgba(56, 189, 248, 0.15);
        }
        #tooltip .t-title { font-size: 13.5px; font-weight: 700; color: #0284c7; margin-bottom: 5px; line-height: 1.35; }
        #tooltip .t-publisher { font-size: 11.5px; color: #475569; margin-bottom: 6px; }
        #tooltip .t-meta { display: flex; flex-wrap: wrap; gap: 6px; font-size: 11px; color: #475569; margin-bottom: 5px; }
        #tooltip .t-badge { background: #f8fafc; padding: 2px 6px; border-radius: 4px; border: 1px solid #e2e8f0; }
        #tooltip .t-badge-idx { background: #f0fdf4; color: #166534; font-weight: 600; padding: 2px 6px; border-radius: 4px; border: 1px solid #bbf7d0; }
        #tooltip .t-action { font-size: 10.5px; color: #b45309; margin-top: 6px; padding-top: 6px; border-top: 1px solid #e2e8f0; }
    </style>
</head>
<body>
    <div id="webgl-container">
        <div class="hud-controls">
            <span>⚡ WebGL Engine: <strong style="color:#0284c7;">""" + f"{len(df):,}" + """ revistas</strong></span>
            <button class="hud-btn" id="btn-recenter">⌖ Recentrar</button>
            <span style="font-size:11px; color:#94a3b8;">| Rueda: Zoom • Arrastrar: Pan • <strong style="color:#b45309;">🖱️ Clic Derecho: Abrir en OpenAlex ↗</strong></span>
        </div>
        <canvas id="glcanvas"></canvas>
        <div id="tooltip"></div>
    </div>

    <script>
        const DATA = """ + payload_json + """;
        const container = document.getElementById('webgl-container');
        const canvas = document.getElementById('glcanvas');
        const tooltip = document.getElementById('tooltip');
        const btnRecenter = document.getElementById('btn-recenter');

        const gl = canvas.getContext('webgl', { antialias: true, alpha: false });
        if (!gl) {
            container.innerHTML = '<div style="color:red; padding:20px;">WebGL no disponible en el navegador.</div>';
        }

        function resizeCanvas() {
            const width = container.clientWidth;
            const height = container.clientHeight;
            if (canvas.width !== width || canvas.height !== height) {
                canvas.width = width;
                canvas.height = height;
                gl.viewport(0, 0, width, height);
            }
        }
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
            void main() {
                vec2 pos = (a_position * u_zoom) + u_translation;
                float aspect = u_resolution.x / u_resolution.y;
                vec2 clipSpace = vec2(pos.x / aspect, pos.y);
                gl_Position = vec4(clipSpace, 0.0, 1.0);
                gl_PointSize = clamp(a_size * sqrt(u_zoom), 4.0, 38.0);
                v_color = a_color;
            }
        `;

        const fsSource = `
            precision mediump float;
            varying vec4 v_color;
            void main() {
                vec2 coord = gl_PointCoord - vec2(0.5);
                float dist = length(coord);
                if (dist > 0.5) {
                    discard;
                }
                float alpha = smoothstep(0.5, 0.38, dist);
                float ring = smoothstep(0.48, 0.44, dist) - smoothstep(0.44, 0.40, dist);
                vec3 finalColor = mix(v_color.rgb, vec3(1.0, 1.0, 1.0), ring * 0.35);
                gl_FragColor = vec4(finalColor, v_color.a * alpha);
            }
        `;

        function createShader(gl, type, source) {
            const shader = gl.createShader(type);
            gl.shaderSource(shader, source);
            gl.compileShader(shader);
            if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
                console.error(gl.getShaderInfoLog(shader));
                gl.deleteShader(shader);
                return null;
            }
            return shader;
        }

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
        for (let i = 0; i < DATA.total; i++) {
            posData[i * 2] = DATA.x[i];
            posData[i * 2 + 1] = DATA.y[i];
        }
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

        function render() {
            resizeCanvas();
            gl.clearColor(1.0, 1.0, 1.0, 1.0);
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
        }

        render();

        btnRecenter.addEventListener('click', () => {
            zoom = 1.0; transX = 0.0; transY = 0.0;
            render();
        });

        canvas.addEventListener('mousedown', (e) => {
            if (e.button === 0) {
                isDragging = true;
                startX = e.clientX;
                startY = e.clientY;
            }
        });

        window.addEventListener('mouseup', () => { isDragging = false; });

        window.addEventListener('mousemove', (e) => {
            if (isDragging) {
                const dx = (e.clientX - startX) / (canvas.width / 2);
                const dy = -(e.clientY - startY) / (canvas.height / 2);
                transX += dx;
                transY += dy;
                startX = e.clientX;
                startY = e.clientY;
                render();
            }
        });

        canvas.addEventListener('wheel', (e) => {
            e.preventDefault();
            const factor = e.deltaY < 0 ? 1.15 : 0.87;
            zoom = Math.max(0.3, Math.min(zoom * factor, 45.0));
            render();
            checkHover(e);
        }, { passive: false });

        function getScreenCoords(normX, normY) {
            const aspect = canvas.width / canvas.height;
            const clipX = (normX * zoom + transX) / aspect;
            const clipY = normY * zoom + transY;
            const screenX = (clipX + 1.0) * 0.5 * canvas.width;
            const screenY = (1.0 - (clipY + 1.0) * 0.5) * canvas.height;
            return { x: screenX, y: screenY };
        }

        function checkHover(e) {
            const rect = canvas.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            const mouseY = e.clientY - rect.top;

            let closestIdx = -1;
            let minDist = 22.0;

            for (let i = 0; i < DATA.total; i++) {
                const s = getScreenCoords(DATA.x[i], DATA.y[i]);
                const d = Math.hypot(mouseX - s.x, mouseY - s.y);
                if (d < minDist) {
                    minDist = d;
                    closestIdx = i;
                }
            }

            if (closestIdx !== -1) {
                hoveredIdx = closestIdx;
                tooltip.innerHTML = `
                    <div class="t-title">📖 ` + DATA.titles[closestIdx] + `</div>
                    <div class="t-publisher">🏢 <strong>Editorial:</strong> ` + DATA.publishers[closestIdx] + ` • <strong>País:</strong> ` + DATA.countries[closestIdx] + `</div>
                    <div class="t-meta">
                        <span class="t-badge">🏷️ ` + DATA.communities[closestIdx] + `</span>
                        <span class="t-badge">📄 ` + DATA.works[closestIdx].toLocaleString() + ` arts</span>
                        <span class="t-badge">📊 ` + DATA.citations[closestIdx].toLocaleString() + ` citas</span>
                        <span class="t-badge">⚡ FWCI: ` + DATA.fwci[closestIdx] + `</span>
                    </div>
                    <div class="t-meta">
                        <span class="t-badge">🏆 H-index: ` + DATA.h_index[closestIdx] + `</span>
                        <span class="t-badge">📈 PR: ` + DATA.pagerank[closestIdx] + `</span>
                        <span class="t-badge">💎 OA Diamante: ` + DATA.pct_diamond[closestIdx] + `%</span>
                    </div>
                    <div class="t-meta">
                        <span class="t-badge-idx">DOAJ: ` + DATA.doaj[closestIdx] + `</span>
                        <span class="t-badge-idx">SciELO: ` + DATA.scielo[closestIdx] + `</span>
                        <span class="t-badge-idx">Scopus: ` + DATA.scopus[closestIdx] + `</span>
                    </div>
                    <div class="t-action">🖱️ <strong>Clic Derecho:</strong> Abrir revista en OpenAlex ↗</div>
                `;
                tooltip.style.display = 'block';
                
                let tx = e.clientX - rect.left + 15;
                let ty = e.clientY - rect.top + 15;
                if (tx + 430 > canvas.width) tx -= 450;
                if (ty + 230 > canvas.height) ty -= 240;
                tooltip.style.left = tx + 'px';
                tooltip.style.top = ty + 'px';
            } else {
                hoveredIdx = -1;
                tooltip.style.display = 'none';
            }
        }

        canvas.addEventListener('mousemove', checkHover);
        canvas.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });

        canvas.addEventListener('contextmenu', (e) => {
            e.preventDefault();
            if (hoveredIdx !== -1) {
                const url = DATA.urls[hoveredIdx];
                if (url) {
                    const targetUrl = url.startsWith('http') ? url : 'https://openalex.org/' + url;
                    window.open(targetUrl, '_blank');
                }
            }
        });
    </script>
</body>
</html>"""
    return html_code

