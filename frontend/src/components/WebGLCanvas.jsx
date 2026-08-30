import React, { useRef, useEffect, useState } from 'react';
import { useAppStore } from '../store';
import { Maximize2, ZoomIn, ZoomOut, RotateCcw, ExternalLink } from 'lucide-react';

export default function WebGLCanvas({ points = [], colorMode = 'year', sizeMode = 'citations', height = 650 }) {
  const containerRef = useRef(null);
  const canvasRef = useRef(null);
  const { theme } = useAppStore();
  
  const [tooltipData, setTooltipData] = useState(null);
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });
  const [renderCount, setRenderCount] = useState(0);

  // WebGL State refs
  const stateRef = useRef({
    zoom: 1.0,
    transX: 0.0,
    transY: 0.0,
    isDragging: false,
    startX: 0,
    startY: 0,
    hoveredIdx: -1,
    gl: null,
    program: null,
    posBuffer: null,
    colorBuffer: null,
    sizeBuffer: null,
    total: 0,
    payload: null
  });

  const isDark = theme === 'oscuro' || theme === 'navy';
  const clearColor = theme === 'oscuro' ? [0.059, 0.090, 0.165] : theme === 'navy' ? [0.027, 0.090, 0.192] : [1.0, 1.0, 1.0];

  useEffect(() => {
    if (!points || points.length === 0) return;
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container) return;

    const gl = canvas.getContext('webgl', { antialias: true, alpha: false });
    if (!gl) return;

    stateRef.current.gl = gl;

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
          gl_PointSize = clamp(a_size * sqrt(u_zoom), 2.5, 36.0);
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
          float alpha = smoothstep(0.5, 0.40, dist);
          float ring = smoothstep(0.5, 0.46, dist);
          vec3 col = mix(v_color.rgb, v_color.rgb * 0.85, ring * 0.3);
          gl_FragColor = vec4(col, v_color.a * alpha);
      }
    `;

    function createShader(gl, type, source) {
      const shader = gl.createShader(type);
      gl.shaderSource(shader, source);
      gl.compileShader(shader);
      return shader;
    }

    const program = gl.createProgram();
    gl.attachShader(program, createShader(gl, gl.VERTEX_SHADER, vsSource));
    gl.attachShader(program, createShader(gl, gl.FRAGMENT_SHADER, fsSource));
    gl.linkProgram(program);
    gl.useProgram(program);

    gl.enable(gl.BLEND);
    gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);

    // Normalize coordinates
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    points.forEach((p) => {
      const x = Number(p.umap_x) || 0;
      const y = Number(p.umap_y) || 0;
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    });

    const spanX = maxX > minX ? maxX - minX : 1.0;
    const spanY = maxY > minY ? maxY - minY : 1.0;

    const total = points.length;
    const posData = new Float32Array(total * 2);
    const colorData = new Float32Array(total * 4);
    const sizeData = new Float32Array(total);

    // Color palettes
    const palette = ["#0284c7", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1", "#14b8a6", "#e11d48", "#a855f7", "#38bdf8", "#22c55e"];
    
    // Size max
    let maxCit = 1;
    points.forEach(p => {
      const c = Number(p.cited_by_count) || 0;
      if (c > maxCit) maxCit = c;
    });

    const uniqueComms = Array.from(new Set(points.map(p => p.community_name || 'General')));

    for (let i = 0; i < total; i++) {
      const p = points[i];
      const ux = Number(p.umap_x) || 0;
      const uy = Number(p.umap_y) || 0;
      posData[i * 2] = ((ux - minX) / spanX) * 1.8 - 0.9;
      posData[i * 2 + 1] = ((uy - minY) / spanY) * 1.8 - 0.9;

      // Color
      if (colorMode === 'year' && p.publication_year) {
        const y = Number(p.publication_year) || 2000;
        const normY = Math.max(0, Math.min(1, (y - 1990) / 36.0));
        // Turbo color approximation
        const r = Math.min(1, Math.max(0, 0.1 + normY * 1.2));
        const g = Math.min(1, Math.max(0, Math.sin(normY * Math.PI) * 0.9 + 0.1));
        const b = Math.min(1, Math.max(0, (1 - normY) * 1.1));
        colorData[i * 4] = r;
        colorData[i * 4 + 1] = g;
        colorData[i * 4 + 2] = b;
        colorData[i * 4 + 3] = 0.88;
      } else if (colorMode === 'community' || p.community_name) {
        const commIdx = Math.max(0, uniqueComms.indexOf(p.community_name || 'General'));
        const hex = palette[commIdx % palette.length];
        colorData[i * 4] = parseInt(hex.slice(1, 3), 16) / 255;
        colorData[i * 4 + 1] = parseInt(hex.slice(3, 5), 16) / 255;
        colorData[i * 4 + 2] = parseInt(hex.slice(5, 7), 16) / 255;
        colorData[i * 4 + 3] = 0.88;
      } else {
        colorData[i * 4] = 0.02;
        colorData[i * 4 + 1] = 0.52;
        colorData[i * 4 + 2] = 0.78;
        colorData[i * 4 + 3] = 0.88;
      }

      // Size
      const cit = Number(p.cited_by_count) || 0;
      sizeData[i] = 4.0 + 12.0 * Math.sqrt(Math.min(1, cit / (maxCit || 1)));
    }

    const posBuffer = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, posBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, posData, gl.STATIC_DRAW);

    const colorBuffer = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, colorBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, colorData, gl.STATIC_DRAW);

    const sizeBuffer = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, sizeBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, sizeData, gl.STATIC_DRAW);

    stateRef.current.program = program;
    stateRef.current.posBuffer = posBuffer;
    stateRef.current.colorBuffer = colorBuffer;
    stateRef.current.sizeBuffer = sizeBuffer;
    stateRef.current.total = total;
    stateRef.current.payload = { posData, points };
    setRenderCount(total);

    render();
  }, [points, colorMode, sizeMode, theme]);

  const render = () => {
    const s = stateRef.current;
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!s.gl || !canvas || !container) return;

    const width = container.clientWidth;
    const height = container.clientHeight;
    if (canvas.width !== width || canvas.height !== height) {
      canvas.width = width;
      canvas.height = height;
      s.gl.viewport(0, 0, width, height);
    }

    const gl = s.gl;
    gl.clearColor(clearColor[0], clearColor[1], clearColor[2], 1.0);
    gl.clear(gl.COLOR_BUFFER_BIT);

    const uRes = gl.getUniformLocation(s.program, 'u_resolution');
    const uTrans = gl.getUniformLocation(s.program, 'u_translation');
    const uZoom = gl.getUniformLocation(s.program, 'u_zoom');

    gl.uniform2f(uRes, canvas.width, canvas.height);
    gl.uniform2f(uTrans, s.transX, s.transY);
    gl.uniform1f(uZoom, s.zoom);

    const aPos = gl.getAttribLocation(s.program, 'a_position');
    gl.bindBuffer(gl.ARRAY_BUFFER, s.posBuffer);
    gl.enableVertexAttribArray(aPos);
    gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0);

    const aColor = gl.getAttribLocation(s.program, 'a_color');
    gl.bindBuffer(gl.ARRAY_BUFFER, s.colorBuffer);
    gl.enableVertexAttribArray(aColor);
    gl.vertexAttribPointer(aColor, 4, gl.FLOAT, false, 0, 0);

    const aSize = gl.getAttribLocation(s.program, 'a_size');
    gl.bindBuffer(gl.ARRAY_BUFFER, s.sizeBuffer);
    gl.enableVertexAttribArray(aSize);
    gl.vertexAttribPointer(aSize, 1, gl.FLOAT, false, 0, 0);

    gl.drawArrays(gl.POINTS, 0, s.total);
  };

  const handleMouseDown = (e) => {
    if (e.button === 0) {
      stateRef.current.isDragging = true;
      stateRef.current.startX = e.clientX;
      stateRef.current.startY = e.clientY;
    }
  };

  const handleMouseUp = () => {
    stateRef.current.isDragging = false;
  };

  const handleMouseMove = (e) => {
    const s = stateRef.current;
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container) return;

    if (s.isDragging) {
      const dx = (e.clientX - s.startX) / (canvas.width / 2);
      const dy = -(e.clientY - s.startY) / (canvas.height / 2);
      s.transX += dx;
      s.transY += dy;
      s.startX = e.clientX;
      s.startY = e.clientY;
      render();
    } else {
      // Hit testing
      const rect = canvas.getBoundingClientRect();
      const mouseX = e.clientX - rect.left;
      const mouseY = e.clientY - rect.top;

      const aspect = canvas.width / canvas.height;
      const clipX = (mouseX / canvas.width) * 2.0 - 1.0;
      const clipY = -((mouseY / canvas.height) * 2.0 - 1.0);

      const dataX = (clipX * aspect - s.transX) / s.zoom;
      const dataY = (clipY - s.transY) / s.zoom;

      let closest = -1;
      let minDist = 0.045 / s.zoom;

      if (s.payload) {
        const { posData, points } = s.payload;
        for (let i = 0; i < s.total; i++) {
          const dx = posData[i * 2] - dataX;
          const dy = posData[i * 2 + 1] - dataY;
          const d = Math.sqrt(dx * dx + dy * dy);
          if (d < minDist) {
            minDist = d;
            closest = i;
          }
        }

        s.hoveredIdx = closest;
        if (closest >= 0) {
          setTooltipData(points[closest]);
          let tx = mouseX + 16;
          let ty = mouseY + 16;
          if (tx + 360 > container.clientWidth) tx = mouseX - 360;
          if (ty + 160 > container.clientHeight) ty = mouseY - 160;
          setTooltipPos({ x: tx, y: ty });
        } else {
          setTooltipData(null);
        }
      }
    }
  };

  const handleWheel = (e) => {
    e.preventDefault();
    const s = stateRef.current;
    const canvas = canvasRef.current;
    if (!canvas) return;

    const factor = e.deltaY < 0 ? 1.15 : 0.87;
    const newZoom = Math.max(0.5, Math.min(s.zoom * factor, 60.0));

    const rect = canvas.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const mouseY = e.clientY - rect.top;

    const aspect = canvas.width / canvas.height;
    const clipX = (mouseX / canvas.width) * 2.0 - 1.0;
    const clipY = -((mouseY / canvas.height) * 2.0 - 1.0);

    s.transX = clipX * aspect - (clipX * aspect - s.transX) * (newZoom / s.zoom);
    s.transY = clipY - (clipY - s.transY) * (newZoom / s.zoom);
    s.zoom = newZoom;

    render();
  };

  const handleContextMenu = (e) => {
    e.preventDefault();
    const s = stateRef.current;
    if (s.hoveredIdx >= 0 && s.payload) {
      const p = s.payload.points[s.hoveredIdx];
      const url = p.id || (p.doi ? `https://doi.org/${p.doi}` : null);
      if (url) {
        window.open(url.startsWith('http') ? url : `https://openalex.org/${url}`, '_blank');
      }
    }
  };

  const handleRecenter = () => {
    stateRef.current.zoom = 1.0;
    stateRef.current.transX = 0.0;
    stateRef.current.transY = 0.0;
    render();
  };

  return (
    <div
      ref={containerRef}
      style={{
        position: 'relative',
        width: '100%',
        height: `${height}px`,
        background: 'var(--webgl-bg)',
        borderRadius: '12px',
        border: '1px solid var(--border-color)',
        overflow: 'hidden',
        boxShadow: 'var(--card-shadow)'
      }}
      onMouseUp={handleMouseUp}
      onMouseLeave={handleMouseUp}
    >
      {/* HUD Bar */}
      <div style={{
        position: 'absolute',
        top: '14px',
        left: '16px',
        zIndex: 10,
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        background: isDark ? 'rgba(15, 23, 42, 0.85)' : 'rgba(255, 255, 255, 0.92)',
        backdropFilter: 'blur(10px)',
        padding: '6px 14px',
        borderRadius: '8px',
        border: '1px solid var(--border-color)',
        fontSize: '12px',
        fontWeight: '600',
        color: 'var(--text-main)',
        boxShadow: '0 4px 16px rgba(0,0,0,0.06)'
      }}>
        <span>⚡ WebGL GPU: <strong style={{ color: 'var(--accent-primary)' }}>{renderCount.toLocaleString()} puntos</strong></span>
        <button
          onClick={handleRecenter}
          style={{
            background: 'var(--bg-input)',
            border: '1px solid var(--border-color)',
            color: 'var(--text-main)',
            padding: '3px 8px',
            borderRadius: '5px',
            fontSize: '11px',
            fontWeight: '600',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '4px'
          }}
        >
          <RotateCcw size={11} /> Recentrar
        </button>
        <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
          Rueda: Zoom • Arrastrar: Pan • <strong style={{ color: '#f59e0b' }}>🖱️ Clic Derecho: Abrir en OpenAlex ↗</strong>
        </span>
      </div>

      <canvas
        ref={canvasRef}
        style={{ width: '100%', height: '100%', display: 'block', cursor: 'crosshair' }}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onWheel={handleWheel}
        onContextMenu={handleContextMenu}
      />

      {/* Tooltip Card */}
      {tooltipData && (
        <div style={{
          position: 'absolute',
          left: `${tooltipPos.x}px`,
          top: `${tooltipPos.y}px`,
          zIndex: 100,
          pointerEvents: 'none',
          background: isDark ? 'rgba(15, 23, 42, 0.96)' : 'rgba(255, 255, 255, 0.98)',
          backdropFilter: 'blur(12px)',
          border: '1px solid var(--border-color)',
          borderRadius: '10px',
          padding: '12px 16px',
          maxWidth: '380px',
          boxShadow: isDark ? '0 16px 36px rgba(0,0,0,0.7)' : '0 16px 36px rgba(0,0,0,0.12)',
          fontSize: '12px',
          color: 'var(--text-main)'
        }}>
          <div style={{ fontSize: '13px', fontWeight: '700', color: 'var(--accent-primary)', marginBottom: '5px', lineHeight: 1.35 }}>
            {tooltipData.display_name || tooltipData.title || 'Elemento'}
          </div>
          {tooltipData.publisher && (
            <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', marginBottom: '6px' }}>
              🏛️ {tooltipData.publisher} ({tooltipData.country_code})
            </div>
          )}
          {tooltipData.authors && (
            <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', marginBottom: '6px' }}>
              ✍️ {tooltipData.authors}
            </div>
          )}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '5px', marginTop: '6px' }}>
            {tooltipData.community_name && <span className="badge">🏷️ {tooltipData.community_name}</span>}
            {tooltipData.publication_year && <span className="badge">📅 {tooltipData.publication_year}</span>}
            {tooltipData.works_count !== undefined && <span className="badge">📄 {Number(tooltipData.works_count).toLocaleString()} arts</span>}
            {tooltipData.cited_by_count !== undefined && <span className="badge">✨ {Number(tooltipData.cited_by_count).toLocaleString()} citas</span>}
            {tooltipData.fwci_avg !== undefined && <span className="badge">⚡ FWCI: {Number(tooltipData.fwci_avg).toFixed(2)}</span>}
            {tooltipData.pct_oa_diamond !== undefined && <span className="badge">💎 {Number(tooltipData.pct_oa_diamond).toFixed(1)}% Diamante</span>}
            {tooltipData.is_in_doaj && <span className="badge success">✓ DOAJ</span>}
            {tooltipData.is_in_scielo && <span className="badge success">✓ SciELO</span>}
            {tooltipData.is_scopus && <span className="badge success">✓ Scopus</span>}
          </div>
          <div style={{ fontSize: '10.5px', color: '#f59e0b', marginTop: '8px', paddingTop: '6px', borderTop: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', gap: '4px' }}>
            <ExternalLink size={11} /> Clic derecho para abrir en OpenAlex ↗
          </div>
        </div>
      )}
    </div>
  );
}
