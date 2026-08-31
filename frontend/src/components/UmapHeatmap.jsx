import React, { useEffect, useRef, useMemo, Component, useCallback } from 'react';
import chroma from 'chroma-js';
import { line, curveCatmullRom } from 'd3-shape';

/**
 * Error boundary to ensure heatmap render errors do not crash the entire application
 */
class HeatmapErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, message: '' };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, message: error?.message || 'Error desconocido' };
  }

  componentDidCatch(error, info) {
    console.error('[UmapHeatmap] Error de renderizado:', error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ color: '#ef4444', fontSize: '11px', padding: '8px', border: '1px solid #ef4444', borderRadius: '8px', background: 'rgba(239,68,68,0.1)' }}>
          ⚠️ Error en mapa de calor: {this.state.message}
        </div>
      );
    }
    return this.props.children;
  }
}

/**
 * Inner Canvas Renderer for UMAP Heatmap + Points + Spline Trajectories
 *
 * Props:
 *   colorScale  – 'cluster' | 'standard' | 'viridis' | 'cividis' | 'spectral'
 *   bgColor     – CSS color string for canvas background fill (theme-aware)
 *   isDark      – boolean: dark theme (affects label/waypoint colors)
 *   labelColor  – CSS color string for text labels
 */
const UmapHeatmapInner = ({
  points        = [],
  width         = 400,
  height        = 400,
  colorScale    = 'standard',
  sigma         = 0.08,
  resolution    = 120,
  showPoints    = true,
  showLabels    = false,
  trajectories  = [],
  onPointHover  = null,
  highlightEntity = null,
  pointRadius   = 2.5,
  bgColor       = 'transparent',
  isDark        = true,
  labelColor    = 'rgba(248, 250, 252, 0.9)',
}) => {
  const canvasRef      = useRef(null);
  const boundsRef      = useRef({ minX: 0, maxX: 1, minY: 0, maxY: 1, safeRangeX: 1, safeRangeY: 1 });
  const validPointsRef = useRef([]);

  const scales = useMemo(() => ({
    standard: ['#38a169', '#ecc94b', '#e53e3e'],
    viridis:  ['#440154', '#3b528b', '#21918c', '#5ec962', '#fde725'],
    cividis:  ['#00204d', '#414d6b', '#7c7b78', '#b9ad71', '#ffea46'],
    spectral: ['#2b83ba', '#abdda4', '#ffffbf', '#fdae61', '#d7191c']
  }), []);

  // ─── decide mode ───────────────────────────────────────────────────────────
  const isClusterMode = colorScale === 'cluster';

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    // Fill background
    ctx.clearRect(0, 0, width, height);
    if (bgColor && bgColor !== 'transparent') {
      ctx.fillStyle = bgColor;
      ctx.fillRect(0, 0, width, height);
    }

    try {
      const validPoints = points.filter(
        p => p && Number.isFinite(p.x) && Number.isFinite(p.y)
      );
      validPointsRef.current = validPoints;
      if (validPoints.length === 0) return;

      // 1. Coordinate boundaries
      let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
      let minV = Infinity, maxV = -Infinity;

      for (const p of validPoints) {
        if (p.x < minX) minX = p.x;
        if (p.x > maxX) maxX = p.x;
        if (p.y < minY) minY = p.y;
        if (p.y > maxY) maxY = p.y;
        if (!isClusterMode) {
          const v = p.value ?? 0;
          if (Number.isFinite(v)) {
            if (v < minV) minV = v;
            if (v > maxV) maxV = v;
          }
        }
      }

      const rangeX = maxX - minX || 1;
      const rangeY = maxY - minY || 1;
      const padX = rangeX * 0.08;
      const padY = rangeY * 0.08;
      minX -= padX; maxX += padX;
      minY -= padY; maxY += padY;
      const safeRangeX = maxX - minX;
      const safeRangeY = maxY - minY;
      boundsRef.current = { minX, maxX, minY, maxY, safeRangeX, safeRangeY };

      // ── 2a. CLUSTER MODE: skip heatmap, just draw colored dots ──────────────
      if (isClusterMode) {
        if (showPoints) {
          for (let i = 0; i < validPoints.length; i++) {
            const p  = validPoints[i];
            const cx = ((p.x - minX) / safeRangeX) * width;
            const cy = ((p.y - minY) / safeRangeY) * height;
            if (!Number.isFinite(cx) || !Number.isFinite(cy)) continue;

            const dotColor = p.clusterColor || p.color || '#38bdf8';
            const isHi    = highlightEntity && (p.entityId === highlightEntity || p.label === highlightEntity);
            const r       = isHi ? pointRadius * 1.8 : pointRadius;

            ctx.beginPath();
            ctx.arc(cx, cy, r, 0, Math.PI * 2);
            ctx.fillStyle = dotColor;
            ctx.fill();
            ctx.lineWidth   = isHi ? 2 : 0.8;
            ctx.strokeStyle = isDark ? 'rgba(0,0,0,0.65)' : 'rgba(255,255,255,0.7)';
            ctx.stroke();

            if (showLabels && p.label) {
              const fontSize = Math.max(9, Math.min(13, Math.round(9 * (width / 320))));
              ctx.font       = `bold ${fontSize}px system-ui, sans-serif`;
              ctx.textAlign  = 'center';
              // Shadow for readability on any background
              ctx.fillStyle    = isDark ? 'rgba(0,0,0,0.7)' : 'rgba(255,255,255,0.7)';
              ctx.fillText(p.label, cx + 1, cy - fontSize * 0.6 + 1);
              ctx.fillStyle = labelColor;
              ctx.fillText(p.label, cx, cy - fontSize * 0.6);
            }
          }
        }

      // ── 2b. INDICATOR / HEATMAP MODE ────────────────────────────────────────
      } else {
        if (minV >= maxV) maxV = minV + 1;

        // Percentile clipping 2–98%
        const values = validPoints.map(p => p.value ?? 0).filter(Number.isFinite);
        let colorDomainMin = minV, colorDomainMax = maxV;
        if (values.length > 4) {
          const sorted = [...values].sort((a, b) => a - b);
          const p2  = sorted[Math.floor(sorted.length * 0.02)] ?? minV;
          const p98 = sorted[Math.floor(sorted.length * 0.98)] ?? maxV;
          if (p2 < p98) { colorDomainMin = p2; colorDomainMax = p98; }
        }

        const palette = scales[colorScale] || scales.standard;
        const scaleFn = chroma.scale(palette).domain([colorDomainMin, colorDomainMax]);

        // Offscreen Gaussian density heatmap
        const off = document.createElement('canvas');
        off.width  = resolution;
        off.height = resolution;
        const offCtx = off.getContext('2d');

        if (offCtx) {
          const imgData     = offCtx.createImageData(resolution, resolution);
          const data        = imgData.data;
          const gridPts     = validPoints.map(p => ({
            gx: ((p.x - minX) / safeRangeX) * resolution,
            gy: ((p.y - minY) / safeRangeY) * resolution,
            v: p.value ?? 0
          }));

          const s           = Math.max(sigma, 0.01) * resolution;
          const s2          = s * s;
          const radius      = Math.ceil(3 * s);
          const densityMap  = new Float32Array(resolution * resolution);
          const valueMap    = new Float32Array(resolution * resolution);
          const weightSumMap = new Float32Array(resolution * resolution);

          for (const p of gridPts) {
            const cx = Math.round(p.gx);
            const cy = Math.round(p.gy);
            const x0 = Math.max(0, cx - radius);
            const x1 = Math.min(resolution - 1, cx + radius);
            const y0 = Math.max(0, cy - radius);
            const y1 = Math.min(resolution - 1, cy + radius);
            for (let py = y0; py <= y1; py++) {
              const ddy = p.gy - py;
              for (let px = x0; px <= x1; px++) {
                const ddx = p.gx - px;
                const d2  = ddx * ddx + ddy * ddy;
                if (d2 > 9 * s2) continue;
                const w   = Math.exp(-d2 / (2 * s2));
                const idx = py * resolution + px;
                densityMap[idx]   += w;
                weightSumMap[idx] += w;
                valueMap[idx]     += w * p.v;
              }
            }
          }

          let maxDensity = 0;
          for (let i = 0; i < resolution * resolution; i++) {
            const ws = weightSumMap[i];
            if (ws > 0) valueMap[i] /= ws;
            if (densityMap[i] > maxDensity) maxDensity = densityMap[i];
          }

          const alphaNorm = maxDensity > 0 ? maxDensity * 0.10 : 1;

          for (let i = 0; i < resolution * resolution; i++) {
            const sumW = densityMap[i];
            const pIdx = i * 4;
            if (sumW > 0.001) {
              const color = scaleFn(valueMap[i]).rgba();
              const alpha = Math.min(0.92, sumW / alphaNorm);
              data[pIdx]     = Math.round(color[0]);
              data[pIdx + 1] = Math.round(color[1]);
              data[pIdx + 2] = Math.round(color[2]);
              data[pIdx + 3] = Math.round(255 * alpha);
            } else {
              data[pIdx + 3] = 0;
            }
          }

          offCtx.putImageData(imgData, 0, 0);
          ctx.imageSmoothingEnabled = true;
          ctx.drawImage(off, 0, 0, width, height);
        }

        // Scatter dots (indicator-colored)
        if (showPoints) {
          for (let i = 0; i < validPoints.length; i++) {
            const p  = validPoints[i];
            const cx = ((p.x - minX) / safeRangeX) * width;
            const cy = ((p.y - minY) / safeRangeY) * height;
            if (!Number.isFinite(cx) || !Number.isFinite(cy)) continue;

            const dotColor = p.value !== undefined ? scaleFn(p.value).hex() : '#38bdf8';
            const isHi    = highlightEntity && (p.entityId === highlightEntity || p.label === highlightEntity);
            const r       = isHi ? pointRadius * 1.8 : pointRadius;

            ctx.beginPath();
            ctx.arc(cx, cy, r, 0, Math.PI * 2);
            ctx.fillStyle = dotColor;
            ctx.fill();
            ctx.lineWidth   = isHi ? 1.5 : 0.6;
            ctx.strokeStyle = isDark ? 'rgba(0,0,0,0.7)' : 'rgba(255,255,255,0.7)';
            ctx.stroke();

            if (showLabels && p.label) {
              const fontSize = Math.max(9, Math.min(13, Math.round(9 * (width / 320))));
              ctx.font       = `bold ${fontSize}px system-ui, sans-serif`;
              ctx.textAlign  = 'center';
              ctx.fillStyle  = isDark ? 'rgba(0,0,0,0.85)' : 'rgba(255,255,255,0.85)';
              ctx.fillText(p.label, cx + 1, cy - fontSize * 0.6 + 1);
              ctx.fillStyle = labelColor;
              ctx.fillText(p.label, cx, cy - fontSize * 0.6);
            }
          }
        }
      }

      // ── 3. Spline Trajectories (Catmull-Rom) ────────────────────────────────
      if (trajectories && trajectories.length > 0) {
        const curveGen = line()
          .x(d => d.cx)
          .y(d => d.cy)
          .curve(curveCatmullRom.alpha(0.5))
          .context(ctx);

        for (const traj of trajectories) {
          if (!traj || !Array.isArray(traj.points) || traj.points.length < 2) continue;

          const tPoints = traj.points.map(pt => {
            const cx = ((pt.x - minX) / safeRangeX) * width;
            const cy = ((pt.y - minY) / safeRangeY) * height;
            return { cx, cy, year: pt.year };
          }).filter(p => Number.isFinite(p.cx) && Number.isFinite(p.cy));

          if (tPoints.length < 2) continue;

          const trajColor = traj.color || (traj.is_ref ? '#10b981' : '#38bdf8');
          const trajWidth = traj.width || (traj.is_ref ? 4 : 2);

          // Shadow
          ctx.beginPath();
          curveGen(tPoints);
          ctx.strokeStyle = 'rgba(0, 0, 0, 0.55)';
          ctx.lineWidth   = trajWidth + 2.5;
          ctx.stroke();

          // Spline
          ctx.beginPath();
          curveGen(tPoints);
          ctx.strokeStyle = trajColor;
          ctx.lineWidth   = trajWidth;
          ctx.stroke();

          // Waypoint nodes
          const nodeStroke = isDark ? '#0f172a' : '#ffffff';
          for (let j = 0; j < tPoints.length; j++) {
            const node = tPoints[j];
            const isEdge = j === 0 || j === tPoints.length - 1;
            const nr     = isEdge ? trajWidth + 2.5 : trajWidth + 1;

            ctx.beginPath();
            ctx.arc(node.cx, node.cy, nr, 0, Math.PI * 2);
            ctx.fillStyle   = trajColor;
            ctx.fill();
            ctx.strokeStyle = nodeStroke;
            ctx.lineWidth   = 1;
            ctx.stroke();

            ctx.beginPath();
            ctx.arc(node.cx, node.cy, 1.5, 0, Math.PI * 2);
            ctx.fillStyle = '#ffffff';
            ctx.fill();

            if (isEdge && showLabels && node.year) {
              const yrStr = String(node.year).slice(-2);
              ctx.font       = 'bold 9px system-ui, sans-serif';
              ctx.textAlign  = 'center';
              ctx.fillStyle  = isDark ? '#f8fafc' : '#0f172a';
              ctx.fillText(`'${yrStr}`, node.cx, node.cy - nr - 3);
            }
          }
        }
      }

    } catch (err) {
      console.error('[UmapHeatmap] Canvas render error:', err);
      ctx.fillStyle = 'rgba(239, 68, 68, 0.2)';
      ctx.fillRect(0, 0, width, height);
      ctx.fillStyle = '#ef4444';
      ctx.font      = '11px sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('Error al renderizar mapa', width / 2, height / 2);
    }
  }, [points, width, height, colorScale, sigma, resolution, showPoints, showLabels,
      trajectories, highlightEntity, pointRadius, scales, bgColor, isDark, labelColor, isClusterMode]);

  // Hover detection
  const handleMouseMove = useCallback((e) => {
    if (!onPointHover) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect   = canvas.getBoundingClientRect();
    const mouseX = (e.clientX - rect.left) * (width / rect.width);
    const mouseY = (e.clientY - rect.top)  * (height / rect.height);

    const { minX, safeRangeX, minY, safeRangeY } = boundsRef.current;
    const pts = validPointsRef.current;

    let bestPoint = null;
    let bestDist  = 20 * 20;

    for (let i = 0; i < pts.length; i++) {
      const p  = pts[i];
      const cx = ((p.x - minX) / safeRangeX) * width;
      const cy = ((p.y - minY) / safeRangeY) * height;
      const d2 = (cx - mouseX) ** 2 + (cy - mouseY) ** 2;
      if (d2 < bestDist) { bestDist = d2; bestPoint = p; }
    }

    onPointHover(bestPoint || null, bestPoint ? e.clientX : 0, bestPoint ? e.clientY : 0);
  }, [onPointHover, width, height]);

  const handleMouseLeave = useCallback(() => {
    onPointHover?.(null, 0, 0);
  }, [onPointHover]);

  return (
    <canvas
      ref={canvasRef}
      width={width}
      height={height}
      style={{
        display: 'block',
        width,
        height,
        background: 'transparent',
        cursor: onPointHover ? 'crosshair' : 'default',
        borderRadius: '8px'
      }}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
    />
  );
};

export const UmapHeatmap = (props) => (
  <HeatmapErrorBoundary>
    <UmapHeatmapInner {...props} />
  </HeatmapErrorBoundary>
);

export default UmapHeatmap;
