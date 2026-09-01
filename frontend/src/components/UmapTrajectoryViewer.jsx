import React, { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import { UmapHeatmap } from './UmapHeatmap';
import { useAppStore } from '../store';
import { 
  Activity, 
  Maximize2, 
  X, 
  Sliders,
} from 'lucide-react';

// -------------------------------------------------------------------
// Paleta fija de 30 colores distinguibles para clusters / entidades
// -------------------------------------------------------------------
const CLUSTER_PALETTE = [
  '#38bdf8', '#10b981', '#f59e0b', '#a78bfa', '#f43f5e',
  '#06b6d4', '#84cc16', '#fb923c', '#818cf8', '#ec4899',
  '#22d3ee', '#4ade80', '#fbbf24', '#c084fc', '#fb7185',
  '#67e8f9', '#86efac', '#fcd34d', '#d8b4fe', '#fda4af',
  '#0ea5e9', '#16a34a', '#d97706', '#7c3aed', '#be123c',
  '#0284c7', '#15803d', '#b45309', '#6d28d9', '#9f1239'
];

function clusterColor(key, palette) {
  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
  }
  return palette[hash % palette.length];
}

// -------------------------------------------------------------------
// Theme-aware backgrounds for canvas container and modal
// -------------------------------------------------------------------
const THEME_BG = {
  claro:  { container: '#f0f4f8', canvas: '#ffffff', modal: '#f8fafc', cardGrid: '#e8eef5' },
  oscuro: { container: '#090d16', canvas: '#030712', modal: '#090d16', cardGrid: '#0a0f1d' },
  navy:   { container: '#030a16', canvas: '#040d1f', modal: '#030a16', cardGrid: '#051020' },
};

// Label text color per theme
const THEME_LABEL = {
  claro:  'rgba(15, 23, 42, 0.85)',
  oscuro: 'rgba(248, 250, 252, 0.92)',
  navy:   'rgba(224, 242, 254, 0.92)',
};

const DEFAULT_VARIABLES = [
  { id: 'fwci_avg',      label: 'FWCI Promedio',        format: v => Number(v || 0).toFixed(2),            unit: '' },
  { id: 'pct_oa_diamond',label: '% OA Diamante',        format: v => `${Number(v || 0).toFixed(1)}%`,       unit: '%' },
  { id: 'pct_top_10',    label: '% Top 10%',            format: v => `${Number(v || 0).toFixed(2)}%`,       unit: '%' },
  { id: 'pct_top_1',     label: '% Top 1%',             format: v => `${Number(v || 0).toFixed(2)}%`,       unit: '%' },
  { id: 'pct_lang_en',   label: '% Idioma Inglés',      format: v => `${Number(v || 0).toFixed(1)}%`,       unit: '%' },
  { id: 'num_documents', label: 'Artículos Publicados', format: v => Number(v || 0).toLocaleString(),        unit: 'arts' }
];

export default function UmapTrajectoryViewer({
  title    = "Espacio UMAP y Trayectorias de Desempeño",
  subtitle = "Proyección topológica no lineal continua (UMAP 2D) con mapas de calor y trayectorias evolutivas.",
  points   = [],
  trajectories = {},
  variables     = DEFAULT_VARIABLES,
  defaultVariable = 'fwci_avg',
  allowTrajectoryFilter = true,
  showGridSection = true,
  initialActiveEntities = null,
  defaultShowLabels = true,
  height = 480
}) {
  const { theme } = useAppStore();
  const bg = THEME_BG[theme] || THEME_BG.oscuro;
  const labelColor = THEME_LABEL[theme] || THEME_LABEL.oscuro;
  const isDark = theme !== 'claro';

  const [selectedVar, setSelectedVar]   = useState(defaultVariable);
  const [colorScale, setColorScale]     = useState('standard');
  const [colorByCluster, setColorByCluster] = useState(true);
  const [showLabels, setShowLabels]     = useState(defaultShowLabels);
  const [hoveredInfo, setHoveredInfo]   = useState(null);
  const [modalItem, setModalItem]       = useState(null);

  // Active trajectories filter
  const entityKeys = useMemo(() => Object.keys(trajectories || {}), [trajectories]);

  // Initialize to empty set; useEffect below will populate it once trajectories arrive
  const [activeEntities, setActiveEntities] = useState(() => new Set());
  const initializedRef = useRef(false);

  useEffect(() => {
    if (entityKeys.length === 0) return;          // trajectories not loaded yet
    if (initializedRef.current) return;           // already initialized, don't override user choices
    initializedRef.current = true;

    if (initialActiveEntities && initialActiveEntities.length > 0) {
      // Only keep the requested entities that actually exist in the data
      const valid = initialActiveEntities.filter(k => entityKeys.includes(k));
      setActiveEntities(new Set(valid.length > 0 ? valid : entityKeys));
    } else {
      setActiveEntities(new Set(entityKeys));
    }
  }, [entityKeys, initialActiveEntities]);

  const toggleEntity = (k) => {
    setActiveEntities(prev => {
      const next = new Set(prev);
      if (next.has(k)) { if (next.size > 1) next.delete(k); }
      else next.add(k);
      return next;
    });
  };
  const selectOnlyEntity = (k) => setActiveEntities(new Set([k, 'LATAM']));
  const selectAllEntities = () => setActiveEntities(new Set(entityKeys));

  // Determine cluster key per point (cluster field > entityId > country_code)
  const clusterKey = useCallback((p) =>
    p.cluster != null ? String(p.cluster)
    : p.country_code  || p.entityId || p.id || '__default__'
  , []);

  // Build points for main canvas (either cluster-colored or variable heatmap)
  const mappedPoints = useMemo(() => {
    if (points && points.length > 0) {
      return points.map(p => {
        const ck = clusterKey(p);
        const entityId = p.country_code || p.journal_id || p.id || '';
        // If entity filter is active, only show label for active entities
        const isEntityActive = activeEntities.size === 0 || !allowTrajectoryFilter || activeEntities.has(entityId) || activeEntities.has(ck);
        return {
          x:            p.umap_x ?? p.x,
          y:            p.umap_y ?? p.y,
          value:        colorByCluster ? null : (p[selectedVar] ?? p.value ?? 0),
          clusterColor: colorByCluster ? clusterColor(ck, CLUSTER_PALETTE) : null,
          label:        isEntityActive ? (p.display_name || p.country_name || p.name || p.label || '') : '',
          entityId:     entityId,
          cluster:      ck,
          raw: p
        };
      });
    }

    // Trajectory-derived points: ONLY generate points for activeEntities
    const pts = [];
    Object.keys(trajectories || {}).forEach(k => {
      // Filter by active entities so inactive countries don't clutter the map
      if (!activeEntities.has(k)) return;

      const ent = trajectories[k];
      if (ent && Array.isArray(ent.points)) {
        const isRef = ent.is_ref || k === 'LATAM';
        const color = isRef ? '#10b981' : clusterColor(k, CLUSTER_PALETTE);

        ent.points.forEach((pt, ptIdx) => {
          const isLatest = ptIdx === ent.points.length - 1;
          const isStart  = ptIdx === 0;
          
          // Label only start and end points for active entities
          let ptLabel = '';
          if (isLatest) {
            ptLabel = `${ent.name} (${pt.year})`;
          } else if (isStart) {
            ptLabel = `'${String(pt.year).slice(-2)}`;
          }

          pts.push({
            x: pt.x,
            y: pt.y,
            value: colorByCluster ? null : (pt[selectedVar] ?? 0),
            clusterColor: colorByCluster ? color : null,
            label: ptLabel,
            entityId: k,
            cluster: k,
            year: pt.year,
            raw: pt
          });
        });
      }
    });
    return pts;
  }, [points, trajectories, activeEntities, allowTrajectoryFilter, selectedVar, colorByCluster, clusterKey]);

  // Points for grid heatmaps (always variable-colored, no cluster)
  const gridPoints = useMemo(() => {
    if (points && points.length > 0) {
      return points.map(p => ({
        x: p.umap_x ?? p.x,
        y: p.umap_y ?? p.y,
        label: p.display_name || p.country_name || p.name || '',
        entityId: p.country_code || p.id || '',
        raw: p
      }));
    }
    return Object.keys(trajectories || {}).flatMap(k => {
      const ent = trajectories[k];
      return (ent?.points || []).map(pt => ({
        x: pt.x, y: pt.y, label: `${ent.name} (${pt.year})`, entityId: k, raw: pt
      }));
    });
  }, [points, trajectories]);

  // Active trajectories list
  const activeTrajList = useMemo(() => {
    return Object.keys(trajectories || {})
      .filter(k => activeEntities.has(k))
      .map(k => {
        const t = trajectories[k];
        return {
          name: t.name || k,
          is_ref: t.is_ref || k === 'LATAM',
          color: t.is_ref || k === 'LATAM' ? '#10b981' : clusterColor(k, CLUSTER_PALETTE),
          width: t.is_ref || k === 'LATAM' ? 4 : 2,
          points: t.points
        };
      });
  }, [trajectories, activeEntities]);

  // Hover handler
  const handlePointHover = useCallback((point, clientX, clientY) => {
    setHoveredInfo(point ? { point, x: clientX, y: clientY } : null);
  }, []);

  const currentVarObj = variables.find(v => v.id === selectedVar) || variables[0];

  // Unique clusters for legend
  const clusterKeys = useMemo(() => {
    if (!colorByCluster) return [];
    const seen = new Set();
    mappedPoints.forEach(p => seen.add(p.cluster));
    return [...seen].filter(Boolean).sort();
  }, [mappedPoints, colorByCluster]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* ── Header Card ── */}
      <div className="card" style={{ padding: '18px 22px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '14px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Activity size={20} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '17px', fontWeight: '800', margin: 0 }}>{title}</h3>
            </div>
            <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginTop: '4px', margin: 0 }}>{subtitle}</p>
          </div>

          {/* Controls */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            {/* Cluster / Variable toggle */}
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${colorByCluster ? 'active' : ''}`}
                onClick={() => setColorByCluster(true)}
                title="Colorear por grupos/clusters"
              >
                🎨 Clusters
              </button>
              <button
                className={`segmented-pill-btn ${!colorByCluster ? 'active' : ''}`}
                onClick={() => setColorByCluster(false)}
                title="Colorear por indicador numérico"
              >
                🌡 Indicador
              </button>
            </div>

            {/* Variable selector — only when indicator mode */}
            {!colorByCluster && (
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span style={{ fontSize: '11.5px', fontWeight: '700', color: 'var(--text-muted)' }}>Capa:</span>
                <select
                  value={selectedVar}
                  onChange={e => setSelectedVar(e.target.value)}
                  style={{ fontSize: '12px', padding: '6px 10px', borderRadius: '8px', fontWeight: '600' }}
                >
                  {variables.map(v => (
                    <option key={v.id} value={v.id}>{v.label}</option>
                  ))}
                </select>
              </div>
            )}

            {/* Color scale — only when indicator mode */}
            {!colorByCluster && (
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span style={{ fontSize: '11.5px', fontWeight: '700', color: 'var(--text-muted)' }}>Paleta:</span>
                <select
                  value={colorScale}
                  onChange={e => setColorScale(e.target.value)}
                  style={{ fontSize: '12px', padding: '6px 8px', borderRadius: '8px' }}
                >
                  <option value="standard">Standard (Verde-Rojo)</option>
                  <option value="viridis">Viridis (Accesible)</option>
                  <option value="cividis">Cividis (Alto Contraste)</option>
                  <option value="spectral">Spectral</option>
                </select>
              </div>
            )}

            {/* Labels checkbox */}
            <label style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', fontWeight: '600', cursor: 'pointer', userSelect: 'none' }}>
              <input
                type="checkbox"
                checked={showLabels}
                onChange={e => setShowLabels(e.target.checked)}
                style={{ accentColor: 'var(--accent-primary)', cursor: 'pointer' }}
              />
              <span>Etiquetas</span>
            </label>

            {/* Maximize */}
            <button
              className="btn-secondary"
              onClick={() => setModalItem({ name: colorByCluster ? 'Clusters' : currentVarObj.label, varId: selectedVar })}
              style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '11.5px', padding: '6px 10px' }}
              title="Maximizar en alta resolución"
            >
              <Maximize2 size={13} />
            </button>
          </div>
        </div>

        {/* Entity pills (trajectory filter) */}
        {allowTrajectoryFilter && entityKeys.length > 1 && (
          <div style={{ marginTop: '16px', paddingTop: '12px', borderTop: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            <span style={{ fontSize: '11px', fontWeight: '800', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', marginRight: '4px' }}>
              Trayectorias Activas ({activeEntities.size}/{entityKeys.length}):
            </span>
            <button
              className="segmented-pill-btn"
              onClick={selectAllEntities}
              style={{ fontSize: '11px', padding: '3px 8px', height: 'auto' }}
            >
              Mostrar Todas
            </button>
            {entityKeys.map(k => {
              const ent = trajectories[k];
              const isAct = activeEntities.has(k);
              const isRef = ent.is_ref || k === 'LATAM';
              const dotCol = isRef ? '#10b981' : clusterColor(k, CLUSTER_PALETTE);
              return (
                <button
                  key={k}
                  onClick={() => toggleEntity(k)}
                  onDoubleClick={() => selectOnlyEntity(k)}
                  style={{
                    display: 'inline-flex', alignItems: 'center', gap: '4px',
                    fontSize: '11.5px', fontWeight: isAct ? '700' : '500',
                    padding: '3px 9px', borderRadius: '16px',
                    border: isAct ? `1.5px solid ${dotCol}` : '1px solid var(--border-color)',
                    background: isAct ? `${dotCol}20` : 'transparent',
                    color: isAct ? dotCol : 'var(--text-muted)',
                    cursor: 'pointer', transition: 'all 0.15s ease'
                  }}
                  title={`Click alternar · Doble click aislar ${ent.name}`}
                >
                  <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: isAct ? dotCol : '#64748b' }} />
                  {ent.name || k}
                </button>
              );
            })}
          </div>
        )}

        {/* ── Main Canvas ── */}
        <div style={{
          position: 'relative',
          marginTop: '16px',
          background: bg.container,
          borderRadius: '14px',
          border: '1px solid var(--border-color)',
          overflow: 'hidden',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          minHeight: `${height}px`
        }}>
          <UmapHeatmap
            points={mappedPoints}
            width={840}
            height={height}
            colorScale={colorByCluster ? 'cluster' : colorScale}
            sigma={colorByCluster ? 0 : 0.08}
            resolution={140}
            showPoints={true}
            showLabels={showLabels}
            trajectories={activeTrajList}
            onPointHover={handlePointHover}
            pointRadius={points.length > 100 ? 2 : 3.5}
            isDark={isDark}
            labelColor={labelColor}
            bgColor={bg.canvas}
          />

          {/* Cluster legend overlay (bottom-left) */}
          {colorByCluster && clusterKeys.length > 0 && clusterKeys.length <= 25 && (
            <div style={{
              position: 'absolute', bottom: '12px', left: '12px',
              background: isDark ? 'rgba(15,23,42,0.85)' : 'rgba(255,255,255,0.88)',
              backdropFilter: 'blur(8px)',
              border: '1px solid var(--border-color)',
              borderRadius: '10px', padding: '8px 10px',
              display: 'flex', flexWrap: 'wrap', gap: '5px 10px',
              maxWidth: '340px', pointerEvents: 'none', zIndex: 20
            }}>
              {clusterKeys.map(ck => (
                <span key={ck} style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '10px', fontWeight: '700', color: 'var(--text-muted)' }}>
                  <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: clusterColor(ck, CLUSTER_PALETTE), flexShrink: 0 }} />
                  {ck}
                </span>
              ))}
            </div>
          )}

          {/* Hover card */}
          {hoveredInfo?.point && (
            <div style={{
              position: 'absolute', bottom: '16px', right: '16px',
              background: isDark ? 'rgba(15,23,42,0.92)' : 'rgba(255,255,255,0.95)',
              backdropFilter: 'blur(10px)',
              border: '1px solid var(--border-color)',
              borderRadius: '10px', padding: '10px 14px',
              boxShadow: '0 10px 25px rgba(0,0,0,0.35)',
              pointerEvents: 'none', zIndex: 30, maxWidth: '300px'
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '4px' }}>
                <span style={{ fontSize: '13px', fontWeight: '800', color: 'var(--text-main)' }}>
                  {hoveredInfo.point.label || hoveredInfo.point.entityId}
                </span>
                {hoveredInfo.point.year && (
                  <span style={{ fontSize: '10px', fontWeight: '800', background: 'var(--accent-primary)', color: '#fff', padding: '1px 6px', borderRadius: '10px' }}>
                    {hoveredInfo.point.year}
                  </span>
                )}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '5px 12px', fontSize: '11px', marginTop: '6px' }}>
                {variables.map(v => {
                  const val = hoveredInfo.point.raw?.[v.id] ?? (v.id === selectedVar ? hoveredInfo.point.value : null);
                  if (val == null) return null;
                  return (
                    <div key={v.id} style={{ display: 'flex', justifyContent: 'space-between', gap: '8px' }}>
                      <span style={{ color: 'var(--text-muted)' }}>{v.label}:</span>
                      <strong style={{ color: 'var(--accent-primary)' }}>{v.format(val)}</strong>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Legend badge top-right */}
          {!colorByCluster && (
            <div style={{
              position: 'absolute', top: '12px', right: '12px',
              background: isDark ? 'rgba(15,23,42,0.8)' : 'rgba(255,255,255,0.8)',
              border: '1px solid var(--border-color)',
              borderRadius: '8px', padding: '6px 10px',
              fontSize: '11px', color: 'var(--text-muted)',
              display: 'flex', alignItems: 'center', gap: '8px'
            }}>
              <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#10b981' }} />
                Región LATAM (Ref.)
              </span>
              <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#38bdf8' }} />
                Países / Revistas
              </span>
            </div>
          )}
        </div>
      </div>

      {/* ── Grid of Variable Heatmaps ── */}
      {showGridSection && (
        <div className="card" style={{ padding: '20px 22px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '10px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Sliders size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '15px', fontWeight: '800', margin: 0 }}>
                Cuadrícula de Mapas de Calor por Indicador (Espacio UMAP)
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Concentración topológica de cada dimensión con trayectorias superpuestas.
            </span>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '16px' }}>
            {variables.map(v => {
              const ptsForVar = (gridPoints.length > 0 ? gridPoints : mappedPoints).map(p => ({
                x:       p.x ?? p.umap_x,
                y:       p.y ?? p.umap_y,
                value:   p.raw?.[v.id] ?? (v.id === selectedVar ? p.value : 0) ?? 0,
                label:   p.label,
                entityId: p.entityId
              }));

              const values = ptsForVar.map(p => p.value).filter(Number.isFinite);
              const minV = values.length ? Math.min(...values) : 0;
              const maxV = values.length ? Math.max(...values) : 1;
              const avgV = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;

              return (
                <div
                  key={v.id}
                  style={{
                    background: bg.cardGrid,
                    border: selectedVar === v.id && !colorByCluster
                      ? '1.5px solid var(--accent-primary)'
                      : '1px solid var(--border-color)',
                    borderRadius: '12px', padding: '12px',
                    display: 'flex', flexDirection: 'row',
                    alignItems: 'center', gap: '10px',
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                    transition: 'border-color 0.2s ease', position: 'relative'
                  }}
                >
                  <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column' }}>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
                      <span style={{
                        fontSize: '11px', fontWeight: '800', textTransform: 'uppercase',
                        letterSpacing: '0.04em', color: 'var(--text-muted)',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'
                      }}>
                        {v.label}
                      </span>
                      <button
                        onClick={() => { setColorByCluster(false); setSelectedVar(v.id); setModalItem({ name: v.label, varId: v.id }); }}
                        style={{ background: 'transparent', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', padding: '2px' }}
                        title="Maximizar"
                      >
                        <Maximize2 size={12} />
                      </button>
                    </div>

                    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', background: bg.canvas, borderRadius: '8px', overflow: 'hidden' }}>
                      <UmapHeatmap
                        points={ptsForVar}
                        width={230}
                        height={180}
                        colorScale={colorScale}
                        sigma={0.08}
                        resolution={100}
                        showPoints={true}
                        showLabels={false}
                        trajectories={activeTrajList}
                        pointRadius={1.5}
                        isDark={isDark}
                        labelColor={labelColor}
                        bgColor={bg.canvas}
                      />
                    </div>
                  </div>

                  {/* Colorbar */}
                  <div style={{
                    display: 'flex', flexDirection: 'column', alignItems: 'center',
                    justifyContent: 'space-between', width: '46px', height: '180px',
                    paddingTop: '20px', flexShrink: 0
                  }}>
                    <span style={{ fontSize: '9px', fontWeight: '700', color: 'var(--text-muted)' }}>{v.format(maxV)}</span>
                    <div style={{
                      position: 'relative', width: '8px', height: '110px', borderRadius: '4px',
                      background: colorScale === 'standard'
                        ? 'linear-gradient(to bottom, #e53e3e, #ecc94b, #38a169)'
                        : colorScale === 'viridis'
                          ? 'linear-gradient(to bottom, #fde725, #5ec962, #21918c, #3b528b, #440154)'
                          : 'linear-gradient(to bottom, #ffea46, #b9ad71, #7c7b78, #414d6b, #00204d)'
                    }}>
                      <div style={{
                        position: 'absolute', left: '-3px', width: '14px', height: '2px',
                        background: isDark ? '#ffffff' : '#0f172a', borderRadius: '2px',
                        top: `${Math.max(0, Math.min(100, 100 - ((avgV - minV) / (maxV - minV || 1)) * 100))}%`
                      }} />
                    </div>
                    <span style={{ fontSize: '9px', fontWeight: '700', color: 'var(--text-muted)' }}>{v.format(minV)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Maximize Modal ── */}
      {modalItem && (
        <div style={{
          position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(0, 0, 0, 0.85)', backdropFilter: 'blur(8px)',
          display: 'flex', justifyContent: 'center', alignItems: 'center',
          zIndex: 9999, padding: '24px'
        }}>
          <div style={{
            background: bg.modal,
            border: '1px solid var(--border-color)',
            borderRadius: '16px', padding: '24px',
            maxWidth: '1000px', width: '100%', maxHeight: '92vh',
            display: 'flex', flexDirection: 'column', gap: '16px',
            boxShadow: '0 25px 50px -12px rgba(0,0,0,0.7)'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <h3 style={{ fontSize: '18px', fontWeight: '800', margin: 0, color: 'var(--text-main)' }}>
                  {modalItem.name} — Espacio UMAP de Alta Resolución
                </h3>
                <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Visualización maximizada continua</span>
              </div>
              <button className="btn-secondary" onClick={() => setModalItem(null)} style={{ padding: '6px', borderRadius: '8px' }}>
                <X size={18} />
              </button>
            </div>
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', background: bg.canvas, borderRadius: '12px', padding: '16px' }}>
              <UmapHeatmap
                points={mappedPoints}
                width={900}
                height={560}
                colorScale={colorByCluster ? 'cluster' : colorScale}
                sigma={colorByCluster ? 0 : 0.08}
                resolution={180}
                showPoints={true}
                showLabels={true}
                trajectories={activeTrajList}
                pointRadius={3}
                isDark={isDark}
                labelColor={labelColor}
                bgColor={bg.canvas}
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
