import React, { useState, useMemo } from 'react';
import { useTranslation } from '../i18n';
import PlotlyChart from './PlotlyChart';
import { 
  Globe2, 
  GitFork, 
  FileText, 
  Award, 
  Download, 
  Sliders, 
  Search, 
  Layers,
  ChevronDown,
  ChevronUp,
  MapPin
} from 'lucide-react';

function getCountryFlag(code) {
  if (!code || code.length !== 2) return '🌐';
  const codePoints = code
    .toUpperCase()
    .split('')
    .map(c => 127397 + c.charCodeAt(0));
  return String.fromCodePoint(...codePoints);
}

export default function ConnectionMapViewer({
  collabData,
  title,
  subtitle,
  anchorName,
  anchorCode,
  loading = false,
  scopeType = 'country' // 'country' | 'journal'
}) {
  const { t } = useTranslation();

  const [minWeight, setMinWeight] = useState(1);
  const [tableSearch, setTableSearch] = useState('');
  const [showTable, setShowTable] = useState(true);
  const [geoCenter, setGeoCenter] = useState('latam'); // 'latam' | 'world'

  const rawEdges = collabData?.edges || [];
  const rawNodes = collabData?.nodes || [];
  const summary = collabData?.summary || {};

  // Max edge weight for dynamic scaling
  const maxEdgeWeight = useMemo(() => {
    if (rawEdges.length === 0) return 1;
    return Math.max(...rawEdges.map(e => e.weight || 1));
  }, [rawEdges]);

  // Max node count for marker scaling
  const maxNodeCount = useMemo(() => {
    if (rawNodes.length === 0) return 1;
    return Math.max(...rawNodes.map(n => n.count || 1));
  }, [rawNodes]);

  // Filtered edges based on minWeight slider
  const filteredEdges = useMemo(() => {
    return rawEdges.filter(e => (e.weight || 0) >= minWeight);
  }, [rawEdges, minWeight]);

  // Visible nodes in filtered edges or anchor node
  const visibleNodeIds = useMemo(() => {
    const ids = new Set();
    if (anchorCode) ids.add(anchorCode.toUpperCase());
    filteredEdges.forEach(e => {
      ids.add(e.source);
      ids.add(e.target);
    });
    return ids;
  }, [filteredEdges, anchorCode]);

  const filteredNodes = useMemo(() => {
    if (filteredEdges.length === 0 && anchorCode) {
      return rawNodes.filter(n => n.id === anchorCode.toUpperCase());
    }
    return rawNodes.filter(n => visibleNodeIds.has(n.id));
  }, [rawNodes, visibleNodeIds, filteredEdges, anchorCode]);

  // Prepare Plotly scattergeo traces
  const geoTraces = useMemo(() => {
    if (!collabData || rawNodes.length === 0) return [];
    const traces = [];

    // 1. Edge lines
    filteredEdges.forEach(e => {
      const isAnchorEdge = anchorCode && (e.source === anchorCode || e.target === anchorCode);
      const strokeWidth = Math.max(1.2, Math.min(6.5, (e.weight / maxEdgeWeight) * 6 + 1.2));
      const lineColor = isAnchorEdge 
        ? 'rgba(14, 165, 233, 0.70)' 
        : 'rgba(148, 163, 184, 0.45)';

      traces.push({
        type: 'scattergeo',
        lat: [e.source_lat, e.target_lat],
        lon: [e.source_lon, e.target_lon],
        mode: 'lines',
        line: {
          width: strokeWidth,
          color: lineColor
        },
        hoverinfo: 'text',
        text: `<b>${getCountryFlag(e.source)} ${e.source_name || e.source}</b> ⇄ <b>${getCountryFlag(e.target)} ${e.target_name || e.target}</b><br>Artículos en coautoría: <b>${(e.weight || 0).toLocaleString()}</b>`,
        showlegend: false
      });
    });

    // 2. Node markers (Non-anchor partners)
    const partnerNodes = filteredNodes.filter(n => !n.is_anchor);
    if (partnerNodes.length > 0) {
      traces.push({
        type: 'scattergeo',
        lat: partnerNodes.map(n => n.lat),
        lon: partnerNodes.map(n => n.lon),
        mode: 'markers+text',
        text: partnerNodes.map(n => n.id),
        textposition: 'top center',
        textfont: { size: 10, color: 'var(--text-main)' },
        marker: {
          size: partnerNodes.map(n => Math.max(8, Math.min(22, 8 + Math.sqrt(n.count / (maxNodeCount || 1)) * 14))),
          color: '#0284c7',
          line: { color: '#ffffff', width: 1.2 },
          opacity: 0.92
        },
        hoverinfo: 'text',
        hovertext: partnerNodes.map(n => `<b>${getCountryFlag(n.id)} ${n.name} (${n.id})</b><br>Artículos con participación: <b>${(n.count || 0).toLocaleString()}</b>`),
        name: t('connection_map.partner_nodes') || 'Países Socios'
      });
    }

    // 3. Anchor node marker (highlighted)
    const anchorNode = filteredNodes.find(n => n.is_anchor);
    if (anchorNode) {
      traces.push({
        type: 'scattergeo',
        lat: [anchorNode.lat],
        lon: [anchorNode.lon],
        mode: 'markers+text',
        text: [`★ ${anchorNode.id}`],
        textposition: 'bottom center',
        textfont: { size: 12, color: '#10b981', family: 'sans-serif', weight: 'bold' },
        marker: {
          size: 24,
          color: '#10b981',
          symbol: 'circle',
          line: { color: '#ffffff', width: 2.5 },
          opacity: 1
        },
        hoverinfo: 'text',
        hovertext: [`<b>★ ${getCountryFlag(anchorNode.id)} ${anchorNode.name} (${anchorNode.id}) — País Sede</b><br>Total artículos: <b>${(anchorNode.count || 0).toLocaleString()}</b>`],
        name: t('connection_map.anchor_node') || 'País Sede'
      });
    }

    return traces;
  }, [collabData, rawNodes, filteredEdges, filteredNodes, anchorCode, maxEdgeWeight, maxNodeCount, t]);

  // Filtered Table pairs
  const tablePairs = useMemo(() => {
    if (!filteredEdges) return [];
    let list = filteredEdges;
    if (tableSearch.trim()) {
      const q = tableSearch.toLowerCase().trim();
      list = list.filter(e => 
        (e.source_name && e.source_name.toLowerCase().includes(q)) ||
        (e.target_name && e.target_name.toLowerCase().includes(q)) ||
        e.source.toLowerCase().includes(q) ||
        e.target.toLowerCase().includes(q)
      );
    }
    return list;
  }, [filteredEdges, tableSearch]);

  // Export CSV
  const handleExportCSV = () => {
    if (rawEdges.length === 0) return;
    const headers = [
      'Rank',
      'Pais_A',
      'Codigo_A',
      'Pais_B',
      'Codigo_B',
      'Articulos_Coautoria',
      'Porcentaje_Total'
    ];
    const totArticles = summary.total_coauthored_articles || 1;
    const rows = rawEdges.map((e, i) => [
      i + 1,
      `"${e.source_name || e.source}"`,
      e.source,
      `"${e.target_name || e.target}"`,
      e.target,
      e.weight || 0,
      `${(((e.weight || 0) / totArticles) * 100).toFixed(2)}%`
    ]);
    const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    const fnamePrefix = scopeType === 'journal' ? 'coautoria_revista' : 'coautoria_pais';
    link.setAttribute('download', `${fnamePrefix}_${(anchorCode || 'global').toLowerCase()}_${new Date().toISOString().slice(0, 10)}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  // Geo layout config
  const geoLayoutConfig = useMemo(() => {
    if (geoCenter === 'latam') {
      return {
        scope: 'world',
        showcountries: true,
        countrycolor: 'rgba(156, 163, 175, 0.35)',
        projection: { type: 'equirectangular' },
        center: { lat: 10, lon: -45 },
        projection_scale: 1.45
      };
    }
    return {
      scope: 'world',
      showcountries: true,
      countrycolor: 'rgba(156, 163, 175, 0.35)',
      projection: { type: 'equirectangular' },
      center: { lat: 15, lon: 0 },
      projection_scale: 1.05
    };
  }, [geoCenter]);

  if (loading) {
    return (
      <div className="card" style={{ padding: '36px 20px', textAlign: 'center' }}>
        <div className="spinner" style={{ margin: '0 auto 12px auto' }} />
        <span style={{ fontSize: '13px', color: 'var(--text-muted)' }}>
          {t('common.loading') || 'Cargando matriz de coautoría internacional...'}
        </span>
      </div>
    );
  }

  const defaultTitle = t('connection_map.title') || 'Matriz de Coautoría País-País (Connection Map Global)';
  const defaultSubtitle = scopeType === 'journal'
    ? (t('connection_map.subtitle_journal') || 'Red de países de autores que co-publican en esta revista.')
    : (t('connection_map.subtitle_country') || 'Red de colaboración observada en los artículos de revistas editadas en este país.');

  return (
    <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', flexWrap: 'wrap', gap: '14px' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
            <Globe2 size={20} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '17px', fontWeight: '700', margin: 0 }}>
              {title || defaultTitle}
            </h3>
          </div>
          <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', margin: 0, lineHeight: '1.5' }}>
            {subtitle || defaultSubtitle}
          </p>
        </div>

        {/* Actions */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
          {/* Projection switcher */}
          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${geoCenter === 'latam' ? 'active' : ''}`}
              onClick={() => setGeoCenter('latam')}
              style={{ fontSize: '11.5px', padding: '5px 10px' }}
            >
              {t('connection_map.center_latam') || 'América'}
            </button>
            <button
              className={`segmented-pill-btn ${geoCenter === 'world' ? 'active' : ''}`}
              onClick={() => setGeoCenter('world')}
              style={{ fontSize: '11.5px', padding: '5px 10px' }}
            >
              {t('connection_map.center_world') || 'Global'}
            </button>
          </div>

          <button
            className="btn-secondary"
            onClick={handleExportCSV}
            disabled={rawEdges.length === 0}
            style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', padding: '6px 12px' }}
            title={t('connection_map.download_csv') || 'Descargar pares de colaboración en CSV'}
          >
            <Download size={14} /> {t('tables.download_csv') || 'CSV'}
          </button>
        </div>
      </div>

      {/* KPI Chips */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: '12px' }}>
        <div style={{ background: 'var(--bg-input)', borderRadius: '8px', padding: '10px 14px', border: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', gap: '10px' }}>
          <MapPin size={18} color="#0284c7" />
          <div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: '600' }}>
              {t('connection_map.kpi_countries') || 'Países Representados'}
            </div>
            <div style={{ fontSize: '17px', fontWeight: '700', color: 'var(--text-main)' }}>
              {summary.total_countries || rawNodes.length}
            </div>
          </div>
        </div>

        <div style={{ background: 'var(--bg-input)', borderRadius: '8px', padding: '10px 14px', border: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', gap: '10px' }}>
          <GitFork size={18} color="#8b5cf6" />
          <div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: '600' }}>
              {t('connection_map.kpi_edges') || 'Pares de Coautoría'}
            </div>
            <div style={{ fontSize: '17px', fontWeight: '700', color: 'var(--text-main)' }}>
              {filteredEdges.length} {minWeight > 1 ? `(de ${rawEdges.length})` : ''}
            </div>
          </div>
        </div>

        <div style={{ background: 'var(--bg-input)', borderRadius: '8px', padding: '10px 14px', border: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', gap: '10px' }}>
          <FileText size={18} color="#10b981" />
          <div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: '600' }}>
              {t('connection_map.kpi_articles') || 'Artículos Co-publicados'}
            </div>
            <div style={{ fontSize: '17px', fontWeight: '700', color: 'var(--text-main)' }}>
              {(summary.total_coauthored_articles || 0).toLocaleString()}
            </div>
          </div>
        </div>

        <div style={{ background: 'var(--bg-input)', borderRadius: '8px', padding: '10px 14px', border: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', gap: '10px' }}>
          <Award size={18} color="#f59e0b" />
          <div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: '600' }}>
              {t('connection_map.kpi_top_partner') || 'Principal Socio'}
            </div>
            <div style={{ fontSize: '14px', fontWeight: '700', color: 'var(--text-main)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '170px' }}>
              {summary.top_partner ? (
                <>
                  {getCountryFlag(summary.top_partner.code)} {summary.top_partner.name} <span style={{ fontSize: '11px', color: 'var(--text-muted)', fontWeight: 'normal' }}>({summary.top_partner.collaborations.toLocaleString()})</span>
                </>
              ) : '—'}
            </div>
          </div>
        </div>
      </div>

      {/* Threshold Slider Filter */}
      {rawEdges.length > 5 && maxEdgeWeight > 2 && (
        <div style={{ background: 'var(--bg-input)', padding: '10px 16px', borderRadius: '8px', border: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '14px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexGrow: 1, minWidth: '260px' }}>
            <Sliders size={16} color="var(--text-muted)" />
            <span style={{ fontSize: '12.5px', fontWeight: '600', color: 'var(--text-main)', whiteSpace: 'nowrap' }}>
              {t('connection_map.min_weight') || 'Fuerza mínima de enlace:'}
            </span>
            <input
              type="range"
              min="1"
              max={Math.min(maxEdgeWeight, Math.max(10, Math.floor(maxEdgeWeight * 0.4)))}
              value={minWeight}
              onChange={(e) => setMinWeight(Number(e.target.value))}
              style={{ flexGrow: 1, cursor: 'pointer' }}
            />
            <span className="badge" style={{ fontSize: '12px', fontWeight: '700', padding: '2px 8px', minWidth: '45px', textAlign: 'center' }}>
              ≥ {minWeight}
            </span>
          </div>

          {minWeight > 1 && (
            <button
              className="btn-secondary"
              onClick={() => setMinWeight(1)}
              style={{ fontSize: '11.5px', padding: '3px 8px' }}
            >
              {t('common.reset') || 'Restablecer'}
            </button>
          )}
        </div>
      )}

      {/* Plotly World Map */}
      <div style={{ border: '1px solid var(--border-color)', borderRadius: '10px', overflow: 'hidden', background: 'var(--bg-card)' }}>
        {geoTraces.length > 0 ? (
          <PlotlyChart
            data={geoTraces}
            layout={{
              geo: geoLayoutConfig,
              height: 520,
              margin: { l: 0, r: 0, t: 10, b: 10 },
              paper_bgcolor: 'transparent',
              plot_bgcolor: 'transparent',
              showlegend: true,
              legend: {
                orientation: 'h',
                x: 0.02,
                y: 0.05,
                bgcolor: 'rgba(15, 23, 42, 0.75)',
                bordercolor: 'rgba(148, 163, 184, 0.3)',
                borderwidth: 1,
                font: { size: 11 }
              }
            }}
          />
        ) : (
          <div style={{ padding: '60px 20px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '13px' }}>
            {t('connection_map.no_data') || 'No se registraron redes de coautoría internacional para esta selección.'}
          </div>
        )}
      </div>

      {/* Top Pairs Collapsible Table */}
      {filteredEdges.length > 0 && (
        <div style={{ border: '1px solid var(--border-color)', borderRadius: '8px', overflow: 'hidden' }}>
          <div 
            onClick={() => setShowTable(!showTable)}
            style={{ 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'space-between', 
              padding: '10px 14px', 
              background: 'var(--bg-input)', 
              cursor: 'pointer',
              userSelect: 'none'
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Layers size={16} color="var(--accent-primary)" />
              <span style={{ fontSize: '13.5px', fontWeight: '700' }}>
                {t('connection_map.table_title') || 'Top Pares de Colaboración Bilateral'}
              </span>
              <span className="badge" style={{ fontSize: '11px' }}>
                {tablePairs.length}
              </span>
            </div>
            {showTable ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          </div>

          {showTable && (
            <div style={{ padding: '12px' }}>
              {/* Search filter for table */}
              <div style={{ marginBottom: '10px', maxWidth: '280px', position: 'relative' }}>
                <Search size={14} style={{ position: 'absolute', left: '10px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
                <input
                  type="text"
                  value={tableSearch}
                  onChange={(e) => setTableSearch(e.target.value)}
                  placeholder={t('connection_map.search_partner') || 'Filtrar por país...'}
                  style={{ width: '100%', padding: '6px 10px 6px 30px', fontSize: '12px', borderRadius: '6px', border: '1px solid var(--border-color)', background: 'var(--bg-card)' }}
                />
              </div>

              <div className="data-table-container" style={{ maxHeight: '320px' }}>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th style={{ width: '50px' }}>#</th>
                      <th>{t('connection_map.origin') || 'País A'}</th>
                      <th>{t('connection_map.partner') || 'País B'}</th>
                      <th style={{ textAlign: 'right' }}>{t('connection_map.collaborations') || 'Artículos Co-publicados'}</th>
                      <th style={{ width: '120px' }}>{t('connection_map.share') || '% Total'}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tablePairs.slice(0, 50).map((e, idx) => {
                      const pct = summary.total_coauthored_articles > 0 
                        ? ((e.weight / summary.total_coauthored_articles) * 100).toFixed(1)
                        : '—';
                      return (
                        <tr key={idx}>
                          <td style={{ color: 'var(--text-muted)', fontSize: '12px' }}>{idx + 1}</td>
                          <td>
                            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontWeight: '600' }}>
                              <span>{getCountryFlag(e.source)}</span>
                              <span>{e.source_name || e.source}</span>
                              <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>({e.source})</span>
                            </span>
                          </td>
                          <td>
                            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontWeight: '600' }}>
                              <span>{getCountryFlag(e.target)}</span>
                              <span>{e.target_name || e.target}</span>
                              <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>({e.target})</span>
                            </span>
                          </td>
                          <td style={{ textAlign: 'right', fontWeight: '700' }}>
                            {e.weight?.toLocaleString()}
                          </td>
                          <td>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                              <div style={{ flexGrow: 1, height: '6px', background: 'var(--border-color)', borderRadius: '3px', overflow: 'hidden' }}>
                                <div style={{ width: `${Math.min(100, Number(pct) * 2)}%`, height: '100%', background: 'var(--accent-primary)', borderRadius: '3px' }} />
                              </div>
                              <span style={{ fontSize: '11.5px', color: 'var(--text-muted)', minWidth: '36px', textAlign: 'right' }}>
                                {pct}%
                              </span>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
