import React, { useState, useMemo } from 'react';
import { useTranslation } from '../i18n';
import PlotlyChart from './PlotlyChart';
import { 
  Radar, 
  Grid, 
  Eye, 
  Layers, 
  Search, 
  ArrowUpRight, 
  ArrowDownRight, 
  Minus,
  CheckCircle2,
  Info,
  ExternalLink
} from 'lucide-react';

function getCountryFlag(code) {
  if (!code || code.length !== 2) return '🌐';
  const codePoints = code
    .toUpperCase()
    .split('')
    .map(c => 127397 + c.charCodeAt(0));
  return String.fromCodePoint(...codePoints);
}

function formatVal(val, unit) {
  if (val == null || isNaN(val)) return '—';
  if (unit === '%') return `${Number(val).toFixed(1)}%`;
  if (Number(val) < 1) return Number(val).toFixed(3);
  return Number(val).toFixed(2);
}

function computeDelta(fullVal, recVal, unit) {
  if (fullVal == null || recVal == null) return null;
  const diff = recVal - fullVal;
  if (Math.abs(diff) < 0.001) {
    return { text: '0.0', isZero: true, color: 'var(--text-muted)', bg: 'rgba(148, 163, 184, 0.1)' };
  }
  const isPositive = diff > 0;
  const sign = isPositive ? '+' : '';
  const text = unit === '%' ? `${sign}${diff.toFixed(1)} pp` : `${sign}${diff.toFixed(2)}`;
  return {
    text,
    isPositive,
    color: isPositive ? '#10b981' : '#ef4444',
    bg: isPositive ? 'rgba(16, 185, 129, 0.12)' : 'rgba(239, 68, 68, 0.12)'
  };
}

export default function CountryRadarViewer({
  radarData,
  initialCountry = 'MX'
}) {
  const { t } = useTranslation();
  
  const [viewMode, setViewMode] = useState('single'); // 'single' | 'grid'
  const [selectedCountry, setSelectedCountry] = useState(initialCountry);
  const [showFull, setShowFull] = useState(true);
  const [showRecent, setShowRecent] = useState(true);
  const [showLatam, setShowLatam] = useState(true);
  const [gridSearch, setGridSearch] = useState('');

  const axes = useMemo(() => radarData?.axes || [], [radarData]);
  const profiles = useMemo(() => radarData?.profiles || {}, [radarData]);
  const latamProfile = useMemo(() => radarData?.latam || null, [radarData]);
  
  const countryCodes = useMemo(() => {
    return Object.keys(profiles).sort((a, b) => {
      const nameA = profiles[a]?.country_name || a;
      const nameB = profiles[b]?.country_name || b;
      return nameA.localeCompare(nameB);
    });
  }, [profiles]);

  // Ensure selected country is valid
  const currentCountryCode = profiles[selectedCountry] 
    ? selectedCountry 
    : (countryCodes[0] || 'MX');
  const currentProfile = profiles[currentCountryCode];

  // Build Plotly trace for detailed single country radar
  const singleRadarTraces = useMemo(() => {
    if (!currentProfile || axes.length === 0) return [];

    const thetaLabels = axes.map(a => a.label);
    const thetaClosed = [...thetaLabels, thetaLabels[0]];
    const traces = [];

    // Full Period
    if (showFull && currentProfile.full) {
      const rVals = axes.map(a => currentProfile.full[a.key] ?? 0);
      rVals.push(rVals[0]);
      const hoverText = axes.map(a => 
        `<b>${a.label}</b><br>Periodo Completo: <b>${formatVal(currentProfile.raw_full?.[a.key], a.unit)}</b><br>Escala Norm: ${(currentProfile.full[a.key] ?? 0).toFixed(2)}`
      );
      hoverText.push(hoverText[0]);

      traces.push({
        type: 'scatterpolar',
        r: rVals,
        theta: thetaClosed,
        fill: 'toself',
        name: t('regional.radar_full_period') || 'Periodo Completo',
        line: { color: '#0284c7', width: 2.5 },
        fillcolor: 'rgba(2, 132, 199, 0.22)',
        text: hoverText,
        hoverinfo: 'text'
      });
    }

    // Recent Period
    if (showRecent && currentProfile.recent) {
      const rVals = axes.map(a => currentProfile.recent[a.key] ?? 0);
      rVals.push(rVals[0]);
      const hoverText = axes.map(a => 
        `<b>${a.label}</b><br>Reciente (2021–2025): <b>${formatVal(currentProfile.raw_recent?.[a.key], a.unit)}</b><br>Escala Norm: ${(currentProfile.recent[a.key] ?? 0).toFixed(2)}`
      );
      hoverText.push(hoverText[0]);

      traces.push({
        type: 'scatterpolar',
        r: rVals,
        theta: thetaClosed,
        fill: 'toself',
        name: t('regional.radar_recent_period') || 'Reciente (2021–2025)',
        line: { color: '#ef4444', width: 2.5 },
        fillcolor: 'rgba(239, 68, 68, 0.22)',
        text: hoverText,
        hoverinfo: 'text'
      });
    }

    // LATAM Baseline Reference
    if (showLatam && latamProfile?.recent) {
      const rVals = axes.map(a => latamProfile.recent[a.key] ?? 0);
      rVals.push(rVals[0]);
      const hoverText = axes.map(a => 
        `<b>${a.label}</b><br>Referencia LATAM: <b>${formatVal(latamProfile.raw_recent?.[a.key], a.unit)}</b><br>Escala Norm: ${(latamProfile.recent[a.key] ?? 0).toFixed(2)}`
      );
      hoverText.push(hoverText[0]);

      traces.push({
        type: 'scatterpolar',
        r: rVals,
        theta: thetaClosed,
        name: `${t('regional.radar_latam_ref') || 'Ref. LATAM'} (Reciente)`,
        line: { color: '#10b981', width: 2, dash: 'dash' },
        fill: 'none',
        text: hoverText,
        hoverinfo: 'text'
      });
    }

    return traces;
  }, [currentProfile, axes, showFull, showRecent, showLatam, latamProfile, t]);

  // Filtered countries for grid view
  const filteredGridCountries = useMemo(() => {
    if (!gridSearch.trim()) return countryCodes;
    const q = gridSearch.toLowerCase();
    return countryCodes.filter(c => {
      const name = (profiles[c]?.country_name || '').toLowerCase();
      return name.includes(q) || c.toLowerCase().includes(q);
    });
  }, [countryCodes, profiles, gridSearch]);

  if (!radarData || Object.keys(profiles).length === 0) {
    return null;
  }

  return (
    <div className="card" style={{ marginBottom: '24px' }}>
      {/* Top Header with title and view mode toggle */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '14px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <div style={{ 
            width: '36px', 
            height: '36px', 
            borderRadius: '8px', 
            background: 'rgba(2, 132, 199, 0.12)', 
            display: 'flex', 
            alignItems: 'center', 
            justifyContent: 'center',
            color: 'var(--accent-primary)'
          }}>
            <Radar size={20} />
          </div>
          <div>
            <h3 style={{ fontSize: '17px', fontWeight: '700', margin: 0 }}>
              {t('regional.radar_profiles_title')}
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('regional.radar_profiles_desc')}
            </span>
          </div>
        </div>

        {/* View mode segmented pills */}
        <div className="segmented-pills">
          <button
            className={`segmented-pill-btn ${viewMode === 'single' ? 'active' : ''}`}
            onClick={() => setViewMode('single')}
            style={{ display: 'inline-flex', alignItems: 'center', gap: '6px' }}
          >
            <Eye size={14} />
            {t('regional.radar_view_single')}
          </button>
          <button
            className={`segmented-pill-btn ${viewMode === 'grid' ? 'active' : ''}`}
            onClick={() => setViewMode('grid')}
            style={{ display: 'inline-flex', alignItems: 'center', gap: '6px' }}
          >
            <Grid size={14} />
            {t('regional.radar_view_grid')}
          </button>
        </div>
      </div>

      {/* --- MODE 1: SINGLE COUNTRY DETAILED VIEW --- */}
      {viewMode === 'single' && currentProfile && (
        <div>
          {/* Controls Bar */}
          <div style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '12px',
            padding: '12px 16px',
            background: 'var(--bg-input, rgba(255, 255, 255, 0.03))',
            borderRadius: '10px',
            border: '1px solid var(--border-color)',
            marginBottom: '20px'
          }}>
            {/* Country Selector */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <span style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text-main)' }}>
                {t('regional.radar_select_country')}:
              </span>
              <select
                value={currentCountryCode}
                onChange={(e) => setSelectedCountry(e.target.value)}
                style={{
                  padding: '7px 14px',
                  borderRadius: '8px',
                  background: 'var(--bg-card)',
                  color: 'var(--text-main)',
                  border: '1px solid var(--border-color)',
                  fontSize: '13px',
                  fontWeight: '600',
                  cursor: 'pointer'
                }}
              >
                {countryCodes.map(code => {
                  const p = profiles[code];
                  return (
                    <option key={code} value={code}>
                      {getCountryFlag(code)} {p?.country_name || code} ({p?.num_documents?.toLocaleString() || 0} arts)
                    </option>
                  );
                })}
              </select>
            </div>

            {/* Layer Toggles */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap', fontSize: '12px' }}>
              <label style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer' }}>
                <input
                  type="checkbox"
                  checked={showFull}
                  onChange={(e) => setShowFull(e.target.checked)}
                />
                <span style={{ display: 'inline-block', width: '10px', height: '10px', borderRadius: '50%', background: '#0284c7' }} />
                <span>{t('regional.radar_full_period')}</span>
              </label>

              <label style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer' }}>
                <input
                  type="checkbox"
                  checked={showRecent}
                  onChange={(e) => setShowRecent(e.target.checked)}
                />
                <span style={{ display: 'inline-block', width: '10px', height: '10px', borderRadius: '50%', background: '#ef4444' }} />
                <span>{t('regional.radar_recent_period')}</span>
              </label>

              <label style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer' }}>
                <input
                  type="checkbox"
                  checked={showLatam}
                  onChange={(e) => setShowLatam(e.target.checked)}
                />
                <span style={{ display: 'inline-block', width: '10px', height: '3px', background: '#10b981', borderTop: '2px dashed #10b981' }} />
                <span>{t('regional.radar_latam_ref')}</span>
              </label>
            </div>
          </div>

          {/* 2-Column Responsive Body */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))',
            gap: '24px',
            alignItems: 'start'
          }}>
            {/* Left: Interactive Radar Chart */}
            <div style={{
              background: 'var(--bg-input, rgba(0,0,0,0.02))',
              borderRadius: '12px',
              padding: '16px',
              border: '1px solid var(--border-color)',
              minHeight: '430px'
            }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span style={{ fontSize: '20px' }}>{getCountryFlag(currentCountryCode)}</span>
                  <strong style={{ fontSize: '15px' }}>{currentProfile.country_name}</strong>
                  <span style={{ fontSize: '11px', padding: '2px 8px', borderRadius: '12px', background: 'var(--bg-card)', border: '1px solid var(--border-color)', color: 'var(--text-muted)' }}>
                    {currentCountryCode}
                  </span>
                </div>
                <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                  Normalizado [0–1] vs Máximo Regional
                </span>
              </div>

              <PlotlyChart
                data={singleRadarTraces}
                layout={{
                  polar: {
                    radialaxis: { 
                      visible: true, 
                      range: [0, 1.05],
                      tickfont: { size: 10 },
                      gridcolor: 'rgba(148, 163, 184, 0.15)'
                    },
                    angularaxis: {
                      tickfont: { size: 11, weight: 'bold' },
                      rotation: 90,
                      direction: 'clockwise'
                    }
                  },
                  height: 380,
                  margin: { l: 45, r: 45, t: 30, b: 35 },
                  legend: { orientation: 'h', y: -0.12, x: 0.1 }
                }}
                style={{ minHeight: '380px', height: '380px' }}
              />

              <div style={{ fontSize: '11px', color: 'var(--text-muted)', textAlign: 'center', marginTop: '4px' }}>
                {t('regional.radar_norm_note')}
              </div>
            </div>

            {/* Right: Comparative Indicators Table & Dynamics */}
            <div>
              {/* Header Badges */}
              <div style={{ 
                display: 'grid', 
                gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', 
                gap: '10px', 
                marginBottom: '16px' 
              }}>
                <div style={{
                  padding: '10px 14px',
                  borderRadius: '10px',
                  background: 'var(--bg-input, rgba(255, 255, 255, 0.02))',
                  border: '1px solid var(--border-color)'
                }}>
                  <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Revistas Registradas</div>
                  <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--accent-primary)', marginTop: '2px' }}>
                    {currentProfile.num_journals?.toLocaleString() || 0}
                  </div>
                </div>

                <div style={{
                  padding: '10px 14px',
                  borderRadius: '10px',
                  background: 'var(--bg-input, rgba(255, 255, 255, 0.02))',
                  border: '1px solid var(--border-color)'
                }}>
                  <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Artículos Publicados</div>
                  <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px' }}>
                    {currentProfile.num_documents?.toLocaleString() || 0}
                  </div>
                </div>
              </div>

              {/* Performance Dimensions Table */}
              <div className="data-table-container" style={{ border: '1px solid var(--border-color)', borderRadius: '10px' }}>
                <table className="data-table" style={{ fontSize: '12px', margin: 0 }}>
                  <thead>
                    <tr>
                      <th style={{ textAlign: 'left' }}>Dimensión</th>
                      <th style={{ textAlign: 'right' }}>Completo</th>
                      <th style={{ textAlign: 'right' }}>Reciente</th>
                      <th style={{ textAlign: 'center' }}>Variación (Δ)</th>
                      <th style={{ textAlign: 'right' }}>Ref. LATAM</th>
                      <th style={{ textAlign: 'center' }}>Fuerza Norm.</th>
                    </tr>
                  </thead>
                  <tbody>
                    {axes.map(axis => {
                      const k = axis.key;
                      const valFull = currentProfile.raw_full?.[k];
                      const valRec = currentProfile.raw_recent?.[k];
                      const valLatam = latamProfile?.raw_recent?.[k];
                      const normRec = currentProfile.recent?.[k] ?? 0;
                      const delta = computeDelta(valFull, valRec, axis.unit);

                      return (
                        <tr key={k}>
                          <td style={{ fontWeight: '600', color: 'var(--text-main)' }}>
                            {axis.label}
                          </td>
                          <td style={{ textAlign: 'right', color: 'var(--text-muted)' }}>
                            {formatVal(valFull, axis.unit)}
                          </td>
                          <td style={{ textAlign: 'right', fontWeight: '700', color: '#ef4444' }}>
                            {formatVal(valRec, axis.unit)}
                          </td>
                          <td style={{ textAlign: 'center' }}>
                            {delta ? (
                              <span style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: '3px',
                                padding: '2px 6px',
                                borderRadius: '6px',
                                fontSize: '11px',
                                fontWeight: '700',
                                color: delta.color,
                                background: delta.bg
                              }}>
                                {delta.isPositive ? <ArrowUpRight size={12} /> : delta.isZero ? <Minus size={12} /> : <ArrowDownRight size={12} />}
                                {delta.text}
                              </span>
                            ) : '—'}
                          </td>
                          <td style={{ textAlign: 'right', color: '#10b981', fontWeight: '600' }}>
                            {formatVal(valLatam, axis.unit)}
                          </td>
                          <td style={{ textAlign: 'center', minWidth: '90px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                              <div style={{
                                flex: 1,
                                height: '6px',
                                borderRadius: '3px',
                                background: 'rgba(148, 163, 184, 0.2)',
                                overflow: 'hidden'
                              }}>
                                <div style={{
                                  width: `${Math.min(100, Math.round(normRec * 100))}%`,
                                  height: '100%',
                                  borderRadius: '3px',
                                  background: normRec > 0.7 ? '#10b981' : normRec > 0.4 ? 'var(--accent-primary)' : '#f59e0b'
                                }} />
                              </div>
                              <span style={{ fontSize: '10px', color: 'var(--text-muted)', width: '28px', textAlign: 'right' }}>
                                {Math.round(normRec * 100)}%
                              </span>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* Informative Footer */}
              <div style={{ 
                marginTop: '12px', 
                padding: '10px 12px', 
                borderRadius: '8px', 
                background: 'rgba(2, 132, 199, 0.05)', 
                border: '1px solid rgba(2, 132, 199, 0.15)',
                fontSize: '11px',
                color: 'var(--text-muted)',
                display: 'flex',
                alignItems: 'center',
                gap: '8px'
              }}>
                <Info size={15} color="var(--accent-primary)" style={{ flexShrink: 0 }} />
                <span>
                  Los perfiles multidimensionales combinan impacto normalizado (<strong>FWCI</strong>), acceso abierto soberano (<strong>OA Diamante</strong>), excelencia citacional (<strong>Top 10% y Top 1%</strong>), apertura lingüística (<strong>% Inglés</strong>) y percentil normalizado.
                </span>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* --- MODE 2: REGIONAL COMPARATIVE GRID VIEW --- */}
      {viewMode === 'grid' && (
        <div>
          {/* Search bar & Grid info */}
          <div style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '12px',
            marginBottom: '16px'
          }}>
            <div style={{ position: 'relative', width: '280px' }}>
              <Search size={15} style={{ position: 'absolute', left: '10px', top: '10px', color: 'var(--text-muted)' }} />
              <input
                type="text"
                placeholder={t('regional.radar_grid_search')}
                value={gridSearch}
                onChange={(e) => setGridSearch(e.target.value)}
                style={{
                  width: '100%',
                  padding: '7px 12px 7px 32px',
                  borderRadius: '8px',
                  border: '1px solid var(--border-color)',
                  background: 'var(--bg-input)',
                  color: 'var(--text-main)',
                  fontSize: '13px'
                }}
              />
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '14px', fontSize: '12px', color: 'var(--text-muted)' }}>
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: '5px' }}>
                <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#0284c7' }} />
                {t('regional.radar_full_period')}
              </span>
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: '5px' }}>
                <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#ef4444' }} />
                {t('regional.radar_recent_period')}
              </span>
              <span>• {filteredGridCountries.length} países</span>
            </div>
          </div>

          {/* Cards Grid */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
            gap: '16px'
          }}>
            {filteredGridCountries.map(code => {
              const p = profiles[code];
              if (!p) return null;

              const thetaLabels = axes.map(a => a.label);
              const thetaClosed = [...thetaLabels, thetaLabels[0]];

              // Traces for mini-radar
              const rFull = axes.map(a => p.full?.[a.key] ?? 0);
              rFull.push(rFull[0]);
              const rRec = axes.map(a => p.recent?.[a.key] ?? 0);
              rRec.push(rRec[0]);

              const cardTraces = [
                {
                  type: 'scatterpolar',
                  r: rFull,
                  theta: thetaClosed,
                  fill: 'toself',
                  name: 'Completo',
                  line: { color: '#0284c7', width: 1.5 },
                  fillcolor: 'rgba(2, 132, 199, 0.18)',
                  hoverinfo: 'skip'
                },
                {
                  type: 'scatterpolar',
                  r: rRec,
                  theta: thetaClosed,
                  fill: 'toself',
                  name: 'Reciente',
                  line: { color: '#ef4444', width: 1.5 },
                  fillcolor: 'rgba(239, 68, 68, 0.18)',
                  hoverinfo: 'skip'
                }
              ];

              return (
                <div
                  key={code}
                  onClick={() => {
                    setSelectedCountry(code);
                    setViewMode('single');
                  }}
                  style={{
                    padding: '14px',
                    borderRadius: '12px',
                    border: '1px solid var(--border-color)',
                    background: 'var(--bg-card)',
                    cursor: 'pointer',
                    transition: 'all 0.2s ease',
                    boxShadow: '0 2px 8px rgba(0, 0, 0, 0.04)',
                    position: 'relative'
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.borderColor = 'var(--accent-primary)';
                    e.currentTarget.style.transform = 'translateY(-2px)';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.borderColor = 'var(--border-color)';
                    e.currentTarget.style.transform = 'translateY(0)';
                  }}
                >
                  {/* Card Title */}
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '6px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{ fontSize: '18px' }}>{getCountryFlag(code)}</span>
                      <div>
                        <div style={{ fontSize: '14px', fontWeight: '700', color: 'var(--text-main)' }}>
                          {p.country_name}
                        </div>
                        <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                          {p.num_journals?.toLocaleString()} revs • {p.num_documents?.toLocaleString()} arts
                        </div>
                      </div>
                    </div>
                    <ExternalLink size={14} color="var(--text-muted)" />
                  </div>

                  {/* Compact Plotly Radar */}
                  <PlotlyChart
                    data={cardTraces}
                    layout={{
                      polar: {
                        radialaxis: { visible: false, range: [0, 1.05] },
                        angularaxis: { 
                          tickfont: { size: 9, color: 'var(--text-muted)' },
                          rotation: 90,
                          direction: 'clockwise'
                        }
                      },
                      height: 200,
                      margin: { l: 24, r: 24, t: 18, b: 18 },
                      showlegend: false
                    }}
                    config={{ staticPlot: true, responsive: true }}
                    style={{ minHeight: '200px', height: '200px' }}
                  />

                  {/* Quick Key Metrics in Card */}
                  <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    fontSize: '11px',
                    paddingTop: '8px',
                    borderTop: '1px solid var(--border-color)',
                    color: 'var(--text-muted)'
                  }}>
                    <span>FWCI: <strong style={{ color: 'var(--text-main)' }}>{p.raw_recent?.fwci_avg ?? '—'}</strong></span>
                    <span>Diamante: <strong style={{ color: '#10b981' }}>{p.raw_recent?.pct_oa_diamond ? `${p.raw_recent.pct_oa_diamond}%` : '—'}</strong></span>
                    <span>Inglés: <strong style={{ color: '#0284c7' }}>{p.raw_recent?.pct_lang_en ? `${p.raw_recent.pct_lang_en}%` : '—'}</strong></span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
