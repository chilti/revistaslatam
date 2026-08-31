import React, { useState, useEffect } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import KpiCard from '../components/KpiCard';
import PlotlyChart from '../components/PlotlyChart';
import { 
  BookOpen, 
  FileText, 
  Zap, 
  Sparkles, 
  Globe2, 
  Layers, 
  TrendingUp, 
  Table, 
  Compass, 
  Radar, 
  PlusCircle,
  CheckCircle2,
  GitCommit,
  BarChart2,
  Activity,
  Layers3
} from 'lucide-react';

export default function RegionalPage() {
  const { addDossierItem } = useAppStore();
  
  // State
  const [kpis, setKpis] = useState(null);
  const [choroplethData, setChoroplethData] = useState([]);
  const [selectedMapIndicator, setSelectedMapIndicator] = useState('num_journals');
  const [periods, setPeriods] = useState(null);
  const [distributions, setDistributions] = useState({ oa: [], languages: [] });
  
  // Thematic hierarchy
  const [thematicViewType, setThematicViewType] = useState('sunburst'); // 'sunburst' | 'treemap'
  const [sunburstData, setSunburstData] = useState(null);
  const [treemapData, setTreemapData] = useState(null);
  const [selectedSunburstInd, setSelectedSunburstInd] = useState('fwci_avg_recent');
  const [sunburstUnclassified, setSunburstUnclassified] = useState(true);
  
  // Dumbbell Chart & Gaps
  const [periodGaps, setPeriodGaps] = useState([]);
  const [selectedDumbbellInd, setSelectedDumbbellInd] = useState('fwci'); // 'fwci' | 'diamond' | 'top10' | 'english'

  // Stacked Bars
  const [stackedData, setStackedData] = useState([]);
  const [stackedMode, setStackedMode] = useState('oa'); // 'oa' | 'lang'

  // Stream Graph
  const [streamData, setStreamData] = useState([]);

  // Diverging Bars
  const [divergingData, setDivergingData] = useState([]);
  const [divergingInd, setDivergingInd] = useState('fwci_avg');

  const [thematicLevel, setThematicLevel] = useState('domain');
  const [thematicProfiles, setThematicProfiles] = useState(null);
  const [annualTrends, setAnnualTrends] = useState([]);
  const [annualWindow, setAnnualWindow] = useState(0);
  const [rankingsPeriod, setRankingsPeriod] = useState('full');
  const [rankings, setRankings] = useState([]);
  const [trajectories, setTrajectories] = useState(null);
  const [scatterData, setScatterData] = useState([]);
  const [scatterX, setScatterX] = useState('num_documents');
  const [scatterY, setScatterY] = useState('fwci_avg');
  const [loading, setLoading] = useState(true);

  const MAP_INDICATORS = [
    { id: 'num_journals', label: 'Número de Revistas' },
    { id: 'num_documents', label: 'Artículos' },
    { id: 'fwci_avg', label: 'FWCI Promedio' },
    { id: 'pct_top_10', label: '% Top 10%' },
    { id: 'pct_top_1', label: '% Top 1%' },
    { id: 'pct_oa_diamond', label: '% OA Diamante' },
    { id: 'pct_oa_total', label: '% OA Total' },
    { id: 'pct_oa_gold', label: '% OA Gold' },
    { id: 'pct_oa_green', label: '% OA Verde' },
    { id: 'pct_oa_hybrid', label: '% OA Híbrido' },
    { id: 'pct_oa_bronze', label: '% OA Bronce' },
    { id: 'pct_oa_closed', label: '% Cerrado' },
    { id: 'pct_lang_es', label: '% Idioma Español' },
    { id: 'pct_lang_en', label: '% Idioma Inglés' },
    { id: 'pct_lang_pt', label: '% Idioma Portugués' },
  ];

  const SUNBURST_INDICATORS = [
    { id: 'fwci_avg_recent', label: 'FWCI (2021-2025)' },
    { id: 'avg_percentile_recent', label: 'Percentil (2021-2025)' },
    { id: 'pct_top_1_recent', label: '% Top 1% (2021-2025)' },
    { id: 'pct_top_10_recent', label: '% Top 10% (2021-2025)' },
    { id: 'pct_oa_gold_recent', label: '% OA Gold (2021-2025)' },
    { id: 'fwci_avg_full', label: 'FWCI (Todo)' },
    { id: 'avg_percentile_full', label: 'Percentil (Todo)' },
    { id: 'pct_top_1_full', label: '% Top 1% (Todo)' },
    { id: 'pct_top_10_full', label: '% Top 10% (Todo)' },
    { id: 'pct_oa_gold_full', label: '% OA Gold (Todo)' },
  ];

  const SCATTER_INDICATORS = [
    { id: 'num_documents', label: 'Documentos' },
    { id: 'fwci_avg', label: 'FWCI Promedio' },
    { id: 'pct_top_10', label: '% Top 10%' },
    { id: 'pct_top_1', label: '% Top 1%' },
    { id: 'avg_percentile', label: 'Percentil Promedio' },
    { id: 'pct_oa_diamond', label: '% OA Diamante' },
    { id: 'pct_oa_gold', label: '% OA Gold' },
    { id: 'pct_oa_total', label: '% OA Total' }
  ];

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        const [kpiRes, choroRes, periodsRes, distRes, annualRes, rankRes, trajRes, gapsRes, stackRes, streamRes] = await Promise.all([
          api.get('/regional/kpis'),
          api.get(`/regional/choropleth?indicator=${selectedMapIndicator}`),
          api.get('/regional/periods-comparison'),
          api.get('/regional/distributions'),
          api.get(`/regional/annual-trends?window=${annualWindow}`),
          api.get(`/regional/rankings?period=${rankingsPeriod}`),
          api.get('/regional/trajectories'),
          api.get('/regional/period-gaps'),
          api.get('/regional/stacked-oa-languages'),
          api.get('/regional/thematic-stream')
        ]);

        setKpis(kpiRes.data);
        setChoroplethData(choroRes.data);
        setPeriods(periodsRes.data);
        setDistributions(distRes.data);
        setAnnualTrends(annualRes.data);
        setRankings(rankRes.data);
        setTrajectories(trajRes.data);
        setPeriodGaps(gapsRes.data);
        setStackedData(stackRes.data);
        setStreamData(streamRes.data);
      } catch (err) {
        console.error('Error loading regional data:', err);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  // Reload Choropleth on indicator change
  useEffect(() => {
    api.get(`/regional/choropleth?indicator=${selectedMapIndicator}`)
      .then(res => setChoroplethData(res.data))
      .catch(console.error);
  }, [selectedMapIndicator]);

  // Reload Sunburst / Treemap
  useEffect(() => {
    if (thematicViewType === 'sunburst') {
      api.get(`/regional/sunburst?indicator=${selectedSunburstInd}&include_unclassified=${sunburstUnclassified}`)
        .then(res => setSunburstData(res.data))
        .catch(console.error);
    } else {
      api.get(`/regional/treemap?indicator=${selectedSunburstInd}&include_unclassified=${sunburstUnclassified}`)
        .then(res => setTreemapData(res.data))
        .catch(console.error);
    }
  }, [thematicViewType, selectedSunburstInd, sunburstUnclassified]);

  // Reload Thematic Profiles
  useEffect(() => {
    api.get(`/regional/thematic-profiles?level=${thematicLevel}`)
      .then(res => setThematicProfiles(res.data))
      .catch(console.error);
  }, [thematicLevel]);

  // Reload Annual Trends
  useEffect(() => {
    api.get(`/regional/annual-trends?window=${annualWindow}`)
      .then(res => setAnnualTrends(res.data))
      .catch(console.error);
  }, [annualWindow]);

  // Reload Rankings
  useEffect(() => {
    api.get(`/regional/rankings?period=${rankingsPeriod}`)
      .then(res => setRankings(res.data))
      .catch(console.error);
  }, [rankingsPeriod]);

  // Reload Scatter
  useEffect(() => {
    api.get(`/regional/journals-scatter?x_col=${scatterX}&y_col=${scatterY}`)
      .then(res => setScatterData(res.data))
      .catch(console.error);
  }, [scatterX, scatterY]);

  // Reload Diverging Bars
  useEffect(() => {
    api.get(`/regional/diverging-bars?indicator=${divergingInd}`)
      .then(res => setDivergingData(res.data))
      .catch(console.error);
  }, [divergingInd]);

  // Prepare Choropleth Trace
  const choroTrace = [{
    type: 'choropleth',
    locations: choroplethData.map(d => d.country_code_iso3),
    locationmode: 'ISO-3',
    z: choroplethData.map(d => Number(d[selectedMapIndicator]) || 0),
    text: choroplethData.map(d => `${d.country_name} (${d.country_code})<br>Revistas: ${d.num_journals || 0}<br>Artículos: ${(d.num_documents || 0).toLocaleString()}`),
    colorscale: 'Viridis',
    colorbar: {
      title: MAP_INDICATORS.find(m => m.id === selectedMapIndicator)?.label || 'Valor',
      thickness: 15
    },
    marker: { line: { color: 'white', width: 0.5 } }
  }];

  const choroLayout = {
    title: `${MAP_INDICATORS.find(m => m.id === selectedMapIndicator)?.label} por País`,
    geo: {
      showcountries: true,
      countrycolor: '#cbd5e1',
      showcoastlines: true,
      coastlinecolor: '#94a3b8',
      showland: true,
      landcolor: '#f8fafc',
      showocean: true,
      oceancolor: '#e0f2fe',
      projection: { type: 'natural earth' },
      center: { lat: -5, lon: -70 },
      lataxis: { range: [-60, 35] },
      lonaxis: { range: [-120, -30] }
    },
    height: 520,
    margin: { l: 0, r: 0, t: 30, b: 0 }
  };

  // Sunburst Trace
  const sunburstTrace = sunburstData && sunburstData.nodes.length > 0 ? [{
    type: 'sunburst',
    ids: sunburstData.nodes.map(n => n.id),
    labels: sunburstData.nodes.map(n => n.label),
    parents: sunburstData.nodes.map(n => n.parent),
    values: sunburstData.nodes.map(n => n.value),
    marker: {
      colors: sunburstData.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true,
      colorbar: { title: SUNBURST_INDICATORS.find(s => s.id === selectedSunburstInd)?.label || 'Indicador' }
    },
    branchvalues: 'total',
    hovertemplate: '<b>%{label}</b><br>Artículos: %{value:,.0f}<br>Color: %{color:.2f}<extra></extra>'
  }] : [];

  // Treemap Trace
  const treemapTrace = treemapData && treemapData.nodes.length > 0 ? [{
    type: 'treemap',
    ids: treemapData.nodes.map(n => n.id),
    labels: treemapData.nodes.map(n => n.label),
    parents: treemapData.nodes.map(n => n.parent),
    values: treemapData.nodes.map(n => n.value),
    marker: {
      colors: treemapData.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true,
      colorbar: { title: SUNBURST_INDICATORS.find(s => s.id === selectedSunburstInd)?.label || 'Indicador' }
    },
    branchvalues: 'total',
    hovertemplate: '<b>%{label}</b><br>Artículos: %{value:,.0f}<br>Color: %{color:.2f}<extra></extra>'
  }] : [];

  // Dumbbell Chart Traces
  const dumbbellTraces = [];
  if (periodGaps.length > 0) {
    const valKeyFull = selectedDumbbellInd === 'fwci' ? 'fwci_avg_full' :
                       selectedDumbbellInd === 'diamond' ? 'pct_oa_diamond_full' :
                       selectedDumbbellInd === 'top10' ? 'pct_top_10_full' : 'pct_lang_en_full';
    const valKeyRec = selectedDumbbellInd === 'fwci' ? 'fwci_avg_recent' :
                      selectedDumbbellInd === 'diamond' ? 'pct_oa_diamond_recent' :
                      selectedDumbbellInd === 'top10' ? 'pct_top_10_recent' : 'pct_lang_en_recent';

    // Lines connecting Full -> Recent
    periodGaps.forEach(d => {
      dumbbellTraces.push({
        x: [d[valKeyFull], d[valKeyRec]],
        y: [d.country_name, d.country_name],
        type: 'scatter',
        mode: 'lines',
        line: { color: '#94a3b8', width: 2.5 },
        showlegend: false,
        hoverinfo: 'none'
      });
    });

    // Full Period Dots
    dumbbellTraces.push({
      x: periodGaps.map(d => d[valKeyFull]),
      y: periodGaps.map(d => d.country_name),
      type: 'scatter',
      mode: 'markers',
      name: 'Periodo Histórico',
      marker: { color: '#0284c7', size: 10, symbol: 'circle' },
      hovertemplate: '<b>%{y}</b> (Histórico): %{x:.2f}<extra></extra>'
    });

    // Recent Period Dots
    dumbbellTraces.push({
      x: periodGaps.map(d => d[valKeyRec]),
      y: periodGaps.map(d => d.country_name),
      type: 'scatter',
      mode: 'markers',
      name: 'Reciente (2021–2025)',
      marker: { color: '#10b981', size: 11, symbol: 'diamond' },
      hovertemplate: '<b>%{y}</b> (2021–2025): %{x:.2f}<extra></extra>'
    });
  }

  // 100% Stacked Bars Traces
  const stackedTraces = [];
  if (stackedData.length > 0) {
    if (stackedMode === 'oa') {
      const oaTypes = [
        { key: 'oa_diamond', label: 'Diamante', color: '#0284c7' },
        { key: 'oa_gold', label: 'Gold (APC)', color: '#f59e0b' },
        { key: 'oa_green', label: 'Verde (Repositorio)', color: '#10b981' },
        { key: 'oa_hybrid', label: 'Híbrido', color: '#8b5cf6' },
        { key: 'oa_bronze', label: 'Bronce', color: '#d97706' },
        { key: 'oa_closed', label: 'Cerrado', color: '#64748b' }
      ];
      oaTypes.forEach(t => {
        stackedTraces.push({
          x: stackedData.map(d => d[t.key]),
          y: stackedData.map(d => d.country_name),
          name: t.label,
          type: 'bar',
          orientation: 'h',
          marker: { color: t.color },
          hovertemplate: `<b>%{y}</b>: %{x}% ${t.label}<extra></extra>`
        });
      });
    } else {
      const langTypes = [
        { key: 'lang_es', label: 'Español', color: '#0284c7' },
        { key: 'lang_pt', label: 'Portugués', color: '#10b981' },
        { key: 'lang_en', label: 'Inglés', color: '#f59e0b' },
        { key: 'lang_other', label: 'Otros', color: '#94a3b8' }
      ];
      langTypes.forEach(t => {
        stackedTraces.push({
          x: stackedData.map(d => d[t.key]),
          y: stackedData.map(d => d.country_name),
          name: t.label,
          type: 'bar',
          orientation: 'h',
          marker: { color: t.color },
          hovertemplate: `<b>%{y}</b>: %{x}% ${t.label}<extra></extra>`
        });
      });
    }
  }

  // Stream Graph Traces
  const streamTraces = [];
  if (streamData.length > 0) {
    const domains = ['Health Sciences', 'Social Sciences', 'Physical Sciences', 'Life Sciences'];
    const colors = ['#0284c7', '#10b981', '#f59e0b', '#8b5cf6'];
    domains.forEach((dom, i) => {
      if (streamData[0] && dom in streamData[0]) {
        streamTraces.push({
          x: streamData.map(d => d.year),
          y: streamData.map(d => d[dom]),
          name: dom,
          type: 'scatter',
          mode: 'lines',
          stackgroup: 'one',
          line: { shape: 'spline', color: colors[i % colors.length] },
          hovertemplate: `<b>${dom}</b> (%{x}): %{y:,.0f} artículos<extra></extra>`
        });
      }
    });
  }

  // Diverging Bar Trace
  const divergingTrace = divergingData.length > 0 ? [{
    x: divergingData.map(d => d.deviation),
    y: divergingData.map(d => d.country_name),
    type: 'bar',
    orientation: 'h',
    marker: {
      color: divergingData.map(d => d.deviation >= 0 ? '#10b981' : '#ef4444')
    },
    hovertemplate: '<b>%{y}</b><br>Valor: %{customdata[0]:.2f}<br>Línea Base: %{customdata[1]:.2f}<br>Desviación: %{x:+.2f}<extra></extra>',
    customdata: divergingData.map(d => [d.actual_value, d.baseline])
  }] : [];

  // Trajectories Traces
  const trajTraces = [];
  if (trajectories && typeof trajectories === 'object') {
    Object.keys(trajectories).forEach(k => {
      const item = trajectories[k];
      if (item && Array.isArray(item.points) && item.points.length > 0) {
        trajTraces.push({
          x: item.points.map(p => p.x),
          y: item.points.map(p => p.y),
          mode: 'lines+markers',
          name: item.name || k,
          text: item.points.map(p => `${item.name || k} (${p.year})`),
          line: {
            shape: 'spline',
            width: item.is_ref ? 4 : 2,
            color: item.is_ref ? '#10b981' : undefined
          },
          marker: { size: item.is_ref ? 6 : 4 }
        });
      }
    });
  }


  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '32px' }}>
      {/* Header & Title */}
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: '800' }}>Panorama Regional (Latinoamérica e Iberoamérica)</h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '4px' }}>
          Visión macro de la producción científica, brechas temporales, modelos de acceso abierto y cartografía temática.
        </p>
      </div>

      {/* 5 Top KPI Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: '16px' }}>
        <KpiCard
          title="Revistas Indexadas"
          value={kpis?.num_journals?.toLocaleString()}
          subtitle="En OpenAlex Snapshot"
          icon={BookOpen}
        />
        <KpiCard
          title="Total Artículos"
          value={kpis?.total_works?.toLocaleString()}
          subtitle="Producción Histórica"
          icon={FileText}
        />
        <KpiCard
          title="FWCI Promedio"
          value={kpis?.fwci_avg}
          subtitle="Citas Ponderadas por Campo"
          icon={Zap}
          badge={kpis?.fwci_avg >= 1.0 ? 'Superior al Mundo' : 'Regional'}
        />
        <KpiCard
          title="% OA Diamante"
          value={`${kpis?.pct_oa_diamond}%`}
          subtitle="Sin cobro por APC"
          icon={Sparkles}
          badge="Modelo Diamante"
        />
        <KpiCard
          title="% OA Total"
          value={`${kpis?.pct_oa_total}%`}
          subtitle="Acceso Abierto Global"
          icon={Globe2}
        />
      </div>

      {/* Map Section */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>🗺️ Mapa Regional por Indicador</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Distribución espacial y comparativa entre los 20 países latinoamericanos.
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <select
              value={selectedMapIndicator}
              onChange={(e) => setSelectedMapIndicator(e.target.value)}
              style={{ fontWeight: '600' }}
            >
              {MAP_INDICATORS.map(m => (
                <option key={m.id} value={m.id}>{m.label}</option>
              ))}
            </select>

            <button
              className="segmented-pill-btn"
              onClick={() => addDossierItem({
                key: `map_${selectedMapIndicator}`,
                title: `Mapa Regional: ${MAP_INDICATORS.find(m => m.id === selectedMapIndicator)?.label}`,
                context: 'Distribución espacial del indicador seleccionado entre países latinoamericanos.',
                category: 'Mapas Geográficos',
                data: choroplethData
              })}
              style={{ display: 'flex', alignItems: 'center', gap: '4px', background: 'var(--bg-input)', border: '1px solid var(--border-color)' }}
            >
              <PlusCircle size={13} /> Guardar
            </button>
          </div>
        </div>

        <PlotlyChart data={choroTrace} layout={choroLayout} />
      </div>

      {/* DUMBBELL CHART: Brechas de Periodos */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <GitCommit size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
                Gráfico de Mancuerna (Dumbbell Chart) — Brecha Histórico vs Reciente (2021–2025)
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Visualiza la aceleración o desaceleración cienciométrica de cada país entre el promedio histórico (punto azul) y el último lustro (diamante verde).
            </span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${selectedDumbbellInd === 'fwci' ? 'active' : ''}`}
              onClick={() => setSelectedDumbbellInd('fwci')}
            >
              FWCI Promedio
            </button>
            <button
              className={`segmented-pill-btn ${selectedDumbbellInd === 'diamond' ? 'active' : ''}`}
              onClick={() => setSelectedDumbbellInd('diamond')}
            >
              % OA Diamante
            </button>
            <button
              className={`segmented-pill-btn ${selectedDumbbellInd === 'top10' ? 'active' : ''}`}
              onClick={() => setSelectedDumbbellInd('top10')}
            >
              % Top 10%
            </button>
            <button
              className={`segmented-pill-btn ${selectedDumbbellInd === 'english' ? 'active' : ''}`}
              onClick={() => setSelectedDumbbellInd('english')}
            >
              % Inglés
            </button>
          </div>
        </div>

        <PlotlyChart
          data={dumbbellTraces}
          layout={{
            height: 540,
            margin: { l: 140, r: 20, t: 20, b: 40 },
            xaxis: {
              title: selectedDumbbellInd === 'fwci' ? 'FWCI Ponderado' : 'Porcentaje (%)',
              zeroline: false
            },
            yaxis: { autorange: 'reversed' },
            legend: { orientation: 'h', y: 1.08, x: 0.2 }
          }}
        />
      </div>

      {/* 100% STACKED BAR CHARTS: Composición de Vías OA e Idiomas */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <BarChart2 size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
                Barras Apiladas 100% (100% Stacked Bar) — Vías de Acceso e Idiomas por País
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Composición porcentual de publicaciones según modalidad de acceso abierto o idioma editorial por país.
            </span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${stackedMode === 'oa' ? 'active' : ''}`}
              onClick={() => setStackedMode('oa')}
            >
              Vías de Acceso Abierto
            </button>
            <button
              className={`segmented-pill-btn ${stackedMode === 'lang' ? 'active' : ''}`}
              onClick={() => setStackedMode('lang')}
            >
              Idiomas de Publicación
            </button>
          </div>
        </div>

        <PlotlyChart
          data={stackedTraces}
          layout={{
            barmode: 'stack',
            height: 520,
            margin: { l: 140, r: 20, t: 20, b: 40 },
            xaxis: { title: 'Porcentaje Acumulado (%)', range: [0, 100] },
            yaxis: { autorange: 'reversed' },
            legend: { orientation: 'h', y: 1.08, x: 0.1 }
          }}
        />
      </div>

      {/* STREAM GRAPH: Evolución de Grandes Áreas Temáticas */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' }}>
          <Activity size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            Stream Graph / Área Apilada — Dinámica Histórica de Grandes Áreas (1985–2025)
          </h3>
        </div>
        <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          Evolución y peso relativo del volumen de producción científica en América Latina por dominios de conocimiento.
        </p>

        <PlotlyChart
          data={streamTraces}
          layout={{
            height: 380,
            margin: { l: 50, r: 20, t: 20, b: 40 },
            xaxis: { title: 'Año de Publicación' },
            yaxis: { title: 'Artículos Publicados' },
            legend: { orientation: 'h', y: 1.1, x: 0.15 }
          }}
        />
      </div>

      {/* THEMATIC HIERARCHY: SUNBURST & TREEMAP SWITCHER */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Layers3 size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
                Estructura Temática Jerárquica: Dominio → Campo → Subcampo → Tópico
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Alterna entre la vista radial (Sunburst) y la vista rectangular compacta (Treemap).
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${thematicViewType === 'sunburst' ? 'active' : ''}`}
                onClick={() => setThematicViewType('sunburst')}
              >
                🏵️ Sunburst
              </button>
              <button
                className={`segmented-pill-btn ${thematicViewType === 'treemap' ? 'active' : ''}`}
                onClick={() => setThematicViewType('treemap')}
              >
                🌲 Treemap
              </button>
            </div>

            <select
              value={selectedSunburstInd}
              onChange={(e) => setSelectedSunburstInd(e.target.value)}
              style={{ fontWeight: '600' }}
            >
              {SUNBURST_INDICATORS.map(s => (
                <option key={s.id} value={s.id}>{s.label}</option>
              ))}
            </select>

            <label style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={sunburstUnclassified}
                onChange={(e) => setSunburstUnclassified(e.target.checked)}
              />
              <span>Sin Clasificación</span>
            </label>
          </div>
        </div>

        {thematicViewType === 'sunburst' ? (
          <PlotlyChart data={sunburstTrace} layout={{ height: 560, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
        ) : (
          <PlotlyChart data={treemapTrace} layout={{ height: 560, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
        )}
      </div>

      {/* DIVERGING BARS: Posicionamiento frente a la Media Regional */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              Gráfico de Barras Divergentes (Diverging Bar Chart) — Desviación de la Media
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Países que superan la línea base (verde) vs países por debajo de la media regional (rojo).
            </span>
          </div>

          <select
            value={divergingInd}
            onChange={(e) => setDivergingInd(e.target.value)}
            style={{ fontWeight: '600' }}
          >
            <option value="fwci_avg">FWCI (Base = 1.0 Mundial)</option>
            <option value="pct_oa_diamond">% OA Diamante (Base = Media LATAM)</option>
            <option value="pct_top_10">% Top 10% (Base = Media LATAM)</option>
            <option value="pct_lang_en">% Inglés (Base = Media LATAM)</option>
          </select>
        </div>

        <PlotlyChart
          data={divergingTrace}
          layout={{
            height: 500,
            margin: { l: 140, r: 20, t: 20, b: 40 },
            xaxis: { title: 'Desviación Absoluta respecto a Línea Base', zeroline: true, zerolinewidth: 2, zerolinecolor: '#334155' },
            yaxis: { autorange: 'reversed' }
          }}
        />
      </div>

      {/* Thematic Profiles Table */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>🔬 Perfiles Temáticos de Países</h3>
          
          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${thematicLevel === 'domain' ? 'active' : ''}`}
              onClick={() => setThematicLevel('domain')}
            >
              Dominio
            </button>
            <button
              className={`segmented-pill-btn ${thematicLevel === 'field' ? 'active' : ''}`}
              onClick={() => setThematicLevel('field')}
            >
              Campo
            </button>
            <button
              className={`segmented-pill-btn ${thematicLevel === 'subfield' ? 'active' : ''}`}
              onClick={() => setThematicLevel('subfield')}
            >
              Subcampo
            </button>
          </div>
        </div>

        {thematicProfiles && thematicProfiles.data.length > 0 && (
          <div className="data-table-container" style={{ maxHeight: '380px' }}>
            <table className="data-table">
              <thead>
                <tr>
                  {thematicProfiles.columns.map(col => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {thematicProfiles.data.map((row, idx) => (
                  <tr key={idx}>
                    {thematicProfiles.columns.map(col => (
                      <td key={col}>
                        {typeof row[col] === 'number' ? row[col].toLocaleString() : row[col]}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Annual Trends */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>⏳ Tendencias Anuales (1970–2026)</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Evolución histórica de producción, impacto y modalidades de acceso abierto.</span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${annualWindow === 0 ? 'active' : ''}`}
              onClick={() => setAnnualWindow(0)}
            >
              Datos Crudos
            </button>
            <button
              className={`segmented-pill-btn ${annualWindow === 3 ? 'active' : ''}`}
              onClick={() => setAnnualWindow(3)}
            >
              Suavizado (w=3)
            </button>
            <button
              className={`segmented-pill-btn ${annualWindow === 5 ? 'active' : ''}`}
              onClick={() => setAnnualWindow(5)}
            >
              Suavizado (w=5)
            </button>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', gap: '20px' }}>
          <PlotlyChart
            data={[
              {
                x: annualTrends.map(d => d.year),
                y: annualTrends.map(d => d.num_documents),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Documentos',
                line: { color: '#0284c7', width: 2 }
              }
            ]}
            layout={{ title: 'Evolución de Documentos Publicados', height: 300 }}
          />

          <PlotlyChart
            data={[
              {
                x: annualTrends.map(d => d.year),
                y: annualTrends.map(d => d.fwci_avg),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'FWCI Promedio',
                line: { color: '#10b981', width: 2 }
              },
              {
                x: annualTrends.map(d => d.year),
                y: annualTrends.map(() => 1.0),
                type: 'scatter',
                mode: 'lines',
                name: 'Media Mundial (1.0)',
                line: { color: '#ef4444', dash: 'dash' }
              }
            ]}
            layout={{ title: 'Evolución del FWCI Promedio', height: 300 }}
          />
        </div>
      </div>

      {/* Global Trajectories in UMAP */}
      <div className="card">
        <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '8px' }}>
          📈 Trayectorias de Desempeño Latam (Global 2000–2025)
        </h3>
        <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          Evolución comparativa de todos los países e Iberoamérica (línea verde gruesa) en el espacio UMAP.
        </p>
        <PlotlyChart
          data={trajTraces}
          layout={{ height: 520, xaxis: { title: 'Dimensión 1' }, yaxis: { title: 'Dimensión 2' } }}
        />
      </div>

      {/* Country Rankings Table */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>🏆 Tabla Comparativa por País (Ranking)</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Desempeño general de los 20 países latinoamericanos.</span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${rankingsPeriod === 'full' ? 'active' : ''}`}
              onClick={() => setRankingsPeriod('full')}
            >
              Periodo Completo
            </button>
            <button
              className={`segmented-pill-btn ${rankingsPeriod === 'recent' ? 'active' : ''}`}
              onClick={() => setRankingsPeriod('recent')}
            >
              Reciente (2021-2025)
            </button>
          </div>
        </div>

        <div className="data-table-container" style={{ maxHeight: '420px' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Código</th>
                <th>País</th>
                <th>Revistas</th>
                <th>Documentos</th>
                <th>FWCI</th>
                <th>% Top 10%</th>
                <th>% Top 1%</th>
                <th>% OA Diamante</th>
                <th>% OA Gold</th>
                <th>% Español</th>
                <th>% Inglés</th>
              </tr>
            </thead>
            <tbody>
              {rankings.map((r, idx) => (
                <tr key={idx}>
                  <td><strong>{r.country_code}</strong></td>
                  <td>{r.country_name}</td>
                  <td>{r.num_journals?.toLocaleString()}</td>
                  <td>{r.num_documents?.toLocaleString()}</td>
                  <td>{Number(r.fwci_avg || 0).toFixed(2)}</td>
                  <td>{Number(r.pct_top_10 || 0).toFixed(3)}%</td>
                  <td>{Number(r.pct_top_1 || 0).toFixed(3)}%</td>
                  <td>{Number(r.pct_oa_diamond || 0).toFixed(1)}%</td>
                  <td>{Number(r.pct_oa_gold || 0).toFixed(1)}%</td>
                  <td>{Number(r.pct_lang_es || 0).toFixed(1)}%</td>
                  <td>{Number(r.pct_lang_en || 0).toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Dynamic Scatter Explorer */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>🎯 Explorador de Revistas — Scatter Plot Dinámico</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Correlación interactiva entre indicadores bibliométricos para revistas latinoamericanas.</span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700' }}>Eje X:</span>
              <select value={scatterX} onChange={(e) => setScatterX(e.target.value)}>
                {SCATTER_INDICATORS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
              </select>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700' }}>Eje Y:</span>
              <select value={scatterY} onChange={(e) => setScatterY(e.target.value)}>
                {SCATTER_INDICATORS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
              </select>
            </div>
          </div>
        </div>

        <PlotlyChart
          data={[{
            x: scatterData.map(d => d[scatterX]),
            y: scatterData.map(d => d[scatterY]),
            mode: 'markers',
            marker: { size: 7, color: '#0284c7', opacity: 0.65 },
            text: scatterData.map(d => `${d.display_name} (${d.country_name})<br>${scatterX}: ${d[scatterX]}<br>${scatterY}: ${d[scatterY]}`),
            type: 'scatter'
          }]}
          layout={{
            height: 480,
            xaxis: { title: SCATTER_INDICATORS.find(s => s.id === scatterX)?.label },
            yaxis: { title: SCATTER_INDICATORS.find(s => s.id === scatterY)?.label }
          }}
        />
      </div>
    </div>
  );
}
