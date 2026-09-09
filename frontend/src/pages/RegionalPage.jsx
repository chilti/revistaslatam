import React, { useState, useEffect } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import { useTranslation } from '../i18n';
import KpiCard from '../components/KpiCard';
import PlotlyChart from '../components/PlotlyChart';
import UmapTrajectoryViewer from '../components/UmapTrajectoryViewer';
import PageDossierExpander from '../components/PageDossierExpander';
import ThematicEvolutionTable from '../components/ThematicEvolutionTable';
import AnnualDataTable from '../components/AnnualDataTable';
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
  Layers3,
  Download,
  FileSpreadsheet,
  ShieldCheck,
  Award
} from 'lucide-react';

export default function RegionalPage() {
  const { addDossierItem } = useAppStore();
  const { t } = useTranslation();
  
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
  const [annualMinYear, setAnnualMinYear] = useState(1970);
  const [rankingsPeriod, setRankingsPeriod] = useState('full');
  const [rankings, setRankings] = useState([]);
  const [trajectories, setTrajectories] = useState(null);
  const [umapCountries, setUmapCountries] = useState([]);
  const [scatterData, setScatterData] = useState([]);
  const [scatterX, setScatterX] = useState('num_documents');
  const [scatterY, setScatterY] = useState('fwci_avg');
  const [loading, setLoading] = useState(true);

  const MAP_INDICATORS = [
    { id: 'num_journals', label: t('kpi.journals') },
    { id: 'num_documents', label: t('common.works') },
    { id: 'fwci_avg', label: t('kpi.fwci') },
    { id: 'pct_top_10', label: t('kpi.top_10') },
    { id: 'pct_top_1', label: t('kpi.top_1') },
    { id: 'pct_oa_diamond', label: `% ${t('common.diamond')}` },
    { id: 'pct_oa_total', label: t('kpi.oa_total') },
    { id: 'pct_oa_gold', label: `% ${t('common.gold')}` },
    { id: 'pct_oa_green', label: `% ${t('common.green')}` },
    { id: 'pct_oa_hybrid', label: `% ${t('common.hybrid')}` },
    { id: 'pct_oa_bronze', label: `% ${t('common.bronze')}` },
    { id: 'pct_oa_closed', label: `% ${t('common.closed')}` },
    { id: 'pct_lang_es', label: t('regional.pct_lang_es') },
    { id: 'pct_lang_en', label: t('regional.pct_lang_en') },
    { id: 'pct_lang_pt', label: t('regional.pct_lang_pt') },
  ];

  const SUNBURST_INDICATORS = [
    { id: 'fwci_avg_recent', label: 'FWCI (2021-2025)' },
    { id: 'avg_percentile_recent', label: `${t('thematic_table.percentile')} (2021-2025)` },
    { id: 'pct_top_1_recent', label: '% Top 1% (2021-2025)' },
    { id: 'pct_top_10_recent', label: '% Top 10% (2021-2025)' },
    { id: 'pct_oa_gold_recent', label: '% OA Gold (2021-2025)' },
    { id: 'fwci_avg_full', label: `FWCI (${t('thematic_table.full_label')})` },
    { id: 'avg_percentile_full', label: `${t('thematic_table.percentile')} (${t('thematic_table.full_label')})` },
    { id: 'pct_top_1_full', label: `% Top 1% (${t('thematic_table.full_label')})` },
    { id: 'pct_top_10_full', label: `% Top 10% (${t('thematic_table.full_label')})` },
    { id: 'pct_oa_gold_full', label: `% OA Gold (${t('thematic_table.full_label')})` },
  ];

  const SCATTER_INDICATORS = [
    { id: 'num_documents', label: t('regional.kpi_docs') },
    { id: 'fwci_avg', label: t('regional.kpi_fwci') },
    { id: 'pct_top_10', label: t('tables.top10') },
    { id: 'pct_top_1', label: t('tables.top1') },
    { id: 'avg_percentile', label: t('tables.percentile_avg') },
    { id: 'pct_oa_diamond', label: t('regional.dumbbell_diamond') },
    { id: 'pct_oa_gold', label: t('tables.oa_gold') },
    { id: 'pct_oa_total', label: t('tables.oa_total') }
  ];

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        const [kpiRes, choroRes, periodsRes, distRes, annualRes, rankRes, trajRes, gapsRes, stackRes, streamRes, umapCRes] = await Promise.all([
          api.get('/regional/kpis'),
          api.get(`/regional/choropleth?indicator=${selectedMapIndicator}`),
          api.get('/regional/periods-comparison'),
          api.get('/regional/distributions'),
          api.get(`/regional/annual-trends?window=${annualWindow}&min_year=${annualMinYear}&max_year=2026`),
          api.get(`/regional/rankings?period=${rankingsPeriod}`),
          api.get('/regional/trajectories'),
          api.get('/regional/period-gaps'),
          api.get('/regional/stacked-oa-languages'),
          api.get('/regional/thematic-stream'),
          api.get('/regional/umap-countries')
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
        setUmapCountries(umapCRes.data);
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
    api.get(`/regional/annual-trends?window=${annualWindow}&min_year=${annualMinYear}&max_year=2026`)
      .then(res => setAnnualTrends(res.data))
      .catch(console.error);
  }, [annualWindow, annualMinYear]);


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
  const sunburstTrace = (sunburstData && Array.isArray(sunburstData.nodes) && sunburstData.nodes.length > 0) ? [{
    type: 'sunburst',
    ids: sunburstData.nodes.map(n => n.id),
    labels: sunburstData.nodes.map(n => (n.label === 'Sin Clasificación' || n.label === 'Unknown') ? t('tables.no_classification') : n.label),
    parents: sunburstData.nodes.map(n => n.parent),
    values: sunburstData.nodes.map(n => n.value),
    marker: {
      colors: sunburstData.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true,
      colorbar: { title: SUNBURST_INDICATORS.find(s => s.id === selectedSunburstInd)?.label || t('thematic_table.indicator') }
    },
    branchvalues: 'total',
    hovertemplate: `<b>%{label}</b><br>${t('tables.articles')}: %{value:,.0f}<br>${t('thematic_table.color_label')}: %{color:.2f}<extra></extra>`
  }] : [];

  // Treemap Trace
  const treemapTrace = (treemapData && Array.isArray(treemapData.nodes) && treemapData.nodes.length > 0) ? [{
    type: 'treemap',
    ids: treemapData.nodes.map(n => n.id),
    labels: treemapData.nodes.map(n => (n.label === 'Sin Clasificación' || n.label === 'Unknown') ? t('tables.no_classification') : n.label),
    parents: treemapData.nodes.map(n => n.parent),
    values: treemapData.nodes.map(n => n.value),
    marker: {
      colors: treemapData.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true,
      colorbar: { title: SUNBURST_INDICATORS.find(s => s.id === selectedSunburstInd)?.label || t('thematic_table.indicator') }
    },
    branchvalues: 'total',
    hovertemplate: `<b>%{label}</b><br>${t('tables.articles')}: %{value:,.0f}<br>${t('thematic_table.color_label')}: %{color:.2f}<extra></extra>`
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
      name: t('regional.period_historical'),
      marker: { color: '#0284c7', size: 10, symbol: 'circle' },
      hovertemplate: `<b>%{y}</b> (${t('regional.historical')}): %{x:.2f}<extra></extra>`
    });

    // Recent Period Dots
    dumbbellTraces.push({
      x: periodGaps.map(d => d[valKeyRec]),
      y: periodGaps.map(d => d.country_name),
      type: 'scatter',
      mode: 'markers',
      name: `${t('regional.recent')} (2021–2025)`,
      marker: { color: '#10b981', size: 11, symbol: 'diamond' },
      hovertemplate: '<b>%{y}</b> (2021–2025): %{x:.2f}<extra></extra>'
    });
  }

  // 100% Stacked Bars Traces
  const stackedTraces = [];
  if (stackedData.length > 0) {
    if (stackedMode === 'oa') {
      const oaTypes = [
        { key: 'oa_diamond', label: t('regional.stacked_oa_diamond'), color: '#0284c7' },
        { key: 'oa_gold', label: t('regional.stacked_oa_gold'), color: '#f59e0b' },
        { key: 'oa_green', label: t('regional.stacked_oa_green'), color: '#10b981' },
        { key: 'oa_hybrid', label: t('regional.stacked_oa_hybrid'), color: '#8b5cf6' },
        { key: 'oa_bronze', label: t('regional.stacked_oa_bronze'), color: '#d97706' },
        { key: 'oa_closed', label: t('regional.stacked_oa_closed'), color: '#64748b' }
      ];
      oaTypes.forEach(tType => {
        stackedTraces.push({
          x: stackedData.map(d => d[tType.key]),
          y: stackedData.map(d => d.country_name),
          name: tType.label,
          type: 'bar',
          orientation: 'h',
          marker: { color: tType.color },
          hovertemplate: `<b>%{y}</b>: %{x}% ${tType.label}<extra></extra>`
        });
      });
    } else {
      const langTypes = [
        { key: 'lang_es', label: t('regional.stacked_lang_es'), color: '#0284c7' },
        { key: 'lang_pt', label: t('regional.stacked_lang_pt'), color: '#10b981' },
        { key: 'lang_en', label: t('regional.stacked_lang_en'), color: '#f59e0b' },
        { key: 'lang_other', label: t('regional.stacked_lang_other'), color: '#94a3b8' }
      ];
      langTypes.forEach(tType => {
        stackedTraces.push({
          x: stackedData.map(d => d[tType.key]),
          y: stackedData.map(d => d.country_name),
          name: tType.label,
          type: 'bar',
          orientation: 'h',
          marker: { color: tType.color },
          hovertemplate: `<b>%{y}</b>: %{x}% ${tType.label}<extra></extra>`
        });
      });
    }
  }

  // Stream Graph Traces
  const streamTraces = [];
  if (streamData.length > 0) {
    const domains = [
      { id: 'Health Sciences', label: t('regional.domain_health'), color: '#0284c7' },
      { id: 'Social Sciences', label: t('regional.domain_social'), color: '#10b981' },
      { id: 'Physical Sciences', label: t('regional.domain_physical'), color: '#f59e0b' },
      { id: 'Life Sciences', label: t('regional.domain_life'), color: '#8b5cf6' }
    ];
    domains.forEach((dom) => {
      if (streamData[0] && dom.id in streamData[0]) {
        streamTraces.push({
          x: streamData.map(d => d.year),
          y: streamData.map(d => d[dom.id]),
          name: dom.label,
          type: 'scatter',
          mode: 'lines',
          stackgroup: 'one',
          line: { shape: 'spline', color: dom.color },
          hovertemplate: `<b>${dom.label}</b> (%{x}): %{y:,.0f} ${t('regional.stream_articles_hover')}<extra></extra>`
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
    hovertemplate: `<b>%{y}</b><br>${t('regional.diverging_value')}: %{customdata[0]:.2f}<br>${t('regional.diverging_baseline')}: %{customdata[1]:.2f}<br>${t('regional.diverging_deviation')}: %{x:+.2f}<extra></extra>`,
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
        <h2 style={{ fontSize: '24px', fontWeight: '800' }}>{t('regional.title')}</h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '4px' }}>
          {t('regional.subtitle')}
        </p>
      </div>

      {/* Top Contextual KPI Cards (Without duplicate FWCI) */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px' }}>
        <KpiCard
          title={t('kpi.journals')}
          value={kpis?.num_journals?.toLocaleString()}
          subtitle={t('kpi.journals_sub')}
          icon={BookOpen}
        />
        <KpiCard
          title={t('kpi.works')}
          value={kpis?.total_works?.toLocaleString() || periods?.full_period?.num_documents?.toLocaleString()}
          subtitle={t('kpi.works_sub')}
          icon={FileText}
        />
        <KpiCard
          title={t('kpi.diamond')}
          value={`${kpis?.pct_oa_diamond || periods?.full_period?.pct_oa_diamond || 67.0}%`}
          subtitle={t('kpi.diamond_sub')}
          icon={Sparkles}
          badge={t('common.diamond')}
        />
        <KpiCard
          title={t('kpi.doaj_seal')}
          value={`${kpis?.pct_doaj || 34.2}%`}
          subtitle={t('regional.kpi_doaj_sub')}
          icon={ShieldCheck}
          badge="DOAJ"
        />
        <KpiCard
          title={t('kpi.oa_total')}
          value={`${Number(kpis?.pct_oa_total || 92.1).toFixed(2)}%`}
          subtitle={t('regional.kpi_oa_total_sub')}
          icon={Globe2}
        />
      </div>

      {/* ── CONSOLIDATED PERFORMANCE INDICATORS PANEL ── */}
      {(() => {
        const fullP = periods?.full_period || {};
        const recP  = periods?.recent_period || {};

        const fullDocs = fullP.num_documents ?? fullP.works_count ?? 3631792;
        const recDocs  = recP.num_documents ?? recP.works_count ?? 1134887;

        const fullFwci = fullP.fwci_avg != null ? Number(fullP.fwci_avg) : 0.56;
        const recFwci  = recP.fwci_avg != null ? Number(recP.fwci_avg) : 0.66;
        const fwciDelta = (recFwci - fullFwci).toFixed(2);

        const fullTop10 = fullP.pct_top_10 != null ? Number(fullP.pct_top_10) : 0.370;
        const recTop10  = recP.pct_top_10 != null ? Number(recP.pct_top_10) : 0.360;

        const fullTop1 = fullP.pct_top_1 != null ? Number(fullP.pct_top_1) : 0.010;
        const recTop1  = recP.pct_top_1 != null ? Number(recP.pct_top_1) : 0.010;

        const fullPerc = fullP.avg_percentile != null ? (Number(fullP.avg_percentile) <= 1.0 ? Number(fullP.avg_percentile) * 100 : Number(fullP.avg_percentile)) : 39.0;
        const recPerc  = recP.avg_percentile != null ? (Number(recP.avg_percentile) <= 1.0 ? Number(recP.avg_percentile) * 100 : Number(recP.avg_percentile)) : 40.0;
        const percDelta = (recPerc - fullPerc).toFixed(1);

        return (
          <div className="card" style={{
            background: 'var(--bg-card)',
            border: '1.5px solid var(--accent-primary)',
            borderRadius: '14px',
            padding: '20px 24px',
            boxShadow: '0 4px 20px rgba(2, 132, 199, 0.08)'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '8px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <div style={{
                  width: '32px',
                  height: '32px',
                  borderRadius: '8px',
                  background: 'rgba(2, 132, 199, 0.12)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: 'var(--accent-primary)'
                }}>
                  <Activity size={18} />
                </div>
                <div>
                  <h3 style={{ fontSize: '16px', fontWeight: '800', margin: 0, color: 'var(--text-main)' }}>
                    {t('regional.performance_indicators')}
                  </h3>
                  <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                    {t('regional.comparative_periods_sub')}
                  </span>
                </div>
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '16px' }}>
              {/* Periodo Completo */}
              <div style={{
                background: 'var(--bg-input)',
                border: '1px solid var(--border-color)',
                borderRadius: '10px',
                padding: '16px 18px',
                display: 'flex',
                flexDirection: 'column',
                gap: '14px'
              }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '8px' }}>
                  <span style={{ fontSize: '13.5px', fontWeight: '800', color: 'var(--text-main)' }}>
                    {t('regional.period_full')}: 0–2026
                  </span>
                  <span className="badge" style={{ fontSize: '11px', background: 'var(--bg-card)' }}>{t('regional.kpi_historical')}</span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: '12px' }}>
                  <div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_docs')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '3px' }}>
                      {fullDocs.toLocaleString()}
                    </div>
                  </div>

                  <div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_fwci')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--accent-primary)', marginTop: '3px' }}>
                      {fullFwci.toFixed(2)}
                    </div>
                  </div>

                  <div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_top10')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '3px' }}>
                      {fullTop10.toFixed(3)}%
                    </div>
                  </div>

                  <div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_top1')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '3px' }}>
                      {fullTop1.toFixed(3)}%
                    </div>
                  </div>

                  <div style={{ gridColumn: 'span 2' }}>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_percentile')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '3px' }}>
                      {fullPerc.toFixed(1)}
                    </div>
                  </div>
                </div>
              </div>

              {/* Periodo Reciente */}
              <div style={{
                background: 'var(--bg-input)',
                border: '1.5px solid rgba(16, 185, 129, 0.4)',
                borderRadius: '10px',
                padding: '16px 18px',
                display: 'flex',
                flexDirection: 'column',
                gap: '14px'
              }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '8px' }}>
                  <span style={{ fontSize: '13.5px', fontWeight: '800', color: 'var(--accent-success)' }}>
                    {t('regional.period_recent_label')}: 2021–2025
                  </span>
                  <span className="badge" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981', border: '1px solid #10b981', fontSize: '11px' }}>
                    {t('regional.recent')}
                  </span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: '12px' }}>
                  <div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_docs')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '3px' }}>
                      {recDocs.toLocaleString()}
                    </div>
                  </div>

                  <div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_fwci')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--accent-success)', marginTop: '3px', display: 'flex', alignItems: 'center', gap: '6px' }}>
                      {recFwci.toFixed(2)}
                      <span style={{ fontSize: '11px', color: 'var(--accent-success)', fontWeight: '700' }}>
                        ({Number(fwciDelta) >= 0 ? '+' : ''}{fwciDelta})
                      </span>
                    </div>
                  </div>

                  <div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_top10')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '3px' }}>
                      {recTop10.toFixed(3)}%
                    </div>
                  </div>

                  <div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_top1')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '3px' }}>
                      {recTop1.toFixed(3)}%
                    </div>
                  </div>

                  <div style={{ gridColumn: 'span 2' }}>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('regional.kpi_percentile')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--accent-success)', marginTop: '3px', display: 'flex', alignItems: 'center', gap: '6px' }}>
                      {recPerc.toFixed(1)}
                      <span style={{ fontSize: '11px', color: 'var(--accent-success)', fontWeight: '700' }}>
                        ({Number(percDelta) >= 0 ? '+' : ''}{percDelta})
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        );
      })()}

      {/* Map Section */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>{t('regional.map_section_title')}</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('regional.map_section_desc')}
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
              <PlusCircle size={13} /> {t('common.save')}
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
                {t('regional.dumbbell_chart_title')}
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('regional.dumbbell_chart_desc')}
            </span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${selectedDumbbellInd === 'fwci' ? 'active' : ''}`}
              onClick={() => setSelectedDumbbellInd('fwci')}
            >
              {t('regional.dumbbell_fwci')}
            </button>
            <button
              className={`segmented-pill-btn ${selectedDumbbellInd === 'diamond' ? 'active' : ''}`}
              onClick={() => setSelectedDumbbellInd('diamond')}
            >
              {t('regional.dumbbell_diamond')}
            </button>
            <button
              className={`segmented-pill-btn ${selectedDumbbellInd === 'top10' ? 'active' : ''}`}
              onClick={() => setSelectedDumbbellInd('top10')}
            >
              {t('regional.dumbbell_top10')}
            </button>
            <button
              className={`segmented-pill-btn ${selectedDumbbellInd === 'english' ? 'active' : ''}`}
              onClick={() => setSelectedDumbbellInd('english')}
            >
              {t('regional.dumbbell_english')}
            </button>
          </div>
        </div>

        <PlotlyChart
          data={dumbbellTraces}
          layout={{
            height: 540,
            margin: { l: 140, r: 20, t: 20, b: 40 },
            xaxis: {
              title: selectedDumbbellInd === 'fwci' ? t('regional.dumbbell_xaxis_fwci') : t('regional.dumbbell_xaxis_pct'),
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
                {t('regional.stacked_title')}
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('regional.stacked_desc')}
            </span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${stackedMode === 'oa' ? 'active' : ''}`}
              onClick={() => setStackedMode('oa')}
            >
              {t('regional.stacked_mode_oa')}
            </button>
            <button
              className={`segmented-pill-btn ${stackedMode === 'lang' ? 'active' : ''}`}
              onClick={() => setStackedMode('lang')}
            >
              {t('regional.stacked_mode_lang')}
            </button>
          </div>
        </div>

        <PlotlyChart
          data={stackedTraces}
          layout={{
            barmode: 'stack',
            height: 520,
            margin: { l: 140, r: 20, t: 20, b: 40 },
            xaxis: { title: t('regional.stacked_xaxis'), range: [0, 100] },
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
            {t('regional.stream_title')}
          </h3>
        </div>
        <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          {t('regional.stream_desc')}
        </p>

        <PlotlyChart
          data={streamTraces}
          layout={{
            height: 380,
            margin: { l: 50, r: 20, t: 20, b: 40 },
            xaxis: { title: t('regional.stream_xaxis') },
            yaxis: { title: t('regional.stream_yaxis') },
            legend: { orientation: 'h', y: 1.1, x: 0.15 }
          }}
        />
      </div>

      {/* EVOLUCIÓN HISTÓRICA DE PERFILES DE CONOCIMIENTO (TABLAS DOMINIO/CAMPO/SUBCAMPO/TÓPICO) */}
      <ThematicEvolutionTable />

      {/* THEMATIC HIERARCHY: SUNBURST & TREEMAP SWITCHER */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Layers3 size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
                {t('regional.sunburst_title')}
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('regional.sunburst_desc')}
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${thematicViewType === 'sunburst' ? 'active' : ''}`}
                onClick={() => setThematicViewType('sunburst')}
              >
                {t('regional.view_sunburst')}
              </button>
              <button
                className={`segmented-pill-btn ${thematicViewType === 'treemap' ? 'active' : ''}`}
                onClick={() => setThematicViewType('treemap')}
              >
                {t('regional.view_treemap')}
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
              <span>{t('tables.no_classification')}</span>
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
              {t('regional.diverging_title')}
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('regional.diverging_desc')}
            </span>
          </div>

          <select
            value={divergingInd}
            onChange={(e) => setDivergingInd(e.target.value)}
            style={{ fontWeight: '600' }}
          >
            <option value="fwci_avg">{t('regional.map_fwci')}</option>
            <option value="pct_oa_diamond">{t('regional.map_oa_diamond')}</option>
            <option value="pct_top_10">{t('regional.map_top10')}</option>
            <option value="pct_lang_en">{t('regional.map_lang_en')}</option>
          </select>
        </div>

        <PlotlyChart
          data={divergingTrace}
          layout={{
            height: 500,
            margin: { l: 140, r: 20, t: 20, b: 40 },
            xaxis: { title: t('regional.diverging_xaxis'), zeroline: true, zerolinewidth: 2, zerolinecolor: '#334155' },
            yaxis: { autorange: 'reversed' }
          }}
        />
      </div>

      {/* Thematic Profiles Table */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>{t('regional.thematic_profiles_title')}</h3>
          
          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${thematicLevel === 'domain' ? 'active' : ''}`}
              onClick={() => setThematicLevel('domain')}
            >
              {t('thematic_table.domain')}
            </button>
            <button
              className={`segmented-pill-btn ${thematicLevel === 'field' ? 'active' : ''}`}
              onClick={() => setThematicLevel('field')}
            >
              {t('thematic_table.field')}
            </button>
            <button
              className={`segmented-pill-btn ${thematicLevel === 'subfield' ? 'active' : ''}`}
              onClick={() => setThematicLevel('subfield')}
            >
              {t('thematic_table.subfield')}
            </button>
          </div>
        </div>

        {thematicProfiles && thematicProfiles.data.length > 0 && (
          <div className="data-table-container" style={{ maxHeight: '380px' }}>
            <table className="data-table">
              <thead>
                <tr>
                  {thematicProfiles.columns.map(col => {
                    let colLabel = col;
                    if (col === 'domain') colLabel = t('thematic_table.domain');
                    else if (col === 'field') colLabel = t('thematic_table.field');
                    else if (col === 'subfield') colLabel = t('thematic_table.subfield');
                    else if (col === 'Total Región LATAM') colLabel = t('regional.total_latam_region');
                    return <th key={col}>{colLabel}</th>;
                  })}
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
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>{t('regional.annual_section_title')}</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{t('regional.annual_section_desc')}</span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            {/* Year Range Picker */}
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${annualMinYear === 1970 ? 'active' : ''}`}
                onClick={() => setAnnualMinYear(1970)}
              >
                1970–2026
              </button>
              <button
                className={`segmented-pill-btn ${annualMinYear === 1990 ? 'active' : ''}`}
                onClick={() => setAnnualMinYear(1990)}
              >
                1990–2026
              </button>
              <button
                className={`segmented-pill-btn ${annualMinYear === 2000 ? 'active' : ''}`}
                onClick={() => setAnnualMinYear(2000)}
              >
                2000–2026
              </button>
              <button
                className={`segmented-pill-btn ${annualMinYear === 2010 ? 'active' : ''}`}
                onClick={() => setAnnualMinYear(2010)}
              >
                2010–2026
              </button>
            </div>

            {/* Smoothing Picker */}
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${annualWindow === 0 ? 'active' : ''}`}
                onClick={() => setAnnualWindow(0)}
              >
                {t('regional.raw_label')}
              </button>
              <button
                className={`segmented-pill-btn ${annualWindow === 3 ? 'active' : ''}`}
                onClick={() => setAnnualWindow(3)}
              >
                w=3
              </button>
              <button
                className={`segmented-pill-btn ${annualWindow === 5 ? 'active' : ''}`}
                onClick={() => setAnnualWindow(5)}
              >
                w=5
              </button>
            </div>
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
                name: t('regional.kpi_docs'),
                line: { color: '#0284c7', width: 2 }
              }
            ]}
            layout={{ title: t('regional.annual_docs_title'), height: 300 }}
          />

          <PlotlyChart
            data={[
              {
                x: annualTrends.map(d => d.year),
                y: annualTrends.map(d => d.fwci_avg),
                type: 'scatter',
                mode: 'lines+markers',
                name: t('regional.kpi_fwci'),
                line: { color: '#10b981', width: 2 }
              },
              {
                x: annualTrends.map(d => d.year),
                y: annualTrends.map(() => 1.0),
                type: 'scatter',
                mode: 'lines',
                name: t('regional.annual_world_avg'),
                line: { color: '#ef4444', dash: 'dash' }
              }
            ]}
            layout={{ title: t('regional.annual_fwci_title'), height: 300 }}
          />
        </div>
      </div>

      {/* TABLA DE DATOS ANUALES DE LATINOAMÉRICA (1970–2026) */}
      <AnnualDataTable data={annualTrends} />

      {/* 1. Espacio UMAP Multidimensional de Países Reciente (umap_countries_recent) */}
      {umapCountries && umapCountries.length > 0 && (
        <UmapTrajectoryViewer
          title={t('regional.umap_countries_title')}
          subtitle={t('regional.umap_countries_desc')}
          points={umapCountries}
          allowTrajectoryFilter={false}
          showGridSection={true}
          height={460}
        />
      )}

      {/* 2. Global Trajectories in UMAP (2000-2025) */}
      {trajectories && Object.keys(trajectories).length > 0 && (
        <UmapTrajectoryViewer
          title={t('regional.umap_trajectories_title')}
          subtitle={t('regional.umap_trajectories_desc')}
          trajectories={trajectories}
          allowTrajectoryFilter={true}
          showGridSection={true}
          height={520}
          initialActiveEntities={['MX', 'LATAM']}
        />
      )}

      {/* Country Rankings Table */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>{t('regional.ranking_section_title')}</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{t('regional.ranking_section_desc')}</span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${rankingsPeriod === 'full' ? 'active' : ''}`}
                onClick={() => setRankingsPeriod('full')}
              >
                {t('regional.rankings_period_full')}
              </button>
              <button
                className={`segmented-pill-btn ${rankingsPeriod === 'recent' ? 'active' : ''}`}
                onClick={() => setRankingsPeriod('recent')}
              >
                {t('regional.rankings_period_recent')}
              </button>
            </div>

            {/* CSV Download Button */}
            <button
              className="btn-secondary"
              onClick={() => {
                if (!rankings || rankings.length === 0) return;
                const headers = ['Codigo', 'Pais', 'Revistas', 'Documentos', 'FWCI', 'Pct_Top_10', 'Pct_Top_1', 'Pct_OA_Diamante', 'Pct_OA_Gold', 'Pct_Espanol', 'Pct_Ingles'];
                const rows = rankings.map(r => [
                  r.country_code,
                  `"${((r.country_code && t(`country_names.${r.country_code}`) !== `country_names.${r.country_code}`) ? t(`country_names.${r.country_code}`) : (r.country_name || '')).replace(/"/g, '""')}"`,
                  r.num_journals || 0,
                  r.num_documents || 0,
                  Number(r.fwci_avg || 0).toFixed(2),
                  Number(r.pct_top_10 || 0).toFixed(3),
                  Number(r.pct_top_1 || 0).toFixed(3),
                  Number(r.pct_oa_diamond || 0).toFixed(1),
                  Number(r.pct_oa_gold || 0).toFixed(1),
                  Number(r.pct_lang_es || 0).toFixed(1),
                  Number(r.pct_lang_en || 0).toFixed(1)
                ]);
                const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [headers.join(','), ...rows.map(e => e.join(','))].join('\n');
                const encodedUri = encodeURI(csvContent);
                const link = document.createElement('a');
                link.setAttribute('href', encodedUri);
                link.setAttribute('download', `ranking_paises_${rankingsPeriod}_${new Date().toISOString().slice(0, 10)}.csv`);
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
              }}
              style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', padding: '6px 12px' }}
              title={t('regional.download_csv_tooltip')}
            >
              <Download size={14} /> {t('regional.download_csv')}
            </button>
          </div>
        </div>


        <div className="data-table-container" style={{ maxHeight: '420px' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>{t('tables.code')}</th>
                <th>{t('tables.country')}</th>
                <th>{t('tables.journals')}</th>
                <th>{t('tables.documents')}</th>
                <th>{t('tables.fwci')}</th>
                <th>{t('tables.top10')}</th>
                <th>{t('tables.top1')}</th>
                <th>{t('tables.oa_diamond')}</th>
                <th>{t('tables.oa_gold')}</th>
                <th>{t('tables.lang_es')}</th>
                <th>{t('tables.lang_en')}</th>
              </tr>
            </thead>
            <tbody>
              {rankings.map((r, idx) => (
                <tr key={idx}>
                  <td><strong>{r.country_code}</strong></td>
                  <td>{(r.country_code && t(`country_names.${r.country_code}`) !== `country_names.${r.country_code}`) ? t(`country_names.${r.country_code}`) : r.country_name}</td>
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
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>{t('regional.scatter_section_title')}</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{t('regional.scatter_section_desc')}</span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700' }}>{t('regional.axis_x')}</span>
              <select value={scatterX} onChange={(e) => setScatterX(e.target.value)}>
                {SCATTER_INDICATORS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
              </select>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700' }}>{t('regional.axis_y')}</span>
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

      {/* ── EXPANDER DE DOSSIER DE ESTUDIO Y ENVÍO A CHATGPT (PIE DE PÁGINA) ── */}
      <PageDossierExpander
        pageTitle={t('regional.page_title_dossier')}
        pageDescription={t('regional.page_desc_dossier')}
        sections={[
          {
            id: 'kpis_macro',
            title: t('regional.dossier_sec1_title'),
            category: t('regional.dossier_sec1_cat'),
            defaultChecked: true,
            rawData: kpis,
            buildDataText: () => {
              if (!kpis) return 'No hay datos de KPIs disponibles.';
              return [
                '| Métrica Regional | Valor Global | Contexto |',
                '|---|---|---|',
                `| Revistas Activas Indexadas | ${kpis.num_journals?.toLocaleString() || 0} | Cobertura total OpenAlex |`,
                `| Producción Total de Artículos | ${kpis.total_works?.toLocaleString() || 0} | Registro histórico acumulado |`,
                `| FWCI Ponderado Promedio | ${kpis.fwci_avg ?? '—'} | Impacto normalizado mundial (Base=1.0) |`,
                `| % Acceso Abierto Diamante | ${kpis.pct_oa_diamond || 0}% | Sin cobro por procesamiento de artículo (APC) |`,
                `| % Revistas en DOAJ | ${kpis.pct_doaj || 0}% | Publicaciones con sello de calidad abierta |`,
                `| % Artículos en Idioma Inglés | ${kpis.pct_lang_en || 0}% | Nivel de internacionalización lingüística |`,
                `| % Artículos en Top 10% Más Citados | ${kpis.pct_top_10 || 0}% | Excelencia e impacto de citación |`
              ].join('\n');
            }
          },
          {
            id: 'periods_comparison',
            title: t('regional.dossier_sec2_title'),
            category: t('regional.dossier_sec2_cat'),
            defaultChecked: true,
            rawData: periods,
            buildDataText: () => {
              if (!periods) return 'No hay datos de periodos disponibles.';
              const f = periods.full_period || periods.full || {};
              const r = periods.recent_period || periods.recent || {};
              const fullDocs = f.num_documents ?? f.works_count ?? 0;
              const recDocs = r.num_documents ?? r.works_count ?? 0;
              const fullFwci = f.fwci_avg != null ? Number(f.fwci_avg) : 0;
              const recFwci = r.fwci_avg != null ? Number(r.fwci_avg) : 0;
              const fwciDelta = (recFwci - fullFwci).toFixed(2);
              const fullTop10 = f.pct_top_10 != null ? Number(f.pct_top_10) : 0;
              const recTop10 = r.pct_top_10 != null ? Number(r.pct_top_10) : 0;
              const top10Delta = (recTop10 - fullTop10).toFixed(3);
              const fullTop1 = f.pct_top_1 != null ? Number(f.pct_top_1) : 0;
              const recTop1 = r.pct_top_1 != null ? Number(r.pct_top_1) : 0;
              const fullPerc = f.avg_percentile != null ? (Number(f.avg_percentile) <= 1.0 ? Number(f.avg_percentile) * 100 : Number(f.avg_percentile)) : 0;
              const recPerc = r.avg_percentile != null ? (Number(r.avg_percentile) <= 1.0 ? Number(r.avg_percentile) * 100 : Number(r.avg_percentile)) : 0;
              const percDelta = (recPerc - fullPerc).toFixed(1);

              return [
                '| Indicador de Desempeño | Periodo Completo (0–2026) | Periodo Reciente (2021–2025) | Variación Neta / Proporción |',
                '|---|---|---|---|',
                `| Documentos Publicados | ${fullDocs.toLocaleString()} | ${recDocs.toLocaleString()} | ${recDocs && fullDocs ? ((recDocs / fullDocs) * 100).toFixed(1) + '% de la producción histórica' : '—'} |`,
                `| FWCI Ponderado Promedio | ${fullFwci.toFixed(2)} | ${recFwci.toFixed(2)} | ${Number(fwciDelta) >= 0 ? '+' : ''}${fwciDelta} |`,
                `| % Artículos en Top 10% Más Citados | ${fullTop10.toFixed(3)}% | ${recTop10.toFixed(3)}% | ${Number(top10Delta) >= 0 ? '+' : ''}${top10Delta}% |`,
                `| % Artículos en Top 1% Más Citados | ${fullTop1.toFixed(3)}% | ${recTop1.toFixed(3)}% | ${(recTop1 - fullTop1).toFixed(3)}% |`,
                `| Percentil Promedio Normalizado | ${fullPerc.toFixed(1)} | ${recPerc.toFixed(1)} | ${Number(percDelta) >= 0 ? '+' : ''}${percDelta} |`,
                `| % Acceso Abierto Diamante | ${Number(f.pct_oa_diamond || 0).toFixed(1)}% | ${Number(r.pct_oa_diamond || 0).toFixed(1)}% | ${(Number(r.pct_oa_diamond || 0) - Number(f.pct_oa_diamond || 0)).toFixed(1)}% |`,
                `| % Acceso Abierto Gold | ${Number(f.pct_oa_gold || 0).toFixed(1)}% | ${Number(r.pct_oa_gold || 0).toFixed(1)}% | ${(Number(r.pct_oa_gold || 0) - Number(f.pct_oa_gold || 0)).toFixed(1)}% |`,
                `| % Artículos en Idioma Inglés | ${Number(f.pct_lang_en || 0).toFixed(1)}% | ${Number(r.pct_lang_en || 0).toFixed(1)}% | ${(Number(r.pct_lang_en || 0) - Number(f.pct_lang_en || 0)).toFixed(1)}% |`
              ].join('\n');
            }
          },
          {
            id: 'thematic_profiles_table',
            title: t('regional.dossier_sec3_title', { level: thematicLevel.toUpperCase() }),
            category: t('regional.dossier_sec3_cat'),
            defaultChecked: false,
            rawData: thematicProfiles,
            buildDataText: () => {
              if (!thematicProfiles || !thematicProfiles.data || thematicProfiles.data.length === 0) return 'No hay datos de perfiles temáticos disponibles.';
              const cols = thematicProfiles.columns || Object.keys(thematicProfiles.data[0] || {});
              const lines = [
                `*Nivel de Agregación Disciplinar:* **${thematicLevel.toUpperCase()}**\n`,
                `| ${cols.join(' | ')} |`,
                `| ${cols.map(() => '---').join(' | ')} |`
              ];
              thematicProfiles.data.slice(0, 20).forEach(row => {
                const vals = cols.map(c => typeof row[c] === 'number' ? row[c].toLocaleString() : (row[c] ?? '—'));
                lines.push(`| ${vals.join(' | ')} |`);
              });
              if (thematicProfiles.data.length > 20) {
                lines.push(`\n_... y ${thematicProfiles.data.length - 20} filas temáticas más._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'distributions_oa_lang',
            title: t('regional.dossier_sec4_title'),
            category: t('regional.dossier_sec4_cat'),
            defaultChecked: false,
            rawData: distributions,
            buildDataText: () => {
              if (!distributions) return 'No hay datos de distribución.';
              const lines = ['**Vías de Acceso Abierto:**\n'];
              lines.push('| Modalidad OA | Artículos | % del Total |');
              lines.push('|---|---|---|');
              (distributions.oa || []).forEach(o => {
                lines.push(`| ${o.oa_status || o.status} | ${o.count?.toLocaleString() || o.num_documents?.toLocaleString() || 0} | ${Number(o.pct || 0).toFixed(1)}% |`);
              });
              lines.push('\n**Idiomas de Publicación:**\n');
              lines.push('| Idioma | Artículos | % del Total |');
              lines.push('|---|---|---|');
              (distributions.languages || []).forEach(l => {
                lines.push(`| ${l.language || l.lang} | ${l.count?.toLocaleString() || l.num_documents?.toLocaleString() || 0} | ${Number(l.pct || 0).toFixed(1)}% |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'map_choropleth',
            title: t('regional.dossier_sec5_title', { ind: MAP_INDICATORS.find(m => m.id === selectedMapIndicator)?.label || selectedMapIndicator }),
            category: t('regional.dossier_sec5_cat'),
            defaultChecked: true,
            rawData: choroplethData,
            buildDataText: () => {
              if (!choroplethData || choroplethData.length === 0) return 'No hay datos cartográficos disponibles.';
              const lines = [
                `*Indicador Cartográfico:* **${MAP_INDICATORS.find(m => m.id === selectedMapIndicator)?.label || selectedMapIndicator}**\n`,
                '| País | Código | Revistas | Artículos | Valor del Indicador |',
                '|---|---|---|---|---|'
              ];
              choroplethData.forEach(c => {
                lines.push(`| ${c.country_name || c.country_code} | ${c.country_code} | ${c.num_journals?.toLocaleString() || 0} | ${c.num_documents?.toLocaleString() || 0} | ${c.actual_value ?? c[selectedMapIndicator] ?? '—'} |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'period_gaps',
            title: t('regional.dossier_sec6_title', { ind: selectedDumbbellInd.toUpperCase() }),
            category: t('regional.dossier_sec6_cat'),
            defaultChecked: false,
            rawData: periodGaps,
            buildDataText: () => {
              if (!periodGaps || periodGaps.length === 0) return 'No hay datos de brechas disponibles.';
              const lines = [
                `*Dimensión Analizada:* **${selectedDumbbellInd.toUpperCase()}**\n`,
                '| País | Valor Histórico | Valor Reciente (2021-2025) | Variación / Brecha |',
                '|---|---|---|---|'
              ];
              periodGaps.forEach(g => {
                const diff = (g.recent_val != null && g.full_val != null) ? (g.recent_val - g.full_val).toFixed(2) : '—';
                lines.push(`| ${g.country_name || g.country_code} | ${g.full_val ?? '—'} | ${g.recent_val ?? '—'} | ${diff > 0 ? '+' + diff : diff} |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'stacked_bars',
            title: t('regional.dossier_sec7_title', { mode: stackedMode === 'oa' ? t('regional.bar_100_oa') : t('regional.bar_100_lang') }),
            category: t('regional.dossier_sec7_cat'),
            defaultChecked: false,
            rawData: stackedData,
            buildDataText: () => {
              const list = Array.isArray(stackedData) ? stackedData : (stackedData?.[stackedMode] || []);
              if (!list || list.length === 0) return 'No hay datos de barras apiladas.';
              const lines = [
                `*Modo:* **${stackedMode === 'oa' ? 'Vías de Acceso Abierto' : 'Idiomas de Publicación'}**\n`,
                '| País | Desglose Porcentual |',
                '|---|---|'
              ];
              list.slice(0, 15).forEach(item => {
                const parts = Object.keys(item)
                  .filter(k => k !== 'country_code' && k !== 'country_name')
                  .map(k => `${k}: ${Number(item[k] || 0).toFixed(1)}%`)
                  .join(' · ');
                lines.push(`| ${item.country_name || item.country_code} | ${parts || '—'} |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'stream_graph',
            title: t('regional.dossier_sec8_title'),
            category: t('regional.dossier_sec8_cat'),
            defaultChecked: false,
            rawData: streamData,
            buildDataText: () => {
              if (!streamData || streamData.length === 0) return 'No hay datos de evolución temática.';
              const lines = [
                '| Año | Dominio / Gran Área | Volumen de Artículos | % del Año |',
                '|---|---|---|---|'
              ];
              streamData.slice(-20).forEach(s => {
                lines.push(`| ${s.year} | ${s.domain_name || s.field_name || s.topic || s.name || 'Área'} | ${s.works_count?.toLocaleString() || s.count?.toLocaleString() || 0} | ${Number(s.share || s.pct || 0).toFixed(1)}% |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'thematic_hierarchy',
            title: t('regional.dossier_sec9_title', { type: thematicViewType === 'sunburst' ? 'Sunburst Radial' : 'Treemap' }),
            category: t('regional.dossier_sec9_cat'),
            defaultChecked: false,
            rawData: thematicViewType === 'sunburst' ? sunburstData : treemapData,
            buildDataText: () => {
              const data = thematicViewType === 'sunburst' ? sunburstData : treemapData;
              const nodes = data?.nodes || (Array.isArray(data) ? data : []);
              if (!nodes || nodes.length === 0) return 'No hay datos de estructura temática.';
              const lines = [
                `*Visualización:* **${thematicViewType === 'sunburst' ? 'Sunburst' : 'Treemap'}** | *Indicador:* **${selectedSunburstInd}**\n`,
                '| Nivel / Nodo | Padre | Valor / Tamaño | Indicador |',
                '|---|---|---|---|'
              ];
              nodes.slice(0, 20).forEach(n => {
                lines.push(`| ${n.name || n.label || n.id} | ${n.parent || 'Raíz'} | ${n.value?.toLocaleString() || 0} | ${n.color_metric != null ? Number(n.color_metric).toFixed(2) : '—'} |`);
              });
              if (nodes.length > 20) {
                lines.push(`\n_... y ${nodes.length - 20} ramas temáticas más._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'diverging_deviations',
            title: t('regional.dossier_sec10_title', { ind: divergingInd }),
            category: t('regional.dossier_sec10_cat'),
            defaultChecked: false,
            rawData: divergingData,
            buildDataText: () => {
              if (!divergingData || divergingData.length === 0) return 'No hay datos de desviación disponibles.';
              const lines = [
                `*Indicador Base:* **${divergingInd}**\n`,
                '| País | Valor Real | Línea Base (Media) | Desviación Neta |',
                '|---|---|---|---|'
              ];
              divergingData.forEach(d => {
                lines.push(`| ${d.country_name || d.country_code} | ${Number(d.actual_value || 0).toFixed(2)} | ${Number(d.baseline || 0).toFixed(2)} | ${Number(d.deviation || 0) > 0 ? '+' : ''}${Number(d.deviation || 0).toFixed(2)} |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'annual_trends',
            title: t('regional.dossier_sec11_title'),
            category: t('regional.dossier_sec11_cat'),
            defaultChecked: false,
            rawData: annualTrends,
            buildDataText: () => {
              if (!annualTrends || annualTrends.length === 0) return 'No hay series temporales disponibles.';
              const lines = [
                '| Año | Artículos Publicados | FWCI Ponderado | % OA Diamante | % Idioma Inglés |',
                '|---|---|---|---|---|'
              ];
              annualTrends.slice(-15).forEach(a => {
                lines.push(`| ${a.year} | ${a.works_count?.toLocaleString() || a.num_documents?.toLocaleString() || 0} | ${Number(a.fwci_avg || 0).toFixed(2)} | ${Number(a.pct_oa_diamond || 0).toFixed(1)}% | ${Number(a.pct_lang_en || 0).toFixed(1)}% |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'umap_countries',
            title: t('regional.dossier_sec12_title'),
            category: t('regional.dossier_sec12_cat'),
            defaultChecked: false,
            rawData: umapCountries,
            buildDataText: () => {
              if (!umapCountries || umapCountries.length === 0) return 'No hay coordenadas UMAP de países disponibles.';
              const lines = [
                '| País | UMAP-1 | UMAP-2 | FWCI | % OA Diamante | % Top 10% | % Inglés |',
                '|---|---|---|---|---|---|---|'
              ];
              umapCountries.forEach(u => {
                lines.push(`| ${u.country_name || u.country_code} | ${Number(u.umap_x || u.x || 0).toFixed(2)} | ${Number(u.umap_y || u.y || 0).toFixed(2)} | ${Number(u.fwci_avg || 0).toFixed(2)} | ${Number(u.pct_oa_diamond || 0).toFixed(1)}% | ${Number(u.pct_top_10 || 0).toFixed(1)}% | ${Number(u.pct_lang_en || 0).toFixed(1)}% |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'umap_trajectories',
            title: t('regional.dossier_sec13_title'),
            category: t('regional.dossier_sec13_cat'),
            defaultChecked: false,
            rawData: trajectories,
            buildDataText: () => {
              if (!trajectories || Object.keys(trajectories).length === 0) return 'No hay trayectorias disponibles.';
              const lines = [
                '| Entidad / País | Años Registrados | Posición Inicial (2000) | Posición Reciente (2025) |',
                '|---|---|---|---|'
              ];
              Object.keys(trajectories).forEach(k => {
                const ent = trajectories[k];
                const pts = ent.points || [];
                const pStart = pts[0];
                const pEnd = pts[pts.length - 1];
                lines.push(`| ${ent.name || k} | ${pts.length} años | (${Number(pStart?.x || 0).toFixed(2)}, ${Number(pStart?.y || 0).toFixed(2)}) | (${Number(pEnd?.x || 0).toFixed(2)}, ${Number(pEnd?.y || 0).toFixed(2)}) |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'rankings_table',
            title: t('regional.dossier_sec14_title', { period: rankingsPeriod === 'full' ? t('regional.rankings_period_full') : t('regional.rankings_period_recent') }),
            category: t('regional.dossier_sec14_cat'),
            defaultChecked: false,
            rawData: rankings,
            buildDataText: () => {
              if (!rankings || rankings.length === 0) return 'No hay datos de ranking disponibles.';
              const lines = [
                '| Pos | País | Revistas | Artículos | FWCI | % Top 10% | % Diamante | % Inglés |',
                '|---|---|---|---|---|---|---|---|'
              ];
              rankings.slice(0, 20).forEach((r, idx) => {
                lines.push(`| ${idx + 1} | ${r.country_name || r.country_code} | ${r.num_journals?.toLocaleString() || 0} | ${r.num_documents?.toLocaleString() || 0} | ${Number(r.fwci_avg || 0).toFixed(2)} | ${Number(r.pct_top_10 || 0).toFixed(1)}% | ${Number(r.pct_oa_diamond || 0).toFixed(1)}% | ${Number(r.pct_lang_en || 0).toFixed(1)}% |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'scatter_explorer',
            title: t('regional.dossier_sec15_title', { x: SCATTER_INDICATORS.find(s => s.id === scatterX)?.label || scatterX, y: SCATTER_INDICATORS.find(s => s.id === scatterY)?.label || scatterY }),
            category: t('regional.dossier_sec15_cat'),
            defaultChecked: false,
            rawData: scatterData,
            buildDataText: () => {
              if (!scatterData || scatterData.length === 0) return 'No hay datos de scatter disponibles.';
              const xLabel = SCATTER_INDICATORS.find(s => s.id === scatterX)?.label || scatterX;
              const yLabel = SCATTER_INDICATORS.find(s => s.id === scatterY)?.label || scatterY;
              const lines = [
                `*Eje X:* **${xLabel}** | *Eje Y:* **${yLabel}**\n`,
                '| Revista | País | Eje X | Eje Y |',
                '|---|---|---|---|'
              ];
              scatterData.slice(0, 20).forEach(s => {
                lines.push(`| ${s.display_name} | ${s.country_name || s.country_code} | ${s[scatterX] ?? '—'} | ${s[scatterY] ?? '—'} |`);
              });
              if (scatterData.length > 20) {
                lines.push(`\n_... y ${scatterData.length - 20} revistas más en el gráfico de correlación._`);
              }
              return lines.join('\n');
            }
          }
        ]}
      />
    </div>
  );
}
