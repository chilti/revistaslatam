import React, { useState, useEffect, useMemo } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import { useTranslation } from '../i18n';
import KpiCard from '../components/KpiCard';
import PlotlyChart from '../components/PlotlyChart';
import UmapTrajectoryViewer from '../components/UmapTrajectoryViewer';
import PageDossierExpander from '../components/PageDossierExpander';
import ThematicEvolutionTable from '../components/ThematicEvolutionTable';
import CountryThematicProfilesTable from '../components/CountryThematicProfilesTable';
import AnnualDataTable from '../components/AnnualDataTable';
import ConnectionMapViewer from '../components/ConnectionMapViewer';
import { 
  BookOpen, 
  FileText, 
  Zap, 
  Sparkles, 
  Globe2, 
  TrendingUp, 
  ShieldCheck, 
  Compass,
  PlusCircle,
  ExternalLink,
  Grid,
  TrendingDown,
  Layers,
  Download,
  Activity,
  Award,
  ChevronDown,
  ChevronUp,
  Search,
  FileSpreadsheet,
  BarChart2,
  PieChart,
  Share2,
  Check
} from 'lucide-react';

export default function CountryPage() {
  const { selectedCountry, setSelectedCountry, setSelectedJournal, setActiveSection, addDossierItem } = useAppStore();
  const { t } = useTranslation();
  
  const [countriesList, setCountriesList] = useState([]);
  const [copiedLink, setCopiedLink] = useState(false);

  const handleShareCountry = () => {
    const url = `${window.location.origin}${window.location.pathname}?section=country&country=${encodeURIComponent(selectedCountry)}`;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(url).then(() => {
        setCopiedLink(true);
        setTimeout(() => setCopiedLink(false), 2500);
      });
    } else {
      const textarea = document.createElement('textarea');
      textarea.value = url;
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
      setCopiedLink(true);
      setTimeout(() => setCopiedLink(false), 2500);
    }
  };

  const [summary, setSummary] = useState(null);
  const [annualTrends, setAnnualTrends] = useState([]);
  const [annualWindow, setAnnualWindow] = useState(0);
  const [thematicViewType, setThematicViewType] = useState('sunburst'); // 'sunburst' | 'treemap'
  const [countrySunburst, setCountrySunburst] = useState(null);
  const [countryTreemap, setCountryTreemap] = useState(null);
  const [sunburstIndicator, setSunburstIndicator] = useState('fwci_avg_recent');
  const [sunburstUnclassified, setSunburstUnclassified] = useState(true);
  const [journals, setJournals] = useState([]);
  const [trajectory, setTrajectory] = useState({});
  const [umapJournals, setUmapJournals] = useState([]);
  const [landscapeArticles, setLandscapeArticles] = useState({ country_articles: [], bg_articles: [] });
  const [globalBgArts, setGlobalBgArts] = useState([]);
  const [umapTableOpen, setUmapTableOpen] = useState(false);
  const [umapTableSearch, setUmapTableSearch] = useState('');
  const [scatterPeriod, setScatterPeriod] = useState('recent'); // 'recent' | 'full'
  const [scatterDataList, setScatterDataList] = useState([]);
  const [dynScatterX, setDynScatterX] = useState('num_documents');
  const [dynScatterY, setDynScatterY] = useState('fwci_avg');
  const [piePeriod, setPiePeriod] = useState('full'); // 'full' | 'recent'

  // Specialization Matrix (RCA)
  const [rcaData, setRcaData] = useState(null);
  const [rcaLevel, setRcaLevel] = useState('domain'); // 'domain' | 'field'

  // Slope Data
  const [slopeData, setSlopeData] = useState([]);

  // Beeswarm / Strip plot
  const [journalsDist, setJournalsDist] = useState([]);
  const [beeswarmMetric, setBeeswarmMetric] = useState('fwci_avg');
  
  const [scatterX, setScatterX] = useState('works_count');
  const [scatterY, setScatterY] = useState('fwci_avg');
  
  const [collabData, setCollabData] = useState(null);
  const [collabLoading, setCollabLoading] = useState(false);
  
  const [loading, setLoading] = useState(true);

  const localizedCountry = t(`country_names.${selectedCountry}`);
  const countryDisplayName = (localizedCountry && localizedCountry !== `country_names.${selectedCountry}`)
    ? localizedCountry
    : (summary?.country_name || selectedCountry);

  const DYNAMIC_SCATTER_INDICATORS = React.useMemo(() => [
    { id: 'num_documents', label: t('country.scatter_ind_docs') },
    { id: 'fwci_avg', label: t('country.scatter_ind_fwci') },
    { id: 'pct_top_10', label: t('country.scatter_ind_top10') },
    { id: 'pct_top_1', label: t('country.scatter_ind_top1') },
    { id: 'avg_percentile', label: t('country.scatter_ind_percentile') },
    { id: 'pct_oa_total', label: t('country.scatter_ind_oa_total') },
    { id: 'pct_oa_diamond', label: t('country.scatter_ind_oa_diamond') },
    { id: 'pct_oa_gold', label: t('country.scatter_ind_oa_gold') },
    { id: 'pct_oa_green', label: t('country.scatter_ind_oa_green') },
    { id: 'pct_oa_hybrid', label: t('country.scatter_ind_oa_hybrid') },
    { id: 'pct_oa_bronze', label: t('country.scatter_ind_oa_bronze') },
    { id: 'pct_oa_closed', label: t('country.scatter_ind_oa_closed') },
    { id: 'pct_authors_domestic', label: t('country.scatter_ind_domestic_authors') },
    { id: 'pct_lang_es', label: t('country.scatter_ind_lang_es') },
    { id: 'pct_lang_en', label: t('country.scatter_ind_lang_en') },
    { id: 'pct_lang_pt', label: t('country.scatter_ind_lang_pt') },
    { id: 'pct_lang_fr', label: t('country.scatter_ind_lang_fr') },
    { id: 'pct_lang_de', label: t('country.scatter_ind_lang_de') },
    { id: 'pct_lang_it', label: t('country.scatter_ind_lang_it') },
    { id: 'pct_lang_other', label: t('country.scatter_ind_lang_other') },
    { id: 'cited_by_count', label: t('country.scatter_ind_citations') },
    { id: 'h_index', label: t('country.scatter_ind_h_index') },
    { id: 'i10_index', label: t('country.scatter_ind_i10_index') },
    { id: 'citedness_2yr', label: t('country.scatter_ind_citedness_2yr') },
    { id: 'pagerank', label: t('country.scatter_ind_pagerank') },
    { id: 'eigenfactor', label: t('country.scatter_ind_eigenfactor') }
  ], [t]);


  // Load countries catalog
  useEffect(() => {
    api.get('/countries').then(res => setCountriesList(res.data)).catch(console.error);
  }, []);

  // Load RCA Matrix
  useEffect(() => {
    api.get(`/countries/specialization-matrix?level=${rcaLevel}`)
      .then(res => setRcaData(res.data))
      .catch(console.error);
  }, [rcaLevel]);

  // Load country details on selectedCountry change
  useEffect(() => {
    if (!selectedCountry) return;
    setLoading(true);

    Promise.all([
      api.get(`/countries/${selectedCountry}/summary`),
      api.get(`/countries/${selectedCountry}/annual?window=${annualWindow}&min_year=1970&max_year=2026`),
      api.get(`/countries/${selectedCountry}/journals`),
      api.get(`/countries/${selectedCountry}/trajectory`),
      api.get(`/countries/${selectedCountry}/umap-journals`),
      api.get(`/countries/${selectedCountry}/landscape?limit=2500`),
      api.get(`/countries/${selectedCountry}/slope-data`),
      api.get(`/countries/${selectedCountry}/journals-distribution`)
    ]).then(([sumRes, annRes, jRes, trajRes, umapJRes, landRes, slopeRes, distRes]) => {
      setSummary(sumRes.data);
      setAnnualTrends(annRes.data);
      setJournals(jRes.data);
      setTrajectory(trajRes.data);
      setUmapJournals(umapJRes.data);
      setLandscapeArticles(landRes.data);
      // If bg_articles is empty, load global background landscape
      if (!landRes.data?.bg_articles?.length) {
        api.get('/maps/articles?limit=5000').then(r => setGlobalBgArts(r.data || [])).catch(() => {});
      }
      setSlopeData(slopeRes.data);
      setJournalsDist(distRes.data);
    }).catch(console.error).finally(() => setLoading(false));
  }, [selectedCountry]);

  // Load country collaboration network
  useEffect(() => {
    if (!selectedCountry) return;
    setCollabLoading(true);
    api.get(`/countries/${selectedCountry}/collaboration`)
      .then(res => setCollabData(res.data))
      .catch(err => {
        console.error('Error fetching country collaboration:', err);
        setCollabData(null);
      })
      .finally(() => setCollabLoading(false));
  }, [selectedCountry]);

  // Load sunburst / treemap
  useEffect(() => {
    if (!selectedCountry) return;
    if (thematicViewType === 'sunburst') {
      api.get(`/countries/${selectedCountry}/sunburst?indicator=${sunburstIndicator}&include_unclassified=${sunburstUnclassified}`)
        .then(res => setCountrySunburst(res.data))
        .catch(console.error);
    } else {
      api.get(`/countries/${selectedCountry}/treemap?indicator=${sunburstIndicator}&include_unclassified=${sunburstUnclassified}`)
        .then(res => setCountryTreemap(res.data))
        .catch(console.error);
    }
  }, [selectedCountry, thematicViewType, sunburstIndicator, sunburstUnclassified]);

  // Reload annual on window change
  useEffect(() => {
    if (!selectedCountry) return;
    api.get(`/countries/${selectedCountry}/annual?window=${annualWindow}&min_year=1970&max_year=2026`)
      .then(res => setAnnualTrends(res.data))
      .catch(console.error);
  }, [annualWindow]);

  // Load dynamic journal scatter data on selectedCountry or scatterPeriod change
  useEffect(() => {
    if (!selectedCountry) return;
    api.get(`/countries/${selectedCountry}/journals-scatter?period=${scatterPeriod}`)
      .then(res => setScatterDataList(Array.isArray(res.data) ? res.data : []))
      .catch(err => {
        console.error('Error loading journals scatter:', err);
        setScatterDataList([]);
      });
  }, [selectedCountry, scatterPeriod]);


  const pData = summary?.full_period || {};
  const recData = summary?.recent_period || {};

  // Sunburst Trace
  const sunburstTrace = (countrySunburst && Array.isArray(countrySunburst.nodes) && countrySunburst.nodes.length > 0) ? [{
    type: 'sunburst',
    ids: countrySunburst.nodes.map(n => n.id),
    labels: countrySunburst.nodes.map(n => (n.label === 'Sin Clasificación' || n.label === 'Unknown') ? t('country.hierarchy_unclassified') : n.label),
    parents: countrySunburst.nodes.map(n => n.parent),
    values: countrySunburst.nodes.map(n => n.value),
    marker: {
      colors: countrySunburst.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true,
      colorbar: { title: t('country.hierarchy_hover_metric') }
    },
    branchvalues: 'total',
    hovertemplate: `<b>%{label}</b><br>${t('country.hierarchy_hover_articles')}: %{value:,.0f}<br>${t('country.hierarchy_hover_metric')}: %{color:.2f}<extra></extra>`
  }] : [];

  // Treemap Trace
  const treemapTrace = (countryTreemap && Array.isArray(countryTreemap.nodes) && countryTreemap.nodes.length > 0) ? [{
    type: 'treemap',
    ids: countryTreemap.nodes.map(n => n.id),
    labels: countryTreemap.nodes.map(n => (n.label === 'Sin Clasificación' || n.label === 'Unknown') ? t('country.hierarchy_unclassified') : n.label),
    parents: countryTreemap.nodes.map(n => n.parent),
    values: countryTreemap.nodes.map(n => n.value),
    marker: {
      colors: countryTreemap.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true,
      colorbar: { title: t('country.hierarchy_hover_metric') }
    },
    branchvalues: 'total',
    hovertemplate: `<b>%{label}</b><br>${t('country.hierarchy_hover_articles')}: %{value:,.0f}<br>${t('country.hierarchy_hover_metric')}: %{color:.2f}<extra></extra>`
  }] : [];

  // Trajectory Trace
  const trajTraces = [];
  Object.keys(trajectory).forEach(k => {
    const item = trajectory[k];
    trajTraces.push({
      x: item.points.map(p => p.x),
      y: item.points.map(p => p.y),
      mode: 'lines+markers+text',
      name: item.name,
      text: item.points.map(p => String(p.year).slice(-2)),
      textposition: 'top center',
      line: { shape: 'spline', width: item.is_ref ? 4 : 2, color: item.is_ref ? '#10b981' : '#0284c7' }
    });
  });

  // Dual-Axis Chart Trace: Volume (Bar) + FWCI (Line)
  const validAnnual = (annualTrends || []).filter(d => d.year >= 1970 && d.year <= 2026);
  const dualAxisTraces = [
    {
      x: validAnnual.map(d => d.year),
      y: validAnnual.map(d => d.num_documents),
      name: t('country.trace_published_docs'),
      type: 'bar',
      marker: { color: 'rgba(2, 132, 199, 0.65)' },
      yaxis: 'y'
    },
    {
      x: validAnnual.map(d => d.year),
      y: validAnnual.map(d => d.fwci_avg),
      name: t('country.trace_fwci_avg'),
      type: 'scatter',
      mode: 'lines+markers',
      line: { color: '#10b981', width: 3 },
      marker: { size: 6, color: '#10b981' },
      yaxis: 'y2'
    },
    {
      x: validAnnual.map(d => d.year),
      y: validAnnual.map(() => 1.0),
      name: t('country.trace_world_avg'),
      type: 'scatter',
      mode: 'lines',
      line: { color: '#ef4444', dash: 'dash', width: 1.5 },
      yaxis: 'y2'
    }
  ];


  // Beeswarm / Strip Plot Traces
  const beeswarmTraces = [{
    type: 'box',
    y: journalsDist.map(d => d[beeswarmMetric]),
    boxpoints: 'all',
    jitter: 0.45,
    pointpos: 0,
    marker: {
      color: '#0284c7',
      size: 7,
      opacity: 0.75
    },
    fillcolor: 'rgba(2, 132, 199, 0.1)',
    line: { color: '#0284c7' },
    text: journalsDist.map(d => `${d.display_name}<br>${t('country.beeswarm_hover_val')}: ${d[beeswarmMetric]}<br>${t('country.beeswarm_hover_docs')}: ${d.works_count}`),
    hoverinfo: 'text+y',
    name: countryDisplayName
  }];

  // Slope Chart Traces
  const slopeTraces = [];
  slopeData.forEach((s, idx) => {
    const isClimbed = s.rank_change > 0;
    const color = isClimbed ? '#10b981' : (s.rank_change < 0 ? '#ef4444' : '#94a3b8');
    const labelMap = {
      'fwci_avg': t('country.slope_fwci'),
      'pct_oa_diamond': t('country.slope_diamond'),
      'pct_top_10': t('country.slope_top10'),
      'num_documents': t('country.slope_docs')
    };
    
    slopeTraces.push({
      x: [t('country.slope_x_historical'), t('country.slope_x_recent')],
      y: [s.rank_full, s.rank_recent],
      type: 'scatter',
      mode: 'lines+markers+text',
      name: labelMap[s.indicator] || s.indicator,
      line: { color: color, width: 3 },
      marker: { size: 10, color: color },
      text: [`${t('country.slope_rank_label')}${s.rank_full}`, `${t('country.slope_rank_label')}${s.rank_recent}`],
      textposition: ['top left', 'top right'],
      hovertemplate: `<b>${labelMap[s.indicator] || s.indicator}</b><br>${t('country.slope_hover_hist')}: #${s.rank_full} (${s.val_full})<br>${t('country.slope_hover_recent')}: #${s.rank_recent} (${s.val_recent})<extra></extra>`
    });
  });

  // Landscape Traces
  const countryArts = landscapeArticles?.country_articles || (Array.isArray(landscapeArticles) ? landscapeArticles : []);
  const bgArts = landscapeArticles?.bg_articles || [];
  const landscapeTraces = [];

  const effectiveBgArts = bgArts.length > 0 ? bgArts : globalBgArts;

  if (effectiveBgArts.length > 0) {
    landscapeTraces.push({
      x: effectiveBgArts.map(a => a.umap_x),
      y: effectiveBgArts.map(a => a.umap_y),
      mode: 'markers',
      type: 'scatter',
      name: t('country.landscape_trace_other'),
      marker: { size: 3.5, color: '#94a3b8', opacity: 0.22 },
      hoverinfo: 'skip'
    });
  }

  // Escalamiento P98 estilo SinapsisAI dashboard_v2.py / map.html
  const countryArtSizes = useMemo(() => {
    if (!countryArts || countryArts.length === 0) return 2.5;
    const raw = countryArts.map(a => Number(a.fwci != null ? a.fwci : (a.cited_by_count || 0)));
    const nonZeros = raw.filter(v => v > 0).sort((a, b) => a - b);
    const p98 = nonZeros.length > 5 ? nonZeros[Math.floor(nonZeros.length * 0.98)] : (nonZeros[nonZeros.length - 1] || 1.0);
    const cap = Math.max(p98, 0.1);
    const rMin = 2.0;
    const rMax = 5.0;
    return raw.map(v => {
      const norm = Math.min(1.0, Math.max(0.0, v / cap));
      return rMin + (rMax - rMin) * Math.sqrt(norm);
    });
  }, [countryArts]);

  if (countryArts.length > 0) {
    const years = countryArts.map(a => Number(a.publication_year || 0)).filter(y => y > 0);
    const minYr = years.length ? Math.min(...years) : 1990;
    const maxYr = years.length ? Math.max(...years) : 2026;

    landscapeTraces.push({
      x: countryArts.map(a => a.umap_x),
      y: countryArts.map(a => a.umap_y),
      mode: 'markers',
      type: 'scatter',
      name: t('country.landscape_trace_country', { country: countryDisplayName }),
      marker: {
        size: countryArtSizes,
        color: countryArts.map(a => a.publication_year),
        colorscale: 'Turbo',
        cmin: minYr,
        cmax: maxYr,
        colorbar: { title: t('country.landscape_colorbar_title'), x: 1.02 },
        opacity: 0.85,
        line: { width: 0.3, color: '#ffffff' }
      },
      text: countryArts.map(a => a.title),
      customdata: countryArts.map(a => [
        a.journal_name || '—',
        a.publication_year || '—',
        a.fwci != null ? Number(a.fwci).toFixed(2) : '—',
        a.community_name || '—'
      ]),
      hovertemplate: `<b>%{text}</b><br>${t('country.landscape_hover_journal')}: %{customdata[0]}<br>${t('country.landscape_hover_year')}: %{customdata[1]} | FWCI: %{customdata[2]}<br>${t('country.landscape_hover_community')}: %{customdata[3]}<extra></extra>`
    });
  }

  // Dynamic Scatter Plot Data & Stats
  const validScatterRows = scatterDataList.filter(d => d[dynScatterX] != null && d[dynScatterY] != null && !isNaN(d[dynScatterX]) && !isNaN(d[dynScatterY]));
  
  const calcStats = (vals) => {
    if (!vals.length) return { mean: 0, median: 0, std: 0, min: 0, max: 0 };
    const sorted = [...vals].sort((a, b) => a - b);
    const sum = sorted.reduce((a, b) => a + b, 0);
    const mean = sum / sorted.length;
    const median = sorted.length % 2 === 0 
      ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2 
      : sorted[Math.floor(sorted.length / 2)];
    const variance = sorted.reduce((acc, v) => acc + Math.pow(v - mean, 2), 0) / (sorted.length > 1 ? sorted.length - 1 : 1);
    const std = Math.sqrt(variance);
    return { mean, median, std, min: sorted[0], max: sorted[sorted.length - 1] };
  };

  const xVals = validScatterRows.map(d => Number(d[dynScatterX]));
  const yVals = validScatterRows.map(d => Number(d[dynScatterY]));
  const statsX = calcStats(xVals);
  const statsY = calcStats(yVals);

  let pearsonR = 0;
  if (validScatterRows.length > 1) {
    let num = 0, denX = 0, denY = 0;
    for (let i = 0; i < validScatterRows.length; i++) {
      const dx = xVals[i] - statsX.mean;
      const dy = yVals[i] - statsY.mean;
      num += dx * dy;
      denX += dx * dx;
      denY += dy * dy;
    }
    pearsonR = (denX > 0 && denY > 0) ? num / Math.sqrt(denX * denY) : 0;
  }

  const xLabel = DYNAMIC_SCATTER_INDICATORS.find(i => i.id === dynScatterX)?.label || dynScatterX;
  const yLabel = DYNAMIC_SCATTER_INDICATORS.find(i => i.id === dynScatterY)?.label || dynScatterY;

  const dynamicScatterTraces = [{
    x: xVals,
    y: yVals,
    mode: 'markers',
    type: 'scatter',
    text: validScatterRows.map(d => d.display_name),
    customdata: validScatterRows.map(d => [d.id, d.num_documents || 0, d.fwci_avg || 0]),
    marker: {
      size: 9,
      color: '#0284c7',
      line: { width: 0.8, color: '#ffffff' },
      opacity: 0.85
    },
    hovertemplate: `<b>%{text}</b><br>${xLabel}: %{x:,.2f}<br>${yLabel}: %{y:,.2f}<br>Documentos: %{customdata[1]:,}<extra></extra>`
  }];

  // OA & Language Pie Traces
  const activePieData = piePeriod === 'recent' ? (summary?.recent_period || {}) : (summary?.full_period || {});

  const oaPieValues = [
    { label: t('common.diamond'), value: Number(activePieData.pct_oa_diamond || 0), color: '#38bdf8' },
    { label: t('common.gold'), value: Number(activePieData.pct_oa_gold || 0), color: '#fbbf24' },
    { label: t('common.green'), value: Number(activePieData.pct_oa_green || 0), color: '#34d399' },
    { label: t('common.hybrid'), value: Number(activePieData.pct_oa_hybrid || 0), color: '#a78bfa' },
    { label: t('common.bronze'), value: Number(activePieData.pct_oa_bronze || 0), color: '#fb923c' },
    { label: t('common.closed'), value: Number(activePieData.pct_oa_closed || 0), color: '#f87171' },
  ].filter(item => item.value > 0);

  const oaPieTrace = [{
    type: 'pie',
    values: oaPieValues.map(v => v.value),
    labels: oaPieValues.map(v => v.label),
    marker: {
      colors: oaPieValues.map(v => v.color)
    },
    hole: 0.4,
    textinfo: 'label+percent',
    hoverinfo: 'label+percent+value',
    hovertemplate: '<b>%{label}</b>: %{value:.1f}%<extra></extra>'
  }];

  const langPieValues = [
    { label: t('country.lang_name_es'), value: Number(activePieData.pct_lang_es || 0), color: '#a855f7' },
    { label: t('country.lang_name_en'), value: Number(activePieData.pct_lang_en || 0), color: '#38bdf8' },
    { label: t('country.lang_name_pt'), value: Number(activePieData.pct_lang_pt || 0), color: '#f59e0b' },
    { label: t('country.lang_name_fr'), value: Number(activePieData.pct_lang_fr || 0), color: '#ec4899' },
    { label: t('country.lang_name_de'), value: Number(activePieData.pct_lang_de || 0), color: '#10b981' },
    { label: t('country.lang_name_it'), value: Number(activePieData.pct_lang_it || 0), color: '#6366f1' },
    { label: t('country.lang_name_other'), value: Number(activePieData.pct_lang_other || 0), color: '#94a3b8' },
  ].filter(item => item.value > 0);

  const langPieTrace = [{
    type: 'pie',
    values: langPieValues.map(v => v.value),
    labels: langPieValues.map(v => v.label),
    marker: {
      colors: langPieValues.map(v => v.color)
    },
    hole: 0.4,
    textinfo: 'label+percent',
    hoverinfo: 'label+percent+value',
    hovertemplate: '<b>%{label}</b>: %{value:.1f}%<extra></extra>'
  }];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '28px' }}>
      {/* Country Selector Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
        <div>
          <h2 style={{ fontSize: '24px', fontWeight: '800' }}>
            {countryDisplayName} ({selectedCountry})
          </h2>
          <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
            {t('country.subtitle')}
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <label style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text-muted)' }}>
            {t('country.select_country')}:
          </label>
          <select
            value={selectedCountry}
            onChange={(e) => setSelectedCountry(e.target.value)}
            style={{ fontSize: '14px', fontWeight: '700', padding: '8px 16px' }}
          >
            {countriesList.map(c => (
              <option key={c.country_code} value={c.country_code}>
                {t(`country_names.${c.country_code}`) || c.country_name} ({c.country_code}) — {c.num_journals} {t('common.journals')}
              </option>
            ))}
          </select>
          <button
            onClick={handleShareCountry}
            title={t('buttons.share')}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              padding: '8px 14px',
              borderRadius: '8px',
              background: copiedLink ? '#10b981' : 'var(--bg-input)',
              color: copiedLink ? '#ffffff' : 'var(--text-main)',
              border: '1px solid var(--border-color)',
              fontSize: '13px',
              fontWeight: '700',
              cursor: 'pointer',
              transition: 'all 0.2s ease',
              boxShadow: copiedLink ? '0 2px 8px rgba(16, 185, 129, 0.3)' : 'none'
            }}
          >
            {copiedLink ? <Check size={15} /> : <Share2 size={15} />}
            <span>{copiedLink ? t('country.copied_link') : t('buttons.share')}</span>
          </button>
        </div>
      </div>

      {/* Top Contextual KPI Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px' }}>
        <KpiCard
          title={t('kpi.journals')}
          value={summary?.num_journals?.toLocaleString()}
          subtitle={t('kpi.journals_sub')}
          icon={BookOpen}
        />
        <KpiCard
          title={t('kpi.works')}
          value={summary?.total_works?.toLocaleString() || summary?.full_period?.num_documents?.toLocaleString()}
          subtitle={t('kpi.works_sub')}
          icon={FileText}
        />
        <KpiCard
          title={t('kpi.fwci')}
          value={pData?.fwci_avg != null ? Number(pData.fwci_avg).toFixed(2) : '0.00'}
          subtitle={t('kpi.fwci_sub')}
          icon={Zap}
        />
        <KpiCard
          title={t('kpi.diamond')}
          value={`${pData?.pct_oa_diamond ?? summary?.full_period?.pct_oa_diamond ?? 0}%`}
          subtitle={t('kpi.diamond_sub')}
          icon={Sparkles}
          badge={t('common.diamond')}
        />
        <KpiCard
          title={t('kpi.doaj_seal')}
          value={`${pData?.pct_doaj ?? summary?.full_period?.pct_doaj ?? 0}%`}
          subtitle={t('regional.kpi_doaj_sub')}
          icon={ShieldCheck}
          badge="DOAJ"
        />
      </div>

      {/* ── CONSOLIDATED PERFORMANCE INDICATORS PANEL FOR COUNTRY ── */}
      {(() => {
        const fullP = summary?.full_period || {};
        const recP  = summary?.recent_period || {};

        const fullDocs = fullP.num_documents ?? fullP.works_count ?? summary?.total_works ?? 0;
        const recDocs  = recP.num_documents ?? recP.works_count ?? 0;

        const fullFwci = fullP.fwci_avg != null ? Number(fullP.fwci_avg) : 0;
        const recFwci  = recP.fwci_avg != null ? Number(recP.fwci_avg) : 0;
        const fwciDelta = (recFwci - fullFwci).toFixed(2);

        const fullTop10 = fullP.pct_top_10 != null ? Number(fullP.pct_top_10) : 0;
        const recTop10  = recP.pct_top_10 != null ? Number(recP.pct_top_10) : 0;
        const top10Delta = (recTop10 - fullTop10).toFixed(2);

        const fullTop1 = fullP.pct_top_1 != null ? Number(fullP.pct_top_1) : 0;
        const recTop1  = recP.pct_top_1 != null ? Number(recP.pct_top_1) : 0;
        const top1Delta = (recTop1 - fullTop1).toFixed(2);

        const fullPerc = fullP.avg_percentile != null ? (Number(fullP.avg_percentile) <= 1.0 ? Number(fullP.avg_percentile) * 100 : Number(fullP.avg_percentile)) : 0;
        const recPerc  = recP.avg_percentile != null ? (Number(recP.avg_percentile) <= 1.0 ? Number(recP.avg_percentile) * 100 : Number(recP.avg_percentile)) : 0;
        const percDelta = (recPerc - fullPerc).toFixed(1);

        const langEs = Number(fullP.pct_lang_es || 0);
        const langEn = Number(fullP.pct_lang_en || 0);
        const langPt = Number(fullP.pct_lang_pt || 0);
        const langOther = fullP.pct_lang_other != null 
          ? Number(fullP.pct_lang_other) 
          : Math.max(0, 100 - (langEs + langEn + langPt));

        return (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            {/* 1. Impacto y Citación & Periodo Reciente */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '16px' }}>
              {/* Impacto y Citación (Periodo Completo) */}
              <div className="card" style={{ padding: '18px 20px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '10px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Activity size={18} style={{ color: 'var(--primary-color, #3b82f6)' }} />
                    <span style={{ fontSize: '15px', fontWeight: '800', color: 'var(--text-main)' }}>
                      📊 {t('country.card_impact_history')}
                    </span>
                  </div>
                  <span className="badge" style={{ fontSize: '11px' }}>0–2026</span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '14px' }}>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.table_docs')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {fullDocs.toLocaleString()}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_fwci_avg')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--primary-color, #3b82f6)', marginTop: '2px' }}>
                      {fullFwci.toFixed(2)}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_top10')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#f59e0b', marginTop: '2px' }}>
                      {fullTop10.toFixed(2)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_top1')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#ec4899', marginTop: '2px' }}>
                      {fullTop1.toFixed(3)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_percentile')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {fullPerc.toFixed(1)}
                    </div>
                  </div>
                </div>
              </div>

              {/* Periodo Reciente: 2021-2025 */}
              <div className="card" style={{ padding: '18px 20px', display: 'flex', flexDirection: 'column', gap: '14px', border: '1.5px solid rgba(16, 185, 129, 0.4)' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '10px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Sparkles size={18} style={{ color: '#10b981' }} />
                    <span style={{ fontSize: '15px', fontWeight: '800', color: '#10b981' }}>
                      ⚡ {t('country.card_recent_period')}
                    </span>
                  </div>
                  <span className="badge" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981', border: '1px solid #10b981', fontSize: '11px' }}>
                    {t('country.badge_recent_5years')}
                  </span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '14px' }}>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.table_docs')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {recDocs.toLocaleString()}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_fwci_avg')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      {recFwci.toFixed(2)}
                      <span style={{ fontSize: '11px', color: '#10b981', fontWeight: '700' }}>
                        ({Number(fwciDelta) >= 0 ? '+' : ''}{fwciDelta})
                      </span>
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_top10')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#f59e0b', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      {recTop10.toFixed(2)}%
                      <span style={{ fontSize: '11px', color: Number(top10Delta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                        ({Number(top10Delta) >= 0 ? '+' : ''}{top10Delta}%)
                      </span>
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_top1')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#ec4899', marginTop: '2px' }}>
                      {recTop1.toFixed(3)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_percentile')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      {recPerc.toFixed(1)}
                      <span style={{ fontSize: '11px', color: '#10b981', fontWeight: '700' }}>
                        ({Number(percDelta) >= 0 ? '+' : ''}{percDelta})
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* 2. Ciencia Abierta y Visibilidad & Distribución Lingüística */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '16px' }}>
              {/* Ciencia Abierta y Visibilidad */}
              <div className="card" style={{ padding: '18px 20px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '10px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <ShieldCheck size={18} style={{ color: '#38bdf8' }} />
                    <span style={{ fontSize: '15px', fontWeight: '800', color: 'var(--text-main)' }}>
                      🔓 {t('country.card_open_science')}
                    </span>
                  </div>
                  <span className="badge" style={{ fontSize: '11px' }}>{t('country.stat_access_index')}</span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))', gap: '12px' }}>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_oa_diamond')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#38bdf8', marginTop: '2px' }}>
                      {Number(fullP.pct_oa_diamond || 0).toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_oa_gold')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {Number(fullP.pct_oa_gold || 0).toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_oa_green')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#34d399', marginTop: '2px' }}>
                      {Number(fullP.pct_oa_green || 0).toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_scopus')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {Number(fullP.pct_scopus || 0).toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_doaj')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px' }}>
                      {Number(fullP.pct_doaj || 0).toFixed(1)}%
                    </div>
                  </div>
                </div>
              </div>

              {/* Distribución Lingüística de Publicación */}
              <div className="card" style={{ padding: '18px 20px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '10px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Globe2 size={18} style={{ color: '#a855f7' }} />
                    <span style={{ fontSize: '15px', fontWeight: '800', color: 'var(--text-main)' }}>
                      🌐 {t('country.card_language_dist')}
                    </span>
                  </div>
                  <span className="badge" style={{ fontSize: '11px' }}>{t('country.stat_languages')}</span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))', gap: '12px' }}>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_lang_es')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#a855f7', marginTop: '2px' }}>
                      {langEs.toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_lang_en')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#38bdf8', marginTop: '2px' }}>
                      {langEn.toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_lang_pt')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#f59e0b', marginTop: '2px' }}>
                      {langPt.toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('country.stat_lang_other')}</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-muted)', marginTop: '2px' }}>
                      {langOther.toFixed(1)}%
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        );
      })()}

      {/* DUAL-AXIS CHART: Producción Anual vs FWCI */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px', marginBottom: '8px', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <TrendingUp size={18} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              {t('country.dual_axis_title')}
            </h3>
          </div>
        </div>
        <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          {t('country.dual_axis_desc')}
        </p>

        <PlotlyChart
          data={dualAxisTraces}
          layout={{
            height: 380,
            margin: { l: 60, r: 60, t: 20, b: 40 },
            xaxis: { title: t('country.axis_pub_year') },
            yaxis: { title: t('country.trace_published_docs'), side: 'left', showgrid: true },
            yaxis2: {
              title: t('country.trace_fwci_avg'),
              side: 'right',
              overlaying: 'y',
              showgrid: false,
              rangemode: 'tozero'
            },
            legend: { orientation: 'h', y: 1.1, x: 0.1 }
          }}
        />
      </div>

      {/* TABLA DE INDICADORES HISTÓRICOS DEL PAÍS (DATOS CRUDOS, W=3, W=5) */}
      <AnnualDataTable 
        data={annualTrends} 
        countryCode={selectedCountry}
        countryName={countryDisplayName}
      />

      {/* 1. TRAYECTORIA DE DESEMPEÑO (UMAP PAÍS VS LATAM) */}
      {trajectory && Object.keys(trajectory).length > 0 && (
        <UmapTrajectoryViewer
          title={`📈 ${t('country.trajectory_title', { country: countryDisplayName })}`}
          subtitle={t('country.trajectory_subtitle')}
          trajectories={trajectory}
          allowTrajectoryFilter={true}
          showGridSection={true}
          height={460}
        />
      )}

      {/* 2. MAPA UMAP DE SIMILITUD ENTRE REVISTAS DEL PAÍS */}
      {umapJournals && umapJournals.length > 0 && (
        <>
          <UmapTrajectoryViewer
            title={`🌌 ${t('country.umap_similarity_title', { country: countryDisplayName })}`}
            subtitle={t('country.umap_similarity_subtitle', { count: umapJournals.length })}
            points={umapJournals}
            allowTrajectoryFilter={false}
            showGridSection={true}
            height={460}
            defaultShowLabels={false}
          />

          {/* EXPANDABLE: VER TABLA DE DATOS UMAP (REVISTAS) */}
          <div className="card" style={{ marginTop: '-12px' }}>
            <div 
              onClick={() => setUmapTableOpen(!umapTableOpen)}
              style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', cursor: 'pointer', userSelect: 'none' }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <FileSpreadsheet size={18} style={{ color: 'var(--primary-color, #3b82f6)' }} />
                <h4 style={{ fontSize: '15px', fontWeight: '700', margin: 0 }}>
                  📊 {t('country.umap_table_toggle')}
                </h4>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    if (!umapJournals || umapJournals.length === 0) return;
                    const headers = [
                      t('country.table_journal'),
                      t('country.table_docs'),
                      t('country.stat_lang_en'),
                      t('country.stat_oa_diamond'),
                      t('country.stat_fwci_avg'),
                      t('country.stat_top10'),
                      t('country.stat_top1'),
                      t('country.table_percentile')
                    ];
                    const rows = umapJournals.map(j => [
                      `"${(j.display_name || '').replace(/"/g, '""')}"`,
                      j.num_documents || 0,
                      Number(j.pct_lang_en || 0).toFixed(1),
                      Number(j.pct_oa_diamond || 0).toFixed(1),
                      Number(j.fwci_avg || 0).toFixed(3),
                      Number(j.pct_top_10 || 0).toFixed(3),
                      Number(j.pct_top_1 || 0).toFixed(3),
                      Number(j.avg_percentile || 0).toFixed(3)
                    ]);
                    const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
                    const encodedUri = encodeURI(csvContent);
                    const link = document.createElement('a');
                    link.setAttribute('href', encodedUri);
                    link.setAttribute('download', `datos_umap_revistas_${selectedCountry}.csv`);
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                  }}
                  className="btn-secondary"
                  style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '12px', padding: '4px 10px' }}
                >
                  <Download size={13} /> {t('common.download_csv')}
                </button>
                {umapTableOpen ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
              </div>
            </div>

            {umapTableOpen && (
              <div style={{ marginTop: '14px' }}>
                <div style={{ position: 'relative', maxWidth: '320px', marginBottom: '12px' }}>
                  <Search size={14} style={{ position: 'absolute', left: '10px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
                  <input
                    type="text"
                    className="input-search"
                    placeholder={t('country.search_journal_placeholder')}
                    value={umapTableSearch}
                    onChange={(e) => setUmapTableSearch(e.target.value)}
                    style={{ paddingLeft: '32px', width: '100%', fontSize: '12.5px', borderRadius: '6px' }}
                  />
                </div>

                <div className="data-table-container" style={{ maxHeight: '360px' }}>
                  <table className="data-table" style={{ fontSize: '12px' }}>
                    <thead>
                      <tr>
                        <th>{t('country.table_journal')}</th>
                        <th style={{ textAlign: 'right' }}>{t('country.table_docs')}</th>
                        <th style={{ textAlign: 'right' }}>{t('country.stat_lang_en')}</th>
                        <th style={{ textAlign: 'right' }}>{t('country.stat_oa_diamond')}</th>
                        <th style={{ textAlign: 'right' }}>{t('country.stat_fwci_avg')}</th>
                        <th style={{ textAlign: 'right' }}>{t('country.stat_top10')}</th>
                        <th style={{ textAlign: 'right' }}>{t('country.stat_top1')}</th>
                        <th style={{ textAlign: 'right' }}>{t('country.table_percentile')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {umapJournals
                        .filter(j => !umapTableSearch || (j.display_name || '').toLowerCase().includes(umapTableSearch.toLowerCase()))
                        .map((j, idx) => (
                          <tr key={j.id || idx}>
                            <td style={{ fontWeight: '600', maxWidth: '260px', whiteSpace: 'nowrap', textOverflow: 'ellipsis', overflow: 'hidden' }} title={j.display_name}>
                              <button
                                onClick={() => {
                                  setSelectedJournal(j.id);
                                  setActiveSection('journal');
                                }}
                                style={{ background: 'none', border: 'none', color: 'var(--primary-color, #3b82f6)', cursor: 'pointer', textAlign: 'left', padding: 0, font: 'inherit', fontWeight: '600' }}
                              >
                                {j.display_name}
                              </button>
                            </td>
                            <td style={{ textAlign: 'right' }}>{Number(j.num_documents || 0).toLocaleString()}</td>
                            <td style={{ textAlign: 'right' }}>{Number(j.pct_lang_en || 0).toFixed(1)}%</td>
                            <td style={{ textAlign: 'right', color: '#38bdf8', fontWeight: '600' }}>{Number(j.pct_oa_diamond || 0).toFixed(1)}%</td>
                            <td style={{ textAlign: 'right', color: Number(j.fwci_avg || 0) >= 1.0 ? '#10b981' : 'inherit' }}>{Number(j.fwci_avg || 0).toFixed(3)}</td>
                            <td style={{ textAlign: 'right' }}>{Number(j.pct_top_10 || 0).toFixed(3)}%</td>
                            <td style={{ textAlign: 'right' }}>{Number(j.pct_top_1 || 0).toFixed(3)}%</td>
                            <td style={{ textAlign: 'right' }}>{Number(j.avg_percentile || 0).toFixed(3)}</td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </>
      )}

      {/* 3. EVOLUCIÓN EN EL PAISAJE CIENTÍFICO (LATAM) */}
      {landscapeTraces.length > 0 && (
        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '6px', flexWrap: 'wrap', gap: '8px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Compass size={20} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '17px', fontWeight: '700', margin: 0 }}>
                🌌 {t('country.landscape_title', { country: countryDisplayName })}
              </h3>
            </div>
            <span className="badge" style={{ fontSize: '11px' }}>
              {t('country.landscape_badge', { count: countryArts.length.toLocaleString() })}
            </span>
          </div>
          <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginBottom: '14px' }}>
            {t('country.landscape_desc', { country: countryDisplayName })}
          </p>

          <PlotlyChart
            data={landscapeTraces}
            layout={{
              height: 520,
              margin: { l: 30, r: 30, t: 20, b: 30 },
              xaxis: { showgrid: true, zeroline: false },
              yaxis: { showgrid: true, zeroline: false },
              legend: { orientation: 'h', y: 1.08, x: 0.1 }
            }}
          />

          <div style={{ marginTop: '12px', padding: '10px 14px', background: 'rgba(2, 132, 199, 0.08)', borderRadius: '8px', borderLeft: '4px solid var(--accent-primary)', fontSize: '12.5px', color: 'var(--text-muted)' }}>
            💡 <strong>{t('country.landscape_note_title')}</strong> {t('country.landscape_note_desc', { country: countryDisplayName })}
          </div>
        </div>
      )}

      {/* HEATMAP DE ESPECIALIZACIÓN TEMÁTICA (RCA) */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Grid size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
                {t('country.rca_title')}
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('country.rca_desc')}
            </span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${rcaLevel === 'domain' ? 'active' : ''}`}
              onClick={() => setRcaLevel('domain')}
            >
              {t('country.rca_pill_domains', { count: 6 })}
            </button>
            <button
              className={`segmented-pill-btn ${rcaLevel === 'field' ? 'active' : ''}`}
              onClick={() => setRcaLevel('field')}
            >
              {t('country.rca_pill_fields', { count: 28 })}
            </button>
          </div>
        </div>

        {rcaData && rcaData.countries && (
          <PlotlyChart
            data={[{
              type: 'heatmap',
              z: rcaData.matrix,
              x: rcaData.disciplines,
              y: rcaData.countries.map(c => t(`country_names.${c.code}`) || c.name),
              colorscale: 'YlGnBu',
              colorbar: { title: t('country.rca_colorbar') },
              hovertemplate: `<b>${t('country.rca_hover_country')}:</b> %{y}<br><b>${t('country.rca_hover_discipline')}:</b> %{x}<br><b>RCA:</b> %{z:.2f}<extra></extra>`
            }]}
            layout={{
              height: rcaLevel === 'domain' ? 440 : 540,
              margin: { l: 140, r: 20, t: 20, b: 90 },
              xaxis: { tickangle: -40 }
            }}
          />
        )}
      </div>

      {/* BEESWARM / STRIP PLOT & SLOPE CHART */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(420px, 1fr))', gap: '20px' }}>
        {/* Beeswarm Plot */}
        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px', flexWrap: 'wrap', gap: '10px' }}>
            <h3 style={{ fontSize: '15px', fontWeight: '700' }}>
              🔬 {t('country.beeswarm_title')}
            </h3>
            <select
              value={beeswarmMetric}
              onChange={(e) => setBeeswarmMetric(e.target.value)}
              style={{ fontSize: '12px', fontWeight: '600' }}
            >
              <option value="fwci_avg">{t('country.stat_fwci_avg')}</option>
              <option value="pct_oa_diamond">{t('country.stat_oa_diamond')}</option>
              <option value="h_index">{t('tables.h_index')}</option>
              <option value="works_count">{t('tables.articles')}</option>
            </select>
          </div>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            {t('country.beeswarm_desc')}
          </p>
          <PlotlyChart
            data={beeswarmTraces}
            layout={{
              height: 340,
              margin: { l: 50, r: 20, t: 10, b: 30 },
              yaxis: { title: beeswarmMetric }
            }}
          />
        </div>

        {/* Slope Chart */}
        <div className="card">
          <h3 style={{ fontSize: '15px', fontWeight: '700', marginBottom: '4px' }}>
            📈 {t('country.slope_title')}
          </h3>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            {t('country.slope_desc')}
          </p>
          <PlotlyChart
            data={slopeTraces}
            layout={{
              height: 340,
              margin: { l: 80, r: 80, t: 20, b: 30 },
              yaxis: { autorange: 'reversed', title: t('country.slope_yaxis_title') },
              legend: { orientation: 'h', y: -0.15 }
            }}
          />
        </div>
      </div>

      {/* 4. EXPLORADOR DE REVISTAS - SCATTER PLOT DINÁMICO */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <BarChart2 size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700', margin: 0 }}>
                {t('country.dynamic_scatter_title')}
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('country.dynamic_scatter_subtitle', { country: countryDisplayName, count: validScatterRows.length })}
            </span>
          </div>

          {/* Period Selector */}
          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${scatterPeriod === 'recent' ? 'active' : ''}`}
              onClick={() => setScatterPeriod('recent')}
            >
              {t('country.scatter_pill_recent')}
            </button>
            <button
              className={`segmented-pill-btn ${scatterPeriod === 'full' ? 'active' : ''}`}
              onClick={() => setScatterPeriod('full')}
            >
              {t('country.scatter_pill_full')}
            </button>
          </div>
        </div>

        {/* Axis Selectors */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: '14px', marginBottom: '14px', padding: '12px 16px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: '700', marginBottom: '4px', color: 'var(--text-main)' }}>
              {t('country.axis_x_indicator')}
            </label>
            <select
              value={dynScatterX}
              onChange={(e) => setDynScatterX(e.target.value)}
              style={{ width: '100%', fontSize: '13px', padding: '6px 10px', borderRadius: '6px' }}
            >
              {DYNAMIC_SCATTER_INDICATORS.map(ind => (
                <option key={ind.id} value={ind.id}>{ind.label}</option>
              ))}
            </select>
          </div>

          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: '700', marginBottom: '4px', color: 'var(--text-main)' }}>
              {t('country.axis_y_indicator')}
            </label>
            <select
              value={dynScatterY}
              onChange={(e) => setDynScatterY(e.target.value)}
              style={{ width: '100%', fontSize: '13px', padding: '6px 10px', borderRadius: '6px' }}
            >
              {DYNAMIC_SCATTER_INDICATORS.map(ind => (
                <option key={ind.id} value={ind.id}>{ind.label}</option>
              ))}
            </select>
          </div>
        </div>

        {/* Scatter Chart */}
        <PlotlyChart
          data={dynamicScatterTraces}
          layout={{
            height: 440,
            margin: { l: 60, r: 30, t: 20, b: 50 },
            xaxis: {
              title: xLabel,
              showgrid: true,
              zeroline: true
            },
            yaxis: {
              title: yLabel,
              showgrid: true,
              zeroline: true
            },
            showlegend: false
          }}
          onClick={(data) => {
            if (data?.points?.[0]?.customdata?.[0]) {
              setSelectedJournal(data.points[0].customdata[0]);
              setActiveSection('journal');
            }
          }}
        />

        {/* Descriptive Statistics & Pearson Correlation Panel */}
        {statsX && statsY && (
          <div style={{ marginTop: '16px', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '12px' }}>
            <div style={{ padding: '12px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '12px' }}>
              <div style={{ fontWeight: '700', color: 'var(--text-main)', marginBottom: '6px' }}>
                {t('country.stats_title', { label: xLabel })}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px', color: 'var(--text-muted)' }}>
                <span>{t('country.stat_mean')}: <strong>{statsX.mean.toFixed(2)}</strong></span>
                <span>{t('country.stat_median')}: <strong>{statsX.median.toFixed(2)}</strong></span>
                <span>{t('country.stat_std')}: <strong>{statsX.std.toFixed(2)}</strong></span>
                <span>{t('country.stat_min_max')}: <strong>{statsX.min.toFixed(2)} / {statsX.max.toFixed(2)}</strong></span>
              </div>
            </div>

            <div style={{ padding: '12px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '12px' }}>
              <div style={{ fontWeight: '700', color: 'var(--text-main)', marginBottom: '6px' }}>
                {t('country.stats_title', { label: yLabel })}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px', color: 'var(--text-muted)' }}>
                <span>{t('country.stat_mean')}: <strong>{statsY.mean.toFixed(2)}</strong></span>
                <span>{t('country.stat_median')}: <strong>{statsY.median.toFixed(2)}</strong></span>
                <span>{t('country.stat_std')}: <strong>{statsY.std.toFixed(2)}</strong></span>
                <span>{t('country.stat_min_max')}: <strong>{statsY.min.toFixed(2)} / {statsY.max.toFixed(2)}</strong></span>
              </div>
            </div>

            <div style={{ padding: '12px 14px', background: 'rgba(2, 132, 199, 0.1)', borderRadius: '8px', border: '1px solid rgba(2, 132, 199, 0.3)', display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
              <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{t('country.pearson_label')}</span>
              <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--accent-primary)', marginTop: '2px' }}>
                r = {pearsonR != null ? pearsonR.toFixed(3) : '0.000'}
              </div>
              <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '2px' }}>
                {Math.abs(pearsonR) >= 0.7 ? t('country.corr_strong') : (Math.abs(pearsonR) >= 0.4 ? t('country.corr_moderate') : t('country.corr_weak'))}
              </span>
            </div>
          </div>
        )}
      </div>

      {/* 5. DISTRIBUCIÓN Y CARACTERÍSTICAS DE LAS PUBLICACIONES (PIES DE OA E IDIOMAS) */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <PieChart size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700', margin: 0 }}>
                {t('country.pie_section_title')}
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('country.pie_section_subtitle', { country: countryDisplayName })}
            </span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${piePeriod === 'full' ? 'active' : ''}`}
              onClick={() => setPiePeriod('full')}
            >
              {t('country.pie_period_full')}
            </button>
            <button
              className={`segmented-pill-btn ${piePeriod === 'recent' ? 'active' : ''}`}
              onClick={() => setPiePeriod('recent')}
            >
              {t('country.pie_period_recent')}
            </button>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: '20px' }}>
          {/* OA Pie */}
          <div style={{ background: 'var(--bg-input)', borderRadius: '10px', padding: '16px', border: '1px solid var(--border-color)' }}>
            <h4 style={{ fontSize: '14px', fontWeight: '700', textAlign: 'center', marginBottom: '8px', color: 'var(--text-main)' }}>
              {t('country.pie_oa_title')}
            </h4>
            {oaPieValues.length > 0 ? (
              <PlotlyChart
                data={oaPieTrace}
                layout={{
                  height: 320,
                  margin: { l: 20, r: 20, t: 10, b: 20 },
                  legend: { orientation: 'h', y: -0.1 }
                }}
              />
            ) : (
              <div style={{ textAlign: 'center', padding: '60px 20px', color: 'var(--text-muted)' }}>
                {t('country.pie_oa_empty')}
              </div>
            )}
          </div>

          {/* Language Pie */}
          <div style={{ background: 'var(--bg-input)', borderRadius: '10px', padding: '16px', border: '1px solid var(--border-color)' }}>
            <h4 style={{ fontSize: '14px', fontWeight: '700', textAlign: 'center', marginBottom: '8px', color: 'var(--text-main)' }}>
              {t('country.pie_lang_title')}
            </h4>
            {langPieValues.length > 0 ? (
              <PlotlyChart
                data={langPieTrace}
                layout={{
                  height: 320,
                  margin: { l: 20, r: 20, t: 10, b: 20 },
                  legend: { orientation: 'h', y: -0.1 }
                }}
              />
            ) : (
              <div style={{ textAlign: 'center', padding: '60px 20px', color: 'var(--text-muted)' }}>
                {t('country.pie_lang_empty')}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Sunburst & Treemap Section */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              {t('country.hierarchy_title')}
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('country.hierarchy_desc')}
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
            {/* View Switcher: Sunburst vs Treemap */}
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${thematicViewType === 'sunburst' ? 'active' : ''}`}
                onClick={() => setThematicViewType('sunburst')}
              >
                {t('country.hierarchy_sunburst')}
              </button>
              <button
                className={`segmented-pill-btn ${thematicViewType === 'treemap' ? 'active' : ''}`}
                onClick={() => setThematicViewType('treemap')}
              >
                {t('country.hierarchy_treemap')}
              </button>
            </div>

            {/* Indicator Picker */}
            <select
              value={sunburstIndicator}
              onChange={(e) => setSunburstIndicator(e.target.value)}
              style={{ fontWeight: '600' }}
            >
              <option value="fwci_avg_recent">{t('country.scatter_fwci_recent')}</option>
              <option value="avg_percentile_recent">{t('country.scatter_percentile_recent')}</option>
              <option value="pct_top_10_recent">{t('country.scatter_top10_recent')}</option>
              <option value="pct_oa_gold_recent">{t('country.scatter_oa_gold_recent')}</option>
              <option value="fwci_avg_full">{t('country.scatter_fwci_full')}</option>
              <option value="avg_percentile_full">{t('country.scatter_percentile_full')}</option>
              <option value="pct_top_10_full">{t('country.scatter_top10_full')}</option>
              <option value="pct_oa_gold_full">{t('country.scatter_oa_gold_full')}</option>
            </select>

            {/* Include Unclassified Checkbox */}
            <label style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: 'var(--text-muted)', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={sunburstUnclassified}
                onChange={(e) => setSunburstUnclassified(e.target.checked)}
              />
              {t('country.hierarchy_unclassified')}
            </label>
          </div>
        </div>

        {thematicViewType === 'sunburst' ? (
          <PlotlyChart data={sunburstTrace} layout={{ height: 540, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
        ) : (
          <PlotlyChart data={treemapTrace} layout={{ height: 540, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
        )}
      </div>

      {/* 6. ANÁLISIS DE PERFILES TEMÁTICOS DE REVISTAS DEL PAÍS (DOMINIO, CAMPO, SUBCAMPO) */}
      <CountryThematicProfilesTable 
        countryCode={selectedCountry} 
        countryName={countryDisplayName} 
      />

      {/* 7. EVOLUCIÓN HISTÓRICA DE PERFILES DE CONOCIMIENTO DEL PAÍS (DOMINIO, CAMPO, SUBCAMPO, TÓPICO) */}
      <ThematicEvolutionTable 
        countryCode={selectedCountry} 
        countryName={countryDisplayName} 
      />

      {/* 8. MATRIZ DE COAUTORÍA PAÍS-PAÍS (CONNECTION MAP GLOBAL) */}
      <ConnectionMapViewer
        collabData={collabData}
        loading={collabLoading}
        title={`${t('connection_map.title')} — ${countryDisplayName}`}
        subtitle={t('connection_map.subtitle_country')}
        anchorName={countryDisplayName}
        anchorCode={selectedCountry}
        scopeType="country"
      />

      {/* Journals Catalog Table */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              📚 {t('country.catalog_title', { country: countryDisplayName, count: journals.length })}
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('country.catalog_subtitle')}
            </span>
          </div>

          <button
            className="btn-secondary"
            onClick={() => {
              if (!journals || journals.length === 0) return;
              const headers = [t('country.table_journal'), 'OpenAlex_ID', t('tables.issn'), t('tables.publisher'), t('tables.articles'), t('tables.citations'), t('tables.fwci'), t('tables.h_index'), t('tables.pct_diamond'), 'DOAJ'];
              const rows = journals.map(j => [
                `"${(j.display_name || '').replace(/"/g, '""')}"`,
                `"${j.id || ''}"`,
                `"${j.issn_l || ''}"`,
                `"${(j.publisher || '').replace(/"/g, '""')}"`,
                j.works_count || 0,
                j.cited_by_count || 0,
                Number(j.fwci_avg || 0).toFixed(2),
                j.h_index || 0,
                Number(j.pct_oa_diamond || 0).toFixed(1),
                j.is_in_doaj ? t('common.yes') : t('common.no')
              ]);
              const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [headers.join(','), ...rows.map(e => e.join(','))].join('\n');
              const encodedUri = encodeURI(csvContent);
              const link = document.createElement('a');
              link.setAttribute('href', encodedUri);
              link.setAttribute('download', `catalogo_revistas_${selectedCountry}_${new Date().toISOString().slice(0, 10)}.csv`);
              document.body.appendChild(link);
              link.click();
              document.body.removeChild(link);
            }}
            style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', padding: '6px 12px' }}
            title={t('country.catalog_csv_tooltip')}
          >
            <Download size={14} /> {t('tables.download_csv')}
          </button>
        </div>

        <div className="data-table-container" style={{ maxHeight: '420px' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>{t('country.table_journal')}</th>
                <th>{t('tables.issn')}</th>
                <th>{t('tables.publisher')}</th>
                <th>{t('tables.articles')}</th>
                <th>{t('tables.citations')}</th>
                <th>{t('tables.fwci')}</th>
                <th>{t('tables.h_index')}</th>
                <th>{t('tables.pct_diamond')}</th>
                <th>DOAJ</th>
                <th>{t('tables.action')}</th>
              </tr>
            </thead>
            <tbody>
              {journals.map((j, idx) => (
                <tr key={idx}>
                  <td><strong>{j.display_name}</strong></td>
                  <td><code>{j.issn_l || '—'}</code></td>
                  <td>{j.publisher || '—'}</td>
                  <td>{j.works_count?.toLocaleString()}</td>
                  <td>{j.cited_by_count?.toLocaleString()}</td>
                  <td>{Number(j.fwci_avg || 0).toFixed(2)}</td>
                  <td>{j.h_index || '—'}</td>
                  <td>{Number(j.pct_oa_diamond || 0).toFixed(1)}%</td>
                  <td>{j.is_in_doaj ? '✅' : '—'}</td>
                  <td>
                    <button
                      className="btn-primary"
                      style={{ fontSize: '11px', padding: '4px 8px' }}
                      onClick={() => {
                        setSelectedJournal(j.id, j.display_name);
                        setActiveSection('journal');
                      }}
                    >
                      {t('buttons.view_details')}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── EXPANDER DE DOSSIER DE ESTUDIO Y ENVÍO A CHATGPT (PIE DE PÁGINA) ── */}
      <PageDossierExpander
        pageTitle={t('country.dossier_page_title', { country: countryDisplayName })}
        pageDescription={t('country.dossier_page_desc', { country: countryDisplayName })}
        sections={[
          {
            id: 'country_kpis',
            title: t('country.dossier_sec1_title', { country: countryDisplayName }),
            category: t('country.dossier_sec1_cat'),
            defaultChecked: true,
            rawData: { summary, pData },
            buildDataText: () => {
              if (!summary && !pData) return 'No hay datos de perfil disponibles.';
              return [
                `*País:* **${countryDisplayName} (${selectedCountry})**\n`,
                '| Métrica Cienciométrica | Valor Actual | Contexto Nacional |',
                '|---|---|---|',
                `| Revistas Activas en OpenAlex | ${summary?.num_journals?.toLocaleString() || 0} | Publicaciones con sede en el país |`,
                `| Producción Histórica de Artículos | ${summary?.total_works?.toLocaleString() || summary?.full_period?.num_documents?.toLocaleString() || 0} | Volumen acumulado indexado |`,
                `| FWCI Ponderado Promedio | ${pData?.fwci_avg != null ? Number(pData.fwci_avg).toFixed(2) : '—'} | Citas ponderadas por campo (Base mundial=1.0) |`,
                `| % Acceso Abierto Diamante | ${pData?.pct_oa_diamond ?? summary?.full_period?.pct_oa_diamond ?? 0}% | Revistas sin APC para autores |`,
                `| % Revistas con Sello DOAJ | ${pData?.pct_doaj ?? summary?.full_period?.pct_doaj ?? 0}% | Estándares de calidad y visibilidad abierta |`
              ].join('\n');
            }
          },
          {
            id: 'country_performance_panel',
            title: t('country.dossier_sec2_title'),
            category: t('country.dossier_sec2_cat'),
            defaultChecked: true,
            rawData: { full_period: summary?.full_period, recent_period: summary?.recent_period },
            buildDataText: () => {
              const f = summary?.full_period || {};
              const r = summary?.recent_period || {};
              const fullDocs = f.num_documents ?? f.works_count ?? summary?.total_works ?? 0;
              const recDocs  = r.num_documents ?? r.works_count ?? 0;
              const fullFwci = f.fwci_avg != null ? Number(f.fwci_avg) : 0;
              const recFwci  = r.fwci_avg != null ? Number(r.fwci_avg) : 0;
              const fwciDelta = (recFwci - fullFwci).toFixed(2);
              const fullTop10 = f.pct_top_10 != null ? Number(f.pct_top_10) : 0;
              const recTop10  = r.pct_top_10 != null ? Number(r.pct_top_10) : 0;
              const top10Delta = (recTop10 - fullTop10).toFixed(2);
              const fullTop1 = f.pct_top_1 != null ? Number(f.pct_top_1) : 0;
              const recTop1  = r.pct_top_1 != null ? Number(r.pct_top_1) : 0;
              const fullPerc = f.avg_percentile != null ? (Number(f.avg_percentile) <= 1.0 ? Number(f.avg_percentile) * 100 : Number(f.avg_percentile)) : 0;
              const recPerc  = r.avg_percentile != null ? (Number(r.avg_percentile) <= 1.0 ? Number(r.avg_percentile) * 100 : Number(r.avg_percentile)) : 0;
              const percDelta = (recPerc - fullPerc).toFixed(1);

              return [
                '**Comparativa de Impacto y Citación:**\n',
                '| Indicador | Histórico (0–2026) | Reciente (2021–2025) | Variación Neta / Lustro |',
                '|---|---|---|---|',
                `| Documentos Publicados | ${fullDocs.toLocaleString()} | ${recDocs.toLocaleString()} | ${recDocs && fullDocs ? ((recDocs / fullDocs) * 100).toFixed(1) + '% de la producción histórica' : '—'} |`,
                `| FWCI Promedio | ${fullFwci.toFixed(2)} | ${recFwci.toFixed(2)} | ${Number(fwciDelta) >= 0 ? '+' : ''}${fwciDelta} |`,
                `| % Artículos en Top 10% | ${fullTop10.toFixed(2)}% | ${recTop10.toFixed(2)}% | ${Number(top10Delta) >= 0 ? '+' : ''}${top10Delta}% |`,
                `| % Artículos en Top 1% | ${fullTop1.toFixed(3)}% | ${recTop1.toFixed(3)}% | ${(recTop1 - fullTop1).toFixed(3)}% |`,
                `| Percentil Promedio Normalizado | ${fullPerc.toFixed(1)} | ${recPerc.toFixed(1)} | ${Number(percDelta) >= 0 ? '+' : ''}${percDelta} |`,
                '\n**Ciencia Abierta, Indexación e Idiomas (Histórico):**\n',
                '| Métrica | Valor |',
                '|---|---|',
                `| % Acceso Abierto Diamante | ${Number(f.pct_oa_diamond || 0).toFixed(1)}% |`,
                `| % Acceso Abierto Gold | ${Number(f.pct_oa_gold || 0).toFixed(1)}% |`,
                `| % Acceso Abierto Verde (Repositorio) | ${Number(f.pct_oa_green || 0).toFixed(1)}% |`,
                `| % Revistas en Scopus | ${Number(f.pct_scopus || 0).toFixed(1)}% |`,
                `| % Revistas en DOAJ | ${Number(f.pct_doaj || 0).toFixed(1)}% |`,
                `| % Artículos en Español | ${Number(f.pct_lang_es || 0).toFixed(1)}% |`,
                `| % Artículos en Inglés | ${Number(f.pct_lang_en || 0).toFixed(1)}% |`,
                `| % Artículos en Portugués | ${Number(f.pct_lang_pt || 0).toFixed(1)}% |`
              ].join('\n');
            }
          },
          {
            id: 'country_annual',
            title: t('country.dossier_sec3_title'),
            category: t('country.dossier_sec3_cat'),
            defaultChecked: true,
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
            id: 'umap_trajectory_country',
            title: t('country.dossier_sec4_title', { country: countryDisplayName }),
            category: t('country.dossier_sec4_cat'),
            defaultChecked: false,
            rawData: trajectory,
            buildDataText: () => {
              if (!trajectory || Object.keys(trajectory).length === 0) return 'No hay datos de trayectoria disponibles.';
              const lines = [
                '| Entidad | Puntos Registrados | Coordenadas Inicio (2000) | Coordenadas Recientes (2025) |',
                '|---|---|---|---|'
              ];
              Object.keys(trajectory).forEach(k => {
                const ent = trajectory[k];
                const pts = ent?.points || [];
                const pStart = pts[0];
                const pEnd = pts[pts.length - 1];
                lines.push(`| ${ent.name || k} | ${pts.length} años | (${Number(pStart?.x || 0).toFixed(2)}, ${Number(pStart?.y || 0).toFixed(2)}) | (${Number(pEnd?.x || 0).toFixed(2)}, ${Number(pEnd?.y || 0).toFixed(2)}) |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'umap_country_journals',
            title: t('country.dossier_sec5_title', { count: umapJournals.length }),
            category: t('country.dossier_sec5_cat'),
            defaultChecked: false,
            rawData: umapJournals,
            buildDataText: () => {
              if (!umapJournals || umapJournals.length === 0) return 'No hay datos de distribución UMAP disponibles.';
              const lines = [
                '| Revista | Documentos | % Inglés | % OA Diamante | FWCI Promedio | % Top 10% | % Top 1% | Percentil Prom. |',
                '|---|---|---|---|---|---|---|---|'
              ];
              umapJournals.slice(0, 25).forEach(u => {
                lines.push(`| ${u.display_name || u.name} | ${Number(u.num_documents || u.works_count || 0).toLocaleString()} | ${Number(u.pct_lang_en || 0).toFixed(1)}% | ${Number(u.pct_oa_diamond || 0).toFixed(1)}% | ${Number(u.fwci_avg || 0).toFixed(3)} | ${Number(u.pct_top_10 || 0).toFixed(3)}% | ${Number(u.pct_top_1 || 0).toFixed(3)}% | ${Number(u.avg_percentile || 0).toFixed(3)} |`);
              });
              if (umapJournals.length > 25) {
                lines.push(`\n_... y ${umapJournals.length - 25} revistas más en el espacio UMAP._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'country_landscape_evolution',
            title: t('country.dossier_sec6_title', { count: countryArts.length }),
            category: t('country.dossier_sec6_cat'),
            defaultChecked: false,
            rawData: landscapeArticles,
            buildDataText: () => {
              if (!countryArts || countryArts.length === 0) return 'No hay artículos proyectados en el mapa semántico regional.';
              const lines = [
                `*Muestra:* **${countryArts.length.toLocaleString()} artículos de ${countryDisplayName}** proyectados sobre el espacio temático latinoamericano.\n`,
                '| Título del Artículo | Revista | Año | FWCI | Comunidad Temática | Coordenadas (UMAP-1, UMAP-2) |',
                '|---|---|---|---|---|---|'
              ];
              countryArts.slice(0, 20).forEach(a => {
                const titleClean = (a.title || 'Sin título').replace(/\|/g, '-');
                lines.push(`| ${titleClean.slice(0, 60)}... | ${a.journal_name || 'Desconocida'} | ${a.publication_year || '—'} | ${a.fwci != null ? Number(a.fwci).toFixed(2) : '—'} | ${a.community_name || 'General'} | (${Number(a.umap_x || 0).toFixed(2)}, ${Number(a.umap_y || 0).toFixed(2)}) |`);
              });
              if (countryArts.length > 20) {
                lines.push(`\n_... y ${countryArts.length - 20} artículos más en la proyección._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'thematic_specialization_rca',
            title: t('country.dossier_sec7_title'),
            category: t('country.dossier_sec7_cat'),
            defaultChecked: false,
            rawData: rcaData,
            buildDataText: () => {
              if (!rcaData || !rcaData.countries || !rcaData.disciplines) return 'No hay datos de especialización temática.';
              const myCountryIdx = rcaData.countries.findIndex(c => c.code === selectedCountry || c.name === (summary?.country_name || selectedCountry));
              if (myCountryIdx === -1) return 'Datos RCA disponibles para la región.';
              const row = rcaData.matrix[myCountryIdx] || [];
              const lines = [
                `*Nivel:* **${rcaLevel === 'domain' ? 'Grandes Dominios' : 'Campos Disciplinares'}** | *País:* **${countryDisplayName}**\n`,
                '| Disciplina / Área | Índice RCA | Estado de Especialización (RCA > 1.0) |',
                '|---|---|---|'
              ];
              rcaData.disciplines.forEach((disc, idx) => {
                const val = row[idx] != null ? Number(row[idx]) : 0;
                lines.push(`| ${disc} | ${val.toFixed(2)} | ${val >= 1.0 ? '🌟 Ventaja Comparativa Revelada' : 'No especializado'} |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'slope_rankings',
            title: t('country.dossier_sec8_title'),
            category: t('country.dossier_sec8_cat'),
            defaultChecked: false,
            rawData: slopeData,
            buildDataText: () => {
              if (!slopeData || slopeData.length === 0) return 'No hay datos de cambios de ranking.';
              const lines = [
                '| Indicador | Posición Histórica | Posición Reciente (2021–2025) | Variación de Ranking |',
                '|---|---|---|---|'
              ];
              slopeData.forEach(s => {
                const rankDiff = (s.rank_full != null && s.rank_recent != null) ? s.rank_full - s.rank_recent : 0;
                const direction = rankDiff > 0 ? `▲ Ganó ${rankDiff} puestos` : (rankDiff < 0 ? `▼ Perdió ${Math.abs(rankDiff)} puestos` : '= Mantiene puesto');
                lines.push(`| ${s.indicator || s.label} | #${s.rank_full ?? '—'} (${s.val_full ?? '—'}) | #${s.rank_recent ?? '—'} (${s.val_recent ?? '—'}) | ${direction} |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'country_dynamic_scatter',
            title: t('country.dossier_sec9_title', { x: xLabel, y: yLabel }),
            category: t('country.dossier_sec9_cat'),
            defaultChecked: false,
            rawData: { validScatterRows, statsX, statsY, pearsonR, scatterPeriod },
            buildDataText: () => {
              if (!validScatterRows || validScatterRows.length === 0) return 'No hay datos de scatter disponibles.';
              const lines = [
                `*Periodo:* **${scatterPeriod === 'recent' ? 'Reciente (2021–2025)' : 'Completo (0–2026)'}** | *Eje X:* **${xLabel}** | *Eje Y:* **${yLabel}**\n`,
                `*Correlación Lineal de Pearson (r):* **${pearsonR != null ? pearsonR.toFixed(3) : '0.000'}** (${Math.abs(pearsonR) >= 0.7 ? 'Correlación Fuerte' : (Math.abs(pearsonR) >= 0.4 ? 'Correlación Moderada' : 'Correlación Débil o Nula')})\n`,
                '**Estadísticas Descriptivas de los Ejes:**',
                '| Eje / Variable | Media | Mediana | Desv. Estándar | Mínimo | Máximo |',
                '|---|---|---|---|---|---|',
                `| Eje X (${xLabel}) | ${statsX.mean.toFixed(2)} | ${statsX.median.toFixed(2)} | ${statsX.std.toFixed(2)} | ${statsX.min.toFixed(2)} | ${statsX.max.toFixed(2)} |`,
                `| Eje Y (${yLabel}) | ${statsY.mean.toFixed(2)} | ${statsY.median.toFixed(2)} | ${statsY.std.toFixed(2)} | ${statsY.min.toFixed(2)} | ${statsY.max.toFixed(2)} |`,
                '\n**Muestra de Revistas en el Gráfico:**',
                `| Revista | ${xLabel} | ${yLabel} | Documentos | FWCI Promedio |`,
                '|---|---|---|---|---|'
              ];
              validScatterRows.slice(0, 20).forEach(d => {
                lines.push(`| ${d.display_name} | ${Number(d[dynScatterX] || 0).toFixed(2)} | ${Number(d[dynScatterY] || 0).toFixed(2)} | ${Number(d.num_documents || 0).toLocaleString()} | ${Number(d.fwci_avg || 0).toFixed(2)} |`);
              });
              if (validScatterRows.length > 20) {
                lines.push(`\n_... y ${validScatterRows.length - 20} revistas más analizadas._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'country_oa_lang_pies',
            title: t('country.dossier_sec10_title', { period: piePeriod === 'recent' ? '2021–2025' : '0–2026' }),
            category: t('country.dossier_sec10_cat'),
            defaultChecked: false,
            rawData: { oaPieValues, langPieValues, piePeriod },
            buildDataText: () => {
              const lines = [
                `*Periodo:* **${piePeriod === 'recent' ? 'Reciente (2021–2025)' : 'Completo (0–2026)'}**\n`,
                '**Distribución por Vías de Acceso Abierto:**',
                '| Vía de Acceso | Porcentaje (%) |',
                '|---|---|'
              ];
              oaPieValues.forEach(o => {
                lines.push(`| ${o.label} | ${o.value.toFixed(1)}% |`);
              });
              lines.push('\n**Distribución por Idiomas de Publicación:**');
              lines.push('| Idioma | Porcentaje (%) |');
              lines.push('|---|---|');
              langPieValues.forEach(l => {
                lines.push(`| ${l.label} | ${l.value.toFixed(1)}% |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'country_thematic_hierarchy',
            title: t('country.dossier_sec11_title', { type: thematicViewType === 'sunburst' ? 'Sunburst Radial' : 'Treemap' }),
            category: t('country.dossier_sec11_cat'),
            defaultChecked: false,
            rawData: thematicViewType === 'sunburst' ? countrySunburst : countryTreemap,
            buildDataText: () => {
              const data = thematicViewType === 'sunburst' ? countrySunburst : countryTreemap;
              const nodes = data?.nodes || (Array.isArray(data) ? data : []);
              if (!nodes || nodes.length === 0) return 'No hay datos de taxonomía para este país.';
              const lines = [
                `*Visualización:* **${thematicViewType === 'sunburst' ? 'Sunburst' : 'Treemap'}** | *Indicador:* **${sunburstIndicator}**\n`,
                '| Rama / Área Temática | Padre | Artículos / Peso | Métrica |',
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
            id: 'journals_distribution',
            title: t('country.dossier_sec12_title', { ind: beeswarmMetric }),
            category: t('country.dossier_sec12_cat'),
            defaultChecked: false,
            rawData: journalsDist,
            buildDataText: () => {
              if (!journalsDist || journalsDist.length === 0) return 'No hay datos de distribución de revistas.';
              const lines = [
                `*Métrica de Distribución:* **${beeswarmMetric}**\n`,
                '| Revista | Valor Registrado | Artículos Publicados |',
                '|---|---|---|'
              ];
              journalsDist.slice(0, 20).forEach(j => {
                lines.push(`| ${j.display_name} | ${j[beeswarmMetric] ?? '—'} | ${j.works_count?.toLocaleString() || 0} |`);
              });
              if (journalsDist.length > 20) {
                lines.push(`\n_... y ${journalsDist.length - 20} revistas más en la distribución._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'journals_catalog',
            title: t('country.dossier_sec13_title', { count: journals.length }),
            category: t('country.dossier_sec13_cat'),
            defaultChecked: false,
            rawData: journals,
            buildDataText: () => {
              if (!journals || journals.length === 0) return 'No hay catálogo de revistas disponible.';
              const lines = [
                '| Revista | ISSN-L | Editorial / Institución | Artículos | FWCI | % Diamante | DOAJ |',
                '|---|---|---|---|---|---|---|'
              ];
              journals.slice(0, 25).forEach(j => {
                lines.push(`| ${j.display_name} | ${j.issn_l || '—'} | ${j.publisher || '—'} | ${j.works_count?.toLocaleString() || 0} | ${Number(j.fwci_avg || 0).toFixed(2)} | ${Number(j.pct_oa_diamond || 0).toFixed(1)}% | ${j.is_in_doaj ? 'Sí' : 'No'} |`);
              });
              if (journals.length > 25) {
                lines.push(`\n_... y ${journals.length - 25} revistas adicionales en el catálogo nacional._`);
              }
              return lines.join('\n');
            }
          }
        ]}
      />
    </div>
  );
}
