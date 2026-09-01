import React, { useState, useEffect } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import KpiCard from '../components/KpiCard';
import PlotlyChart from '../components/PlotlyChart';
import UmapTrajectoryViewer from '../components/UmapTrajectoryViewer';
import DossierButton from '../components/DossierButton';
import PageDossierExpander from '../components/PageDossierExpander';
import ThematicEvolutionTable from '../components/ThematicEvolutionTable';
import CountryThematicProfilesTable from '../components/CountryThematicProfilesTable';
import AnnualDataTable from '../components/AnnualDataTable';
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
  
  const [loading, setLoading] = useState(true);

  const DYNAMIC_SCATTER_INDICATORS = [
    { id: 'num_documents', label: 'Documentos' },
    { id: 'fwci_avg', label: 'FWCI Promedio' },
    { id: 'pct_top_10', label: '% Top 10%' },
    { id: 'pct_top_1', label: '% Top 1%' },
    { id: 'avg_percentile', label: 'Percentil Promedio' },
    { id: 'pct_oa_total', label: '% OA Total' },
    { id: 'pct_oa_diamond', label: '% OA Diamante' },
    { id: 'pct_oa_gold', label: '% OA Dorado' },
    { id: 'pct_oa_green', label: '% OA Verde' },
    { id: 'pct_oa_hybrid', label: '% OA Híbrido' },
    { id: 'pct_oa_bronze', label: '% OA Bronce' },
    { id: 'pct_oa_closed', label: '% Cerrado' },
    { id: 'pct_authors_domestic', label: 'Autoría Doméstica (%)' },
    { id: 'pct_lang_es', label: '% Español' },
    { id: 'pct_lang_en', label: '% Inglés' },
    { id: 'pct_lang_pt', label: '% Portugués' },
    { id: 'pct_lang_fr', label: '% Francés' },
    { id: 'pct_lang_de', label: '% Alemán' },
    { id: 'pct_lang_it', label: '% Italiano' },
    { id: 'pct_lang_other', label: '% Otros Idiomas' },
    { id: 'cited_by_count', label: 'Citas Totales' },
    { id: 'h_index', label: 'Índice h' },
    { id: 'i10_index', label: 'Índice i10' },
    { id: 'citedness_2yr', label: 'Citas 2 Años (Mean Citedness)' },
    { id: 'pagerank', label: 'PageRank Citas (‰)' },
    { id: 'eigenfactor', label: 'Eigenfactor Score (%)' }
  ];

  const SCATTER_OPTIONS = [
    { id: 'works_count', label: 'Documentos Publicados' },
    { id: 'cited_by_count', label: 'Citas Totales' },
    { id: 'fwci_avg', label: 'FWCI Promedio' },
    { id: 'h_index', label: 'Índice H' },
    { id: 'pct_oa_diamond', label: '% OA Diamante' },
  ];

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
    labels: countrySunburst.nodes.map(n => n.label),
    parents: countrySunburst.nodes.map(n => n.parent),
    values: countrySunburst.nodes.map(n => n.value),
    marker: {
      colors: countrySunburst.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true
    },
    branchvalues: 'total',
    hovertemplate: '<b>%{label}</b><br>Artículos: %{value:,.0f}<br>Color: %{color:.2f}<extra></extra>'
  }] : [];

  // Treemap Trace
  const treemapTrace = (countryTreemap && Array.isArray(countryTreemap.nodes) && countryTreemap.nodes.length > 0) ? [{
    type: 'treemap',
    ids: countryTreemap.nodes.map(n => n.id),
    labels: countryTreemap.nodes.map(n => n.label),
    parents: countryTreemap.nodes.map(n => n.parent),
    values: countryTreemap.nodes.map(n => n.value),
    marker: {
      colors: countryTreemap.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true
    },
    branchvalues: 'total',
    hovertemplate: '<b>%{label}</b><br>Artículos: %{value:,.0f}<br>Color: %{color:.2f}<extra></extra>'
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
      name: 'Artículos Publicados',
      type: 'bar',
      marker: { color: 'rgba(2, 132, 199, 0.65)' },
      yaxis: 'y'
    },
    {
      x: validAnnual.map(d => d.year),
      y: validAnnual.map(d => d.fwci_avg),
      name: 'FWCI Promedio',
      type: 'scatter',
      mode: 'lines+markers',
      line: { color: '#10b981', width: 3 },
      marker: { size: 6, color: '#10b981' },
      yaxis: 'y2'
    },
    {
      x: validAnnual.map(d => d.year),
      y: validAnnual.map(() => 1.0),
      name: 'Media Mundial (1.0)',
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
    text: journalsDist.map(d => `${d.display_name}<br>Valor: ${d[beeswarmMetric]}<br>Artículos: ${d.works_count}`),
    hoverinfo: 'text+y',
    name: summary?.country_name || selectedCountry
  }];

  // Slope Chart Traces
  const slopeTraces = [];
  slopeData.forEach((s, idx) => {
    const isClimbed = s.rank_change > 0;
    const color = isClimbed ? '#10b981' : (s.rank_change < 0 ? '#ef4444' : '#94a3b8');
    const labelMap = {
      'fwci_avg': 'FWCI Ponderado',
      'pct_oa_diamond': '% OA Diamante',
      'pct_top_10': '% Top 10%',
      'num_documents': 'Volumen de Artículos'
    };
    
    slopeTraces.push({
      x: ['Periodo Histórico', 'Reciente (2021–2025)'],
      y: [s.rank_full, s.rank_recent],
      type: 'scatter',
      mode: 'lines+markers+text',
      name: labelMap[s.indicator] || s.indicator,
      line: { color: color, width: 3 },
      marker: { size: 10, color: color },
      text: [`Puesto #${s.rank_full}`, `Puesto #${s.rank_recent}`],
      textposition: ['top left', 'top right'],
      hovertemplate: `<b>${labelMap[s.indicator] || s.indicator}</b><br>Histórico: #${s.rank_full} (${s.val_full})<br>Reciente: #${s.rank_recent} (${s.val_recent})<extra></extra>`
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
      name: 'Otros Artículos LATAM',
      marker: { size: 3.5, color: '#94a3b8', opacity: 0.22 },
      hoverinfo: 'skip'
    });
  }

  if (countryArts.length > 0) {
    const years = countryArts.map(a => Number(a.publication_year || 0)).filter(y => y > 0);
    const minYr = years.length ? Math.min(...years) : 1990;
    const maxYr = years.length ? Math.max(...years) : 2026;

    landscapeTraces.push({
      x: countryArts.map(a => a.umap_x),
      y: countryArts.map(a => a.umap_y),
      mode: 'markers',
      type: 'scatter',
      name: `Artículos de ${summary?.country_name || selectedCountry}`,
      marker: {
        size: 5.5,
        color: countryArts.map(a => a.publication_year),
        colorscale: 'Turbo',
        cmin: minYr,
        cmax: maxYr,
        colorbar: { title: 'Año de Publ.', x: 1.02 },
        opacity: 0.85,
        line: { width: 0.3, color: '#ffffff' }
      },
      text: countryArts.map(a => a.title),
      customdata: countryArts.map(a => [
        a.journal_name || 'Desconocida',
        a.publication_year || '—',
        a.fwci != null ? Number(a.fwci).toFixed(2) : '—',
        a.community_name || 'General'
      ]),
      hovertemplate: '<b>%{text}</b><br>Revista: %{customdata[0]}<br>Año: %{customdata[1]} | FWCI: %{customdata[2]}<br>Comunidad: %{customdata[3]}<extra></extra>'
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
    { label: 'Diamante', value: Number(activePieData.pct_oa_diamond || 0), color: '#38bdf8' },
    { label: 'Gold', value: Number(activePieData.pct_oa_gold || 0), color: '#fbbf24' },
    { label: 'Verde', value: Number(activePieData.pct_oa_green || 0), color: '#34d399' },
    { label: 'Híbrido', value: Number(activePieData.pct_oa_hybrid || 0), color: '#a78bfa' },
    { label: 'Bronce', value: Number(activePieData.pct_oa_bronze || 0), color: '#fb923c' },
    { label: 'Cerrado', value: Number(activePieData.pct_oa_closed || 0), color: '#f87171' },
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
    { label: 'Español', value: Number(activePieData.pct_lang_es || 0), color: '#a855f7' },
    { label: 'Inglés', value: Number(activePieData.pct_lang_en || 0), color: '#38bdf8' },
    { label: 'Portugués', value: Number(activePieData.pct_lang_pt || 0), color: '#f59e0b' },
    { label: 'Francés', value: Number(activePieData.pct_lang_fr || 0), color: '#ec4899' },
    { label: 'Alemán', value: Number(activePieData.pct_lang_de || 0), color: '#10b981' },
    { label: 'Italiano', value: Number(activePieData.pct_lang_it || 0), color: '#6366f1' },
    { label: 'Otros', value: Number(activePieData.pct_lang_other || 0), color: '#94a3b8' },
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
            {summary?.country_name || selectedCountry} ({selectedCountry})
          </h2>
          <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
            Perfil cienciométrico institucional, producción, impacto y posicionamiento en OpenAlex.
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <label style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text-muted)' }}>
            Seleccionar País:
          </label>
          <select
            value={selectedCountry}
            onChange={(e) => setSelectedCountry(e.target.value)}
            style={{ fontSize: '14px', fontWeight: '700', padding: '8px 16px' }}
          >
            {countriesList.map(c => (
              <option key={c.country_code} value={c.country_code}>
                {c.country_name} ({c.country_code}) — {c.num_journals} revistas
              </option>
            ))}
          </select>
          <DossierButton
            item={{
              key: `country_summary_${selectedCountry}`,
              title: `Perfil: ${summary?.country_name || selectedCountry}`,
              context: `${summary?.num_journals?.toLocaleString()} revistas · ${summary?.total_works?.toLocaleString()} artículos · FWCI ${pData?.fwci_avg ?? '—'} · OA Diamante ${pData?.pct_oa_diamond ?? '—'}%`,
              category: 'Perfil País',
              data: pData ? [pData] : []
            }}
            label="Guardar en Contexto IA"
          />
          <button
            onClick={handleShareCountry}
            title="Copiar enlace directo para compartir este país"
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
            <span>{copiedLink ? '¡Enlace copiado!' : 'Compartir'}</span>
          </button>
        </div>
      </div>

      {/* Top Contextual KPI Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px' }}>
        <KpiCard
          title="Revistas Activas"
          value={summary?.num_journals?.toLocaleString()}
          subtitle="En OpenAlex Snapshot"
          icon={BookOpen}
        />
        <KpiCard
          title="Total Artículos"
          value={summary?.total_works?.toLocaleString() || summary?.full_period?.num_documents?.toLocaleString()}
          subtitle="Producción Histórica"
          icon={FileText}
        />
        <KpiCard
          title="FWCI Promedio"
          value={pData?.fwci_avg != null ? Number(pData.fwci_avg).toFixed(2) : '0.00'}
          subtitle="Impacto Normalizado"
          icon={Zap}
        />
        <KpiCard
          title="% OA Diamante"
          value={`${pData?.pct_oa_diamond ?? summary?.full_period?.pct_oa_diamond ?? 0}%`}
          subtitle="Sin Cobro por APC"
          icon={Sparkles}
          badge="Diamante"
        />
        <KpiCard
          title="Revistas DOAJ"
          value={`${pData?.pct_doaj ?? summary?.full_period?.pct_doaj ?? 0}%`}
          subtitle="Con Sello Abierto"
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
                      📊 Impacto y Citación (Histórico)
                    </span>
                  </div>
                  <span className="badge" style={{ fontSize: '11px' }}>0–2026</span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '14px' }}>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Documentos</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {fullDocs.toLocaleString()}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>FWCI Promedio</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--primary-color, #3b82f6)', marginTop: '2px' }}>
                      {fullFwci.toFixed(2)}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% Top 10%</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#f59e0b', marginTop: '2px' }}>
                      {fullTop10.toFixed(2)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% Top 1%</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#ec4899', marginTop: '2px' }}>
                      {fullTop1.toFixed(3)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Percentil Prom. Norm.</div>
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
                      ⚡ Periodo Reciente: 2021–2025
                    </span>
                  </div>
                  <span className="badge" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981', border: '1px solid #10b981', fontSize: '11px' }}>
                    Último Lustro
                  </span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '14px' }}>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Documentos</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {recDocs.toLocaleString()}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>FWCI Promedio</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      {recFwci.toFixed(2)}
                      <span style={{ fontSize: '11px', color: '#10b981', fontWeight: '700' }}>
                        ({Number(fwciDelta) >= 0 ? '+' : ''}{fwciDelta})
                      </span>
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% Top 10%</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#f59e0b', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      {recTop10.toFixed(2)}%
                      <span style={{ fontSize: '11px', color: Number(top10Delta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                        ({Number(top10Delta) >= 0 ? '+' : ''}{top10Delta}%)
                      </span>
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% Top 1%</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#ec4899', marginTop: '2px' }}>
                      {recTop1.toFixed(3)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Percentil Prom. Norm.</div>
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
                      🔓 Ciencia Abierta y Visibilidad
                    </span>
                  </div>
                  <span className="badge" style={{ fontSize: '11px' }}>Acceso e Indexación</span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))', gap: '12px' }}>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% OA Diamante</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#38bdf8', marginTop: '2px' }}>
                      {Number(fullP.pct_oa_diamond || 0).toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% OA Gold</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {Number(fullP.pct_oa_gold || 0).toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% OA Verde</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#34d399', marginTop: '2px' }}>
                      {Number(fullP.pct_oa_green || 0).toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% en Scopus</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                      {Number(fullP.pct_scopus || 0).toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% en DOAJ</div>
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
                      🌐 Distribución Lingüística de Publicación
                    </span>
                  </div>
                  <span className="badge" style={{ fontSize: '11px' }}>Idiomas</span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))', gap: '12px' }}>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% Español</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#a855f7', marginTop: '2px' }}>
                      {langEs.toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% Inglés</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#38bdf8', marginTop: '2px' }}>
                      {langEn.toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% Portugués</div>
                    <div style={{ fontSize: '18px', fontWeight: '800', color: '#f59e0b', marginTop: '2px' }}>
                      {langPt.toFixed(1)}%
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% Otros Idiomas</div>
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
              Gráfico de Eje Dual (Dual-Axis Chart) — Producción Anual vs FWCI Ponderado
            </h3>
          </div>
          <DossierButton
            item={{
              key: `country_annual_${selectedCountry}`,
              title: `Tendencia Anual: ${summary?.country_name || selectedCountry}`,
              context: 'Evolución anual de producción y FWCI ponderado.',
              category: 'Tendencias Anuales',
              data: annualTrends.slice(0, 30)
            }}
          />
        </div>
        <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          Correlación temporal entre el volumen de artículos publicados (barras azules) y el impacto de citación normalizado (línea verde).
        </p>

        <PlotlyChart
          data={dualAxisTraces}
          layout={{
            height: 380,
            margin: { l: 60, r: 60, t: 20, b: 40 },
            xaxis: { title: 'Año de Publicación' },
            yaxis: { title: 'Artículos Publicados', side: 'left', showgrid: true },
            yaxis2: {
              title: 'FWCI Promedio',
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
        countryName={summary?.country_name || selectedCountry}
      />

      {/* 1. TRAYECTORIA DE DESEMPEÑO (UMAP PAÍS VS LATAM) */}
      {trajectory && Object.keys(trajectory).length > 0 && (
        <UmapTrajectoryViewer
          title={`📈 Trayectoria de Desempeño: ${summary?.country_name || selectedCountry} vs Región LATAM (2000–2025)`}
          subtitle="Evolución multidimensional del país proyectada en el espacio UMAP con la referencia regional continua y mapas de calor gaussianos por indicador."
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
            title={`🌌 Mapa UMAP de Similitud de Revistas (${summary?.country_name || selectedCountry})`}
            subtitle={`Distribución topológica 2D de las ${umapJournals.length} revistas científicas del país según sus indicadores de desempeño (FWCI, % Diamante, % Top 10%, % Inglés).`}
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
                  📊 Ver tabla de datos UMAP (Revistas)
                </h4>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    if (!umapJournals || umapJournals.length === 0) return;
                    const headers = ['Revista', 'Documentos', '% Inglés', '% OA Diamante', 'FWCI Promedio', '% Top 10%', '% Top 1%', 'Percentil Promedio'];
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
                  <Download size={13} /> Descargar CSV
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
                    placeholder="🔍 Buscar revista..."
                    value={umapTableSearch}
                    onChange={(e) => setUmapTableSearch(e.target.value)}
                    style={{ paddingLeft: '32px', width: '100%', fontSize: '12.5px', borderRadius: '6px' }}
                  />
                </div>

                <div className="data-table-container" style={{ maxHeight: '360px' }}>
                  <table className="data-table" style={{ fontSize: '12px' }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: 'left' }}>Revista</th>
                        <th style={{ textAlign: 'right' }}>Documentos</th>
                        <th style={{ textAlign: 'right' }}>% Inglés</th>
                        <th style={{ textAlign: 'right' }}>% OA Diamante</th>
                        <th style={{ textAlign: 'right' }}>FWCI Promedio</th>
                        <th style={{ textAlign: 'right' }}>% Top 10%</th>
                        <th style={{ textAlign: 'right' }}>% Top 1%</th>
                        <th style={{ textAlign: 'right' }}>Percentil Promedio</th>
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
                🌌 Evolución de {summary?.country_name || selectedCountry} en el Paisaje Científico (LATAM)
              </h3>
            </div>
            <span className="badge" style={{ fontSize: '11px' }}>
              {countryArts.length} Artículos del País
            </span>
          </div>
          <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginBottom: '14px' }}>
            Mapeo de los artículos de revistas de {summary?.country_name || selectedCountry} sobre el espacio temático global de Latinoamérica. La barra de color ilustra la progresión temporal de las publicaciones.
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
            💡 <strong>Interpretación Temporal:</strong> Los frentes temáticos ocupados por puntos amarillos y rojos indican las líneas científicas de mayor publicación reciente en {summary?.country_name || selectedCountry}, mientras que los puntos azules/morados representan áreas fundacionales históricas.
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
                Mapa de Calor (Heat Map) — Índice de Especialización Científica (RCA)
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Ventaja Comparativa Revelada (RCA &gt; 1.0 en verde indica especialización relativa respecto a toda Latinoamérica).
            </span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${rcaLevel === 'domain' ? 'active' : ''}`}
              onClick={() => setRcaLevel('domain')}
            >
              Grandes Dominios (6)
            </button>
            <button
              className={`segmented-pill-btn ${rcaLevel === 'field' ? 'active' : ''}`}
              onClick={() => setRcaLevel('field')}
            >
              Campos Disciplinares (28)
            </button>
          </div>
        </div>

        {rcaData && rcaData.countries && (
          <PlotlyChart
            data={[{
              type: 'heatmap',
              z: rcaData.matrix,
              x: rcaData.disciplines,
              y: rcaData.countries.map(c => c.name),
              colorscale: 'YlGnBu',
              colorbar: { title: 'Índice RCA' },
              hovertemplate: '<b>País:</b> %{y}<br><b>Disciplina:</b> %{x}<br><b>RCA:</b> %{z:.2f}<extra></extra>'
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
              🔬 Dispersión de Revistas (Beeswarm / Strip Plot)
            </h3>
            <select
              value={beeswarmMetric}
              onChange={(e) => setBeeswarmMetric(e.target.value)}
              style={{ fontSize: '12px', fontWeight: '600' }}
            >
              <option value="fwci_avg">FWCI Promedio</option>
              <option value="pct_oa_diamond">% OA Diamante</option>
              <option value="h_index">Índice H</option>
              <option value="works_count">Artículos Publicados</option>
            </select>
          </div>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            Cada punto representa una revista individual del país distribuida según su desempeño cienciométrico.
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
            📈 Gráfico de Pendientes (Slope Chart) — Movimiento de Rankings
          </h3>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            Ascenso o descenso en la posición relativa dentro de LATAM (Histórico vs 2021–2025).
          </p>
          <PlotlyChart
            data={slopeTraces}
            layout={{
              height: 340,
              margin: { l: 80, r: 80, t: 20, b: 30 },
              yaxis: { autorange: 'reversed', title: 'Puesto en el Ranking LATAM' },
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
                Explorador de Revistas — Scatter Plot Dinámico
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Visualiza la relación entre diferentes indicadores bibliométricos para las revistas de {summary?.country_name || selectedCountry} ({validScatterRows.length} revistas).
            </span>
          </div>

          {/* Period Selector */}
          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${scatterPeriod === 'recent' ? 'active' : ''}`}
              onClick={() => setScatterPeriod('recent')}
            >
              Período Reciente (2021–2025)
            </button>
            <button
              className={`segmented-pill-btn ${scatterPeriod === 'full' ? 'active' : ''}`}
              onClick={() => setScatterPeriod('full')}
            >
              Período Completo
            </button>
          </div>
        </div>

        {/* Axis Selectors */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: '14px', marginBottom: '14px', padding: '12px 16px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: '700', marginBottom: '4px', color: 'var(--text-main)' }}>
              Indicador Eje X:
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
              Indicador Eje Y:
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
                Estadísticas: {xLabel}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px', color: 'var(--text-muted)' }}>
                <span>Media: <strong>{statsX.mean.toFixed(2)}</strong></span>
                <span>Mediana: <strong>{statsX.median.toFixed(2)}</strong></span>
                <span>Desv. Est.: <strong>{statsX.std.toFixed(2)}</strong></span>
                <span>Mín / Máx: <strong>{statsX.min.toFixed(2)} / {statsX.max.toFixed(2)}</strong></span>
              </div>
            </div>

            <div style={{ padding: '12px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '12px' }}>
              <div style={{ fontWeight: '700', color: 'var(--text-main)', marginBottom: '6px' }}>
                Estadísticas: {yLabel}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px', color: 'var(--text-muted)' }}>
                <span>Media: <strong>{statsY.mean.toFixed(2)}</strong></span>
                <span>Mediana: <strong>{statsY.median.toFixed(2)}</strong></span>
                <span>Desv. Est.: <strong>{statsY.std.toFixed(2)}</strong></span>
                <span>Mín / Máx: <strong>{statsY.min.toFixed(2)} / {statsY.max.toFixed(2)}</strong></span>
              </div>
            </div>

            <div style={{ padding: '12px 14px', background: 'rgba(2, 132, 199, 0.1)', borderRadius: '8px', border: '1px solid rgba(2, 132, 199, 0.3)', display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
              <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Correlación Lineal (Pearson)</span>
              <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--accent-primary)', marginTop: '2px' }}>
                r = {pearsonR != null ? pearsonR.toFixed(3) : '0.000'}
              </div>
              <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '2px' }}>
                {Math.abs(pearsonR) >= 0.7 ? 'Correlación Fuerte' : (Math.abs(pearsonR) >= 0.4 ? 'Correlación Moderada' : 'Correlación Débil / Nula')}
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
                Distribución y Características de las Publicaciones
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Composición porcentual de modalidades de acceso abierto e idiomas de publicación en revistas de {summary?.country_name || selectedCountry}.
            </span>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${piePeriod === 'full' ? 'active' : ''}`}
              onClick={() => setPiePeriod('full')}
            >
              Período Completo (0–2026)
            </button>
            <button
              className={`segmented-pill-btn ${piePeriod === 'recent' ? 'active' : ''}`}
              onClick={() => setPiePeriod('recent')}
            >
              Período Reciente (2021–2025)
            </button>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: '20px' }}>
          {/* OA Pie */}
          <div style={{ background: 'var(--bg-input)', borderRadius: '10px', padding: '16px', border: '1px solid var(--border-color)' }}>
            <h4 style={{ fontSize: '14px', fontWeight: '700', textAlign: 'center', marginBottom: '8px', color: 'var(--text-main)' }}>
              🔓 Distribución por Acceso Abierto
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
                Sin datos de acceso abierto disponibles.
              </div>
            )}
          </div>

          {/* Language Pie */}
          <div style={{ background: 'var(--bg-input)', borderRadius: '10px', padding: '16px', border: '1px solid var(--border-color)' }}>
            <h4 style={{ fontSize: '14px', fontWeight: '700', textAlign: 'center', marginBottom: '8px', color: 'var(--text-main)' }}>
              🌐 Distribución por Idiomas
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
                Sin datos de idioma disponibles.
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
              🏵️ Jerarquía Temática Nacional: Dominio → Campo → Subcampo
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Especialización temática y masa crítica de artículos del país. Alterna entre la vista radial (Sunburst) y la vista rectangular compacta (Treemap).
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
            {/* View Switcher: Sunburst vs Treemap */}
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

            {/* Indicator Picker */}
            <select
              value={sunburstIndicator}
              onChange={(e) => setSunburstIndicator(e.target.value)}
              style={{ fontWeight: '600' }}
            >
              <option value="fwci_avg_recent">FWCI (2021-2025)</option>
              <option value="avg_percentile_recent">Percentil (2021-2025)</option>
              <option value="pct_top_10_recent">% Top 10% (2021-2025)</option>
              <option value="pct_oa_gold_recent">% OA Gold (2021-2025)</option>
              <option value="fwci_avg_full">FWCI (Todo el Periodo)</option>
              <option value="avg_percentile_full">Percentil (Todo el Periodo)</option>
              <option value="pct_top_10_full">% Top 10% (Todo el Periodo)</option>
              <option value="pct_oa_gold_full">% OA Gold (Todo el Periodo)</option>
            </select>

            {/* Include Unclassified Checkbox */}
            <label style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: 'var(--text-muted)', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={sunburstUnclassified}
                onChange={(e) => setSunburstUnclassified(e.target.checked)}
              />
              Sin Clasificación
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
        countryName={summary?.country_name || selectedCountry} 
      />

      {/* 7. EVOLUCIÓN HISTÓRICA DE PERFILES DE CONOCIMIENTO DEL PAÍS (DOMINIO, CAMPO, SUBCAMPO, TÓPICO) */}
      <ThematicEvolutionTable 
        countryCode={selectedCountry} 
        countryName={summary?.country_name || selectedCountry} 
      />

      {/* Journals Catalog Table */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              📚 Catálogo de Revistas de {summary?.country_name || selectedCountry} ({journals.length})
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Detalle de publicaciones científicas registradas para este país.
            </span>
          </div>

          <button
            className="btn-secondary"
            onClick={() => {
              if (!journals || journals.length === 0) return;
              const headers = ['Revista', 'OpenAlex_ID', 'ISSN_L', 'Editorial_Institucion', 'Articulos', 'Citas', 'FWCI', 'H_Index', 'Pct_OA_Diamante', 'En_DOAJ'];
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
                j.is_in_doaj ? 'Si' : 'No'
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
            title="Descargar catálogo de revistas en formato CSV"
          >
            <Download size={14} /> Descargar CSV
          </button>
        </div>

        <div className="data-table-container" style={{ maxHeight: '420px' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Revista</th>
                <th>ISSN-L</th>
                <th>Editorial / Institución</th>
                <th>Artículos</th>
                <th>Citas</th>
                <th>FWCI</th>
                <th>H-Index</th>
                <th>% Diamante</th>
                <th>DOAJ</th>
                <th>Acción</th>
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
                      Ver Detalle
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
        pageTitle={`Perfil y Diagnóstico Cienciométrico: ${summary?.country_name || selectedCountry}`}
        pageDescription={`Selecciona cualquiera de las gráficas, tablas o indicadores de ${summary?.country_name || selectedCountry} para generar un reporte integral o consultar a ChatGPT.`}
        sections={[
          {
            id: 'country_kpis',
            title: `1. Indicadores Clave y Perfil Macro (${summary?.country_name || selectedCountry})`,
            category: 'KPIs Principales',
            defaultChecked: true,
            rawData: { summary, pData },
            buildDataText: () => {
              if (!summary && !pData) return 'No hay datos de perfil disponibles.';
              return [
                `*País:* **${summary?.country_name || selectedCountry} (${selectedCountry})**\n`,
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
            title: '2. Panel Consolidado de Desempeño, Impacto y Ciencia Abierta (Histórico vs 2021–2025)',
            category: 'Indicadores de Desempeño',
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
            title: `3. Gráfico de Eje Dual — Producción Anual vs FWCI Ponderado (1970–2026)`,
            category: 'Series de Tiempo',
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
            title: `4. Trayectoria de Desempeño UMAP (${summary?.country_name || selectedCountry} vs Región LATAM 2000–2025)`,
            category: 'Variedades Semánticas / UMAP',
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
            title: `5. Mapa UMAP y Tabla de Similitud de Revistas (${umapJournals.length} revistas)`,
            category: 'Variedades Semánticas / UMAP',
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
            title: `6. Evolución en el Paisaje Científico LATAM (${countryArts.length} artículos del país)`,
            category: 'Variedades Semánticas / UMAP',
            defaultChecked: false,
            rawData: landscapeArticles,
            buildDataText: () => {
              if (!countryArts || countryArts.length === 0) return 'No hay artículos proyectados en el mapa semántico regional.';
              const lines = [
                `*Muestra:* **${countryArts.length.toLocaleString()} artículos de ${summary?.country_name || selectedCountry}** proyectados sobre el espacio temático latinoamericano.\n`,
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
            title: '7. Heatmap de Especialización Temática (Ventajas Comparativas Reveladas - RCA)',
            category: 'Especialización Temática',
            defaultChecked: false,
            rawData: rcaData,
            buildDataText: () => {
              if (!rcaData || !rcaData.countries || !rcaData.disciplines) return 'No hay datos de especialización temática.';
              const myCountryIdx = rcaData.countries.findIndex(c => c.code === selectedCountry || c.name === (summary?.country_name || selectedCountry));
              if (myCountryIdx === -1) return 'Datos RCA disponibles para la región.';
              const row = rcaData.matrix[myCountryIdx] || [];
              const lines = [
                `*Nivel:* **${rcaLevel === 'domain' ? 'Grandes Dominios' : 'Campos Disciplinares'}** | *País:* **${summary?.country_name || selectedCountry}**\n`,
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
            title: '8. Gráfico de Pendiente (Slope Chart) — Cambios de Posición en Rankings LATAM',
            category: 'Evolución de Posicionamiento',
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
            title: `9. Explorador de Revistas — Scatter Plot Dinámico y Correlación (${xLabel} vs ${yLabel})`,
            category: 'Correlaciones Multivariadas',
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
            title: `10. Distribución y Características de las Publicaciones (Pasteles OA e Idiomas - ${piePeriod === 'recent' ? '2021–2025' : '0–2026'})`,
            category: 'Distribuciones',
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
            title: `11. Estructura Temática Jerárquica del País (${thematicViewType === 'sunburst' ? 'Sunburst Radial' : 'Treemap'})`,
            category: 'Taxonomía Científica',
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
            title: `12. Distribución de Revistas por Desempeño (Beeswarm / Jitter - ${beeswarmMetric})`,
            category: 'Distribuciones',
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
            title: `13. Catálogo Completo de Revistas (${journals.length} revistas)`,
            category: 'Catálogo de Publicaciones',
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
