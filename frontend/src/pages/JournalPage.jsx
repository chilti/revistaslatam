import React, { useState, useEffect, useRef } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import KpiCard from '../components/KpiCard';
import PlotlyChart from '../components/PlotlyChart';
import UmapTrajectoryViewer from '../components/UmapTrajectoryViewer';
import ThematicEvolutionTable from '../components/ThematicEvolutionTable';
import AnnualDataTable from '../components/AnnualDataTable';
import DossierButton from '../components/DossierButton';
import PageDossierExpander from '../components/PageDossierExpander';
import { 
  Search, 
  BookOpen, 
  FileText, 
  Zap, 
  TrendingUp, 
  Sparkles, 
  ExternalLink, 
  Layers, 
  Award, 
  Share2,
  Check,
  Calendar,
  Globe,
  Globe2,
  ListFilter,
  Radar,
  Activity,
  BoxSelect,
  GitCommit,
  Download,
  PieChart,
  Compass
} from 'lucide-react';

const DEFAULT_COUNTRIES = [
  { country_code: 'MX', country_name: 'México', num_journals: 593 },
  { country_code: 'BR', country_name: 'Brasil', num_journals: 3883 },
  { country_code: 'CO', country_name: 'Colombia', num_journals: 895 },
  { country_code: 'AR', country_name: 'Argentina', num_journals: 450 },
  { country_code: 'CL', country_name: 'Chile', num_journals: 376 },
  { country_code: 'PE', country_name: 'Perú', num_journals: 437 },
  { country_code: 'EC', country_name: 'Ecuador', num_journals: 278 },
  { country_code: 'CR', country_name: 'Costa Rica', num_journals: 109 },
  { country_code: 'VE', country_name: 'Venezuela', num_journals: 82 },
  { country_code: 'BO', country_name: 'Bolivia', num_journals: 70 },
  { country_code: 'CU', country_name: 'Cuba', num_journals: 65 },
  { country_code: 'UY', country_name: 'Uruguay', num_journals: 61 },
  { country_code: 'PY', country_name: 'Paraguay', num_journals: 41 },
  { country_code: 'PA', country_name: 'Panamá', num_journals: 34 },
  { country_code: 'GT', country_name: 'Guatemala', num_journals: 27 },
  { country_code: 'NI', country_name: 'Nicaragua', num_journals: 26 },
  { country_code: 'HN', country_name: 'Honduras', num_journals: 21 },
  { country_code: 'SV', country_name: 'El Salvador', num_journals: 20 },
  { country_code: 'DO', country_name: 'República Dominicana', num_journals: 14 },
  { country_code: 'PR', country_name: 'Puerto Rico', num_journals: 12 },
];

const DEFAULT_INITIAL_JOURNALS = [
  { id: 'https://openalex.org/S2737081250', display_name: 'Estudios Demográficos y Urbanos', country_code: 'MX', works_count: 1995 }
];

const ARTICLE_SCATTER_INDICATORS = [
  { id: 'fwci', label: 'FWCI' },
  { id: 'cited_by_count', label: 'Citas' },
  { id: 'percentile', label: 'Percentil' },
  { id: 'publication_year', label: 'Año de Publicación' },
  { id: 'is_in_top_10_percent', label: 'Top 10% (0/1)' },
  { id: 'is_in_top_1_percent', label: 'Top 1% (0/1)' },
  { id: 'is_domestic_author', label: 'Autoría Doméstica (0/1)' },
  { id: 'is_retracted', label: 'Retractado (0/1)' },
  { id: 'is_paratext', label: 'Paratexto (0/1)' }
];

export default function JournalPage() {
  const {
    selectedJournalId,
    selectedJournalName,
    setSelectedJournal,
    addDossierItem,
    addExportJob,
    setDownloadsOpen,
    requireAuth
  } = useAppStore();
  
  // Share link state
  const [copiedLink, setCopiedLink] = useState(false);
  const [toastMessage, setToastMessage] = useState(null);

  const handleShareJournal = () => {
    const cleanId = selectedJournalId ? (selectedJournalId.includes('/') ? selectedJournalId.split('/').pop() : selectedJournalId) : '';
    const url = `${window.location.origin}${window.location.pathname}?section=journal&journal_id=${encodeURIComponent(cleanId)}`;
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

  // Search state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  
  // Country & Journal Combo state
  const [countriesList, setCountriesList] = useState(DEFAULT_COUNTRIES);
  const [filterCountry, setFilterCountry] = useState('ALL');
  const [countryJournals, setCountryJournals] = useState(DEFAULT_INITIAL_JOURNALS);
  const [loadingJournals, setLoadingJournals] = useState(false);
  
  // Journal data
  const [details, setDetails] = useState(null);
  const [annualTrends, setAnnualTrends] = useState([]);
  const [thematicViewType, setThematicViewType] = useState('sunburst'); // 'sunburst' | 'treemap'
  const [sunburstData, setSunburstData] = useState(null);
  const [treemapData, setTreemapData] = useState(null);
  const [sunburstInd, setSunburstInd] = useState('fwci_avg_recent');
  const [sunburstUnclassified, setSunburstUnclassified] = useState(true);
  const [piePeriod, setPiePeriod] = useState('full'); // 'full' | 'recent'
  
  // Article Dynamic Scatter Plot state
  const [artScatterX, setArtScatterX] = useState('fwci');
  const [artScatterY, setArtScatterY] = useState('cited_by_count');
  const [scatterArticles, setScatterArticles] = useState([]);
  const [showAllArticlesScatter, setShowAllArticlesScatter] = useState(false);
  
  // New Visualizations data
  const [radarData, setRadarData] = useState(null);
  const [citationsDist, setCitationsDist] = useState({ citations: [], fwci: [], percentiles: [], years: [] });
  const [distPlotType, setDistPlotType] = useState('box'); // 'box' | 'violin'
  const [connectedTraj, setConnectedTraj] = useState([]);
  
  const [articles, setArticles] = useState([]);
  const [articleSort, setArticleSort] = useState('cited_by_count');
  const [articleYearFilter, setArticleYearFilter] = useState('');
  const [articleLimit, setArticleLimit] = useState(100);
  const [loadingArticles, setLoadingArticles] = useState(false);
  
  const [landscapeData, setLandscapeData] = useState({ articles: [], bg_articles: [], dispersion: 0 });
  const [globalBgArts, setGlobalBgArts] = useState([]);
  const [trajectory, setTrajectory] = useState({});
  const [loading, setLoading] = useState(true);
  const [exportingFormat, setExportingFormat] = useState(null);

  const handleExportArticles = async (format) => {
    if (!requireAuth('download_articles')) {
      return;
    }
    if (!selectedJournalId || exportingFormat) return;
    const cleanJid = selectedJournalId.includes('/') ? selectedJournalId.split('/').pop() : selectedJournalId;
    const journalTitle = (details && details.profile && details.profile.display_name) || selectedJournalName || 'Revista';
    
    setExportingFormat(format);
    try {
      const yearVal = (articleYearFilter && !isNaN(parseInt(articleYearFilter))) ? parseInt(articleYearFilter) : null;
      const payload = {
        journal_id: cleanJid,
        format: format,
        year_min: yearVal,
        year_max: yearVal,
        title: `${journalTitle} (${format.toUpperCase()})`
      };
      
      const res = await api.post('/exports/start', payload);
      if (res.data && res.data.job) {
        addExportJob(res.data.job);
        setDownloadsOpen(true);
        setToastMessage(`Exportación iniciada para "${journalTitle}". Revisa el panel de descargas.`);
        setTimeout(() => setToastMessage(null), 6000);
      }
    } catch (err) {
      console.error('Export start error:', err);
      const detail = err.response?.data?.detail || err.message || 'Error desconocido';
      alert(`Error al iniciar la exportación en segundo plano: ${detail}`);
    } finally {
      setExportingFormat(null);
    }
  };

  // Load countries catalog
  useEffect(() => {
    api.get('/countries')
      .then(res => {
        if (res.data && Array.isArray(res.data) && res.data.length > 0) {
          setCountriesList(res.data);
        }
      })
      .catch(console.error);
  }, []);

  // Fetch journals whenever filterCountry changes
  useEffect(() => {
    const code = filterCountry || 'ALL';
    setLoadingJournals(true);
    
    const fetchPromise = code === 'ALL'
      ? api.get('/journals/search?limit=300')
      : api.get(`/countries/${code}/journals`);

    fetchPromise
      .then(res => {
        const jList = res.data || [];
        setCountryJournals(jList);
        if (jList.length > 0 && !selectedJournalId) {
          setSelectedJournal(jList[0].id, jList[0].display_name);
        }
      })
      .catch(err => {
        console.error('Error fetching country journals:', err);
      })
      .finally(() => {
        setLoadingJournals(false);
      });
  }, [filterCountry]);

  // Handle Search Input Debounce
  useEffect(() => {
    if (!searchQuery.trim()) {
      setSearchResults([]);
      return;
    }
    const timer = setTimeout(() => {
      setIsSearching(true);
      api.get(`/journals/search?q=${encodeURIComponent(searchQuery)}&limit=10`)
        .then(res => setSearchResults(res.data || []))
        .catch(console.error)
        .finally(() => setIsSearching(false));
    }, 250);
    return () => clearTimeout(timer);
  }, [searchQuery]);

  // Load journal details
  useEffect(() => {
    if (!selectedJournalId) return;
    setLoading(true);

    const cleanJid = selectedJournalId.includes('/') ? selectedJournalId.split('/').pop() : selectedJournalId;
    const jidParam = encodeURIComponent(cleanJid);

    Promise.all([
      api.get(`/journals/${jidParam}/details`),
      api.get(`/journals/${jidParam}/annual?min_year=1970&max_year=2026`),
      api.get(`/journals/${jidParam}/articles?sort_by=${articleSort}${articleYearFilter ? `&year=${articleYearFilter}` : ''}&limit=${articleLimit}`),
      api.get(`/journals/${jidParam}/articles?limit=0`),
      api.get(`/journals/${jidParam}/landscape`),
      api.get(`/journals/${jidParam}/trajectory`),
      api.get(`/journals/${jidParam}/radar-profile`),
      api.get(`/journals/${jidParam}/citations-distribution`),
      api.get(`/journals/${jidParam}/connected-trajectory`)
    ]).then(([detRes, annRes, artRes, allArtRes, landRes, trajRes, radarRes, citRes, connRes]) => {
      setDetails(detRes.data);
      setAnnualTrends(annRes.data || []);
      setArticles(artRes.data || []);
      setScatterArticles(allArtRes.data || []);
      setLandscapeData(landRes.data || { articles: [], bg_articles: [], dispersion: 0 });
      // If bg_articles is empty (parquet has no background), load from global map
      if (!landRes.data?.bg_articles?.length) {
        api.get('/maps/articles?limit=5000').then(r => setGlobalBgArts(r.data || [])).catch(() => {});
      }
      setTrajectory(trajRes.data || {});
      setRadarData(radarRes.data);
      setCitationsDist(citRes.data || { citations: [], fwci: [] });
      setConnectedTraj(connRes.data || []);

      // If details has country_code and filterCountry differs, sync
      if (detRes.data?.country_code && detRes.data.country_code !== filterCountry && filterCountry !== 'ALL') {
        setFilterCountry(detRes.data.country_code);
      }
    }).catch(console.error).finally(() => setLoading(false));
  }, [selectedJournalId]);

  // Reload articles on sort/year/limit change
  useEffect(() => {
    if (!selectedJournalId || loading) return;
    const cleanJid = selectedJournalId.includes('/') ? selectedJournalId.split('/').pop() : selectedJournalId;
    const jidParam = encodeURIComponent(cleanJid);
    setLoadingArticles(true);
    api.get(`/journals/${jidParam}/articles?sort_by=${articleSort}${articleYearFilter ? `&year=${articleYearFilter}` : ''}&limit=${articleLimit}`)
      .then(res => setArticles(res.data || []))
      .catch(console.error)
      .finally(() => setLoadingArticles(false));
  }, [articleSort, articleYearFilter, articleLimit]);

  // Load sunburst / treemap
  useEffect(() => {
    if (!selectedJournalId) return;
    const cleanJid = selectedJournalId.includes('/') ? selectedJournalId.split('/').pop() : selectedJournalId;
    const jidParam = encodeURIComponent(cleanJid);
    if (thematicViewType === 'sunburst') {
      api.get(`/journals/${jidParam}/sunburst?indicator=${sunburstInd}&include_unclassified=${sunburstUnclassified}`)
        .then(res => setSunburstData(res.data))
        .catch(console.error);
    } else {
      api.get(`/journals/${jidParam}/treemap?indicator=${sunburstInd}&include_unclassified=${sunburstUnclassified}`)
        .then(res => setTreemapData(res.data))
        .catch(console.error);
    }
  }, [selectedJournalId, thematicViewType, sunburstInd, sunburstUnclassified]);


  const pData = details?.full_period || {};
  const recData = details?.recent_period || {};
  const prof = details?.profile || {};

  const fullDocs = pData.num_documents ?? prof.works_count ?? 0;
  const recDocs  = recData.num_documents ?? 0;
  const pctRecDocs = fullDocs > 0 ? ((recDocs / fullDocs) * 100).toFixed(1) : '0';

  const fullCites = prof.cited_by_count != null ? Number(prof.cited_by_count) : 0;
  const recCites = recData.cited_by_count != null ? Number(recData.cited_by_count) : 0;
  const pctRecCites = fullCites > 0 ? ((recCites / fullCites) * 100).toFixed(1) : '0';

  const fullFwci = pData.fwci_avg != null ? Number(pData.fwci_avg) : (prof.fwci_avg != null ? Number(prof.fwci_avg) : 0);
  const recFwci  = recData.fwci_avg != null ? Number(recData.fwci_avg) : 0;
  const fwciDelta = (recFwci - fullFwci).toFixed(2);

  const fullHIndex = prof.h_index ?? '—';
  const recHIndex = recData.h_index ?? '—';

  const fullI10 = prof.i10_index != null ? Number(prof.i10_index) : '—';
  const recI10 = recData.i10_index != null ? Number(recData.i10_index) : '—';

  const fullPagerank = prof.pagerank != null ? Number(prof.pagerank).toFixed(3) : '0.000';
  const recPagerank = recData.pagerank != null ? Number(recData.pagerank).toFixed(3) : fullPagerank;

  const fullEigenfactor = prof.eigenfactor != null ? `${Number(prof.eigenfactor).toFixed(4)}%` : '0.0000%';
  const recEigenfactor = recData.eigenfactor != null ? `${Number(recData.eigenfactor).toFixed(4)}%` : fullEigenfactor;

  const fullTop10 = pData.pct_top_10 != null ? Number(pData.pct_top_10) : (prof.pct_top_10 != null ? Number(prof.pct_top_10) : 0);
  const recTop10  = recData.pct_top_10 != null ? Number(recData.pct_top_10) : 0;
  const top10Delta = (recTop10 - fullTop10).toFixed(2);

  const fullTop1 = pData.pct_top_1 != null ? Number(pData.pct_top_1) : (prof.pct_top_1 != null ? Number(prof.pct_top_1) : 0);
  const recTop1  = recData.pct_top_1 != null ? Number(recData.pct_top_1) : 0;
  const top1Delta = (recTop1 - fullTop1).toFixed(2);

  const fullPerc = pData.avg_percentile != null ? (Number(pData.avg_percentile) <= 1.0 ? Number(pData.avg_percentile) * 100 : Number(pData.avg_percentile)) : (prof.avg_percentile != null ? (Number(prof.avg_percentile) <= 1.0 ? Number(prof.avg_percentile) * 100 : Number(prof.avg_percentile)) : 0);
  const recPerc  = recData.avg_percentile != null ? (Number(recData.avg_percentile) <= 1.0 ? Number(recData.avg_percentile) * 100 : Number(recData.avg_percentile)) : 0;
  const percDelta = (recPerc - fullPerc).toFixed(1);

  const fullOaDiamond = pData.pct_oa_diamond != null ? Number(pData.pct_oa_diamond) : (prof.pct_oa_diamond != null ? Number(prof.pct_oa_diamond) : 0);
  const recOaDiamond  = recData.pct_oa_diamond != null ? Number(recData.pct_oa_diamond) : 0;
  const oaDiamondDelta = (recOaDiamond - fullOaDiamond).toFixed(1);

  const fullOaGold = pData.pct_oa_gold != null ? Number(pData.pct_oa_gold) : (prof.pct_oa_gold != null ? Number(prof.pct_oa_gold) : 0);
  const recOaGold  = recData.pct_oa_gold != null ? Number(recData.pct_oa_gold) : 0;
  const oaGoldDelta = (recOaGold - fullOaGold).toFixed(1);

  // Dual-Axis Chart: Volume vs FWCI
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
      name: 'FWCI Anual',
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


  // Radar Chart Traces
  const radarTraces = [];
  if (radarData && radarData.axes) {
    const axes = [...radarData.axes, radarData.axes[0]]; // Close polygon
    
    // Journal
    const jVals = radarData.axes.map(a => radarData.journal[a] || 0);
    jVals.push(jVals[0]);
    radarTraces.push({
      type: 'scatterpolar',
      r: jVals,
      theta: axes,
      fill: 'toself',
      name: prof.display_name || 'Esta Revista',
      line: { color: '#0284c7', width: 2.5 },
      fillcolor: 'rgba(2, 132, 199, 0.25)'
    });

    // Country
    if (radarData.country) {
      const cVals = radarData.axes.map(a => radarData.country[a] || 0);
      cVals.push(cVals[0]);
      radarTraces.push({
        type: 'scatterpolar',
        r: cVals,
        theta: axes,
        name: `Promedio País (${prof.country_code})`,
        line: { color: '#f59e0b', width: 1.5, dash: 'dot' }
      });
    }

    // LATAM
    if (radarData.latam) {
      const lVals = radarData.axes.map(a => radarData.latam[a] || 0);
      lVals.push(lVals[0]);
      radarTraces.push({
        type: 'scatterpolar',
        r: lVals,
        theta: axes,
        name: 'Referencia LATAM',
        line: { color: '#10b981', width: 1.5, dash: 'dash' }
      });
    }
  }

  // Citations Box / Violin Plot Traces
  const distTraces = citationsDist.citations.length > 0 ? [{
    type: distPlotType,
    y: citationsDist.citations,
    boxpoints: 'outliers',
    marker: { color: '#0284c7', size: 5 },
    line: { color: '#0284c7' },
    name: 'Citas por Artículo',
    boxmean: true
  }] : [];

  // Active Pie Data (Full vs Recent)
  const activePieData = piePeriod === 'recent' ? recData : pData;

  const oaPieValues = [
    { label: 'Diamante', value: Number(activePieData.pct_oa_diamond || 0), color: '#38bdf8' },
    { label: 'Dorado', value: Number(activePieData.pct_oa_gold || 0), color: '#fbbf24' },
    { label: 'Verde', value: Number(activePieData.pct_oa_green || 0), color: '#4ade80' },
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

  // Landscape Semantic Traces (Foco Temático y Deriva)
  const journalArts = landscapeData.articles || [];
  const bgArts = landscapeData.bg_articles || [];
  const artYears = journalArts.map(a => Number(a.publication_year)).filter(y => y && y >= 1950 && y <= 2026);
  const minYr = artYears.length > 0 ? Math.min(...artYears) : 1985;
  const maxYr = artYears.length > 0 ? Math.max(...artYears) : 2026;

  const landscapeTraces = [];
  const effectiveBgArts = bgArts.length > 0 ? bgArts : globalBgArts;
  if (effectiveBgArts.length > 0) {
    landscapeTraces.push({
      x: effectiveBgArts.map(a => a.umap_x),
      y: effectiveBgArts.map(a => a.umap_y),
      mode: 'markers',
      type: 'scatter',
      name: 'Paisaje Regional LATAM',
      marker: {
        size: 3.5,
        color: '#94a3b8',
        opacity: 0.18
      },
      hoverinfo: 'skip'
    });
  }

  if (journalArts.length > 0) {
    landscapeTraces.push({
      x: journalArts.map(a => a.umap_x),
      y: journalArts.map(a => a.umap_y),
      mode: 'markers',
      type: 'scatter',
      name: `Artículos de ${prof.display_name || selectedJournalName || 'Revista'}`,
      marker: {
        size: 7.5,
        color: journalArts.map(a => a.publication_year),
        colorscale: 'Turbo',
        cmin: minYr,
        cmax: maxYr,
        colorbar: { title: 'Año de Publ.', x: 1.02 },
        opacity: 0.9,
        line: { width: 0.8, color: '#ffffff' }
      },
      text: journalArts.map(a => a.title),
      customdata: journalArts.map(a => [
        a.publication_year || '—',
        a.fwci != null ? Number(a.fwci).toFixed(2) : '—',
        a.community_name || 'General'
      ]),
      hovertemplate: '<b>%{text}</b><br>Año: %{customdata[0]} | FWCI: %{customdata[1]}<br>Comunidad: %{customdata[2]}<extra></extra>'
    });
  }

  // Article Dynamic Scatter Plot Calculations
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

  const parseValArt = (val) => {
    if (val === null || val === undefined) return NaN;
    if (typeof val === 'boolean') return val ? 1 : 0;
    const num = Number(val);
    return isNaN(num) ? NaN : num;
  };

  const validArticleScatterRows = (scatterArticles || []).filter(d => 
    !isNaN(parseValArt(d[artScatterX])) && !isNaN(parseValArt(d[artScatterY]))
  );

  const xValsArt = validArticleScatterRows.map(d => parseValArt(d[artScatterX]));
  const yValsArt = validArticleScatterRows.map(d => parseValArt(d[artScatterY]));
  const statsXArt = calcStats(xValsArt);
  const statsYArt = calcStats(yValsArt);

  let pearsonRArt = 0;
  if (validArticleScatterRows.length > 1) {
    let num = 0, denX = 0, denY = 0;
    for (let i = 0; i < validArticleScatterRows.length; i++) {
      const dx = xValsArt[i] - statsXArt.mean;
      const dy = yValsArt[i] - statsYArt.mean;
      num += dx * dy;
      denX += dx * dx;
      denY += dy * dy;
    }
    pearsonRArt = (denX > 0 && denY > 0) ? num / Math.sqrt(denX * denY) : 0;
  }

  const xLabelArt = ARTICLE_SCATTER_INDICATORS.find(i => i.id === artScatterX)?.label || artScatterX;
  const yLabelArt = ARTICLE_SCATTER_INDICATORS.find(i => i.id === artScatterY)?.label || artScatterY;

  const displayedScatterRows = (showAllArticlesScatter || validArticleScatterRows.length <= 1000)
    ? validArticleScatterRows
    : validArticleScatterRows.slice(0, 1000);

  const articleScatterTraces = [{
    x: displayedScatterRows.map(d => parseValArt(d[artScatterX])),
    y: displayedScatterRows.map(d => parseValArt(d[artScatterY])),
    mode: 'markers',
    type: 'scatter',
    text: displayedScatterRows.map(d => d.title || 'Sin Título'),
    customdata: displayedScatterRows.map(d => [
      d.publication_year || '—',
      d.cited_by_count != null ? Number(d.cited_by_count) : 0,
      d.fwci != null ? Number(d.fwci).toFixed(2) : '0.00',
      d.oa_status || '—',
      d.doi ? `https://doi.org/${d.doi}` : ''
    ]),
    marker: {
      size: 8,
      color: '#10b981',
      line: { width: 0.5, color: '#ffffff' },
      opacity: 0.75
    },
    hovertemplate: `<b>%{text}</b><br>Año: %{customdata[0]} | OA: %{customdata[3]}<br>${xLabelArt}: %{x:,.2f}<br>${yLabelArt}: %{y:,.2f}<extra></extra>`
  }];

  // Sunburst Trace
  const sunburstTrace = (sunburstData && Array.isArray(sunburstData.nodes) && sunburstData.nodes.length > 0) ? [{
    type: 'sunburst',
    ids: sunburstData.nodes.map(n => n.id),
    labels: sunburstData.nodes.map(n => n.label),
    parents: sunburstData.nodes.map(n => n.parent),
    values: sunburstData.nodes.map(n => n.value),
    marker: {
      colors: sunburstData.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true
    },
    branchvalues: 'total',
    hovertemplate: '<b>%{label}</b><br>Artículos: %{value:,.0f}<br>Color: %{color:.2f}<extra></extra>'
  }] : [];

  // Treemap Trace
  const treemapTrace = (treemapData && Array.isArray(treemapData.nodes) && treemapData.nodes.length > 0) ? [{
    type: 'treemap',
    ids: treemapData.nodes.map(n => n.id),
    labels: treemapData.nodes.map(n => n.label),
    parents: treemapData.nodes.map(n => n.parent),
    values: treemapData.nodes.map(n => n.value),
    marker: {
      colors: treemapData.nodes.map(n => n.color_val),
      colorscale: 'Viridis',
      showscale: true
    },
    branchvalues: 'total',
    hovertemplate: '<b>%{label}</b><br>Artículos: %{value:,.0f}<br>Color: %{color:.2f}<extra></extra>'
  }] : [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '28px' }}>
      {/* Search and Country Selector Header */}
      <div className="card" style={{ padding: '18px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
          {/* Quick Country + Journal Selector */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>País:</span>
              <select
                value={filterCountry}
                onChange={(e) => setFilterCountry(e.target.value)}
                style={{ fontSize: '13px', fontWeight: '700', padding: '6px 12px' }}
              >
                <option value="ALL">🌎 Todos los Países</option>
                {countriesList.map(c => (
                  <option key={c.country_code} value={c.country_code}>
                    {c.country_name} ({c.country_code})
                  </option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>Revista:</span>
              <select
                value={selectedJournalId}
                onChange={(e) => {
                  const targetJ = countryJournals.find(j => j.id === e.target.value);
                  setSelectedJournal(e.target.value, targetJ ? targetJ.display_name : '');
                }}
                style={{ fontSize: '13px', fontWeight: '700', maxWidth: '380px', padding: '6px 12px' }}
                disabled={loadingJournals}
              >
                {countryJournals.map(j => (
                  <option key={j.id} value={j.id}>
                    {j.display_name} ({j.works_count?.toLocaleString()} docs)
                  </option>
                ))}
              </select>
            </div>
          </div>

          {/* Autocomplete Search Bar */}
          <div style={{ position: 'relative', width: '320px' }}>
            <div style={{ display: 'flex', alignItems: 'center', background: 'var(--bg-input)', border: '1px solid var(--border-color)', borderRadius: '8px', padding: '6px 12px', gap: '8px' }}>
              <Search size={16} color="var(--text-muted)" />
              <input
                type="text"
                placeholder="Buscar revista o ISSN..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                style={{ background: 'transparent', border: 'none', outline: 'none', width: '100%', fontSize: '13px' }}
              />
            </div>

            {searchResults.length > 0 && (
              <div style={{ position: 'absolute', top: '42px', left: 0, right: 0, background: 'var(--bg-card)', border: '1px solid var(--border-color)', borderRadius: '8px', boxShadow: '0 8px 24px rgba(0,0,0,0.15)', zIndex: 100, maxHeight: '280px', overflowY: 'auto' }}>
                {searchResults.map(j => (
                  <div
                    key={j.id}
                    onClick={() => {
                      setSelectedJournal(j.id, j.display_name);
                      setSearchQuery('');
                      setSearchResults([]);
                    }}
                    style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-color)', cursor: 'pointer', fontSize: '12.5px' }}
                    className="search-item-hover"
                  >
                    <strong>{j.display_name}</strong>
                    <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                      {j.publisher || 'Editorial no especificada'} • {j.country_code} • {j.works_count} docs
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Journal Title & Technical Badges */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h2 style={{ fontSize: '24px', fontWeight: '800' }}>
              {prof.display_name || selectedJournalName}
            </h2>
            <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
              {prof.publisher ? `${prof.publisher} • ` : ''}{prof.country_name || prof.country_code} • ISSN-L: <code>{prof.issn_l || 'No disponible'}</code>
            </p>
          </div>

          <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
            {prof.is_in_doaj && (
              <span className="badge" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981', border: '1px solid #10b981' }}>
                ✓ DOAJ Seal
              </span>
            )}
            {prof.is_scopus && (
              <span className="badge" style={{ background: 'rgba(2, 132, 199, 0.15)', color: '#0284c7', border: '1px solid #0284c7' }}>
                ✓ Scopus
              </span>
            )}
            {prof.is_in_scielo && (
              <span className="badge" style={{ background: 'rgba(245, 158, 11, 0.15)', color: '#f59e0b', border: '1px solid #f59e0b' }}>
                ✓ SciELO
              </span>
            )}
            {prof.homepage_url && (
              <a
                href={prof.homepage_url}
                target="_blank"
                rel="noreferrer"
                className="btn-primary"
                style={{ fontSize: '12px', padding: '6px 12px', display: 'flex', alignItems: 'center', gap: '4px' }}
              >
                Sitio Web <ExternalLink size={12} />
              </a>
            )}
            {(prof.id || selectedJournalId) && (
              <a
                href={prof.id?.startsWith('http') ? prof.id : (selectedJournalId?.startsWith('http') ? selectedJournalId : `https://openalex.org/${selectedJournalId}`)}
                target="_blank"
                rel="noreferrer"
                className="badge"
                style={{
                  fontSize: '12px',
                  padding: '6px 12px',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '4px',
                  background: 'rgba(2, 132, 199, 0.12)',
                  color: '#0284c7',
                  border: '1px solid rgba(2, 132, 199, 0.3)',
                  textDecoration: 'none',
                  fontWeight: '600'
                }}
              >
                OpenAlex <ExternalLink size={12} />
              </a>
            )}
            <DossierButton
              item={{
                key: `journal_profile_${selectedJournalId}`,
                title: `Revista: ${prof.display_name || selectedJournalName}`,
                context: `${prof.publisher ? prof.publisher + ' · ' : ''}${prof.country_name || ''} · ISSN-L: ${prof.issn_l || '—'} · ${prof.works_count?.toLocaleString()} artículos · FWCI ${prof.fwci_avg ?? '—'}`,
                category: 'Perfil Revista',
                data: [prof]
              }}
              label="Guardar en Contexto IA"
            />
            <button
              onClick={handleShareJournal}
              title="Copiar enlace directo para compartir esta revista"
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                padding: '6px 14px',
                borderRadius: '8px',
                background: copiedLink ? '#10b981' : 'var(--bg-input)',
                color: copiedLink ? '#ffffff' : 'var(--text-main)',
                border: '1px solid var(--border-color)',
                fontSize: '12px',
                fontWeight: '700',
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                boxShadow: copiedLink ? '0 2px 8px rgba(16, 185, 129, 0.3)' : 'none'
              }}
            >
              {copiedLink ? <Check size={14} /> : <Share2 size={14} />}
              <span>{copiedLink ? '¡Enlace copiado!' : 'Compartir Revista'}</span>
            </button>
          </div>
        </div>
      </div>

      {/* COMPARATIVA DE PERIODOS: COMPLETO (HISTÓRICO) VS RECIENTE (2021-2025) */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '16px' }}>
        {/* Periodo Completo */}
        <div className="card" style={{ padding: '18px 20px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '10px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Activity size={18} style={{ color: 'var(--accent-primary)' }} />
              <span style={{ fontSize: '15px', fontWeight: '800', color: 'var(--text-main)' }}>
                📊 Impacto, Citación y Red (Histórico)
              </span>
            </div>
            <span className="badge" style={{ fontSize: '11px' }}>Periodo Completo</span>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '14px' }}>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Documentos</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                {fullDocs.toLocaleString()}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Total Citas</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#0284c7', marginTop: '2px' }}>
                {prof.cited_by_count?.toLocaleString() || '—'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>FWCI Promedio</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--accent-primary)', marginTop: '2px' }}>
                {fullFwci.toFixed(2)}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Índice H</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#8b5cf6', marginTop: '2px' }}>
                {prof.h_index || '—'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Índice i10</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#6366f1', marginTop: '2px' }}>
                {prof.i10_index != null ? Number(prof.i10_index).toLocaleString() : '—'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>PageRank (‰)</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#06b6d4', marginTop: '2px' }}>
                {prof.pagerank != null ? Number(prof.pagerank).toFixed(3) : '0.000'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Eigenfactor (%)</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#0ea5e9', marginTop: '2px' }}>
                {prof.eigenfactor != null ? `${Number(prof.eigenfactor).toFixed(4)}%` : '0.0000%'}
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
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% OA Diamante</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#38bdf8', marginTop: '2px' }}>
                {fullOaDiamond.toFixed(1)}%
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% OA Dorado</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#fbbf24', marginTop: '2px' }}>
                {fullOaGold.toFixed(1)}%
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
                <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginLeft: '4px', fontWeight: 'normal' }}>
                  ({pctRecDocs}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Total Citas (Lustro)</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#0284c7', marginTop: '2px' }}>
                {recCites.toLocaleString()}
                <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginLeft: '4px', fontWeight: 'normal' }}>
                  ({pctRecCites}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>FWCI Promedio</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recFwci.toFixed(2)}
                <span style={{ fontSize: '11px', color: Number(fwciDelta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(fwciDelta) >= 0 ? '+' : ''}{fwciDelta})
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Índice H (Lustro)</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#8b5cf6', marginTop: '2px' }}>
                {recHIndex}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Índice i10 (Lustro)</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#6366f1', marginTop: '2px' }}>
                {recI10}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>PageRank (‰)</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#06b6d4', marginTop: '2px' }}>
                {recPagerank}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Eigenfactor (%)</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#0ea5e9', marginTop: '2px' }}>
                {recEigenfactor}
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
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#ec4899', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recTop1.toFixed(3)}%
                <span style={{ fontSize: '11px', color: Number(top1Delta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(top1Delta) >= 0 ? '+' : ''}{top1Delta}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>Percentil Prom. Norm.</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recPerc.toFixed(1)}
                <span style={{ fontSize: '11px', color: Number(percDelta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(percDelta) >= 0 ? '+' : ''}{percDelta})
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% OA Diamante</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#38bdf8', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recOaDiamond.toFixed(1)}%
                <span style={{ fontSize: '11px', color: Number(oaDiamondDelta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(oaDiamondDelta) >= 0 ? '+' : ''}{oaDiamondDelta}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>% OA Dorado</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#fbbf24', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recOaGold.toFixed(1)}%
                <span style={{ fontSize: '11px', color: Number(oaGoldDelta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(oaGoldDelta) >= 0 ? '+' : ''}{oaGoldDelta}%)
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* DUAL-AXIS CHART: Artículos vs FWCI */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
          <TrendingUp size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            Gráfico de Eje Dual (Dual-Axis Chart) — Producción Anual vs FWCI
          </h3>
        </div>
        <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          Evolución histórica de artículos publicados (barras) y FWCI alcanzado en cada año (línea verde).
        </p>

        <PlotlyChart
          data={dualAxisTraces}
          layout={{
            height: 360,
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

      {/* TABLA DE INDICADORES HISTÓRICOS ANUALES DE LA REVISTA */}
      <AnnualDataTable
        data={annualTrends}
        journalName={prof.display_name || selectedJournalName}
        journalId={selectedJournalId}
      />

      {/* DISTRIBUCIÓN POR IDIOMA Y TIPO DE ACCESO (PIES DE OA E IDIOMAS) */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <PieChart size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700', margin: 0 }}>
                Distribución por Idioma y Tipo de Acceso
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Composición porcentual de modalidades de acceso abierto e idiomas de publicación en {prof.display_name || selectedJournalName || 'la revista'}.
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

      {/* RADAR DE MADUREZ EDITORIAL & BOXPLOT DE CITAS */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(420px, 1fr))', gap: '20px' }}>
        {/* Radar Chart */}
        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
            <Radar size={18} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '15px', fontWeight: '700' }}>
              Perfil Hexagonal de Madurez Editorial (Radar Chart)
            </h3>
          </div>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            Evaluación multidimensional de la revista frente al promedio nacional y la referencia regional.
          </p>

          <PlotlyChart
            data={radarTraces}
            layout={{
              polar: {
                radialaxis: { visible: true, range: [0, 1] }
              },
              height: 380,
              margin: { l: 40, r: 40, t: 20, b: 30 },
              legend: { orientation: 'h', y: -0.15 }
            }}
          />
        </div>

        {/* Box Plot / Violin Plot */}
        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '4px', flexWrap: 'wrap', gap: '10px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <BoxSelect size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '15px', fontWeight: '700' }}>
                Distribución Real de Citas por Artículo (Box / Violin Plot)
              </h3>
            </div>
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${distPlotType === 'box' ? 'active' : ''}`}
                onClick={() => setDistPlotType('box')}
              >
                Box Plot
              </button>
              <button
                className={`segmented-pill-btn ${distPlotType === 'violin' ? 'active' : ''}`}
                onClick={() => setDistPlotType('violin')}
              >
                Violin Plot
              </button>
            </div>
          </div>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            Evidencia la asimetría de la Ley de Lotka (mediana, percentiles 25/75 y artículos altamente citados).
          </p>

          <PlotlyChart
            data={distTraces}
            layout={{
              height: 380,
              margin: { l: 60, r: 20, t: 20, b: 30 },
              yaxis: { title: 'Número de Citas Recibidas' }
            }}
          />
        </div>
      </div>

      {/* TRAYECTORIA MULTIDIMENSIONAL UMAP (REVISTA VS PAÍS) */}
      {trajectory && Object.keys(trajectory).length > 0 && (
        <UmapTrajectoryViewer
          title={`📈 Trayectoria Multidimensional UMAP: ${prof.display_name || selectedJournalName || 'Revista'} vs ${prof.country_name || prof.country_code || 'País'}`}
          subtitle="Evolución temporal continua del perfil cienciométrico en el espacio 2D UMAP frente al promedio de su país."
          trajectories={trajectory}
          allowTrajectoryFilter={true}
          showGridSection={true}
          height={460}
        />
      )}

      {/* FOCO TEMÁTICO Y DERIVA TEMPORAL */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '6px', flexWrap: 'wrap', gap: '8px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Compass size={20} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '17px', fontWeight: '700', margin: 0 }}>
              🌌 Foco Temático y Deriva Temporal: {prof.display_name || selectedJournalName}
            </h3>
          </div>
          <span className="badge" style={{ fontSize: '11px' }}>
            {journalArts.length > 0 ? `${journalArts.length.toLocaleString()} Artículos en la Muestra` : 'Sin artículos proyectados'}
          </span>
        </div>
        <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          Proyección de los artículos de la revista sobre el paisaje semántico regional. La barra de color ilustra la progresión temporal de las publicaciones.
        </p>

        {journalArts.length > 0 ? (
          <>
            <PlotlyChart
              data={landscapeTraces}
              layout={{
                height: 540,
                margin: { l: 30, r: 30, t: 20, b: 30 },
                xaxis: { showgrid: true, zeroline: false },
                yaxis: { showgrid: true, zeroline: false },
                legend: { orientation: 'h', y: 1.08, x: 0.1 }
              }}
            />

            <div style={{ marginTop: '14px', padding: '12px 16px', background: 'rgba(2, 132, 199, 0.08)', borderRadius: '8px', borderLeft: '4px solid var(--accent-primary)', fontSize: '12.5px', color: 'var(--text-main)', lineHeight: '1.6' }}>
              💡 <strong>Análisis de Foco Temático y Deriva:</strong>
              <ul style={{ margin: '6px 0 0 18px', padding: 0 }}>
                <li><strong>Dispersión Semántica:</strong> <code>{Number(landscapeData.dispersion || 0).toFixed(2)}</code> (Valores bajos indican una revista altamente especializada en un núcleo temático cerrado; valores altos reflejan una revista multidisciplinar o transversal).</li>
                <li><strong>Evolución Temporal:</strong> Si los puntos recientes (amarillos/rojos) coinciden espacialmente con los puntos fundacionales (azules/morados), la revista mantiene su identidad temática original. Si los puntos recientes forman nuevos núcleos o se han desplazado, la revista ha experimentado una <strong>deriva temática</strong> o ampliación de su alcance editorial.</li>
              </ul>
            </div>
          </>
        ) : (
          <div style={{ textAlign: 'center', padding: '50px 20px', color: 'var(--text-muted)' }}>
            ℹ️ Esta revista cuenta con producción registrada; ejecuta el pipeline con un muestreo mayor para proyectar sus artículos en el paisaje general.
          </div>
        )}
      </div>

      {/* Composición Temática: Sunburst & Treemap */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px', flexWrap: 'wrap', gap: '10px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              🏵️ Composición Temática de la Revista
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Especialización disciplinar por Dominio → Campo → Subcampo. Alterna entre la vista radial (Sunburst) y rectangular (Treemap).
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

            {/* Indicator Selector */}
            <select
              value={sunburstInd}
              onChange={(e) => setSunburstInd(e.target.value)}
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
          <PlotlyChart data={sunburstTrace} layout={{ height: 500, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
        ) : (
          <PlotlyChart data={treemapTrace} layout={{ height: 500, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
        )}
      </div>

      {/* EVOLUCIÓN HISTÓRICA DE PERFILES DE CONOCIMIENTO (REVISTA) */}
      <ThematicEvolutionTable
        journalId={selectedJournalId}
        journalName={prof.display_name || selectedJournalName}
      />

      {/* EXPLORADOR DE ARTÍCULOS - SCATTER PLOT DINÁMICO */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <TrendingUp size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700', margin: 0 }}>
                Explorador de Artículos - Scatter Plot Dinámico
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Visualiza la relación entre diferentes indicadores bibliométricos para los artículos de {prof.display_name || selectedJournalName}.
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            <span className="badge" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981', border: '1px solid #10b981', fontSize: '11.5px', fontWeight: '700' }}>
              ✅ Cargados {scatterArticles.length.toLocaleString()} artículos
            </span>
          </div>
        </div>

        {/* Controls: Eje X, Eje Y & Checkbox */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '14px', marginBottom: '16px', flexWrap: 'wrap', background: 'var(--bg-input)', padding: '12px 16px', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '12.5px', fontWeight: '700', color: 'var(--text-muted)' }}>Indicador Eje X:</span>
              <select
                value={artScatterX}
                onChange={(e) => setArtScatterX(e.target.value)}
                style={{ fontSize: '13px', fontWeight: '600', padding: '6px 12px' }}
              >
                {ARTICLE_SCATTER_INDICATORS.map(ind => (
                  <option key={ind.id} value={ind.id}>{ind.label}</option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '12.5px', fontWeight: '700', color: 'var(--text-muted)' }}>Indicador Eje Y:</span>
              <select
                value={artScatterY}
                onChange={(e) => setArtScatterY(e.target.value)}
                style={{ fontSize: '13px', fontWeight: '600', padding: '6px 12px' }}
              >
                {ARTICLE_SCATTER_INDICATORS.map(ind => (
                  <option key={ind.id} value={ind.id}>{ind.label}</option>
                ))}
              </select>
            </div>
          </div>

          {validArticleScatterRows.length > 1000 && (
            <label style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: 'var(--text-muted)', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={showAllArticlesScatter}
                onChange={(e) => setShowAllArticlesScatter(e.target.checked)}
              />
              Mostrar todos los artículos ({validArticleScatterRows.length.toLocaleString()})
            </label>
          )}
        </div>

        {validArticleScatterRows.length > 0 ? (
          <>
            <PlotlyChart
              data={articleScatterTraces}
              layout={{
                height: 480,
                margin: { l: 60, r: 20, t: 20, b: 45 },
                xaxis: { title: xLabelArt, showgrid: true, zeroline: true },
                yaxis: { title: yLabelArt, showgrid: true, zeroline: true },
                showlegend: false
              }}
            />

            {/* Statistics Summary */}
            <div style={{ marginTop: '16px', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '14px' }}>
              {/* Eje X Stats */}
              <div style={{ padding: '12px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '12.5px' }}>
                <strong style={{ color: 'var(--accent-primary)' }}>📊 Estadísticas {xLabelArt}</strong>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '6px', marginTop: '8px', color: 'var(--text-muted)' }}>
                  <div>Media: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsXArt.mean.toFixed(2)}</span></div>
                  <div>Mediana: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsXArt.median.toFixed(2)}</span></div>
                  <div>Desv. Est.: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsXArt.std.toFixed(2)}</span></div>
                  <div>Rango: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>[{statsXArt.min.toFixed(2)} - {statsXArt.max.toFixed(2)}]</span></div>
                </div>
              </div>

              {/* Eje Y Stats */}
              <div style={{ padding: '12px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '12.5px' }}>
                <strong style={{ color: '#10b981' }}>📊 Estadísticas {yLabelArt}</strong>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '6px', marginTop: '8px', color: 'var(--text-muted)' }}>
                  <div>Media: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsYArt.mean.toFixed(2)}</span></div>
                  <div>Mediana: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsYArt.median.toFixed(2)}</span></div>
                  <div>Desv. Est.: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsYArt.std.toFixed(2)}</span></div>
                  <div>Rango: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>[{statsYArt.min.toFixed(2)} - {statsYArt.max.toFixed(2)}]</span></div>
                </div>
              </div>

              {/* Pearson Correlation */}
              <div style={{ padding: '12px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '12.5px', display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
                <div style={{ color: 'var(--text-muted)' }}>Correlación Lineal de Pearson</div>
                <div style={{ fontSize: '20px', fontWeight: '800', color: Math.abs(pearsonRArt) >= 0.5 ? '#10b981' : 'var(--text-main)', marginTop: '4px' }}>
                  r = {pearsonRArt.toFixed(3)}
                </div>
                <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '2px' }}>
                  {Math.abs(pearsonRArt) >= 0.7 ? 'Correlación fuerte' : (Math.abs(pearsonRArt) >= 0.3 ? 'Correlación moderada' : 'Correlación débil o nula')}
                </span>
              </div>
            </div>
          </>
        ) : (
          <div style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
            No hay datos suficientes para generar el gráfico de dispersión de artículos.
          </div>
        )}
      </div>

      {/* Top Articles Table */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              📄 Artículos de la Revista ({articles.length} {articleLimit === 0 ? 'de ' + (prof.works_count?.toLocaleString() || 'total') : 'mostrados'})
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Listado detallado con identificador OpenAlex, métricas de citación e impacto normalizado.
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            {/* Limit Selector */}
            <select
              value={articleLimit}
              onChange={(e) => setArticleLimit(Number(e.target.value))}
              style={{ fontSize: '12.5px', fontWeight: '600' }}
              title="Cantidad de artículos a mostrar"
            >
              <option value={50}>50 artículos</option>
              <option value={100}>100 artículos</option>
              <option value={500}>500 artículos</option>
              <option value={1000}>1,000 artículos</option>
              <option value={0}>Todos los artículos ({prof.works_count?.toLocaleString() || 'Total'})</option>
            </select>

            {/* Sort Selector */}
            <select
              value={articleSort}
              onChange={(e) => setArticleSort(e.target.value)}
              style={{ fontSize: '12.5px', fontWeight: '600' }}
            >
              <option value="cited_by_count">Más Citados</option>
              <option value="fwci">Mayor FWCI</option>
              <option value="publication_year">Más Recientes</option>
            </select>

            {/* Export Buttons */}
            <button
              className="btn-primary"
              disabled={exportingFormat !== null}
              onClick={() => handleExportArticles('json')}
              style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', padding: '6px 12px', opacity: exportingFormat ? 0.7 : 1 }}
              title="Descargar registros completos de OpenAlex en formato JSON comprimido (.json.gz)"
            >
              <Download size={14} /> {exportingFormat === 'json' ? 'Generando JSON (.gz)...' : 'Exportar JSON (.gz)'}
            </button>

            <button
              className="btn-secondary"
              disabled={exportingFormat !== null}
              onClick={() => handleExportArticles('csv')}
              style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', padding: '6px 12px', opacity: exportingFormat ? 0.7 : 1 }}
              title="Descargar en formato OpenAlex CSV estándar de 88 columnas para knoMap"
            >
              <Download size={14} /> {exportingFormat === 'csv' ? 'Generando CSV...' : 'Exportar CSV (88 cols)'}
            </button>
          </div>
        </div>

        <div className="data-table-container" style={{ maxHeight: '450px' }}>
          <table className="data-table" style={{ tableLayout: 'fixed', width: '100%' }}>
            <colgroup>
              <col style={{ width: '38%' }} />
              <col style={{ width: '14%' }} />
              <col style={{ width: '6%' }} />
              <col style={{ width: '7%' }} />
              <col style={{ width: '7%' }} />
              <col style={{ width: '9%' }} />
              <col style={{ width: '11%' }} />
              <col style={{ width: '8%' }} />
            </colgroup>
            <thead>
              <tr>
                <th>Título</th>
                <th>OpenAlex ID</th>
                <th>Año</th>
                <th>Citas</th>
                <th>FWCI</th>
                <th>Percentil</th>
                <th>Acceso Abierto</th>
                <th>DOI</th>
              </tr>
            </thead>
            <tbody>
              {articles.map((art, idx) => (
                <tr key={idx}>
                  <td style={{ overflow: 'hidden' }}>
                    <strong
                      title={art.title || 'Sin título'}
                      style={{
                        display: 'block',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap'
                      }}
                    >
                      {art.title || 'Sin título'}
                    </strong>
                  </td>
                  <td>
                    {art.id ? (
                      <a
                        href={art.id}
                        target="_blank"
                        rel="noreferrer"
                        style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: '4px',
                          color: 'var(--accent-primary)',
                          fontWeight: '600',
                          fontSize: '11px',
                          textDecoration: 'none'
                        }}
                        title={`Abrir ${art.id} en OpenAlex`}
                      >
                        <code>{art.id.replace('https://openalex.org/', '')}</code>
                        <ExternalLink size={11} />
                      </a>
                    ) : '—'}
                  </td>
                  <td>{art.publication_year || '—'}</td>
                  <td>{art.cited_by_count?.toLocaleString() || 0}</td>
                  <td>{Number(art.fwci || 0).toFixed(2)}</td>
                  <td>{Number(art.percentile || 0).toFixed(1)}</td>
                  <td><span className="badge">{art.oa_status || 'closed'}</span></td>
                  <td>
                    {art.doi ? (
                      <a
                        href={art.doi}
                        target="_blank"
                        rel="noreferrer"
                        style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: '4px',
                          color: 'var(--accent-primary)',
                          textDecoration: 'underline',
                          fontSize: '11.5px'
                        }}
                      >
                        DOI <ExternalLink size={11} />
                      </a>
                    ) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── EXPANDER DE DOSSIER DE ESTUDIO Y ENVÍO A CHATGPT (PIE DE PÁGINA) ── */}
      <PageDossierExpander
        pageTitle={`Diagnóstico Cienciométrico de Revista: ${details?.display_name || selectedJournalName}`}
        pageDescription={`Selecciona cualquiera de las gráficas, tablas o indicadores de ${details?.display_name || selectedJournalName} para generar un reporte integral o enviarlo a ChatGPT.`}
        sections={[
          {
            id: 'journal_profile',
            title: `1. Perfil y Métricas Principales (${details?.display_name || selectedJournalName})`,
            category: 'KPIs Principales',
            defaultChecked: true,
            rawData: details,
            buildDataText: () => {
              if (!details) return 'No hay datos de perfil disponibles.';
              return [
                `*Revista:* **${details.display_name || selectedJournalName}**\n`,
                '| Métrica Editorial | Valor Registrado | Estado / Indexación |',
                '|---|---|---|',
                `| OpenAlex ID | ${details.id} | Identificador persistente |`,
                `| ISSN-L | ${details.issn_l || 'No disponible'} | Registro de ISSN |`,
                `| Editorial / Institución | ${details.publisher || '—'} | Filiación editorial |`,
                `| País de Edición | ${details.country_name || details.country_code || '—'} | Sede geográfica |`,
                `| Total Artículos Publicados | ${details.works_count?.toLocaleString() || 0} | Producción acumulada indexada |`,
                `| Total Citas Recibidas | ${details.cited_by_count?.toLocaleString() || 0} | Impacto acumulado |`,
                `| FWCI Ponderado Promedio | ${Number(details.fwci_avg || 0).toFixed(2)} | Impacto normalizado por campo (Base=1.0) |`,
                `| % Acceso Abierto Diamante | ${Number(details.pct_oa_diamond || 0).toFixed(1)}% | Sin cobro por APC |`,
                `| Sello DOAJ | ${details.is_in_doaj ? '✅ Indexada con Sello' : '❌ No'} | Calidad de acceso abierto |`,
                `| Scopus | ${details.is_scopus ? '✅ Indexada' : '❌ No'} | Cobertura en Scopus |`,
                `| SciELO | ${details.is_in_scielo ? '✅ Indexada' : '❌ No'} | Cobertura en SciELO |`
              ].join('\n');
            }
          },
          {
            id: 'journal_performance_periods',
            title: '2. Panel Consolidado de Desempeño, Impacto y Red (Histórico vs Reciente 2021–2025)',
            category: 'Indicadores de Desempeño',
            defaultChecked: true,
            rawData: { full_period: details?.full_period, recent_period: details?.recent_period, profile: details?.profile },
            buildDataText: () => {
              return [
                '| Indicador Cienciométrico | Periodo Completo (Histórico) | Periodo Reciente (2021–2025) | Variación / Dinámica Reciente |',
                '|---|---|---|---|',
                `| Artículos Publicados | ${fullDocs.toLocaleString()} | ${recDocs.toLocaleString()} | ${pctRecDocs}% de la producción histórica |`,
                `| Total Citas Recibidas | ${fullCites.toLocaleString()} | ${recCites.toLocaleString()} | ${pctRecCites}% de las citas totales |`,
                `| FWCI Promedio Normalizado | ${fullFwci.toFixed(2)} | ${recFwci.toFixed(2)} | ${Number(fwciDelta) >= 0 ? '+' : ''}${fwciDelta} |`,
                `| Índice H | ${fullHIndex} | ${recHIndex} | Capacidad de citación sostenida |`,
                `| Índice i10 | ${fullI10} | ${recI10} | Artículos con ≥10 citas |`,
                `| PageRank de Citas (‰) | ${fullPagerank} | ${recPagerank} | Prestigio estructural en la red |`,
                `| Eigenfactor Score (%) | ${fullEigenfactor} | ${recEigenfactor} | Peso e influencia global de la revista |`,
                `| % Artículos en Top 10% Más Citados | ${fullTop10.toFixed(2)}% | ${recTop10.toFixed(2)}% | ${Number(top10Delta) >= 0 ? '+' : ''}${top10Delta}% |`,
                `| % Artículos en Top 1% Más Citados | ${fullTop1.toFixed(3)}% | ${recTop1.toFixed(3)}% | ${Number(top1Delta) >= 0 ? '+' : ''}${top1Delta}% |`,
                `| Percentil Promedio Normalizado | ${fullPerc.toFixed(1)} | ${recPerc.toFixed(1)} | ${Number(percDelta) >= 0 ? '+' : ''}${percDelta} |`,
                `| % Acceso Abierto Diamante | ${fullOaDiamond.toFixed(1)}% | ${recOaDiamond.toFixed(1)}% | ${Number(oaDiamondDelta) >= 0 ? '+' : ''}${oaDiamondDelta}% |`,
                `| % Acceso Abierto Dorado (APC) | ${fullOaGold.toFixed(1)}% | ${recOaGold.toFixed(1)}% | ${Number(oaGoldDelta) >= 0 ? '+' : ''}${oaGoldDelta}% |`
              ].join('\n');
            }
          },
          {
            id: 'journal_annual',
            title: `3. Gráfico de Eje Dual — Producción Anual vs FWCI (${details?.display_name || selectedJournalName})`,
            category: 'Series de Tiempo',
            defaultChecked: true,
            rawData: annualTrends,
            buildDataText: () => {
              if (!annualTrends || annualTrends.length === 0) return 'No hay series anuales disponibles.';
              const lines = [
                '| Año | Artículos | Citas | FWCI | % OA Diamante | % Inglés |',
                '|---|---|---|---|---|---|'
              ];
              annualTrends.slice(-15).forEach(a => {
                lines.push(`| ${a.year} | ${a.works_count?.toLocaleString() || a.num_documents?.toLocaleString() || 0} | ${a.cited_by_count?.toLocaleString() || 0} | ${Number(a.fwci_avg || 0).toFixed(2)} | ${Number(a.pct_oa_diamond || 0).toFixed(1)}% | ${Number(a.pct_lang_en || 0).toFixed(1)}% |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'journal_oa_lang_pies',
            title: `4. Distribución por Idioma y Tipo de Acceso (Pasteles OA e Idiomas - ${piePeriod === 'recent' ? '2021–2025' : '0–2026'})`,
            category: 'Distribuciones',
            defaultChecked: false,
            rawData: { oaPieValues, langPieValues, piePeriod },
            buildDataText: () => {
              const lines = [
                `*Periodo:* **${piePeriod === 'recent' ? 'Reciente (2021–2025)' : 'Completo (0–2026)'}**\n`,
                '**Distribución por Modalidad de Acceso Abierto:**',
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
            id: 'journal_radar',
            title: '5. Perfil Multidimensional (Radar Chart de Madurez Editorial)',
            category: 'Perfil Multidimensional',
            defaultChecked: false,
            rawData: radarData,
            buildDataText: () => {
              if (!radarData || !radarData.axes) return 'No hay datos de radar disponibles.';
              const lines = [
                '| Dimensión Evaluada | Revista | Promedio País | Referencia LATAM |',
                '|---|---|---|---|'
              ];
              radarData.axes.forEach(axis => {
                const jVal = radarData.journal?.[axis] != null ? Number(radarData.journal[axis]).toFixed(2) : '—';
                const cVal = radarData.country?.[axis] != null ? Number(radarData.country[axis]).toFixed(2) : '—';
                const lVal = radarData.latam?.[axis] != null ? Number(radarData.latam[axis]).toFixed(2) : '—';
                lines.push(`| ${axis} | ${jVal} | ${cVal} | ${lVal} |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'citations_distribution',
            title: `6. Distribución del Impacto de Artículos (${distPlotType === 'box' ? 'Box Plot' : 'Violin Plot'} - Ley de Lotka)`,
            category: 'Distribuciones de Impacto',
            defaultChecked: false,
            rawData: citationsDist,
            buildDataText: () => {
              if (!citationsDist || !citationsDist.citations || citationsDist.citations.length === 0) return 'No hay datos de distribución de citas.';
              const cits = citationsDist.citations;
              const sorted = [...cits].sort((a, b) => a - b);
              const min = sorted[0];
              const max = sorted[sorted.length - 1];
              const med = sorted[Math.floor(sorted.length / 2)];
              const p75 = sorted[Math.floor(sorted.length * 0.75)];
              const p90 = sorted[Math.floor(sorted.length * 0.90)];
              return [
                `*Distribución de Citas por Artículo (${cits.length.toLocaleString()} artículos analizados):*\n`,
                '| Estadístico de Dispersión | Citas por Artículo |',
                '|---|---|',
                `| Mínimo | ${min} citas |`,
                `| Mediana (Q2) | ${med} citas |`,
                `| Percentil 75 (Q3) | ${p75} citas |`,
                `| Percentil 90 | ${p90} citas |`,
                `| Máximo de Citas en un Artículo | ${max} citas |`
              ].join('\n');
            }
          },
          {
            id: 'umap_journal_trajectory',
            title: `7. Trayectoria Multidimensional UMAP (${details?.display_name || selectedJournalName})`,
            category: 'Variedades Semánticas / UMAP',
            defaultChecked: false,
            rawData: trajectory,
            buildDataText: () => {
              if (!trajectory || Object.keys(trajectory).length === 0) return 'No hay datos de trayectoria UMAP.';
              const lines = [
                '| Entidad / Referencia | Puntos Registrados | Coordenadas Inicio | Coordenadas Recientes |',
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
            id: 'semantic_landscape',
            title: `8. Foco Temático y Deriva Temporal en el Paisaje Semántico (${landscapeData.articles?.length || 0} artículos)`,
            category: 'Semántica / Procesamiento de Lenguaje Natural',
            defaultChecked: false,
            rawData: landscapeData,
            buildDataText: () => {
              if (!landscapeData || !landscapeData.articles || landscapeData.articles.length === 0) return 'No hay artículos en el mapa semántico.';
              const lines = [
                `*Dispersión Semántica de la Revista:* **${Number(landscapeData.dispersion || 0).toFixed(3)}** (Valores bajos indican alta especialización nuclear; valores altos multidisciplinariedad).\n`,
                '| Título del Artículo | Año | Coordenadas Semánticas (X, Y) | Citas | FWCI |',
                '|---|---|---|---|---|'
              ];
              landscapeData.articles.slice(0, 15).forEach(a => {
                const titleClean = (a.title || 'Sin título').replace(/\|/g, '-');
                lines.push(`| ${titleClean.slice(0, 65)}... | ${a.publication_year || a.year || '—'} | (${Number(a.umap_x || a.x || 0).toFixed(2)}, ${Number(a.umap_y || a.y || 0).toFixed(2)}) | ${a.cited_by_count || 0} | ${a.fwci != null ? Number(a.fwci).toFixed(2) : '—'} |`);
              });
              if (landscapeData.articles.length > 15) {
                lines.push(`\n_... y ${landscapeData.articles.length - 15} artículos más en la proyección semántica._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'journal_thematic_hierarchy',
            title: `9. Composición Temática de la Revista (${thematicViewType === 'sunburst' ? 'Sunburst Radial' : 'Treemap'})`,
            category: 'Taxonomía Científica',
            defaultChecked: false,
            rawData: thematicViewType === 'sunburst' ? sunburstData : treemapData,
            buildDataText: () => {
              const data = thematicViewType === 'sunburst' ? sunburstData : treemapData;
              const nodes = data?.nodes || (Array.isArray(data) ? data : []);
              if (!nodes || nodes.length === 0) return 'No hay datos de taxonomía temática.';
              const lines = [
                `*Visualización:* **${thematicViewType === 'sunburst' ? 'Sunburst' : 'Treemap'}** | *Indicador:* **${sunburstInd}**\n`,
                '| Área / Tópico | Artículos | Métrica de Color |',
                '|---|---|---|'
              ];
              nodes.slice(0, 20).forEach(n => {
                lines.push(`| ${n.name || n.label || n.id} | ${n.value?.toLocaleString() || 0} | ${n.color_metric != null ? Number(n.color_metric).toFixed(2) : '—'} |`);
              });
              if (nodes.length > 20) {
                lines.push(`\n_... y ${nodes.length - 20} ramas temáticas más._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'journal_article_scatter',
            title: `10. Explorador de Artículos — Scatter Plot Dinámico y Correlación (${xLabelArt} vs ${yLabelArt})`,
            category: 'Correlaciones Multivariadas',
            defaultChecked: false,
            rawData: { validArticleScatterRows, statsXArt, statsYArt, pearsonRArt },
            buildDataText: () => {
              if (!validArticleScatterRows || validArticleScatterRows.length === 0) return 'No hay datos de scatter de artículos.';
              const lines = [
                `*Eje X:* **${xLabelArt}** | *Eje Y:* **${yLabelArt}**\n`,
                `*Correlación Lineal de Pearson (r):* **${pearsonRArt != null ? pearsonRArt.toFixed(3) : '0.000'}** (${Math.abs(pearsonRArt) >= 0.7 ? 'Correlación Fuerte' : (Math.abs(pearsonRArt) >= 0.3 ? 'Correlación Moderada' : 'Correlación Débil o Nula')})\n`,
                '**Estadísticas Descriptivas:**',
                '| Eje / Variable | Media | Mediana | Desv. Estándar | Rango [Mín - Máx] |',
                '|---|---|---|---|---|',
                `| Eje X (${xLabelArt}) | ${statsXArt.mean.toFixed(2)} | ${statsXArt.median.toFixed(2)} | ${statsXArt.std.toFixed(2)} | [${statsXArt.min.toFixed(2)} - ${statsXArt.max.toFixed(2)}] |`,
                `| Eje Y (${yLabelArt}) | ${statsYArt.mean.toFixed(2)} | ${statsYArt.median.toFixed(2)} | ${statsYArt.std.toFixed(2)} | [${statsYArt.min.toFixed(2)} - ${statsYArt.max.toFixed(2)}] |`,
                '\n**Muestra de Artículos Analizados:**',
                `| Título del Artículo | Año | ${xLabelArt} | ${yLabelArt} | OA Status |`,
                '|---|---|---|---|---|'
              ];
              validArticleScatterRows.slice(0, 20).forEach(d => {
                const titleClean = (d.title || 'Sin título').replace(/\|/g, '-');
                lines.push(`| ${titleClean.slice(0, 60)}... | ${d.publication_year || '—'} | ${Number(d[artScatterX] || 0).toFixed(2)} | ${Number(d[artScatterY] || 0).toFixed(2)} | ${d.oa_status || 'closed'} |`);
              });
              if (validArticleScatterRows.length > 20) {
                lines.push(`\n_... y ${validArticleScatterRows.length - 20} artículos más en el análisis de dispersión._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'top_articles',
            title: `11. Listado de Artículos Más Citados de la Revista (Top ${Math.min(articles.length, 15)})`,
            category: 'Artículos Destacados',
            defaultChecked: false,
            rawData: articles,
            buildDataText: () => {
              if (!articles || articles.length === 0) return 'No hay artículos registrados.';
              const lines = [
                '| Título del Artículo | Año | Citas | FWCI | Percentil | Acceso |',
                '|---|---|---|---|---|---|'
              ];
              articles.slice(0, 15).forEach(art => {
                const titleClean = (art.title || 'Sin título').replace(/\|/g, '-');
                lines.push(`| ${titleClean.slice(0, 70)}... | ${art.publication_year || '—'} | ${art.cited_by_count?.toLocaleString() || 0} | ${Number(art.fwci || 0).toFixed(2)} | ${Number(art.percentile || 0).toFixed(1)} | ${art.oa_status || 'closed'} |`);
              });
              return lines.join('\n');
            }
          }
        ]}
      />

      {/* Floating Export Toast Notification */}
      {toastMessage && (
        <div
          style={{
            position: 'fixed',
            bottom: '28px',
            right: '28px',
            zIndex: 9999,
            background: 'var(--bg-card)',
            color: 'var(--text-main)',
            border: '1px solid #10b981',
            borderRadius: '12px',
            padding: '12px 18px',
            boxShadow: '0 8px 24px rgba(16, 185, 129, 0.25)',
            display: 'flex',
            alignItems: 'center',
            gap: '12px'
          }}
        >
          <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#10b981' }} />
          <span style={{ fontSize: '13px', fontWeight: '600' }}>{toastMessage}</span>
          <button
            onClick={() => setDownloadsOpen(true)}
            style={{
              background: '#10b981',
              color: '#ffffff',
              border: 'none',
              borderRadius: '6px',
              padding: '5px 12px',
              fontSize: '12px',
              fontWeight: '700',
              cursor: 'pointer'
            }}
          >
            Ver Descargas
          </button>
        </div>
      )}
    </div>
  );
}
