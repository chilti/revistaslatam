import React, { useState, useEffect, useRef, useMemo } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import { useTranslation } from '../i18n';
import KpiCard from '../components/KpiCard';
import PlotlyChart from '../components/PlotlyChart';
import UmapTrajectoryViewer from '../components/UmapTrajectoryViewer';
import ThematicEvolutionTable from '../components/ThematicEvolutionTable';
import AnnualDataTable from '../components/AnnualDataTable';
import PageDossierExpander from '../components/PageDossierExpander';
import ConnectionMapViewer from '../components/ConnectionMapViewer';
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
  Compass,
  BarChart3
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


export default function JournalPage() {
  const {
    selectedJournalId,
    selectedJournalName,
    setSelectedJournal,
    selectedCountry,
    setSelectedCountry,
    addDossierItem,
    addExportJob,
    setDownloadsOpen,
    requireAuth,
    user
  } = useAppStore();
  const { t } = useTranslation();
  
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
  const [filterCountry, setFilterCountry] = useState(selectedCountry || 'MX');
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

  // Coauthorship connection map state
  const [collabData, setCollabData] = useState(null);
  const [collabLoading, setCollabLoading] = useState(false);
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
    const code = filterCountry || selectedCountry || 'MX';
    setLoadingJournals(true);
    
    const fetchPromise = code === 'ALL'
      ? api.get('/journals/search?limit=300')
      : api.get(`/countries/${code}/journals`);

    fetchPromise
      .then(res => {
        const jList = res.data || [];
        setCountryJournals(jList);
        if (jList.length > 0) {
          const currentInList = jList.some(j => j.id === selectedJournalId);
          if (!currentInList) {
            setSelectedJournal(jList[0].id, jList[0].display_name);
          }
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
      const jCountry = detRes.data?.profile?.country_code || detRes.data?.country_code;
      if (jCountry && jCountry !== filterCountry) {
        setFilterCountry(jCountry);
        if (setSelectedCountry) setSelectedCountry(jCountry);
      }
    }).catch(console.error).finally(() => setLoading(false));
  }, [selectedJournalId]);

  // Load journal collaboration network
  useEffect(() => {
    if (!selectedJournalId) return;
    const cleanJid = selectedJournalId.includes('/') ? selectedJournalId.split('/').pop() : selectedJournalId;
    setCollabLoading(true);
    api.get(`/journals/${encodeURIComponent(cleanJid)}/collaboration`)
      .then(res => setCollabData(res.data))
      .catch(err => {
        console.error('Error fetching journal collaboration:', err);
        setCollabData(null);
      })
      .finally(() => setCollabLoading(false));
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
      name: t('country.trace_published_docs'),
      type: 'bar',
      marker: { color: 'rgba(2, 132, 199, 0.65)' },
      yaxis: 'y'
    },
    {
      x: validAnnual.map(d => d.year),
      y: validAnnual.map(d => d.fwci_avg),
      name: t('journal.trace_fwci_annual'),
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


  // Radar Chart Traces
  const getRadarAxisLabel = (axis) => {
    switch (axis) {
      case 'OA Diamante': return t('journal.radar_axis_diamond');
      case 'Multilingüismo': return t('journal.radar_axis_multilingualism');
      case 'Internacionalización': return t('journal.radar_axis_internationalization');
      case 'Indexación': return t('journal.radar_axis_indexing');
      case 'Top 10%': return t('journal.radar_axis_top10');
      case 'FWCI': return t('journal.radar_axis_fwci');
      default: return axis;
    }
  };

  const radarTraces = [];
  if (radarData && radarData.axes) {
    const localizedAxes = radarData.axes.map(getRadarAxisLabel);
    const axes = [...localizedAxes, localizedAxes[0]]; // Close polygon
    
    // Journal
    const jVals = radarData.axes.map(a => radarData.journal[a] || 0);
    jVals.push(jVals[0]);
    radarTraces.push({
      type: 'scatterpolar',
      r: jVals,
      theta: axes,
      fill: 'toself',
      name: prof.display_name || t('journal.this_journal'),
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
        name: t('journal.radar_country_avg', { country: prof.country_code || '' }),
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
        name: t('journal.radar_latam_ref'),
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
    name: t('journal.trace_citations_per_work'),
    boxmean: true
  }] : [];

  // Active Pie Data (Full vs Recent)
  const activePieData = piePeriod === 'recent' ? recData : pData;

  const oaPieValues = [
    { label: t('common.diamond'), value: Number(activePieData.pct_oa_diamond || 0), color: '#38bdf8' },
    { label: t('common.gold'), value: Number(activePieData.pct_oa_gold || 0), color: '#fbbf24' },
    { label: t('common.green'), value: Number(activePieData.pct_oa_green || 0), color: '#4ade80' },
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
      name: t('journal.landscape_regional_ref'),
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
      name: t('journal.landscape_journal_articles', { name: prof.display_name || selectedJournalName || t('journal.default_journal') }),
      marker: {
        size: 7.5,
        color: journalArts.map(a => a.publication_year),
        colorscale: 'Turbo',
        cmin: minYr,
        cmax: maxYr,
        colorbar: { title: t('country.landscape_colorbar_title'), x: 1.02 },
        opacity: 0.9,
        line: { width: 0.8, color: '#ffffff' }
      },
      text: journalArts.map(a => a.title),
      customdata: journalArts.map(a => [
        a.publication_year || '—',
        a.fwci != null ? Number(a.fwci).toFixed(2) : '—',
        a.community_name || 'General'
      ]),
      hovertemplate: `<b>%{text}</b><br>${t('country.landscape_hover_year')}: %{customdata[0]} | FWCI: %{customdata[1]}<br>${t('country.landscape_hover_community')}: %{customdata[2]}<extra></extra>`
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

  const articleScatterIndicators = useMemo(() => [
    { id: 'fwci', label: t('journal.ind_art_fwci') },
    { id: 'cited_by_count', label: t('journal.ind_art_citations') },
    { id: 'percentile', label: t('journal.ind_art_percentile') },
    { id: 'publication_year', label: t('journal.ind_art_year') },
    { id: 'is_in_top_10_percent', label: t('journal.ind_art_top10') },
    { id: 'is_in_top_1_percent', label: t('journal.ind_art_top1') },
    { id: 'is_domestic_author', label: t('journal.ind_art_domestic') },
    { id: 'is_retracted', label: t('journal.ind_art_retracted') },
    { id: 'is_paratext', label: t('journal.ind_art_paratext') }
  ], [t]);

  const xLabelArt = articleScatterIndicators.find(i => i.id === artScatterX)?.label || artScatterX;
  const yLabelArt = articleScatterIndicators.find(i => i.id === artScatterY)?.label || artScatterY;

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
    hovertemplate: `<b>%{text}</b><br>${t('country.landscape_hover_year')}: %{customdata[0]} | OA: %{customdata[3]}<br>${xLabelArt}: %{x:,.2f}<br>${yLabelArt}: %{y:,.2f}<extra></extra>`
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
    hovertemplate: `<b>%{label}</b><br>${t('country.hierarchy_hover_articles')}: %{value:,.0f}<br>${t('thematic_table.color_label')}: %{color:.2f}<extra></extra>`
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
    hovertemplate: `<b>%{label}</b><br>${t('country.hierarchy_hover_articles')}: %{value:,.0f}<br>${t('thematic_table.color_label')}: %{color:.2f}<extra></extra>`
  }] : [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '28px' }}>
      {/* Search and Country Selector Header */}
      <div className="card" style={{ padding: '18px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
          {/* Quick Country + Journal Selector */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('journal.country_label')}</span>
              <select
                value={filterCountry}
                onChange={(e) => {
                  const newC = e.target.value;
                  setFilterCountry(newC);
                  if (newC !== 'ALL' && setSelectedCountry) {
                    setSelectedCountry(newC);
                  }
                }}
                style={{ fontSize: '13px', fontWeight: '700', padding: '6px 12px' }}
              >
                <option value="ALL">{t('journal.all_countries')}</option>
                {countriesList.map(c => (
                  <option key={c.country_code} value={c.country_code}>
                    {(c.country_code && t(`country_names.${c.country_code}`)) || c.country_name} ({c.country_code})
                  </option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('journal.journal_label')}</span>
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
                placeholder={t('journal.search_input_placeholder')}
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
                      if (j.country_code) {
                        setFilterCountry(j.country_code);
                        if (setSelectedCountry) setSelectedCountry(j.country_code);
                      }
                      setSearchQuery('');
                      setSearchResults([]);
                    }}
                    style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-color)', cursor: 'pointer', fontSize: '12.5px' }}
                    className="search-item-hover"
                  >
                    <strong>{j.display_name}</strong>
                    <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                      {j.publisher || t('journal.no_publisher')} • {(j.country_code && t(`country_names.${j.country_code}`)) || j.country_code} • {j.works_count} docs
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
              {prof.publisher ? `${prof.publisher} • ` : ''}{(prof.country_code && t(`country_names.${prof.country_code}`)) || prof.country_name || prof.country_code} • ISSN-L: <code>{prof.issn_l || t('journal.not_available')}</code>
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
                {t('journal.btn_website')} <ExternalLink size={12} />
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
            <button
              onClick={handleShareJournal}
              title={t('journal.share_tooltip')}
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
              <span>{copiedLink ? t('journal.link_copied') : t('journal.share_journal')}</span>
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
                {t('journal.card_impact_network')}
              </span>
            </div>
            <span className="badge" style={{ fontSize: '11px' }}>{t('journal.stat_period_full')}</span>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '14px' }}>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_docs')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                {fullDocs.toLocaleString()}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_citations')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#0284c7', marginTop: '2px' }}>
                {prof.cited_by_count?.toLocaleString() || '—'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_fwci')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--accent-primary)', marginTop: '2px' }}>
                {fullFwci.toFixed(2)}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_h_index')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#8b5cf6', marginTop: '2px' }}>
                {prof.h_index || '—'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_i10')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#6366f1', marginTop: '2px' }}>
                {prof.i10_index != null ? Number(prof.i10_index).toLocaleString() : '—'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_pagerank')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#06b6d4', marginTop: '2px' }}>
                {prof.pagerank != null ? Number(prof.pagerank).toFixed(3) : '0.000'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_eigenfactor')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#0ea5e9', marginTop: '2px' }}>
                {prof.eigenfactor != null ? `${Number(prof.eigenfactor).toFixed(4)}%` : '0.0000%'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_top10')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#f59e0b', marginTop: '2px' }}>
                {fullTop10.toFixed(2)}%
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_top1')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#ec4899', marginTop: '2px' }}>
                {fullTop1.toFixed(3)}%
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_percentile')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                {fullPerc.toFixed(1)}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_oa_diamond')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#38bdf8', marginTop: '2px' }}>
                {fullOaDiamond.toFixed(1)}%
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_oa_gold')}</div>
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
                {t('journal.card_recent_period')}
              </span>
            </div>
            <span className="badge" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981', border: '1px solid #10b981', fontSize: '11px' }}>
              {t('journal.badge_recent_lustrum')}
            </span>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '14px' }}>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_docs')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text-main)', marginTop: '2px' }}>
                {recDocs.toLocaleString()}
                <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginLeft: '4px', fontWeight: 'normal' }}>
                  ({pctRecDocs}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_citations_recent')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#0284c7', marginTop: '2px' }}>
                {recCites.toLocaleString()}
                <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginLeft: '4px', fontWeight: 'normal' }}>
                  ({pctRecCites}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_fwci')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recFwci.toFixed(2)}
                <span style={{ fontSize: '11px', color: Number(fwciDelta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(fwciDelta) >= 0 ? '+' : ''}{fwciDelta})
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_h_index_recent')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#8b5cf6', marginTop: '2px' }}>
                {recHIndex}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_i10_recent')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#6366f1', marginTop: '2px' }}>
                {recI10}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_pagerank')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#06b6d4', marginTop: '2px' }}>
                {recPagerank}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_eigenfactor')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#0ea5e9', marginTop: '2px' }}>
                {recEigenfactor}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_top10')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#f59e0b', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recTop10.toFixed(2)}%
                <span style={{ fontSize: '11px', color: Number(top10Delta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(top10Delta) >= 0 ? '+' : ''}{top10Delta}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_top1')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#ec4899', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recTop1.toFixed(3)}%
                <span style={{ fontSize: '11px', color: Number(top1Delta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(top1Delta) >= 0 ? '+' : ''}{top1Delta}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_percentile')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#10b981', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recPerc.toFixed(1)}
                <span style={{ fontSize: '11px', color: Number(percDelta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(percDelta) >= 0 ? '+' : ''}{percDelta})
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_oa_diamond')}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', color: '#38bdf8', marginTop: '2px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                {recOaDiamond.toFixed(1)}%
                <span style={{ fontSize: '11px', color: Number(oaDiamondDelta) >= 0 ? '#10b981' : '#ef4444', fontWeight: '700' }}>
                  ({Number(oaDiamondDelta) >= 0 ? '+' : ''}{oaDiamondDelta}%)
                </span>
              </div>
            </div>
            <div>
              <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontWeight: '600' }}>{t('journal.stat_oa_gold')}</div>
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
            {t('journal.dual_axis_title')}
          </h3>
        </div>
        <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          {t('journal.dual_axis_desc')}
        </p>

        <PlotlyChart
          data={dualAxisTraces}
          layout={{
            height: 360,
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
                {t('journal.pie_section_title')}
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('journal.pie_section_subtitle', { name: prof.display_name || selectedJournalName || t('journal.the_journal') })}
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

      {/* RADAR DE MADUREZ EDITORIAL & BOXPLOT DE CITAS */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(420px, 1fr))', gap: '20px' }}>
        {/* Radar Chart */}
        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
            <Radar size={18} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '15px', fontWeight: '700' }}>
              {t('journal.radar_title')}
            </h3>
          </div>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            {t('journal.radar_desc')}
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
                {t('journal.box_violin_title')}
              </h3>
            </div>
            <div className="segmented-pills">
              <button
                className={`segmented-pill-btn ${distPlotType === 'box' ? 'active' : ''}`}
                onClick={() => setDistPlotType('box')}
              >
                {t('journal.box_plot_btn')}
              </button>
              <button
                className={`segmented-pill-btn ${distPlotType === 'violin' ? 'active' : ''}`}
                onClick={() => setDistPlotType('violin')}
              >
                {t('journal.violin_plot_btn')}
              </button>
            </div>
          </div>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            {t('journal.box_violin_desc')}
          </p>

          <PlotlyChart
            data={distTraces}
            layout={{
              height: 380,
              margin: { l: 60, r: 20, t: 20, b: 30 },
              yaxis: { title: t('journal.yaxis_citations_received') }
            }}
          />
        </div>
      </div>

      {/* TRAYECTORIA MULTIDIMENSIONAL UMAP (REVISTA VS PAÍS) */}
      {trajectory && Object.keys(trajectory).length > 0 && (
        <UmapTrajectoryViewer
          title={t('journal.trajectory_title', {
            journal: prof.display_name || selectedJournalName || t('journal.default_journal'),
            country: (prof.country_code && t(`country_names.${prof.country_code}`)) || prof.country_name || prof.country_code || t('journal.default_country')
          })}
          subtitle={t('journal.trajectory_subtitle')}
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
              {t('journal.landscape_focus_title', { name: prof.display_name || selectedJournalName || t('journal.default_journal') })}
            </h3>
          </div>
          <span className="badge" style={{ fontSize: '11px' }}>
            {journalArts.length > 0 ? t('journal.landscape_articles_badge', { count: journalArts.length.toLocaleString() }) : t('journal.landscape_no_articles_badge')}
          </span>
        </div>
        <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          {t('journal.landscape_focus_desc')}
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
              {t('journal.drift_analysis_title')}
              <ul style={{ margin: '6px 0 0 18px', padding: 0 }}>
                <li><strong>{t('journal.drift_dispersion_label')}</strong> <code>{Number(landscapeData.dispersion || 0).toFixed(2)}</code> {t('journal.drift_dispersion_desc')}</li>
                <li><strong>{t('journal.drift_temporal_label')}</strong> {t('journal.drift_temporal_desc')}</li>
              </ul>
            </div>
          </>
        ) : (
          <div style={{ textAlign: 'center', padding: '50px 20px', color: 'var(--text-muted)' }}>
            {t('journal.landscape_empty_note')}
          </div>
        )}
      </div>

      {/* Composición Temática: Sunburst & Treemap */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px', flexWrap: 'wrap', gap: '10px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              {t('journal.thematic_comp_title')}
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('journal.thematic_comp_desc')}
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

            {/* Indicator Selector */}
            <select
              value={sunburstInd}
              onChange={(e) => setSunburstInd(e.target.value)}
              style={{ fontWeight: '600' }}
            >
              <option value="fwci_avg_recent">{t('journal.scatter_fwci_recent')}</option>
              <option value="avg_percentile_recent">{t('journal.scatter_percentile_recent')}</option>
              <option value="pct_top_10_recent">{t('journal.scatter_top10_recent')}</option>
              <option value="pct_oa_gold_recent">{t('journal.scatter_oa_gold_recent')}</option>
              <option value="fwci_avg_full">{t('journal.scatter_fwci_full')}</option>
              <option value="avg_percentile_full">{t('journal.scatter_percentile_full')}</option>
              <option value="pct_top_10_full">{t('journal.scatter_top10_full')}</option>
              <option value="pct_oa_gold_full">{t('journal.scatter_oa_gold_full')}</option>
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

      {/* MATRIZ DE COAUTORÍA PAÍS-PAÍS (CONNECTION MAP GLOBAL) */}
      <ConnectionMapViewer
        collabData={collabData}
        loading={collabLoading}
        title={`${t('connection_map.title')} — ${prof.display_name || selectedJournalName || ''}`}
        subtitle={t('connection_map.subtitle_journal')}
        anchorName={prof.display_name || selectedJournalName}
        anchorCode={prof.country_code}
        scopeType="journal"
      />

      {/* EXPLORADOR DE ARTÍCULOS - SCATTER PLOT DINÁMICO */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <TrendingUp size={18} color="var(--accent-primary)" />
              <h3 style={{ fontSize: '16px', fontWeight: '700', margin: 0 }}>
                {t('journal.article_scatter_title')}
              </h3>
            </div>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('journal.article_scatter_desc', { name: prof.display_name || selectedJournalName || t('journal.the_journal') })}
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            <span className="badge" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981', border: '1px solid #10b981', fontSize: '11.5px', fontWeight: '700' }}>
              {t('journal.articles_loaded_badge', { count: scatterArticles.length.toLocaleString() })}
            </span>
          </div>
        </div>

        {/* Controls: Eje X, Eje Y & Checkbox */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '14px', marginBottom: '16px', flexWrap: 'wrap', background: 'var(--bg-input)', padding: '12px 16px', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '12.5px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('journal.axis_x_label')}</span>
              <select
                value={artScatterX}
                onChange={(e) => setArtScatterX(e.target.value)}
                style={{ fontSize: '13px', fontWeight: '600', padding: '6px 12px' }}
              >
                {articleScatterIndicators.map(ind => (
                  <option key={ind.id} value={ind.id}>{ind.label}</option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '12.5px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('journal.axis_y_label')}</span>
              <select
                value={artScatterY}
                onChange={(e) => setArtScatterY(e.target.value)}
                style={{ fontSize: '13px', fontWeight: '600', padding: '6px 12px' }}
              >
                {articleScatterIndicators.map(ind => (
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
              {t('journal.show_all_articles_check', { count: validArticleScatterRows.length.toLocaleString() })}
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
                <strong style={{ color: 'var(--accent-primary)' }}>📊 {t('country.stats_title', { label: xLabelArt })}</strong>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '6px', marginTop: '8px', color: 'var(--text-muted)' }}>
                  <div>{t('country.stat_mean')}: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsXArt.mean.toFixed(2)}</span></div>
                  <div>{t('country.stat_median')}: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsXArt.median.toFixed(2)}</span></div>
                  <div>{t('country.stat_std')}: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsXArt.std.toFixed(2)}</span></div>
                  <div>{t('journal.stat_range')}: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>[{statsXArt.min.toFixed(2)} - {statsXArt.max.toFixed(2)}]</span></div>
                </div>
              </div>

              {/* Eje Y Stats */}
              <div style={{ padding: '12px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '12.5px' }}>
                <strong style={{ color: '#10b981' }}>📊 {t('country.stats_title', { label: yLabelArt })}</strong>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '6px', marginTop: '8px', color: 'var(--text-muted)' }}>
                  <div>{t('country.stat_mean')}: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsYArt.mean.toFixed(2)}</span></div>
                  <div>{t('country.stat_median')}: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsYArt.median.toFixed(2)}</span></div>
                  <div>{t('country.stat_std')}: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>{statsYArt.std.toFixed(2)}</span></div>
                  <div>{t('journal.stat_range')}: <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>[{statsYArt.min.toFixed(2)} - {statsYArt.max.toFixed(2)}]</span></div>
                </div>
              </div>

              {/* Pearson Correlation */}
              <div style={{ padding: '12px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '12.5px', display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
                <div style={{ color: 'var(--text-muted)' }}>{t('journal.pearson_label')}</div>
                <div style={{ fontSize: '20px', fontWeight: '800', color: Math.abs(pearsonRArt) >= 0.5 ? '#10b981' : 'var(--text-main)', marginTop: '4px' }}>
                  r = {pearsonRArt.toFixed(3)}
                </div>
                <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '2px' }}>
                  {Math.abs(pearsonRArt) >= 0.7 ? t('country.corr_strong') : (Math.abs(pearsonRArt) >= 0.3 ? t('country.corr_moderate') : t('country.corr_weak'))}
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
              {t('journal.articles_list_title', { count: articles.length, suffix: articleLimit === 0 ? t('journal.articles_of_total', { total: prof.works_count?.toLocaleString() || 'total' }) : t('journal.articles_shown_suffix') })}
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {t('journal.articles_table_desc')}
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            {/* Limit Selector */}
            <select
              value={articleLimit}
              onChange={(e) => setArticleLimit(Number(e.target.value))}
              style={{ fontSize: '12.5px', fontWeight: '600' }}
              title={t('journal.articles_limit_tooltip')}
            >
              <option value={50}>{t('journal.articles_limit_50')}</option>
              <option value={100}>{t('journal.articles_limit_100')}</option>
              <option value={500}>{t('journal.articles_limit_500')}</option>
              <option value={1000}>{t('journal.articles_limit_1000')}</option>
              <option value={0}>{t('journal.articles_all_option', { total: prof.works_count?.toLocaleString() || 'Total' })}</option>
            </select>

            {/* Sort Selector */}
            <select
              value={articleSort}
              onChange={(e) => setArticleSort(e.target.value)}
              style={{ fontSize: '12.5px', fontWeight: '600' }}
            >
              <option value="cited_by_count">{t('journal.sort_cited')}</option>
              <option value="fwci">{t('journal.sort_fwci')}</option>
              <option value="publication_year">{t('journal.sort_recent')}</option>
            </select>

            {/* Export Buttons */}
            <button
              className="btn-secondary"
              disabled={exportingFormat !== null}
              onClick={() => handleExportArticles('json')}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '6px',
                fontSize: '12px',
                padding: '6px 12px',
                opacity: exportingFormat ? 0.7 : 1,
                background: 'linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(5, 150, 105, 0.25))',
                borderColor: 'rgba(16, 185, 129, 0.45)',
                color: '#10b981',
                fontWeight: '600'
              }}
              title={t('journal.export_json_tooltip')}
            >
              <Download size={14} /> {exportingFormat === 'json' ? t('journal.exporting_json') : t('journal.export_json_btn')}
            </button>

            <button
              className="btn-secondary"
              disabled={exportingFormat !== null}
              onClick={() => handleExportArticles('csv')}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '6px',
                fontSize: '12px',
                padding: '6px 12px',
                opacity: exportingFormat ? 0.7 : 1,
                background: 'linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(5, 150, 105, 0.25))',
                borderColor: 'rgba(16, 185, 129, 0.45)',
                color: '#10b981',
                fontWeight: '600'
              }}
              title={t('journal.export_csv_cols_tooltip')}
            >
              <Download size={14} /> {exportingFormat === 'csv' ? t('journal.exporting_csv') : t('journal.export_csv_cols_btn')}
            </button>

            {user && (
              <button
                className="btn-secondary"
                disabled={exportingFormat !== null}
                onClick={() => handleExportArticles('metrics')}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '6px',
                  fontSize: '12px',
                  padding: '6px 12px',
                  opacity: exportingFormat ? 0.7 : 1,
                  background: 'linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(5, 150, 105, 0.25))',
                  borderColor: 'rgba(16, 185, 129, 0.45)',
                  color: '#10b981',
                  fontWeight: '600'
                }}
                title={t('journal.export_metrics_title') || "Calcular paquete completo de indicadores cienciométricos en segundo plano (.zip)"}
              >
                <BarChart3 size={14} /> {exportingFormat === 'metrics' ? t('journal.exporting_metrics') : t('journal.export_metrics')}
              </button>
            )}
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
                <th>{t('tables.title')}</th>
                <th>{t('tables.openalex_id')}</th>
                <th>{t('tables.year')}</th>
                <th>{t('tables.citations')}</th>
                <th>{t('tables.fwci')}</th>
                <th>{t('tables.percentile')}</th>
                <th>{t('tables.oa_type')}</th>
                <th>DOI</th>
              </tr>
            </thead>
            <tbody>
              {articles.map((art, idx) => (
                <tr key={idx}>
                  <td style={{ overflow: 'hidden' }}>
                    <strong
                      title={art.title || t('tables.no_title')}
                      style={{
                        display: 'block',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap'
                      }}
                    >
                      {art.title || t('tables.no_title')}
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
                        title={t('journal.open_in_openalex', { id: art.id })}
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
                        title={t('journal.open_doi', { doi: art.doi })}
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
        pageTitle={t('journal.dossier_page_title', { name: details?.display_name || selectedJournalName })}
        pageDescription={t('journal.dossier_page_desc', { name: details?.display_name || selectedJournalName })}
        sections={[
          {
            id: 'journal_profile',
            title: t('journal.dossier_sec1_title', { name: details?.display_name || selectedJournalName }),
            category: t('journal.dossier_sec1_cat'),
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
            title: t('journal.dossier_sec2_title'),
            category: t('journal.dossier_sec2_cat'),
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
            title: t('journal.dossier_sec3_title', { name: details?.display_name || selectedJournalName }),
            category: t('journal.dossier_sec3_cat'),
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
            title: t('journal.dossier_sec4_title', { period: piePeriod === 'recent' ? '2021–2025' : '0–2026' }),
            category: t('journal.dossier_sec4_cat'),
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
            title: t('journal.dossier_sec5_title'),
            category: t('journal.dossier_sec5_cat'),
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
            title: t('journal.dossier_sec6_title', { plotType: distPlotType === 'box' ? (t('journal.box_plot_btn') || 'Box Plot') : (t('journal.violin_plot_btn') || 'Violin Plot') }),
            category: t('journal.dossier_sec6_cat'),
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
            title: t('journal.dossier_sec7_title', { name: details?.display_name || selectedJournalName }),
            category: t('journal.dossier_sec7_cat'),
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
            title: t('journal.dossier_sec8_title', { count: landscapeData.articles?.length || 0 }),
            category: t('journal.dossier_sec8_cat'),
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
            title: t('journal.dossier_sec9_title', { viewType: thematicViewType === 'sunburst' ? (t('country.hierarchy_sunburst') || 'Sunburst Radial') : (t('country.hierarchy_treemap') || 'Treemap') }),
            category: t('journal.dossier_sec9_cat'),
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
            title: t('journal.dossier_sec10_title', { x: xLabelArt, y: yLabelArt }),
            category: t('journal.dossier_sec10_cat'),
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
            title: t('journal.dossier_sec11_title', { top: Math.min(articles.length, 15) }),
            category: t('journal.dossier_sec11_cat'),
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
            {t('journal.view_downloads')}
          </button>
        </div>
      )}
    </div>
  );
}
