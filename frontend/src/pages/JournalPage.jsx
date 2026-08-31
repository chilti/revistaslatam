import React, { useState, useEffect, useRef } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import KpiCard from '../components/KpiCard';
import PlotlyChart from '../components/PlotlyChart';
import UmapTrajectoryViewer from '../components/UmapTrajectoryViewer';
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
  Calendar,
  Globe,
  ListFilter,
  Radar,
  Activity,
  BoxSelect,
  GitCommit,
  Download
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
  const { selectedJournalId, selectedJournalName, setSelectedJournal, addDossierItem } = useAppStore();
  
  // Search state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  
  // Country & Journal Combo state
  const [countriesList, setCountriesList] = useState(DEFAULT_COUNTRIES);
  const [filterCountry, setFilterCountry] = useState('MX');
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
  
  const [landscapeData, setLandscapeData] = useState({ articles: [], dispersion: 0 });
  const [trajectory, setTrajectory] = useState({});
  const [loading, setLoading] = useState(true);

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
    const code = filterCountry || 'MX';
    setLoadingJournals(true);
    
    const fetchPromise = code === 'ALL'
      ? api.get('/journals/search?limit=300')
      : api.get(`/countries/${code}/journals`);

    fetchPromise
      .then(res => {
        const jList = res.data || [];
        setCountryJournals(jList);
        if (jList.length > 0 && (!selectedJournalId || !jList.some(j => j.id === selectedJournalId))) {
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

    const jidParam = encodeURIComponent(selectedJournalId);

    Promise.all([
      api.get(`/journals/${jidParam}/details`),
      api.get(`/journals/${jidParam}/annual?min_year=1970&max_year=2026`),
      api.get(`/journals/${jidParam}/articles?sort_by=${articleSort}${articleYearFilter ? `&year=${articleYearFilter}` : ''}&limit=${articleLimit}`),
      api.get(`/journals/${jidParam}/landscape`),
      api.get(`/journals/${jidParam}/trajectory`),
      api.get(`/journals/${jidParam}/radar-profile`),
      api.get(`/journals/${jidParam}/citations-distribution`),
      api.get(`/journals/${jidParam}/connected-trajectory`)
    ]).then(([detRes, annRes, artRes, landRes, trajRes, radarRes, citRes, connRes]) => {
      setDetails(detRes.data);
      setAnnualTrends(annRes.data || []);
      setArticles(artRes.data || []);
      setLandscapeData(landRes.data || { articles: [], dispersion: 0 });
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
    const jidParam = encodeURIComponent(selectedJournalId);
    setLoadingArticles(true);
    api.get(`/journals/${jidParam}/articles?sort_by=${articleSort}${articleYearFilter ? `&year=${articleYearFilter}` : ''}&limit=${articleLimit}`)
      .then(res => setArticles(res.data || []))
      .catch(console.error)
      .finally(() => setLoadingArticles(false));
  }, [articleSort, articleYearFilter, articleLimit]);

  // Load sunburst / treemap
  useEffect(() => {
    if (!selectedJournalId) return;
    const jidParam = encodeURIComponent(selectedJournalId);
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

  // Connected Scatter Plot (Volumen vs FWCI Trajectory)
  const connectedTraces = connectedTraj.length > 0 ? [{
    x: connectedTraj.map(d => d.num_documents),
    y: connectedTraj.map(d => d.fwci_avg),
    text: connectedTraj.map(d => String(d.year)),
    type: 'scatter',
    mode: 'lines+markers+text',
    textposition: 'top center',
    line: { color: '#0284c7', shape: 'spline', width: 2.5 },
    marker: {
      size: 8,
      color: connectedTraj.map(d => d.year),
      colorscale: 'Viridis',
      showscale: true,
      colorbar: { title: 'Año', thickness: 12 }
    },
    hovertemplate: '<b>Año %{text}</b><br>Artículos: %{x}<br>FWCI: %{y:.2f}<extra></extra>'
  }] : [];

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
            <DossierButton
              item={{
                key: `journal_profile_${selectedJournalId}`,
                title: `Revista: ${prof.display_name || selectedJournalName}`,
                context: `${prof.publisher ? prof.publisher + ' · ' : ''}${prof.country_name || ''} · ISSN-L: ${prof.issn_l || '—'} · ${prof.works_count?.toLocaleString()} artículos · FWCI ${prof.fwci_avg ?? '—'}`,
                category: 'Perfil Revista',
                data: [prof]
              }}
              label="Guardar Revista"
            />
          </div>
        </div>
      </div>

      {/* KPI Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px' }}>
        <KpiCard
          title="Total Artículos"
          value={prof.works_count?.toLocaleString()}
          subtitle="En OpenAlex Snapshot"
          icon={BookOpen}
        />
        <KpiCard
          title="Total Citas"
          value={prof.cited_by_count?.toLocaleString()}
          subtitle="Impacto de Citación"
          icon={FileText}
        />
        <KpiCard
          title="FWCI Promedio"
          value={Number(prof.fwci_avg || 0).toFixed(2)}
          subtitle="Citas Ponderadas"
          icon={Zap}
          badge={prof.fwci_avg >= 1.0 ? 'Superior al Mundo' : 'Regional'}
        />
        <KpiCard
          title="Índice H"
          value={prof.h_index || '—'}
          subtitle="Consistencia Editorial"
          icon={Award}
        />
        <KpiCard
          title="% OA Diamante"
          value={`${Number(prof.pct_oa_diamond || 0).toFixed(1)}%`}
          subtitle="Sin Cobro por APC"
          icon={Sparkles}
          badge="Diamante"
        />
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
          title={`📈 Trayectoria Multidimensional UMAP: ${details?.display_name || selectedJournalName || 'Revista'} vs ${details?.country_code || 'País'}`}
          subtitle="Evolución temporal continua del perfil cienciométrico en el espacio 2D UMAP frente al promedio de su país."
          trajectories={trajectory}
          allowTrajectoryFilter={true}
          showGridSection={true}
          height={460}
        />
      )}

      {/* CONNECTED SCATTER PLOT: Trayectoria Volumen vs Impacto */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
          <GitCommit size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            Gráfico de Dispersión Conectado (Connected Scatter Plot) — Trayectoria de Fase
          </h3>
        </div>
        <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          Muestra la evolución conjunta de volumen de producción (Eje X) e impacto FWCI (Eje Y) a lo largo de los años.
        </p>

        <PlotlyChart
          data={connectedTraces}
          layout={{
            height: 380,
            margin: { l: 60, r: 20, t: 20, b: 40 },
            xaxis: { title: 'Volumen de Artículos Publicados' },
            yaxis: { title: 'FWCI Promedio' }
          }}
        />
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

            {/* CSV Download Button */}
            <button
              className="btn-secondary"
              onClick={() => {
                if (!articles || articles.length === 0) return;
                const headers = ['OpenAlex_ID', 'DOI', 'Titulo', 'Ano', 'Citas', 'FWCI', 'Percentil', 'Acceso_Abierto', 'Idioma'];
                const rows = articles.map(art => [
                  `"${art.id || ''}"`,
                  `"${art.doi || ''}"`,
                  `"${(art.title || '').replace(/"/g, '""')}"`,
                  art.publication_year || '',
                  art.cited_by_count || 0,
                  Number(art.fwci || 0).toFixed(2),
                  Number(art.percentile || 0).toFixed(1),
                  `"${art.oa_status || 'closed'}"`,
                  `"${art.language || ''}"`
                ]);
                const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [headers.join(','), ...rows.map(e => e.join(','))].join('\n');
                const encodedUri = encodeURI(csvContent);
                const link = document.createElement('a');
                link.setAttribute('href', encodedUri);
                const countTag = articleLimit === 0 ? 'todos' : `${articles.length}_articulos`;
                link.setAttribute('download', `articulos_${(prof.display_name || 'revista').slice(0, 30).replace(/[^a-zA-Z0-9]/g, '_')}_${countTag}_${new Date().toISOString().slice(0, 10)}.csv`);
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
              }}
              style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', padding: '6px 12px' }}
              title="Descargar listado de artículos en formato CSV"
            >
              <Download size={14} /> Descargar CSV
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
                `| % Artículos en Idioma Inglés | ${Number(details.pct_lang_en || 0).toFixed(1)}% | Internacionalización lingüística |`,
                `| Sello DOAJ | ${details.is_in_doaj ? '✅ Indexada con Sello' : '❌ No'} | Calidad de acceso abierto |`,
                `| Scopus | ${details.is_scopus ? '✅ Indexada' : '❌ No'} | Cobertura en Scopus |`,
                `| SciELO | ${details.is_in_scielo ? '✅ Indexada' : '❌ No'} | Cobertura en SciELO |`
              ].join('\n');
            }
          },
          {
            id: 'journal_annual',
            title: `2. Evolución Anual de Producción y Citas (${details?.display_name || selectedJournalName})`,
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
            id: 'journal_radar',
            title: '3. Perfil Multidimensional (Radar Chart de Desempeño)',
            category: 'Perfil Multidimensional',
            defaultChecked: false,
            rawData: radarData,
            buildDataText: () => {
              if (!radarData || !radarData.dimensions) return 'No hay datos de radar disponibles.';
              const lines = [
                '| Dimensión Evaluada | Valor de la Revista | Media de Referencia |',
                '|---|---|---|'
              ];
              (radarData.dimensions || []).forEach(d => {
                lines.push(`| ${d.label || d.name} | ${Number(d.value || 0).toFixed(2)} | ${Number(d.benchmark || 1.0).toFixed(2)} |`);
              });
              return lines.join('\n');
            }
          },
          {
            id: 'journal_thematic_hierarchy',
            title: `4. Estructura Temática de la Revista (${thematicViewType === 'sunburst' ? 'Sunburst Radial' : 'Treemap'})`,
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
            id: 'citations_distribution',
            title: `5. Distribución del Impacto de Artículos (${distPlotType === 'box' ? 'Box Plot' : 'Violin Plot'})`,
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
                `*Distribución de Citas por Artículo (${cits.length} artículos analizados):*\n`,
                '| Estadístico | Citas por Artículo |',
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
            title: `6. Trayectoria Multidimensional UMAP (${details?.display_name || selectedJournalName})`,
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
            title: `7. Paisaje Semántico de Artículos (Navegador 2D con ${landscapeData.articles?.length || 0} artículos)`,
            category: 'Semántica / Procesamiento de Lenguaje Natural',
            defaultChecked: false,
            rawData: landscapeData,
            buildDataText: () => {
              if (!landscapeData || !landscapeData.articles || landscapeData.articles.length === 0) return 'No hay artículos en el mapa semántico.';
              const lines = [
                `*Dispersión Semántica de la Revista:* **${Number(landscapeData.dispersion || 0).toFixed(3)}**\n`,
                '| Título del Artículo | Año | Coordenadas Semánticas (X, Y) | Citas |',
                '|---|---|---|---|'
              ];
              landscapeData.articles.slice(0, 15).forEach(a => {
                const titleClean = (a.title || 'Sin título').replace(/\|/g, '-');
                lines.push(`| ${titleClean.slice(0, 65)}... | ${a.publication_year || a.year || '—'} | (${Number(a.x || 0).toFixed(2)}, ${Number(a.y || 0).toFixed(2)}) | ${a.cited_by_count || 0} |`);
              });
              if (landscapeData.articles.length > 15) {
                lines.push(`\n_... y ${landscapeData.articles.length - 15} artículos más en la proyección semántica._`);
              }
              return lines.join('\n');
            }
          },
          {
            id: 'top_articles',
            title: `8. Listado de Artículos Más Citados de la Revista (Top ${Math.min(articles.length, 15)})`,
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
    </div>
  );
}
