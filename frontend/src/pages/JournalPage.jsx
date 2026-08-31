import React, { useState, useEffect, useRef } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import KpiCard from '../components/KpiCard';
import PlotlyChart from '../components/PlotlyChart';
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
  GitCommit
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
  const { selectedJournalId, selectedJournalName, setSelectedJournal } = useAppStore();
  
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
  const [sunburstData, setSunburstData] = useState(null);
  const [sunburstInd, setSunburstInd] = useState('fwci_avg_recent');
  
  // New Visualizations data
  const [radarData, setRadarData] = useState(null);
  const [citationsDist, setCitationsDist] = useState({ citations: [], fwci: [], percentiles: [], years: [] });
  const [distPlotType, setDistPlotType] = useState('box'); // 'box' | 'violin'
  const [connectedTraj, setConnectedTraj] = useState([]);
  
  const [articles, setArticles] = useState([]);
  const [articleSort, setArticleSort] = useState('cited_by_count');
  const [articleYearFilter, setArticleYearFilter] = useState('');
  
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
        const list = res.data || [];
        if (list.length > 0) {
          setCountryJournals(list);
          const exists = list.some(j => j.id === selectedJournalId);
          if (!exists) {
            setSelectedJournal(list[0].id, list[0].display_name);
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

  // Search autocomplete
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
      api.get(`/journals/${jidParam}/articles?sort_by=${articleSort}${articleYearFilter ? `&year=${articleYearFilter}` : ''}&limit=100`),
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

      const jCountry = detRes.data?.profile?.country_code;
      if (jCountry && jCountry !== filterCountry && filterCountry !== 'ALL') {
        setFilterCountry(jCountry);
      }
    }).catch(console.error).finally(() => setLoading(false));
  }, [selectedJournalId, articleSort, articleYearFilter]);

  // Load sunburst
  useEffect(() => {
    if (!selectedJournalId) return;
    const jidParam = encodeURIComponent(selectedJournalId);
    api.get(`/journals/${jidParam}/sunburst?indicator=${sunburstInd}`)
      .then(res => setSunburstData(res.data))
      .catch(console.error);
  }, [selectedJournalId, sunburstInd]);

  const pData = details?.full_period || {};
  const recData = details?.recent_period || {};
  const prof = details?.profile || {};

  // Dual-Axis Chart: Volume vs FWCI
  const dualAxisTraces = [
    {
      x: annualTrends.map(d => d.year),
      y: annualTrends.map(d => d.num_documents),
      name: 'Artículos Publicados',
      type: 'bar',
      marker: { color: 'rgba(2, 132, 199, 0.65)' },
      yaxis: 'y'
    },
    {
      x: annualTrends.map(d => d.year),
      y: annualTrends.map(d => d.fwci_avg),
      name: 'FWCI Anual',
      type: 'scatter',
      mode: 'lines+markers',
      line: { color: '#10b981', width: 3 },
      marker: { size: 6, color: '#10b981' },
      yaxis: 'y2'
    },
    {
      x: annualTrends.map(d => d.year),
      y: annualTrends.map(() => 1.0),
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
  const sunburstTrace = sunburstData && sunburstData.nodes?.length > 0 ? [{
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

      {/* Sunburst de la Revista */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px', flexWrap: 'wrap', gap: '10px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            🏵️ Composición Temática de la Revista
          </h3>
          <select
            value={sunburstInd}
            onChange={(e) => setSunburstInd(e.target.value)}
            style={{ fontWeight: '600' }}
          >
            <option value="fwci_avg_recent">FWCI (2021-2025)</option>
            <option value="avg_percentile_recent">Percentil (2021-2025)</option>
            <option value="pct_top_10_recent">% Top 10% (2021-2025)</option>
            <option value="pct_oa_gold_recent">% OA Gold (2021-2025)</option>
          </select>
        </div>

        <PlotlyChart data={sunburstTrace} layout={{ height: 480, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
      </div>

      {/* Top Articles Table */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>📄 Artículos de la Revista ({articles.length})</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Muestra de artículos ordenados por impacto o citación.</span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <select
              value={articleSort}
              onChange={(e) => setArticleSort(e.target.value)}
              style={{ fontSize: '12.5px', fontWeight: '600' }}
            >
              <option value="cited_by_count">Más Citados</option>
              <option value="fwci">Mayor FWCI</option>
              <option value="publication_year">Más Recientes</option>
            </select>
          </div>
        </div>

        <div className="data-table-container" style={{ maxHeight: '420px' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Título</th>
                <th>Año</th>
                <th>Citas</th>
                <th>FWCI</th>
                <th>Acceso Abierto</th>
                <th>Autores</th>
                <th>Enlace</th>
              </tr>
            </thead>
            <tbody>
              {articles.map((art, idx) => (
                <tr key={idx}>
                  <td><strong>{art.title}</strong></td>
                  <td>{art.publication_year}</td>
                  <td>{art.cited_by_count?.toLocaleString()}</td>
                  <td>{Number(art.fwci || 0).toFixed(2)}</td>
                  <td><span className="badge">{art.oa_status || 'closed'}</span></td>
                  <td><span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>{art.authors || '—'}</span></td>
                  <td>
                    {art.doi ? (
                      <a href={art.doi} target="_blank" rel="noreferrer" style={{ color: 'var(--accent-primary)', textDecoration: 'underline', fontSize: '11px' }}>
                        DOI
                      </a>
                    ) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
