import React, { useState, useEffect } from 'react';
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
  ListFilter
} from 'lucide-react';

export default function JournalPage() {
  const { selectedJournalId, selectedJournalName, setSelectedJournal } = useAppStore();
  
  // Search state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  
  // Country & Journal Combo state
  const [countriesList, setCountriesList] = useState([]);
  const [filterCountry, setFilterCountry] = useState('MX');
  const [countryJournals, setCountryJournals] = useState([]);
  const [loadingJournals, setLoadingJournals] = useState(false);
  
  // Journal data
  const [details, setDetails] = useState(null);
  const [annualTrends, setAnnualTrends] = useState([]);
  const [sunburstData, setSunburstData] = useState(null);
  const [sunburstInd, setSunburstInd] = useState('fwci_avg_recent');
  
  const [articles, setArticles] = useState([]);
  const [articleSort, setArticleSort] = useState('cited_by_count');
  const [articleYearFilter, setArticleYearFilter] = useState('');
  
  const [landscapeData, setLandscapeData] = useState({ articles: [], dispersion: 0 });
  const [trajectory, setTrajectory] = useState({});
  const [loading, setLoading] = useState(true);

  // Load countries catalog
  useEffect(() => {
    api.get('/countries')
      .then(res => setCountriesList(res.data))
      .catch(console.error);
  }, []);

  // Load journals for the selected country combo
  useEffect(() => {
    if (!filterCountry) return;
    setLoadingJournals(true);
    
    if (filterCountry === 'ALL') {
      api.get('/journals/search?limit=300')
        .then(res => setCountryJournals(res.data))
        .catch(console.error)
        .finally(() => setLoadingJournals(false));
    } else {
      api.get(`/countries/${filterCountry}/journals`)
        .then(res => setCountryJournals(res.data))
        .catch(console.error)
        .finally(() => setLoadingJournals(false));
    }
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
        .then(res => setSearchResults(res.data))
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
      api.get(`/journals/${jidParam}/annual`),
      api.get(`/journals/${jidParam}/articles?sort_by=${articleSort}${articleYearFilter ? `&year=${articleYearFilter}` : ''}&limit=100`),
      api.get(`/journals/${jidParam}/landscape`),
      api.get(`/journals/${jidParam}/trajectory`)
    ]).then(([detRes, annRes, artRes, landRes, trajRes]) => {
      setDetails(detRes.data);
      setAnnualTrends(annRes.data);
      setArticles(artRes.data);
      setLandscapeData(landRes.data);
      setTrajectory(trajRes.data);

      // Auto-sync country filter if available
      const jCountry = detRes.data?.profile?.country_code;
      if (jCountry && jCountry !== filterCountry && filterCountry !== 'ALL') {
        setFilterCountry(jCountry);
      }
    }).catch(console.error).finally(() => setLoading(false));
  }, [selectedJournalId, articleSort, articleYearFilter]);

  // Sunburst
  useEffect(() => {
    if (!selectedJournalId) return;
    const jidParam = encodeURIComponent(selectedJournalId);
    api.get(`/journals/${jidParam}/sunburst?indicator=${sunburstInd}`)
      .then(res => setSunburstData(res.data))
      .catch(console.error);
  }, [selectedJournalId, sunburstInd]);

  const profile = details?.profile || {};
  const pData = details?.full_period || {};
  const recData = details?.recent_period || {};

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
      line: { shape: 'spline', width: item.is_country ? 3 : 2, color: item.is_country ? '#f59e0b' : '#0284c7' }
    });
  });

  // Landscape Scatter Trace
  const landscapeTraces = [
    {
      x: landscapeData.articles.map(a => a.umap_x),
      y: landscapeData.articles.map(a => a.umap_y),
      mode: 'markers',
      marker: {
        size: 8,
        color: landscapeData.articles.map(a => a.publication_year || 2020),
        colorscale: 'Turbo',
        showscale: true,
        colorbar: { title: 'Año' }
      },
      text: landscapeData.articles.map(a => `${a.title}<br>Año: ${a.publication_year} | FWCI: ${a.fwci}`),
      name: profile.display_name
    }
  ];

  // Radar Trace for recent performance
  const radarCategories = ['FWCI Ponderado', 'Percentil', '% Top 10%', '% Top 1%', '% OA Diamante', '% Autoría Doméstica'];
  const radarValues = [
    Math.min(100, (Number(recData.fwci_avg || profile.fwci_avg || 0) / 2.0) * 100),
    Number(recData.avg_percentile || profile.avg_percentile || 0),
    Math.min(100, (Number(recData.pct_top_10 || 0) / 20.0) * 100),
    Math.min(100, (Number(recData.pct_top_1 || 0) / 2.0) * 100),
    Number(recData.pct_oa_diamond || profile.pct_oa_diamond || 0),
    Number(recData.pct_authors_domestic || profile.pct_authors_domestic || 0)
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '28px' }}>
      {/* Search & Country/Journal Combos Container */}
      <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: '16px', position: 'relative' }}>
        
        {/* Row 1: Instant Text Autocomplete Search */}
        <div>
          <label style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)', marginBottom: '6px', display: 'flex', alignItems: 'center', gap: '6px' }}>
            <Search size={14} color="var(--accent-primary)" /> Buscador Instantáneo por Nombre o ISSN:
          </label>
          <div style={{
            display: 'flex',
            alignItems: 'center',
            background: 'var(--bg-input)',
            border: '1px solid var(--border-color)',
            borderRadius: '8px',
            padding: '8px 14px',
            gap: '10px'
          }}>
            <Search size={16} color="var(--text-muted)" />
            <input
              type="text"
              placeholder="Escribe el nombre de la revista o ISSN (ej. Estudios Demográficos, 0186-7210)..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              style={{ flex: 1, border: 'none', background: 'transparent', fontSize: '13.5px', outline: 'none', color: 'var(--text-main)' }}
            />
            {isSearching && <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Buscando...</span>}
          </div>

          {/* Autocomplete dropdown results */}
          {searchResults.length > 0 && (
            <div style={{
              position: 'absolute',
              top: '74px',
              left: '20px',
              right: '20px',
              zIndex: 100,
              background: 'var(--bg-card)',
              border: '1px solid var(--border-color)',
              borderRadius: '10px',
              boxShadow: 'var(--card-shadow)',
              maxHeight: '340px',
              overflowY: 'auto'
            }}>
              {searchResults.map((j) => (
                <div
                  key={j.id}
                  onClick={() => {
                    setSelectedJournal(j.id, j.display_name);
                    if (j.country_code) setFilterCountry(j.country_code);
                    setSearchQuery('');
                    setSearchResults([]);
                  }}
                  style={{
                    padding: '12px 18px',
                    borderBottom: '1px solid var(--border-color)',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between'
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-input)'}
                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                >
                  <div>
                    <div style={{ fontSize: '13.5px', fontWeight: '700', color: 'var(--text-main)' }}>
                      {j.display_name}
                    </div>
                    <div style={{ fontSize: '11.5px', color: 'var(--text-muted)' }}>
                      {j.country_code} • ISSN: {j.issn_l || 'N/A'} • {j.publisher || 'Editorial no registrada'}
                    </div>
                  </div>
                  <span className="badge">
                    {j.works_count?.toLocaleString()} arts
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Row 2: Cascading Country and Journal Combos */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
          gap: '14px',
          paddingTop: '12px',
          borderTop: '1px solid var(--border-color)'
        }}>
          {/* Country Combo */}
          <div>
            <label style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)', marginBottom: '6px', display: 'flex', alignItems: 'center', gap: '6px' }}>
              <Globe size={14} color="var(--accent-primary)" /> 1. Filtrar por País:
            </label>
            <select
              value={filterCountry}
              onChange={(e) => setFilterCountry(e.target.value)}
              style={{ width: '100%', fontSize: '13.5px', fontWeight: '600', padding: '9px 12px' }}
            >
              <option value="ALL">🌐 Todos los Países (Iberoamérica)</option>
              {countriesList.map(c => (
                <option key={c.country_code} value={c.country_code}>
                  {c.country_name} ({c.country_code}) — {c.num_journals} revistas
                </option>
              ))}
            </select>
          </div>

          {/* Journal Combo */}
          <div>
            <label style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)', marginBottom: '6px', display: 'flex', alignItems: 'center', gap: '6px' }}>
              <ListFilter size={14} color="var(--accent-primary)" /> 2. Seleccionar Revista ({countryJournals.length} disponibles):
            </label>
            <select
              value={selectedJournalId}
              onChange={(e) => {
                const chosen = countryJournals.find(j => j.id === e.target.value);
                setSelectedJournal(e.target.value, chosen?.display_name || selectedJournalName);
              }}
              style={{ width: '100%', fontSize: '13.5px', fontWeight: '600', padding: '9px 12px' }}
            >
              {countryJournals.map(j => (
                <option key={j.id} value={j.id}>
                  {j.display_name} ({j.country_code || 'LATAM'}) — {j.works_count?.toLocaleString()} arts
                </option>
              ))}
            </select>
          </div>
        </div>

      </div>

      {/* Journal Technical Profile Header */}
      <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '10px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <h2 style={{ fontSize: '22px', fontWeight: '800' }}>
                {profile.display_name || selectedJournalName}
              </h2>
              {profile.community_name && (
                <span className="badge">🏷️ {profile.community_name}</span>
              )}
            </div>
            <div style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginTop: '4px' }}>
              ISSN-L: <strong>{profile.issn_l || 'N/A'}</strong> • País: <strong>{profile.country_name || profile.country_code}</strong> • Editorial: <strong>{profile.publisher || 'N/A'}</strong>
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <a
              href={profile.id || selectedJournalId}
              target="_blank"
              rel="noreferrer"
              className="segmented-pill-btn"
              style={{ display: 'flex', alignItems: 'center', gap: '5px', textDecoration: 'none', background: 'var(--bg-input)' }}
            >
              <ExternalLink size={13} /> OpenAlex ↗
            </a>
          </div>
        </div>
      </div>

      {/* KPIs Fila 1: Producción y Citación */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px' }}>
        <KpiCard title="Total Documentos" value={profile.works_count?.toLocaleString()} icon={FileText} />
        <KpiCard title="Total Citas" value={profile.cited_by_count?.toLocaleString()} icon={Award} />
        <KpiCard title="FWCI Promedio" value={Number(profile.fwci_avg || profile['2yr_mean_citedness'] || 0).toFixed(2)} icon={Zap} />
        <KpiCard title="Índice H" value={profile.h_index || '—'} icon={TrendingUp} />
      </div>

      {/* KPIs Fila 2: Indicadores Avanzados & Red */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px' }}>
        <KpiCard title="Índice i10" value={profile.i10_index || '—'} />
        <KpiCard title="PageRank Citas (‰)" value={Number(profile.pagerank || 0).toFixed(3)} />
        <KpiCard title="Eigenfactor Score (%)" value={Number(profile.eigenfactor || 0).toFixed(4)} />
        <KpiCard title="Percentil Promedio" value={Number(profile.avg_percentile || 0).toFixed(1)} />
      </div>

      {/* KPIs Fila 3: Ciencia Abierta & Indexación */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px' }}>
        <KpiCard title="% OA Diamante" value={`${Number(profile.pct_oa_diamond || 0).toFixed(1)}%`} badge="Diamante" />
        <KpiCard title="% OA Dorado" value={`${Number(profile.pct_oa_gold || 0).toFixed(1)}%`} />
        <KpiCard title="En DOAJ" value={profile.is_in_doaj ? '✅ Sí' : '❌ No'} />
        <KpiCard title="En Scopus" value={profile.is_scopus ? '✅ Sí' : '❌ No'} />
      </div>

      {/* Radar de Desempeño Reciente & Trayectoria UMAP */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', gap: '20px' }}>
        {/* Radar */}
        <div className="card">
          <h3 style={{ fontSize: '15px', fontWeight: '700', marginBottom: '8px' }}>
            🕸️ Radar de Desempeño Multidimensional
          </h3>
          <PlotlyChart
            data={[{
              type: 'scatterpolar',
              r: radarValues,
              theta: radarCategories,
              fill: 'toself',
              fillcolor: 'rgba(2, 132, 199, 0.2)',
              line: { color: '#0284c7' },
              name: profile.display_name
            }]}
            layout={{
              polar: {
                radialaxis: { visible: true, range: [0, 100] }
              },
              height: 380,
              margin: { t: 30, b: 30, l: 30, r: 30 }
            }}
          />
        </div>

        {/* Trajectory */}
        <div className="card">
          <h3 style={{ fontSize: '15px', fontWeight: '700', marginBottom: '8px' }}>
            📈 Trayectoria UMAP: Revista vs País
          </h3>
          <PlotlyChart
            data={trajTraces}
            layout={{ height: 380, xaxis: { title: 'UMAP 1' }, yaxis: { title: 'UMAP 2' }, margin: { t: 30, b: 30, l: 30, r: 30 } }}
          />
        </div>
      </div>

      {/* Foco Temático y Deriva Longitudinal */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            🌌 Foco Temático y Deriva Temporal en el Paisaje Regional
          </h3>
          <span className="badge" style={{ fontSize: '12px' }}>
            Dispersión Semántica: {landscapeData.dispersion}
          </span>
        </div>
        <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          Proyección cronológica de los artículos de la revista sobre el espacio UMAP regional. Una menor dispersión indica alta especialización temática; la superposición de colores antiguos (azules) y recientes (amarillos/rojos) indica estabilidad editorial frente a deriva temática.
        </p>
        <PlotlyChart data={landscapeTraces} layout={{ height: 480 }} />
      </div>

      {/* Sunburst of Journal */}
      {sunburstData && sunburstData.nodes.length > 0 && (
        <div className="card">
          <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '12px' }}>
            🏵️ Temáticas de Investigación de la Revista (Sunburst 4 Niveles)
          </h3>
          <PlotlyChart
            data={[{
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
              branchvalues: 'total'
            }]}
            layout={{ height: 480 }}
          />
        </div>
      )}

      {/* Annual Trends of Journal */}
      <div className="card">
        <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '14px' }}>
          ⏳ Tendencias Anuales de la Revista
        </h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(380px, 1fr))', gap: '16px' }}>
          <PlotlyChart
            data={[{ x: annualTrends.map(d => d.year), y: annualTrends.map(d => d.num_documents), type: 'scatter', mode: 'lines+markers', name: 'Documentos' }]}
            layout={{ title: 'Evolución de Documentos', height: 280 }}
          />
          <PlotlyChart
            data={[
              { x: annualTrends.map(d => d.year), y: annualTrends.map(d => d.fwci_avg), type: 'scatter', mode: 'lines+markers', name: 'FWCI Promedio', line: { color: '#10b981' } },
              { x: annualTrends.map(d => d.year), y: annualTrends.map(() => 1.0), type: 'scatter', mode: 'lines', name: 'Media Mundial (1.0)', line: { color: '#ef4444', dash: 'dash' } }
            ]}
            layout={{ title: 'Evolución FWCI', height: 280 }}
          />
        </div>
      </div>

      {/* Detailed Articles Table directly from DuckDB */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              📄 Listado de Artículos de la Revista (Motor DuckDB OLAP)
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Consultas directas a los 3.63M trabajos con latencia &lt; 15 ms.
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <select
              value={articleSort}
              onChange={(e) => setArticleSort(e.target.value)}
            >
              <option value="cited_by_count">Más Citados</option>
              <option value="publication_year">Más Recientes</option>
              <option value="fwci">Mayor FWCI</option>
            </select>
          </div>
        </div>

        <div className="data-table-container" style={{ maxHeight: '420px' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Título del Artículo</th>
                <th>Año</th>
                <th>Citas</th>
                <th>FWCI</th>
                <th>DOI</th>
              </tr>
            </thead>
            <tbody>
              {articles.map((art) => (
                <tr key={art.id}>
                  <td style={{ maxWidth: '420px', whiteSpace: 'normal' }}>
                    <strong>{art.title || 'Sin título'}</strong>
                  </td>
                  <td>{art.publication_year || '—'}</td>
                  <td><strong>{art.cited_by_count?.toLocaleString()}</strong></td>
                  <td>{Number(art.fwci || 0).toFixed(2)}</td>
                  <td>
                    {art.doi ? (
                      <a
                        href={art.doi.startsWith('http') ? art.doi : `https://doi.org/${art.doi}`}
                        target="_blank"
                        rel="noreferrer"
                        style={{ color: 'var(--accent-primary)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: '3px' }}
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
    </div>
  );
}
