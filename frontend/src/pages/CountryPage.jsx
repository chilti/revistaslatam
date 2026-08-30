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
  TrendingUp, 
  ShieldCheck, 
  Compass,
  PlusCircle,
  ExternalLink
} from 'lucide-react';

export default function CountryPage() {
  const { selectedCountry, setSelectedCountry, setSelectedJournal, setActiveSection, addDossierItem } = useAppStore();
  
  const [countriesList, setCountriesList] = useState([]);
  const [summary, setSummary] = useState(null);
  const [annualTrends, setAnnualTrends] = useState([]);
  const [annualWindow, setAnnualWindow] = useState(0);
  const [countrySunburst, setCountrySunburst] = useState(null);
  const [sunburstIndicator, setSunburstIndicator] = useState('fwci_avg_recent');
  const [sunburstUnclassified, setSunburstUnclassified] = useState(true);
  const [journals, setJournals] = useState([]);
  const [trajectory, setTrajectory] = useState({});
  const [landscapeArticles, setLandscapeArticles] = useState([]);
  
  const [scatterX, setScatterX] = useState('works_count');
  const [scatterY, setScatterY] = useState('fwci_avg');
  
  const [loading, setLoading] = useState(true);

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

  // Load country details on selectedCountry change
  useEffect(() => {
    if (!selectedCountry) return;
    setLoading(true);

    Promise.all([
      api.get(`/countries/${selectedCountry}/summary`),
      api.get(`/countries/${selectedCountry}/annual?window=${annualWindow}`),
      api.get(`/countries/${selectedCountry}/journals`),
      api.get(`/countries/${selectedCountry}/trajectory`),
      api.get(`/countries/${selectedCountry}/landscape?limit=2500`)
    ]).then(([sumRes, annRes, jRes, trajRes, landRes]) => {
      setSummary(sumRes.data);
      setAnnualTrends(annRes.data);
      setJournals(jRes.data);
      setTrajectory(trajRes.data);
      setLandscapeArticles(landRes.data);
    }).catch(console.error).finally(() => setLoading(false));
  }, [selectedCountry]);

  // Load sunburst
  useEffect(() => {
    if (!selectedCountry) return;
    api.get(`/countries/${selectedCountry}/sunburst?indicator=${sunburstIndicator}&include_unclassified=${sunburstUnclassified}`)
      .then(res => setCountrySunburst(res.data))
      .catch(console.error);
  }, [selectedCountry, sunburstIndicator, sunburstUnclassified]);

  // Reload annual on window change
  useEffect(() => {
    if (!selectedCountry) return;
    api.get(`/countries/${selectedCountry}/annual?window=${annualWindow}`)
      .then(res => setAnnualTrends(res.data))
      .catch(console.error);
  }, [annualWindow]);

  const pData = summary?.full_period || {};
  const recData = summary?.recent_period || {};

  // Sunburst Trace
  const sunburstTrace = countrySunburst && countrySunburst.nodes.length > 0 ? [{
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
        </div>
      </div>

      {/* KPI Cards Row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px' }}>
        <KpiCard title="Revistas" value={summary?.num_journals?.toLocaleString()} icon={BookOpen} />
        <KpiCard title="Artículos" value={summary?.total_works?.toLocaleString()} icon={FileText} />
        <KpiCard title="FWCI Promedio" value={Number(pData.fwci_avg || 0).toFixed(2)} icon={Zap} />
        <KpiCard title="% Top 10%" value={`${Number(pData.pct_top_10 || 0).toFixed(3)}%`} icon={TrendingUp} />
        <KpiCard title="% OA Diamante" value={`${Number(pData.pct_oa_diamond || 0).toFixed(1)}%`} icon={Sparkles} />
      </div>

      {/* Multi-tier Indicator Breakdown */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '18px' }}>
        {/* Impact & Citation */}
        <div className="card">
          <h3 style={{ fontSize: '14px', fontWeight: '700', color: 'var(--accent-primary)', marginBottom: '10px' }}>
            📊 Impacto y Citación
          </h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', fontSize: '13px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>Documentos:</span>
              <strong>{Number(pData.num_documents || 0).toLocaleString()}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>FWCI Promedio:</span>
              <strong>{Number(pData.fwci_avg || 0).toFixed(2)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% Artículos Top 10%:</span>
              <strong>{Number(pData.pct_top_10 || 0).toFixed(3)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% Artículos Top 1%:</span>
              <strong>{Number(pData.pct_top_1 || 0).toFixed(3)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>Percentil Promedio Norm.:</span>
              <strong>{Number(pData.avg_percentile || 0).toFixed(2)}</strong>
            </div>
          </div>
        </div>

        {/* Open Access & Indexing */}
        <div className="card">
          <h3 style={{ fontSize: '14px', fontWeight: '700', color: 'var(--accent-success)', marginBottom: '10px' }}>
            🔓 Ciencia Abierta e Indexación
          </h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', fontSize: '13px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% OA Diamante:</span>
              <strong>{Number(pData.pct_oa_diamond || 0).toFixed(1)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% OA Gold:</span>
              <strong>{Number(pData.pct_oa_gold || 0).toFixed(1)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% OA Verde:</span>
              <strong>{Number(pData.pct_oa_green || 0).toFixed(1)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% en Scopus:</span>
              <strong>{Number(pData.pct_scopus || 0).toFixed(1)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% en DOAJ:</span>
              <strong>{Number(pData.pct_doaj || 0).toFixed(1)}%</strong>
            </div>
          </div>
        </div>

        {/* Languages & Domestic Authorship */}
        <div className="card">
          <h3 style={{ fontSize: '14px', fontWeight: '700', color: '#f59e0b', marginBottom: '10px' }}>
            🌐 Multilingüismo y Autoría
          </h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', fontSize: '13px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% Idioma Español:</span>
              <strong>{Number(pData.pct_lang_es || 0).toFixed(1)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% Idioma Inglés:</span>
              <strong>{Number(pData.pct_lang_en || 0).toFixed(1)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% Idioma Portugués:</span>
              <strong>{Number(pData.pct_lang_pt || 0).toFixed(1)}%</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>% Autoría Doméstica:</span>
              <strong>{Number(pData.pct_authors_domestic || 0).toFixed(1)}%</strong>
            </div>
          </div>
        </div>
      </div>

      {/* Trajectory in UMAP */}
      <div className="card">
        <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '8px' }}>
          📈 Trayectoria de Desempeño Multidimensional (UMAP 2000–2025)
        </h3>
        <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginBottom: '14px' }}>
          Evolución del país en el espacio latente compuesto por producción, impacto (FWCI, Top 10%), ciencia abierta e idioma inglés frente al promedio de Iberoamérica.
        </p>
        <PlotlyChart
          data={trajTraces}
          layout={{ height: 480, xaxis: { title: 'UMAP Dimensión 1' }, yaxis: { title: 'UMAP Dimensión 2' } }}
        />
      </div>

      {/* Huella Semántica y Evolución Temporal de Artículos */}
      {landscapeArticles.length > 0 && (
        <div className="card">
          <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '8px' }}>
            🌌 Huella Semántica y Evolución Temporal de Artículos de {summary?.country_name}
          </h3>
          <p style={{ fontSize: '12.5px', color: 'var(--text-muted)', marginBottom: '14px' }}>
            Muestra de {landscapeArticles.length.toLocaleString()} artículos del país sobre el paisaje regional UMAP (Coloreado por año en escala Turbo).
          </p>
          <PlotlyChart
            data={[{
              x: landscapeArticles.map(a => a.umap_x),
              y: landscapeArticles.map(a => a.umap_y),
              mode: 'markers',
              marker: {
                size: 6,
                color: landscapeArticles.map(a => a.publication_year || 2020),
                colorscale: 'Turbo',
                showscale: true,
                colorbar: { title: 'Año' },
                opacity: 0.75
              },
              text: landscapeArticles.map(a => `${a.title}<br>Año: ${a.publication_year} | Revista: ${a.journal_name}`),
              type: 'scatter'
            }]}
            layout={{ height: 500, xaxis: { title: 'UMAP 1' }, yaxis: { title: 'UMAP 2' } }}
          />
        </div>
      )}

      {/* Sunburst of Country */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            🏵️ Temáticas de Investigación en {summary?.country_name} (Sunburst 4 Niveles)
          </h3>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <select
              value={sunburstIndicator}
              onChange={(e) => setSunburstIndicator(e.target.value)}
            >
              <option value="fwci_avg_recent">FWCI (2021-2025)</option>
              <option value="avg_percentile_recent">Percentil (2021-2025)</option>
              <option value="pct_top_10_recent">% Top 10% (2021-2025)</option>
              <option value="fwci_avg_full">FWCI (Todo)</option>
            </select>
          </div>
        </div>
        <PlotlyChart data={sunburstTrace} layout={{ height: 500 }} />
      </div>

      {/* Annual Trends */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>⏳ Tendencias Anuales de {summary?.country_name}</h3>
          <div className="segmented-pills">
            <button className={`segmented-pill-btn ${annualWindow === 0 ? 'active' : ''}`} onClick={() => setAnnualWindow(0)}>Crudos</button>
            <button className={`segmented-pill-btn ${annualWindow === 3 ? 'active' : ''}`} onClick={() => setAnnualWindow(3)}>w=3</button>
            <button className={`segmented-pill-btn ${annualWindow === 5 ? 'active' : ''}`} onClick={() => setAnnualWindow(5)}>w=5</button>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', gap: '16px' }}>
          <PlotlyChart
            data={[{ x: annualTrends.map(d => d.year), y: annualTrends.map(d => d.num_documents), type: 'scatter', mode: 'lines+markers', name: 'Documentos' }]}
            layout={{ title: 'Evolución de Documentos', height: 300 }}
          />
          <PlotlyChart
            data={[
              { x: annualTrends.map(d => d.year), y: annualTrends.map(d => d.fwci_avg), type: 'scatter', mode: 'lines+markers', name: 'FWCI Promedio', line: { color: '#10b981' } },
              { x: annualTrends.map(d => d.year), y: annualTrends.map(() => 1.0), type: 'scatter', mode: 'lines', name: 'Media Mundial (1.0)', line: { color: '#ef4444', dash: 'dash' } }
            ]}
            layout={{ title: 'Evolución FWCI', height: 300 }}
          />
        </div>
      </div>

      {/* Scatter Explorer of Country Journals */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '10px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            🎯 Explorador Scatter de Revistas de {summary?.country_name}
          </h3>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <select value={scatterX} onChange={(e) => setScatterX(e.target.value)}>
              {SCATTER_OPTIONS.map(s => <option key={s.id} value={s.id}>Eje X: {s.label}</option>)}
            </select>
            <select value={scatterY} onChange={(e) => setScatterY(e.target.value)}>
              {SCATTER_OPTIONS.map(s => <option key={s.id} value={s.id}>Eje Y: {s.label}</option>)}
            </select>
          </div>
        </div>
        <PlotlyChart
          data={[{
            x: journals.map(j => j[scatterX] || 0),
            y: journals.map(j => j[scatterY] || 0),
            mode: 'markers',
            marker: { size: 8, color: '#0284c7', opacity: 0.7 },
            text: journals.map(j => `${j.display_name}<br>${scatterX}: ${j[scatterX]}<br>${scatterY}: ${j[scatterY]}`),
            type: 'scatter'
          }]}
          layout={{ height: 400, xaxis: { title: SCATTER_OPTIONS.find(s => s.id === scatterX)?.label }, yaxis: { title: SCATTER_OPTIONS.find(s => s.id === scatterY)?.label } }}
        />
      </div>

      {/* Journals in Country Table */}
      <div className="card">
        <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '14px' }}>
          📖 Revistas Publicadas en {summary?.country_name} ({journals.length} revistas)
        </h3>
        <div className="data-table-container" style={{ maxHeight: '420px' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Revista</th>
                <th>Editorial</th>
                <th>Artículos</th>
                <th>Citas</th>
                <th>FWCI</th>
                <th>Índice H</th>
                <th>DOAJ</th>
                <th>SciELO</th>
                <th>Scopus</th>
                <th>Acción</th>
              </tr>
            </thead>
            <tbody>
              {journals.map((j) => (
                <tr key={j.id}>
                  <td><strong>{j.display_name}</strong></td>
                  <td>{j.publisher || 'N/A'}</td>
                  <td>{j.works_count?.toLocaleString()}</td>
                  <td>{j.cited_by_count?.toLocaleString()}</td>
                  <td>{Number(j.fwci_avg || j['2yr_mean_citedness'] || 0).toFixed(2)}</td>
                  <td>{j.h_index || '—'}</td>
                  <td>{j.is_in_doaj ? '✅' : '❌'}</td>
                  <td>{j.is_in_scielo ? '✅' : '❌'}</td>
                  <td>{j.is_scopus ? '✅' : '❌'}</td>
                  <td>
                    <button
                      onClick={() => {
                        setSelectedJournal(j.id, j.display_name);
                        setActiveSection('journal');
                      }}
                      className="segmented-pill-btn"
                      style={{ fontSize: '11px', padding: '4px 8px', background: 'var(--accent-primary)', color: '#ffffff' }}
                    >
                      Ver Ficha
                    </button>
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
