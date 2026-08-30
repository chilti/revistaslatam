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
  CheckCircle2
} from 'lucide-react';

export default function RegionalPage() {
  const { addDossierItem } = useAppStore();
  
  // State
  const [kpis, setKpis] = useState(null);
  const [choroplethData, setChoroplethData] = useState([]);
  const [selectedMapIndicator, setSelectedMapIndicator] = useState('num_journals');
  const [periods, setPeriods] = useState(null);
  const [distributions, setDistributions] = useState({ oa: [], languages: [] });
  const [sunburstData, setSunburstData] = useState(null);
  const [selectedSunburstInd, setSelectedSunburstInd] = useState('fwci_avg_recent');
  const [sunburstUnclassified, setSunburstUnclassified] = useState(true);
  const [thematicLevel, setThematicLevel] = useState('domain');
  const [thematicProfiles, setThematicProfiles] = useState(null);
  const [annualTrends, setAnnualTrends] = useState([]);
  const [annualWindow, setAnnualWindow] = useState(0);
  const [rankingsPeriod, setRankingsPeriod] = useState('full');
  const [rankings, setRankings] = useState([]);
  const [trajectories, setTrajectories] = useState(null);
  const [radarProfiles, setRadarProfiles] = useState(null);
  const [umapSimilarity, setUmapSimilarity] = useState([]);
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

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        const [kpiRes, choroRes, periodsRes, distRes, annualRes, rankRes, trajRes, radarRes, umapRes] = await Promise.all([
          api.get('/regional/kpis'),
          api.get(`/regional/choropleth?indicator=${selectedMapIndicator}`),
          api.get('/regional/periods-comparison'),
          api.get('/regional/distributions'),
          api.get(`/regional/annual-trends?window=${annualWindow}`),
          api.get(`/regional/rankings?period=${rankingsPeriod}`),
          api.get('/regional/trajectories'),
          api.get('/regional/radar-profiles'),
          api.get('/regional/umap-similarity')
        ]);

        setKpis(kpiRes.data);
        setChoroplethData(choroRes.data);
        setPeriods(periodsRes.data);
        setDistributions(distRes.data);
        setAnnualTrends(annualRes.data);
        setRankings(rankRes.data);
        setTrajectories(trajRes.data);
        setRadarProfiles(radarRes.data);
        setUmapSimilarity(umapRes.data);
      } catch (err) {
        console.error('Error loading regional data:', err);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  // Reload Sunburst
  useEffect(() => {
    api.get(`/regional/sunburst?indicator=${selectedSunburstInd}&include_unclassified=${sunburstUnclassified}`)
      .then(res => setSunburstData(res.data))
      .catch(console.error);
  }, [selectedSunburstInd, sunburstUnclassified]);

  // Reload Thematic Profiles
  useEffect(() => {
    api.get(`/regional/thematic-profiles?level=${thematicLevel}`)
      .then(res => setThematicProfiles(res.data))
      .catch(console.error);
  }, [thematicLevel]);

  // Reload Annual Trends on smoothing change
  useEffect(() => {
    api.get(`/regional/annual-trends?window=${annualWindow}`)
      .then(res => setAnnualTrends(res.data))
      .catch(console.error);
  }, [annualWindow]);

  // Reload Rankings on period change
  useEffect(() => {
    api.get(`/regional/rankings?period=${rankingsPeriod}`)
      .then(res => setRankings(res.data))
      .catch(console.error);
  }, [rankingsPeriod]);

  // Reload Scatter explorer
  useEffect(() => {
    api.get(`/regional/journals-scatter?x_col=${scatterX}&y_col=${scatterY}`)
      .then(res => setScatterData(res.data))
      .catch(console.error);
  }, [scatterX, scatterY]);

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
    height: 550,
    margin: { l: 0, r: 0, t: 40, b: 0 }
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

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '32px' }}>
      {/* Header & Page Title */}
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: '800' }}>Panorama Regional</h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '4px' }}>
          Visión macro de la producción, impacto cienciométrico y modelos de acceso abierto en América Latina.
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

      {/* Periods Comparison & Distributions */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', gap: '20px' }}>
        {/* Historical vs Recent */}
        <div className="card">
          <h3 style={{ fontSize: '15px', fontWeight: '700', marginBottom: '12px' }}>
            📈 Desempeño: Periodo Completo vs Reciente (2021-2025)
          </h3>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '14px' }}>
            <div style={{ background: 'var(--bg-input)', padding: '14px', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
              <div style={{ fontSize: '12px', fontWeight: '700', color: 'var(--accent-primary)', marginBottom: '8px' }}>Periodo Histórico</div>
              <div style={{ fontSize: '12px', display: 'flex', flexDirection: 'column', gap: '4px' }}>
                <div><strong>Docs:</strong> {periods?.full_period?.num_documents?.toLocaleString() || '—'}</div>
                <div><strong>FWCI:</strong> {Number(periods?.full_period?.fwci_avg || 0).toFixed(2)}</div>
                <div><strong>Top 10%:</strong> {Number(periods?.full_period?.pct_top_10 || 0).toFixed(3)}%</div>
                <div><strong>Top 1%:</strong> {Number(periods?.full_period?.pct_top_1 || 0).toFixed(3)}%</div>
              </div>
            </div>
            <div style={{ background: 'var(--bg-input)', padding: '14px', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
              <div style={{ fontSize: '12px', fontWeight: '700', color: 'var(--accent-success)', marginBottom: '8px' }}>Reciente (2021-2025)</div>
              <div style={{ fontSize: '12px', display: 'flex', flexDirection: 'column', gap: '4px' }}>
                <div><strong>Docs:</strong> {periods?.recent_period?.num_documents?.toLocaleString() || '—'}</div>
                <div><strong>FWCI:</strong> {Number(periods?.recent_period?.fwci_avg || 0).toFixed(2)}</div>
                <div><strong>Top 10%:</strong> {Number(periods?.recent_period?.pct_top_10 || 0).toFixed(3)}%</div>
                <div><strong>Top 1%:</strong> {Number(periods?.recent_period?.pct_top_1 || 0).toFixed(3)}%</div>
              </div>
            </div>
          </div>
        </div>

        {/* Distributions Pie Charts */}
        <div className="card">
          <h3 style={{ fontSize: '15px', fontWeight: '700', marginBottom: '12px' }}>
            🔓 Acceso Abierto e Idiomas de Publicación
          </h3>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
            <PlotlyChart
              data={[{
                type: 'pie',
                values: distributions.oa.map(d => d.percentage),
                labels: distributions.oa.map(d => d.type),
                textinfo: 'percent+label',
                hole: 0.45
              }]}
              layout={{ height: 230, margin: { l: 10, r: 10, t: 10, b: 10 }, showlegend: false }}
            />
            <PlotlyChart
              data={[{
                type: 'pie',
                values: distributions.languages.map(d => d.percentage),
                labels: distributions.languages.map(d => d.language),
                textinfo: 'percent+label',
                hole: 0.45
              }]}
              layout={{ height: 230, margin: { l: 10, r: 10, t: 10, b: 10 }, showlegend: false }}
            />
          </div>
        </div>
      </div>

      {/* Sunburst Section */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>🏵️ Temáticas de Investigación Regionales (Sunburst 4 Niveles)</h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Jerarquía: Dominio → Campo → Subcampo → Tópico.</span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
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
              <span>Incluir Sin Clasificación</span>
            </label>
          </div>
        </div>

        <PlotlyChart data={sunburstTrace} layout={{ height: 550, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
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
            layout={{ title: 'Evolución de Documentos Publicados', height: 320 }}
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
            layout={{ title: 'Evolución del FWCI Promedio', height: 320 }}
          />
        </div>
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
    </div>
  );
}
