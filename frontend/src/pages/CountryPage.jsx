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
  ExternalLink,
  Grid,
  TrendingDown,
  Layers
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
      api.get(`/countries/${selectedCountry}/landscape?limit=2500`),
      api.get(`/countries/${selectedCountry}/slope-data`),
      api.get(`/countries/${selectedCountry}/journals-distribution`)
    ]).then(([sumRes, annRes, jRes, trajRes, landRes, slopeRes, distRes]) => {
      setSummary(sumRes.data);
      setAnnualTrends(annRes.data);
      setJournals(jRes.data);
      setTrajectory(trajRes.data);
      setLandscapeArticles(landRes.data);
      setSlopeData(slopeRes.data);
      setJournalsDist(distRes.data);
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
    api.get(`/countries/${selectedCountry}/annual?window=${annualWindow}&min_year=1970&max_year=2026`)
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

      {/* KPI Cards Grid */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: '14px' }}>
        <KpiCard
          title="Revistas Activas"
          value={summary?.num_journals?.toLocaleString()}
          subtitle="En OpenAlex Snapshot"
          icon={BookOpen}
        />
        <KpiCard
          title="Total Artículos"
          value={summary?.total_works?.toLocaleString()}
          subtitle="Producción Histórica"
          icon={FileText}
        />
        <KpiCard
          title="FWCI Promedio"
          value={pData?.fwci_avg}
          subtitle="Citas Ponderadas"
          icon={Zap}
          badge={pData?.fwci_avg >= 1.0 ? 'Superior al Mundo' : 'Nivel País'}
        />
        <KpiCard
          title="% OA Diamante"
          value={`${pData?.pct_oa_diamond || 0}%`}
          subtitle="Sin Cobro por APC"
          icon={Sparkles}
          badge="Diamante"
        />
        <KpiCard
          title="Revistas DOAJ"
          value={`${pData?.pct_doaj || 0}%`}
          subtitle="Con Sello Abierto"
          icon={ShieldCheck}
        />
      </div>

      {/* DUAL-AXIS CHART: Producción Anual vs FWCI */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
          <TrendingUp size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            Gráfico de Eje Dual (Dual-Axis Chart) — Producción Anual vs FWCI Ponderado
          </h3>
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

      {/* Sunburst Section */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              🏵️ Jerarquía Temática Nacional: Dominio → Campo → Subcampo
            </h3>
            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              Especialización temática y masa crítica de artículos del país.
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <select
              value={sunburstIndicator}
              onChange={(e) => setSunburstIndicator(e.target.value)}
              style={{ fontWeight: '600' }}
            >
              <option value="fwci_avg_recent">FWCI (2021-2025)</option>
              <option value="avg_percentile_recent">Percentil (2021-2025)</option>
              <option value="pct_top_10_recent">% Top 10% (2021-2025)</option>
              <option value="pct_oa_gold_recent">% OA Gold (2021-2025)</option>
            </select>
          </div>
        </div>

        <PlotlyChart data={sunburstTrace} layout={{ height: 520, margin: { t: 10, l: 10, r: 10, b: 10 } }} />
      </div>

      {/* Journals Catalog Table */}
      <div className="card">
        <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '12px' }}>
          📚 Catálogo de Revistas de {summary?.country_name || selectedCountry} ({journals.length})
        </h3>
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
    </div>
  );
}
