import React, { useState, useEffect } from 'react';
import api from '../api';
import { useTranslation } from '../i18n';
import PlotlyChart from '../components/PlotlyChart';
import { Share2, GitFork, Globe2, CircleDot, Network } from 'lucide-react';

export default function NetworksPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState('collab'); // 'collab' | 'chord' | 'alluvial' | 'sankey'
  const [collabData, setCollabData] = useState({ nodes: [], edges: [] });
  const [sankeyData, setSankeyData] = useState(null);
  const [chordData, setChordData] = useState({ entities: [], matrix: [] });
  const [alluvialData, setAlluvialData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.get('/networks/collaboration'),
      api.get('/networks/sankey'),
      api.get('/networks/chord'),
      api.get('/networks/alluvial')
    ]).then(([collabRes, sankeyRes, chordRes, alluvRes]) => {
      setCollabData(collabRes.data);
      setSankeyData(sankeyRes.data);
      setChordData(chordRes.data);
      setAlluvialData(alluvRes.data);
    }).catch(console.error).finally(() => setLoading(false));
  }, []);

  // Prepare Co-authorship Map Traces
  const geoTraces = [];
  if (collabData.nodes.length > 0) {
    // Node markers
    geoTraces.push({
      type: 'scattergeo',
      lat: collabData.nodes.map(n => n.lat),
      lon: collabData.nodes.map(n => n.lon),
      text: collabData.nodes.map(n => n.name),
      mode: 'markers+text',
      marker: { size: 9, color: '#0284c7' },
      textposition: 'top center',
      name: t('kpi.countries')
    });

    // Edges
    collabData.edges.forEach((e) => {
      const srcNode = collabData.nodes.find(n => n.id === e.source);
      const tgtNode = collabData.nodes.find(n => n.id === e.target);
      if (srcNode && tgtNode) {
        geoTraces.push({
          type: 'scattergeo',
          lat: [srcNode.lat, tgtNode.lat],
          lon: [srcNode.lon, tgtNode.lon],
          mode: 'lines',
          line: {
            width: Math.max(1, Math.min(5, (e.weight || 100) / 800)),
            color: 'rgba(2, 132, 199, 0.55)'
          },
          hoverinfo: 'text',
          text: `${e.source} - ${e.target}: ${e.weight?.toLocaleString()}`,
          showlegend: false
        });
      }
    });
  }

  // Sankey Trace
  const sankeyTrace = sankeyData && sankeyData.node_labels ? [{
    type: 'sankey',
    node: {
      pad: 15,
      thickness: 20,
      line: { color: 'black', width: 0.5 },
      label: sankeyData.node_labels,
      color: '#0284c7'
    },
    link: {
      source: sankeyData.links.source,
      target: sankeyData.links.target,
      value: sankeyData.links.value,
      color: 'rgba(2, 132, 199, 0.25)'
    }
  }] : [];

  // Alluvial Trace
  const alluvialTrace = alluvialData && alluvialData.node_labels ? [{
    type: 'sankey',
    node: {
      pad: 18,
      thickness: 22,
      line: { color: 'black', width: 0.5 },
      label: alluvialData.node_labels,
      color: '#10b981'
    },
    link: {
      source: alluvialData.links.source,
      target: alluvialData.links.target,
      value: alluvialData.links.value,
      color: 'rgba(16, 185, 129, 0.25)'
    }
  }] : [];

  // Chord Heatmap Matrix Trace
  const chordHeatmapTrace = chordData.entities.length > 0 ? [{
    type: 'heatmap',
    z: chordData.matrix,
    x: chordData.entities.map(e => e.name),
    y: chordData.entities.map(e => e.name),
    colorscale: 'Blues',
    colorbar: { title: 'Coautorías' },
    hovertemplate: '<b>%{y}</b> - <b>%{x}</b><br>Coautorías: %{z:,}<extra></extra>'
  }] : [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: '800' }}>
          🌐 {t('networks.title')}
        </h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
          {t('networks.subtitle')}
        </p>

        {/* Tabs */}
        <div className="tab-container" style={{ marginTop: '18px' }}>
          <button
            className={`tab-btn ${activeTab === 'collab' ? 'active' : ''}`}
            onClick={() => setActiveTab('collab')}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <Globe2 size={16} /> {t('networks.tab_coauthorship')}
          </button>
          <button
            className={`tab-btn ${activeTab === 'chord' ? 'active' : ''}`}
            onClick={() => setActiveTab('chord')}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <CircleDot size={16} /> {t('networks.tab_matrix')}
          </button>
          <button
            className={`tab-btn ${activeTab === 'alluvial' ? 'active' : ''}`}
            onClick={() => setActiveTab('alluvial')}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <GitFork size={16} /> {t('networks.sankey_title')}
          </button>
          <button
            className={`tab-btn ${activeTab === 'sankey' ? 'active' : ''}`}
            onClick={() => setActiveTab('sankey')}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <Network size={16} /> {t('networks.tab_sankey')}
          </button>
        </div>
      </div>

      {activeTab === 'collab' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          <div className="card">
            <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '14px' }}>
              🌍 Matriz de Coautoría País-País (Connection Map Global)
            </h3>
            <PlotlyChart
              data={geoTraces}
              layout={{
                geo: {
                  scope: 'world',
                  showcountries: true,
                  countrycolor: 'rgba(200, 200, 200, 0.5)',
                  projection: { type: 'equirectangular' },
                  center: { lat: 5, lon: -60 },
                  projection_scale: 1.4
                },
                height: 560,
                margin: { l: 10, r: 10, t: 30, b: 10 }
              }}
            />
          </div>

          {/* Table */}
          <div className="card">
            <h3 style={{ fontSize: '15px', fontWeight: '700', marginBottom: '12px' }}>
              Top Pares de Colaboración Internacional
            </h3>
            <div className="data-table-container" style={{ maxHeight: '350px' }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>{t('tables.origin_country')}</th>
                    <th>{t('tables.partner_country')}</th>
                    <th>{t('tables.coauthorship_articles')}</th>
                  </tr>
                </thead>
                <tbody>
                  {collabData.edges.map((e, idx) => (
                    <tr key={idx}>
                      <td><strong>{e.source}</strong></td>
                      <td><strong>{e.target}</strong></td>
                      <td>{e.weight?.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {activeTab === 'chord' && (
        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
            <CircleDot size={18} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              Matriz Circular de Coautoría Bilateral en América Latina (Cooperación Sur-Sur)
            </h3>
          </div>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
            Intensidad de artículos científicos co-publicados entre los 10 países con mayor producción en la región.
          </p>
          <PlotlyChart
            data={chordHeatmapTrace}
            layout={{
              height: 540,
              margin: { l: 140, r: 20, t: 20, b: 90 },
              xaxis: { tickangle: -35 }
            }}
          />
        </div>
      )}

      {activeTab === 'alluvial' && (
        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
            <GitFork size={18} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
              Diagrama Alluvial — Flujo Dominio del Conocimiento → Vía de Acceso Abierto
            </h3>
          </div>
          <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '14px' }}>
            Mapea la canalización del conocimiento según el modelo de acceso abierto (Diamante universitario vs Gold con APC).
          </p>
          <PlotlyChart
            data={alluvialTrace}
            layout={{ height: 600, margin: { l: 20, r: 20, t: 20, b: 20 } }}
          />
        </div>
      )}

      {activeTab === 'sankey' && (
        <div className="card">
          <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '14px' }}>
            🔀 Flujo de Producción Científica: Dominio → Campo → Subcampo
          </h3>
          <PlotlyChart
            data={sankeyTrace}
            layout={{ height: 620, margin: { l: 20, r: 20, t: 20, b: 20 } }}
          />
        </div>
      )}
    </div>
  );
}
