import React, { useState, useEffect } from 'react';
import api from '../api';
import PlotlyChart from '../components/PlotlyChart';
import { Share2, GitFork, Globe2 } from 'lucide-react';

export default function NetworksPage() {
  const [activeTab, setActiveTab] = useState('collab');
  const [collabData, setCollabData] = useState({ nodes: [], edges: [] });
  const [sankeyData, setSankeyData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.get('/networks/collaboration'),
      api.get('/networks/sankey')
    ]).then(([collabRes, sankeyRes]) => {
      setCollabData(collabRes.data);
      setSankeyData(sankeyRes.data);
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
      name: 'Países'
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
          text: `${e.source} - ${e.target}: ${e.weight?.toLocaleString()} colaboraciones`,
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

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: '800' }}>
          🌐 Redes de Colaboración Internacional y Flujos Disciplinares
        </h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
          Análisis de coautoría internacional y flujos de conocimiento interdisciplinares para Latinoamérica.
        </p>

        {/* Tabs */}
        <div className="tab-container" style={{ marginTop: '18px' }}>
          <button
            className={`tab-btn ${activeTab === 'collab' ? 'active' : ''}`}
            onClick={() => setActiveTab('collab')}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <Globe2 size={16} /> Red de Coautoría Internacional
          </button>
          <button
            className={`tab-btn ${activeTab === 'sankey' ? 'active' : ''}`}
            onClick={() => setActiveTab('sankey')}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <GitFork size={16} /> Diagrama Sankey Interdisciplinar
          </button>
        </div>
      </div>

      {activeTab === 'collab' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          <div className="card">
            <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '14px' }}>
              🌍 Matriz de Coautoría País-País (Mapa Global)
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
                height: 580,
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
                    <th>País Origen</th>
                    <th>País Socio</th>
                    <th>Artículos en Coautoría</th>
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

      {activeTab === 'sankey' && (
        <div className="card">
          <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '14px' }}>
            🔀 Flujo de Producción Científica: Dominio → Campo → Subcampo
          </h3>
          <PlotlyChart
            data={sankeyTrace}
            layout={{ height: 680, margin: { l: 20, r: 20, t: 20, b: 20 } }}
          />
        </div>
      )}
    </div>
  );
}
