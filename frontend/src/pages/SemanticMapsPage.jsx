import React, { useState, useEffect } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import { useTranslation } from '../i18n';
import WebGLCanvas from '../components/WebGLCanvas';
import PlotlyChart from '../components/PlotlyChart';
import { 
  Sparkles, 
  Layers, 
  Cpu, 
  BarChart3, 
  Filter, 
  Compass, 
  ExternalLink,
  BookOpen,
  FileText,
  Pentagon
} from 'lucide-react';

export default function SemanticMapsPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState('articles'); // 'articles' | 'journals'
  const [engine, setEngine] = useState('webgl'); // 'webgl' | 'plotly'
  
  // Articles state
  const [articlePoints, setArticlePoints] = useState([]);
  const [artColorMode, setArtColorMode] = useState('year');
  const [artCountryFilter, setArtCountryFilter] = useState('');
  const [artCommFilter, setArtCommFilter] = useState('');
  const [artSampleLimit, setArtSampleLimit] = useState(50000);
  const [showConvexHull, setShowConvexHull] = useState(false);
  const [convexHullPoints, setConvexHullPoints] = useState([]);
  
  // Journals state
  const [journalPoints, setJournalPoints] = useState([]);
  const [journalColorMode, setJournalColorMode] = useState('community');
  const [journalCountryFilter, setJournalCountryFilter] = useState('');
  const [journalCommFilter, setJournalCommFilter] = useState('');

  // Filters catalog
  const [filterCatalog, setFilterCatalog] = useState({ countries: [], communities: [] });
  const [loading, setLoading] = useState(true);

  // Load filter options
  useEffect(() => {
    api.get('/maps/filters').then(res => setFilterCatalog(res.data)).catch(console.error);
  }, []);

  // Load articles points
  useEffect(() => {
    if (activeTab !== 'articles') return;
    setLoading(true);
    const params = new URLSearchParams();
    if (artCountryFilter && artCountryFilter !== 'Todos') params.append('country', artCountryFilter);
    if (artCommFilter && artCommFilter !== 'Todas') params.append('community', artCommFilter);
    if (artSampleLimit && artSampleLimit > 0) params.append('limit', artSampleLimit);

    api.get(`/maps/articles?${params.toString()}`)
      .then(res => setArticlePoints(res.data))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [activeTab, artCountryFilter, artCommFilter, artSampleLimit]);

  // Load Convex Hull if enabled
  useEffect(() => {
    if (showConvexHull) {
      const params = new URLSearchParams();
      if (artCountryFilter && artCountryFilter !== 'Todos') params.append('country', artCountryFilter);
      if (artCommFilter && artCommFilter !== 'Todas') params.append('community', artCommFilter);
      api.get(`/maps/convex-hull?${params.toString()}`)
        .then(res => setConvexHullPoints(res.data?.hull || []))
        .catch(console.error);
    } else {
      setConvexHullPoints([]);
    }
  }, [showConvexHull, artCountryFilter, artCommFilter]);

  // Load journals points
  useEffect(() => {
    if (activeTab !== 'journals') return;
    setLoading(true);
    const params = new URLSearchParams();
    if (journalCountryFilter && journalCountryFilter !== 'Todos') params.append('country', journalCountryFilter);
    if (journalCommFilter && journalCommFilter !== 'Todas') params.append('community', journalCommFilter);

    api.get(`/maps/journals?${params.toString()}`)
      .then(res => setJournalPoints(res.data))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [activeTab, journalCountryFilter, journalCommFilter]);

  // Plotly Traces for Articles + Convex Hull
  const commPalette = ["#0284c7", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1", "#14b8a6", "#e11d48", "#a855f7", "#38bdf8", "#22c55e"];
  const uniqueComms = Array.from(new Set(articlePoints.map(p => p.community_name || 'General')));

  const articleTraces = [
    {
      x: articlePoints.map(p => p.umap_x),
      y: articlePoints.map(p => p.umap_y),
      mode: 'markers',
      marker: {
        size: 5,
        color: artColorMode === 'year'
          ? articlePoints.map(p => p.publication_year || 2020)
          : artColorMode === 'community'
          ? articlePoints.map(p => commPalette[Math.max(0, uniqueComms.indexOf(p.community_name || 'General')) % commPalette.length])
          : '#0284c7',
        colorscale: artColorMode === 'year' ? 'Turbo' : undefined,
        showscale: artColorMode === 'year',
        opacity: 0.8
      },
      text: articlePoints.map(p => `${p.title}<br>Revista: ${p.journal_name}<br>Comunidad: ${p.community_name || 'General'}<br>Año: ${p.publication_year} | FWCI: ${p.fwci}`),
      type: 'scatter',
      name: t('maps.tab_articles')
    }
  ];

  if (convexHullPoints.length > 0) {
    const hullLabel = artCountryFilter && artCountryFilter !== 'Todos'
      ? artCountryFilter
      : artCommFilter && artCommFilter !== 'Todas'
      ? artCommFilter
      : 'Global';
    articleTraces.push({
      x: convexHullPoints.map(p => p.x),
      y: convexHullPoints.map(p => p.y),
      mode: 'lines',
      fill: 'toself',
      fillcolor: 'rgba(239, 68, 68, 0.15)',
      line: { color: '#ef4444', width: 2.5, dash: 'solid' },
      name: t('maps.hull_label', { entity: hullLabel }),
      type: 'scatter'
    });
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      {/* Header & Tabs */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h2 style={{ fontSize: '24px', fontWeight: '800' }}>🗺️ {t('maps.title')}</h2>
            <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
              {t('maps.subtitle')}
            </p>
          </div>

          <div className="segmented-pills">
            <button
              className={`segmented-pill-btn ${engine === 'webgl' ? 'active' : ''}`}
              onClick={() => setEngine('webgl')}
              style={{ display: 'flex', alignItems: 'center', gap: '5px' }}
            >
              <Cpu size={14} /> ⚡ {t('maps.gpu_badge')}
            </button>
            <button
              className={`segmented-pill-btn ${engine === 'plotly' ? 'active' : ''}`}
              onClick={() => setEngine('plotly')}
              style={{ display: 'flex', alignItems: 'center', gap: '5px' }}
            >
              <BarChart3 size={14} /> 📊 {t('maps.plotly_badge')}
            </button>
          </div>
        </div>

        {/* Level Tabs */}
        <div className="tab-container" style={{ marginTop: '20px' }}>
          <button
            className={`tab-btn ${activeTab === 'articles' ? 'active' : ''}`}
            onClick={() => setActiveTab('articles')}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <FileText size={16} /> {t('maps.tab_articles')}
          </button>
          <button
            className={`tab-btn ${activeTab === 'journals' ? 'active' : ''}`}
            onClick={() => setActiveTab('journals')}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <BookOpen size={16} /> {t('maps.tab_journals')}
          </button>
        </div>
      </div>

      {/* Tab 1: Artículos */}
      {activeTab === 'articles' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          {/* Controls Bar */}
          <div className="card" style={{ display: 'flex', alignItems: 'center', gap: '14px', flexWrap: 'wrap', padding: '14px 20px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.color_label')}</span>
              <select
                value={artColorMode}
                onChange={(e) => setArtColorMode(e.target.value)}
              >
                <option value="year">{t('maps.color_year')}</option>
                <option value="community">{t('maps.color_community')}</option>
                <option value="uniform">{t('maps.color_uniform')}</option>
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.country_label')}</span>
              <select
                value={artCountryFilter}
                onChange={(e) => setArtCountryFilter(e.target.value)}
              >
                <option value="Todos">{t('maps.all_countries')}</option>
                {filterCatalog.countries.map(c => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.community_label')}</span>
              <select
                value={artCommFilter}
                onChange={(e) => setArtCommFilter(e.target.value)}
              >
                <option value="Todas">{t('maps.all_communities')}</option>
                {filterCatalog.communities.map(c => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.sample_label')}</span>
              <select
                value={artSampleLimit}
                onChange={(e) => setArtSampleLimit(Number(e.target.value))}
              >
                <option value={0}>{t('maps.sample_all')}</option>
                <option value={10000}>{t('maps.pts_10k')}</option>
                <option value={30000}>{t('maps.pts_30k')}</option>
                <option value={50000}>{t('maps.pts_50k')}</option>
                <option value={100000}>{t('maps.pts_100k')}</option>
              </select>
            </div>

            {/* Convex Hull Toggle */}
            <label style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', cursor: 'pointer', marginLeft: 'auto', background: 'var(--bg-input)', padding: '6px 12px', borderRadius: '6px', border: '1px solid var(--border-color)' }}>
              <input
                type="checkbox"
                checked={showConvexHull}
                onChange={(e) => setShowConvexHull(e.target.checked)}
              />
              <Pentagon size={14} color="#ef4444" />
              <span>{t('maps.convex_hull')}</span>
            </label>
          </div>

          {/* Engine Visualizer */}
          {engine === 'webgl' ? (
            <WebGLCanvas
              points={articlePoints}
              convexHull={showConvexHull ? convexHullPoints : []}
              colorMode={artColorMode}
              sizeMode="citations"
              height={700}
            />
          ) : (
            <div className="card">
              <PlotlyChart
                data={articleTraces}
                layout={{ height: 680, title: t('maps.landscape_title', { n: articlePoints.length.toLocaleString() }) }}
              />
            </div>
          )}

          {/* Methodology Card */}
          <div className="card" style={{ background: 'var(--bg-input)' }}>
            <h4 style={{ fontSize: '14.5px', fontWeight: '700', marginBottom: '8px' }}>
              🌌 Metodología de Creación del Paisaje Científico de Artículos (LATAM) — Arquitectura Sinapsis AI
            </h4>
            <ol style={{ fontSize: '12.5px', lineHeight: 1.65, color: 'var(--text-muted)', marginLeft: '20px' }}>
              <li><strong>Semántica Pura:</strong> Extracción de títulos y resúmenes de los 3.54 millones de artículos de OpenAlex, aislando metadatos de autor, institución o país para evitar sesgos geográficos.</li>
              <li><strong>Procesamiento Trilingüe:</strong> Filtro avanzado de <em>stopwords</em> combinadas en Español, Portugués e Inglés.</li>
              <li><strong>Vectorización Densa y Espacio Latente:</strong> Generación de embeddings densos de alta dimensionalidad (<em>Nomic Embed Text v2 / SVD denso</em>) normalizados en norma <em>L₂</em>.</li>
              <li><strong>Clustering en la Dimensión Intrínseca (HDBSCAN):</strong> Agrupamiento basado en densidad no lineal en el espacio latente (<code>min_cluster_size=300</code>, <code>min_samples=30</code>) descubriendo macro-comunidades naturales y separando ruido periférico.</li>
              <li><strong>Etiquetado Semántico Híbrido (Centroides + TF-IDF + LLM):</strong> Para cada clúster se calculan los 10 artículos más cercanos al centroide geométrico y sus palabras clave TF-IDF, sintetizando con un modelo LLM local un nombre temático conciso (2 a 4 palabras en español).</li>
              <li><strong>Sub-etiquetas Jerárquicas (Nivel 2):</strong> Micro-partición con K-Means y re-etiquetado temático para grandes macro-clústeres.</li>
              <li><strong>Variedades No Lineales (UMAP 2D):</strong> Reducción topológica continua con métrica del coseno (<code>n_neighbors=30</code>, <code>min_dist=0.35</code>, <code>spread=1.8</code>) para visualización fluida en GPU WebGL.</li>
              <li><strong>Envolturas Convexas (Convex Hulls):</strong> Delimitación de polígonos mínimos que encierran el territorio temático de un país o revista seleccionados.</li>
            </ol>
          </div>
        </div>
      )}

      {/* Tab 2: Revistas */}
      {activeTab === 'journals' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          {/* Controls Bar */}
          <div className="card" style={{ display: 'flex', alignItems: 'center', gap: '14px', flexWrap: 'wrap', padding: '14px 20px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.color_label')}</span>
              <select
                value={journalColorMode}
                onChange={(e) => setJournalColorMode(e.target.value)}
              >
                <option value="community">{t('maps.color_community')}</option>
                <option value="fwci">{t('maps.color_fwci')}</option>
                <option value="diamond">{t('maps.color_diamond')}</option>
                <option value="country">{t('maps.color_country')}</option>
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.community_label')}</span>
              <select
                value={journalCommFilter}
                onChange={(e) => setJournalCommFilter(e.target.value)}
              >
                <option value="Todas">{t('maps.all_communities')}</option>
                {filterCatalog.communities.map(c => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.country_label')}</span>
              <select
                value={journalCountryFilter}
                onChange={(e) => setJournalCountryFilter(e.target.value)}
              >
                <option value="Todos">{t('maps.all_countries')}</option>
                {filterCatalog.countries.map(c => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Engine Visualizer */}
          {engine === 'webgl' ? (
            <WebGLCanvas
              points={journalPoints}
              colorMode={journalColorMode}
              sizeMode="citations"
              height={700}
            />
          ) : (
            <div className="card">
              <PlotlyChart
                data={[{
                  x: journalPoints.map(p => p.umap_x),
                  y: journalPoints.map(p => p.umap_y),
                  mode: 'markers',
                  marker: {
                    size: 8,
                    color: journalPoints.map(p => p.fwci_avg || 0.5),
                    colorscale: 'Viridis',
                    showscale: true,
                    opacity: 0.85
                  },
                  text: journalPoints.map(p => `${p.display_name}<br>Editorial: ${p.publisher}<br>FWCI: ${p.fwci_avg} | Citas: ${p.cited_by_count}`),
                  type: 'scatter'
                }]}
                layout={{ height: 680, title: `Espacio Semántico 2D de ${journalPoints.length.toLocaleString()} Revistas` }}
              />
            </div>
          )}

          {/* Methodology Card */}
          <div className="card" style={{ background: 'var(--bg-input)' }}>
            <h4 style={{ fontSize: '14.5px', fontWeight: '700', marginBottom: '8px' }}>
              📐 Metodología de Construcción del Espacio Semántico 2D de Revistas — Baricentros y Multimodalidad
            </h4>
            <ol style={{ fontSize: '12.5px', lineHeight: 1.65, color: 'var(--text-muted)', marginLeft: '20px' }}>
              <li><strong>Baricentro de Artículos (Mean Pooling):</strong> Cada una de las 7,509 revistas se posiciona en el centroide geométrico calculado a partir de la totalidad de sus artículos proyectados en el espacio semántico maestro.</li>
              <li><strong>Espacio Híbrido Multimodal (&alpha; = 0.40):</strong> Fusión ponderada de 60% contenido semántico + 40% perfil de rendimiento cienciométrico (FWCI, OA Diamante, % Top 10%, Índice H y PageRank).</li>
              <li><strong>Reducción UMAP 2D:</strong> Preservación de vecindades disciplinares, clusters de afinidad editorial y diferenciación de excelencia científica en el espacio bidimensional.</li>
            </ol>
          </div>
        </div>
      )}
    </div>
  );
}
