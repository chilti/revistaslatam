import React, { useState, useEffect, useMemo } from 'react';
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
  const [artSizeMode, setArtSizeMode] = useState('fwci');
  const [artCountryFilter, setArtCountryFilter] = useState('');
  const [artCommFilter, setArtCommFilter] = useState('');
  const [artSampleLimit, setArtSampleLimit] = useState(50000);
  const [showConvexHull, setShowConvexHull] = useState(false);
  const [convexHullPoints, setConvexHullPoints] = useState([]);
  
  // Journals state
  const [journalPoints, setJournalPoints] = useState([]);
  const [journalColorMode, setJournalColorMode] = useState('community');
  const [journalSizeMode, setJournalSizeMode] = useState('works_count');
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

  // Escalamiento P98 estilo SinapsisAI dashboard_v2.py / map.html
  const articleSizes = useMemo(() => {
    if (!articlePoints || articlePoints.length === 0) return 2.2;
    if (artSizeMode === 'uniform') return 2.2;
    const raw = articlePoints.map(p => {
      const v = artSizeMode === 'fwci' ? Number(p.fwci) || 0 : Number(p.cited_by_count) || 0;
      return v > 0 ? v : 0;
    });
    const nonZeros = raw.filter(v => v > 0).sort((a, b) => a - b);
    const p98 = nonZeros.length > 5 ? nonZeros[Math.floor(nonZeros.length * 0.98)] : (nonZeros[nonZeros.length - 1] || 1.0);
    const cap = Math.max(p98, 0.1);
    const rMin = 2.0;
    const rMax = 5.0;
    return raw.map(v => {
      const norm = Math.min(1.0, Math.max(0.0, v / cap));
      return rMin + (rMax - rMin) * Math.sqrt(norm);
    });
  }, [articlePoints, artSizeMode]);

  const journalSizes = useMemo(() => {
    if (!journalPoints || journalPoints.length === 0) return 4.0;
    if (journalSizeMode === 'uniform') return 4.0;
    const raw = journalPoints.map(p => {
      let v = 0;
      if (journalSizeMode === 'works_count') v = Number(p.works_count) || 0;
      else if (journalSizeMode === 'fwci') v = Number(p.fwci_avg) || 0;
      else v = Number(p.cited_by_count) || 0;
      return v > 0 ? v : 0;
    });
    const nonZeros = raw.filter(v => v > 0).sort((a, b) => a - b);
    const p98 = nonZeros.length > 5 ? nonZeros[Math.floor(nonZeros.length * 0.98)] : (nonZeros[nonZeros.length - 1] || 1.0);
    const cap = Math.max(p98, 0.1);
    const rMin = 3.5;
    const rMax = 8.5;
    return raw.map(v => {
      const norm = Math.min(1.0, Math.max(0.0, v / cap));
      return rMin + (rMax - rMin) * Math.sqrt(norm);
    });
  }, [journalPoints, journalSizeMode]);

  const articleTraces = [
    {
      x: articlePoints.map(p => p.umap_x),
      y: articlePoints.map(p => p.umap_y),
      mode: 'markers',
      marker: {
        size: articleSizes,
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
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.size_label')}</span>
              <select
                value={artSizeMode}
                onChange={(e) => setArtSizeMode(e.target.value)}
              >
                <option value="fwci">{t('maps.size_fwci')}</option>
                <option value="citations">{t('maps.size_citations')}</option>
                <option value="uniform">{t('maps.size_uniform')}</option>
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
              sizeMode={artSizeMode}
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
              🌌 {t('maps.methodology_title')}
            </h4>
            <ol style={{ fontSize: '12.5px', lineHeight: 1.65, color: 'var(--text-muted)', marginLeft: '20px' }}>
              <li><strong>{t('maps.methodology_pure_semantics_title')}:</strong> {t('maps.methodology_pure_semantics_desc')}</li>
              <li><strong>{t('maps.methodology_trilingual_proc_title')}:</strong> {t('maps.methodology_trilingual_proc_desc')}</li>
              <li><strong>{t('maps.methodology_dense_vec_title')}:</strong> {t('maps.methodology_dense_vec_desc')}</li>
              <li><strong>{t('maps.methodology_clustering_title')}:</strong> {t('maps.methodology_clustering_desc')}</li>
              <li><strong>{t('maps.methodology_labeling_title')}:</strong> {t('maps.methodology_labeling_desc')}</li>
              <li><strong>{t('maps.methodology_sublabels_title')}:</strong> {t('maps.methodology_sublabels_desc')}</li>
              <li><strong>{t('maps.methodology_umap_title')}:</strong> {t('maps.methodology_umap_desc')}</li>
              <li><strong>{t('maps.methodology_convex_hulls_title')}:</strong> {t('maps.methodology_convex_hulls_desc')}</li>
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
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)' }}>{t('maps.size_label')}</span>
              <select
                value={journalSizeMode}
                onChange={(e) => setJournalSizeMode(e.target.value)}
              >
                <option value="works_count">{t('maps.size_works')}</option>
                <option value="citations">{t('maps.size_citations')}</option>
                <option value="fwci">{t('maps.size_fwci')}</option>
                <option value="uniform">{t('maps.size_uniform')}</option>
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
              sizeMode={journalSizeMode}
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
                    size: journalSizes,
                    color: journalPoints.map(p => p.fwci_avg || 0.5),
                    colorscale: 'Viridis',
                    showscale: true,
                    opacity: 0.85
                  },
                  text: journalPoints.map(p => `${p.display_name}<br>Editorial: ${p.publisher}<br>FWCI: ${p.fwci_avg} | Citas: ${p.cited_by_count}`),
                  type: 'scatter'
                }]}
                layout={{ height: 680, title: t('maps.journals_space_title', { n: journalPoints.length.toLocaleString() }) }}
              />
            </div>
          )}

          {/* Methodology Card */}
          <div className="card" style={{ background: 'var(--bg-input)' }}>
            <h4 style={{ fontSize: '14.5px', fontWeight: '700', marginBottom: '8px' }}>
              📐 {t('maps.methodology_journals_title')}
            </h4>
            <ol style={{ fontSize: '12.5px', lineHeight: 1.65, color: 'var(--text-muted)', marginLeft: '20px' }}>
              <li><strong>{t('maps.methodology_journals_barycenter_title')}:</strong> {t('maps.methodology_journals_barycenter_desc')}</li>
              <li><strong>{t('maps.methodology_journals_hybrid_title')}:</strong> {t('maps.methodology_journals_hybrid_desc')}</li>
              <li><strong>{t('maps.methodology_journals_umap_title')}:</strong> {t('maps.methodology_journals_umap_desc')}</li>
            </ol>
          </div>
        </div>
      )}
    </div>
  );
}
