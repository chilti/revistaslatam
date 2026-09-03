import { useTranslation } from '../i18n';
import React, { useState, useMemo } from 'react';
import { Download, ChevronDown, ChevronUp, FileSpreadsheet } from 'lucide-react';

const WINDOW_OPTIONS = [
  { id: 0, label: '📊 Datos Crudos' },
  { id: 3, label: '🌊 Suavizado (w=3)' },
  { id: 5, label: '🌌 Suavizado (w=5)' },
];

export default function AnnualDataTable({ 
  data = [], 
  countryName = null, 
  countryCode = null, 
  journalName = null,
  journalId = null,
  title = null, 
  subtitle = null 
}) {
  const { t } = useTranslation();
  const [isOpen, setIsOpen] = useState(true);
  const [activeWindow, setActiveWindow] = useState(0); // 0 = raw, 3 = w3, 5 = w5
  const [sortField, setSortField] = useState('year');
  const [sortAsc, setSortAsc] = useState(false); // Default: newest first

  // Metrics to smooth
  const metricCols = useMemo(() => [
    'num_journals', 'num_documents', 'works_count', 'fwci_avg', 
    'pct_oa_total', 'pct_oa_diamond', 'pct_oa_gold', 'pct_oa_green', 
    'pct_oa_hybrid', 'pct_oa_bronze', 'pct_oa_closed',
    'pct_lang_es', 'pct_lang_en', 'pct_lang_pt', 'pct_lang_fr', 
    'pct_lang_de', 'pct_lang_it', 'avg_percentile', 'pct_top_10', 
    'pct_top_1', 'pct_authors_domestic'
  ], []);

  // Sorted and enriched data with optional rolling smoothing
  const processedData = useMemo(() => {
    if (!data || data.length === 0) return [];

    // Filter valid years and sort chronologically ascending for smoothing
    const validData = data
      .filter(d => d.year != null && Number(d.year) >= 1970 && Number(d.year) <= 2026)
      .map(d => {
        const oaTotal = d.pct_oa_total != null 
          ? Number(d.pct_oa_total) 
          : (Number(d.pct_oa_diamond || 0) + Number(d.pct_oa_gold || 0) + Number(d.pct_oa_green || 0) + Number(d.pct_oa_hybrid || 0) + Number(d.pct_oa_bronze || 0));

        return {
          ...d,
          year: Number(d.year),
          num_journals: d.num_journals != null ? Number(d.num_journals) : null,
          num_documents: Number(d.num_documents || d.works_count || 0),
          fwci_avg: Number(d.fwci_avg || 0),
          pct_oa_total: oaTotal,
          pct_oa_diamond: Number(d.pct_oa_diamond || 0),
          pct_oa_gold: Number(d.pct_oa_gold || 0),
          pct_oa_green: Number(d.pct_oa_green || 0),
          pct_oa_hybrid: Number(d.pct_oa_hybrid || 0),
          pct_oa_bronze: Number(d.pct_oa_bronze || 0),
          pct_oa_closed: Number(d.pct_oa_closed || 0),
          pct_lang_es: Number(d.pct_lang_es || 0),
          pct_lang_en: Number(d.pct_lang_en || 0),
          pct_lang_pt: Number(d.pct_lang_pt || 0),
          pct_lang_fr: Number(d.pct_lang_fr || 0),
          pct_lang_de: Number(d.pct_lang_de || 0),
          pct_lang_it: Number(d.pct_lang_it || 0),
          avg_percentile: Number(d.avg_percentile || 0),
          pct_top_10: Number(d.pct_top_10 || 0),
          pct_top_1: Number(d.pct_top_1 || 0),
          pct_authors_domestic: d.pct_authors_domestic != null ? Number(d.pct_authors_domestic) : null,
        };
      })
      .sort((a, b) => a.year - b.year);

    // Apply rolling mean if activeWindow > 1
    let smoothed = validData;
    if (activeWindow > 1) {
      smoothed = validData.map((row, idx) => {
        const startIdx = Math.max(0, idx - activeWindow + 1);
        const windowRows = validData.slice(startIdx, idx + 1);

        const newRow = { ...row };
        metricCols.forEach(col => {
          const vals = windowRows.map(r => r[col]).filter(v => v != null && !isNaN(v));
          if (vals.length > 0) {
            const sum = vals.reduce((a, b) => a + b, 0);
            newRow[col] = sum / vals.length;
          }
        });
        return newRow;
      });
    }

    // Sort by user's sortField and direction
    return smoothed.sort((a, b) => {
      const va = a[sortField] ?? 0;
      const vb = b[sortField] ?? 0;
      return sortAsc ? (va > vb ? 1 : -1) : (va < vb ? 1 : -1);
    });
  }, [data, activeWindow, sortField, sortAsc, metricCols]);

  const handleSort = (field) => {
    if (sortField === field) {
      setSortAsc(!sortAsc);
    } else {
      setSortField(field);
      setSortAsc(false);
    }
  };

  const hasJournalsCol = processedData.some(d => d.num_journals != null);
  const hasDomesticCol = processedData.some(d => d.pct_authors_domestic != null);

  const handleDownloadCsv = () => {
    if (processedData.length === 0) return;

    const headers = [
      'Año',
      ...(hasJournalsCol ? ['Revistas'] : []),
      'Documentos',
      'FWCI Promedio',
      '% OA Total',
      '% OA Diamante',
      '% OA Gold',
      '% OA Verde',
      '% OA Híbrido',
      '% OA Bronce',
      '% Cerrado',
      '% Español',
      '% Inglés',
      '% Portugués',
      '% Francés',
      '% Alemán',
      '% Italiano',
      'Percentil Prom.',
      '% Top 10',
      '% Top 1',
      ...(hasDomesticCol ? ['% Autoría Doméstica'] : [])
    ];

    const rows = processedData.map(d => {
      const rowVals = [
        d.year,
        ...(hasJournalsCol ? [d.num_journals != null ? Math.round(d.num_journals) : ''] : []),
        Math.round(d.num_documents),
        d.fwci_avg.toFixed(2),
        d.pct_oa_total.toFixed(1),
        d.pct_oa_diamond.toFixed(1),
        d.pct_oa_gold.toFixed(1),
        d.pct_oa_green.toFixed(1),
        d.pct_oa_hybrid.toFixed(1),
        d.pct_oa_bronze.toFixed(1),
        d.pct_oa_closed.toFixed(1),
        d.pct_lang_es.toFixed(1),
        d.pct_lang_en.toFixed(1),
        d.pct_lang_pt.toFixed(1),
        d.pct_lang_fr.toFixed(1),
        d.pct_lang_de.toFixed(1),
        d.pct_lang_it.toFixed(1),
        (d.avg_percentile * (d.avg_percentile <= 1 ? 100 : 1)).toFixed(1),
        d.pct_top_10.toFixed(1),
        d.pct_top_1.toFixed(2),
        ...(hasDomesticCol ? [d.pct_authors_domestic != null ? d.pct_authors_domestic.toFixed(1) : ''] : [])
      ];
      return rowVals.join(',');
    });

    const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [headers.join(','), ...rows].join('\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    const wSuffix = activeWindow > 0 ? `_suavizado_w${activeWindow}` : '_crudos';
    const cleanJid = journalId ? (journalId.includes('/') ? journalId.split('/').pop() : journalId) : null;
    const fileBase = cleanJid 
      ? `indicadores_historicos_${cleanJid}` 
      : (countryCode ? `indicadores_historicos_${countryCode}` : 'datos_anuales_latam');
    link.setAttribute('download', `${fileBase}${wSuffix}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const defaultTitle = journalName
    ? `Indicadores Históricos de ${journalName}`
    : (countryName
      ? `Indicadores Históricos de ${countryName}`
      : '📊 Ver Tabla de Datos Anuales (Latinoamérica 1970–2026)');

  const defaultSubtitle = journalName
    ? `Desglose histórico anual de producción, citación, vías de acceso abierto y distribución lingüística de ${journalName}.`
    : (countryName
      ? `Desglose histórico anual de producción, citación, vías de acceso abierto y distribución lingüística de ${countryName}.`
      : 'Desglose exhaustivo de producción histórica, citación, vías de acceso abierto y distribución lingüística.');

  return (
    <div className="card" style={{ marginTop: '20px' }}>
      {/* Header */}
      <div 
        onClick={() => setIsOpen(!isOpen)} 
        style={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between', 
          cursor: 'pointer',
          userSelect: 'none',
          flexWrap: 'wrap',
          gap: '12px'
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <FileSpreadsheet size={20} style={{ color: 'var(--primary-color, #10b981)' }} />
          <div>
            <h3 style={{ fontSize: '17px', fontWeight: '700', margin: 0 }}>
              {title || defaultTitle}
            </h3>
            <span style={{ fontSize: '12.5px', color: 'var(--text-muted)' }}>
              {subtitle || defaultSubtitle}
            </span>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <button
            onClick={(e) => {
              e.stopPropagation();
              handleDownloadCsv();
            }}
            className="btn-secondary"
            style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', padding: '6px 12px', borderRadius: '6px' }}
            disabled={processedData.length === 0}
          >
            <Download size={14} />
            <span>{t('tables.download_csv')}</span>
          </button>
          {isOpen ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
        </div>
      </div>

      {/* Expanded Table & Window Selector */}
      {isOpen && (
        <div style={{ marginTop: '16px' }}>
          {/* Smoothing Window Tabs */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '10px' }}>
            <div className="segmented-pills">
              {WINDOW_OPTIONS.map(opt => (
                <button
                  key={opt.id}
                  className={`segmented-pill-btn ${activeWindow === opt.id ? 'active' : ''}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    setActiveWindow(opt.id);
                  }}
                >
                  {opt.label}
                </button>
              ))}
            </div>

            <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {activeWindow === 0 
                ? 'Valores anuales directos sin transformación.' 
                : `Promedio móvil centrado en ventana de ${activeWindow} años para reducir ruido.`}
            </span>
          </div>

          {processedData.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '30px', color: 'var(--text-muted)' }}>
              No hay series anuales disponibles.
            </div>
          ) : (
            <div style={{ overflowX: 'auto', maxHeight: '520px', border: '1px solid var(--border-color)', borderRadius: '8px' }}>
              <table className="table-custom" style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                <thead style={{ position: 'sticky', top: 0, zIndex: 3, backgroundColor: 'var(--bg-card)' }}>
                  <tr style={{ borderBottom: '2px solid var(--border-color)' }}>
                    <th onClick={() => handleSort('year')} style={{ position: 'sticky', left: 0, zIndex: 4, backgroundColor: 'var(--bg-card)', cursor: 'pointer', padding: '10px 12px', textAlign: 'center', minWidth: '70px', color: 'var(--text-main)' }}>
                      Año {sortField === 'year' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    {hasJournalsCol && (
                      <th onClick={() => handleSort('num_journals')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px', color: '#c084fc' }}>
                        Revistas {sortField === 'num_journals' ? (sortAsc ? '▲' : '▼') : ''}
                      </th>
                    )}
                    <th onClick={() => handleSort('num_documents')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '95px', color: '#60a5fa' }}>
                      Documentos {sortField === 'num_documents' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('fwci_avg')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: '#34d399' }}>
                      FWCI {sortField === 'fwci_avg' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_oa_total')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px', color: 'var(--text-main)' }}>
                      % OA Total {sortField === 'pct_oa_total' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_oa_diamond')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '95px', color: '#38bdf8' }}>
                      % Diamante {sortField === 'pct_oa_diamond' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_oa_gold')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: '#fbbf24' }}>
                      % Gold {sortField === 'pct_oa_gold' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_oa_green')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: '#4ade80' }}>
                      % Verde {sortField === 'pct_oa_green' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_oa_hybrid')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px', color: '#a78bfa' }}>
                      % Híbrido {sortField === 'pct_oa_hybrid' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_oa_bronze')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px', color: '#fb923c' }}>
                      % Bronce {sortField === 'pct_oa_bronze' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_oa_closed')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px', color: '#f87171' }}>
                      % Cerrado {sortField === 'pct_oa_closed' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_lang_es')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px', color: 'var(--text-main)' }}>
                      % Español {sortField === 'pct_lang_es' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_lang_en')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: 'var(--text-main)' }}>
                      % Inglés {sortField === 'pct_lang_en' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_lang_pt')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px', color: 'var(--text-main)' }}>
                      % Portugués {sortField === 'pct_lang_pt' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_lang_fr')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: 'var(--text-main)' }}>
                      % Francés {sortField === 'pct_lang_fr' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_lang_de')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: 'var(--text-main)' }}>
                      % Alemán {sortField === 'pct_lang_de' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_lang_it')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: 'var(--text-main)' }}>
                      % Italiano {sortField === 'pct_lang_it' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('avg_percentile')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '100px', color: 'var(--text-main)' }}>
                      Percentil Prom. {sortField === 'avg_percentile' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_top_10')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px', color: '#f59e0b' }}>
                      % Top 10 {sortField === 'pct_top_10' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_top_1')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: '#ef4444' }}>
                      % Top 1 {sortField === 'pct_top_1' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    {hasDomesticCol && (
                      <th onClick={() => handleSort('pct_authors_domestic')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '110px', color: 'var(--text-main)' }}>
                        % Doméstica {sortField === 'pct_authors_domestic' ? (sortAsc ? '▲' : '▼') : ''}
                      </th>
                    )}
                  </tr>
                </thead>
                <tbody>
                  {processedData.map((d, idx) => (
                    <tr 
                      key={d.year} 
                      style={{ 
                        borderBottom: '1px solid var(--border-color)',
                        backgroundColor: idx % 2 === 0 ? 'transparent' : 'rgba(2, 132, 199, 0.02)'
                      }}
                    >
                      <td style={{ position: 'sticky', left: 0, zIndex: 2, backgroundColor: 'var(--bg-card)', fontWeight: 'bold', padding: '8px 12px', textAlign: 'center', color: 'var(--text-main)' }}>
                        {d.year}
                      </td>
                      {hasJournalsCol && (
                        <td style={{ textAlign: 'right', padding: '8px 10px', color: '#c084fc' }}>
                          {d.num_journals != null ? Math.round(d.num_journals).toLocaleString() : '—'}
                        </td>
                      )}
                      <td style={{ textAlign: 'right', padding: '8px 10px', fontWeight: '600', color: '#93c5fd' }}>
                        {Math.round(d.num_documents).toLocaleString()}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', fontWeight: 'bold', color: d.fwci_avg >= 1.0 ? '#34d399' : 'inherit' }}>
                        {d.fwci_avg.toFixed(2)}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_oa_total.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#38bdf8', fontWeight: '600' }}>
                        {d.pct_oa_diamond.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#fbbf24' }}>
                        {d.pct_oa_gold.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#4ade80' }}>
                        {d.pct_oa_green.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#a78bfa' }}>
                        {d.pct_oa_hybrid.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#fb923c' }}>
                        {d.pct_oa_bronze.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#f87171' }}>
                        {d.pct_oa_closed.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_lang_es.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_lang_en.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_lang_pt.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_lang_fr.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_lang_de.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_lang_it.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {(d.avg_percentile * (d.avg_percentile <= 1 ? 100 : 1)).toFixed(1)}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#f59e0b', fontWeight: '600' }}>
                        {d.pct_top_10.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#ef4444', fontWeight: '600' }}>
                        {d.pct_top_1.toFixed(2)}%
                      </td>
                      {hasDomesticCol && (
                        <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                          {d.pct_authors_domestic != null ? `${d.pct_authors_domestic.toFixed(1)}%` : '—'}
                        </td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
