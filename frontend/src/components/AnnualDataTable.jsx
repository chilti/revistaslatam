import React, { useState, useMemo } from 'react';
import { Download, ChevronDown, ChevronUp, Table, FileSpreadsheet } from 'lucide-react';

export default function AnnualDataTable({ data = [] }) {
  const [isOpen, setIsOpen] = useState(true);
  const [sortField, setSortField] = useState('year');
  const [sortAsc, setSortAsc] = useState(false); // Default: newest first

  // Sorted and enriched data
  const processedData = useMemo(() => {
    if (!data || data.length === 0) return [];

    const enriched = data.map(d => {
      const oaTotal = d.pct_oa_total != null 
        ? Number(d.pct_oa_total) 
        : (Number(d.pct_oa_diamond || 0) + Number(d.pct_oa_gold || 0) + Number(d.pct_oa_green || 0) + Number(d.pct_oa_hybrid || 0) + Number(d.pct_oa_bronze || 0));

      return {
        ...d,
        year: Number(d.year),
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
      };
    });

    return enriched.sort((a, b) => {
      const va = a[sortField] ?? 0;
      const vb = b[sortField] ?? 0;
      return sortAsc ? (va > vb ? 1 : -1) : (va < vb ? 1 : -1);
    });
  }, [data, sortField, sortAsc]);

  const handleSort = (field) => {
    if (sortField === field) {
      setSortAsc(!sortAsc);
    } else {
      setSortField(field);
      setSortAsc(false);
    }
  };

  const handleDownloadCsv = () => {
    if (processedData.length === 0) return;

    const headers = [
      'Año', 'Documentos', 'FWCI Promedio', '% OA Total', '% OA Diamante', 
      '% OA Gold', '% OA Verde', '% OA Híbrido', '% OA Bronce', '% Cerrado',
      '% Español', '% Inglés', '% Portugués', '% Francés', '% Alemán', '% Italiano',
      'Percentil Prom.', '% Top 10', '% Top 1'
    ];

    const rows = processedData.map(d => [
      d.year,
      d.num_documents,
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
      d.pct_top_1.toFixed(2)
    ]);

    const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [headers.join(','), ...rows.map(e => e.join(','))].join('\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    link.setAttribute('download', 'datos_anuales_latam_1970_2026.csv');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div className="card" style={{ marginTop: '20px' }}>
      {/* Header Toggle */}
      <div 
        onClick={() => setIsOpen(!isOpen)} 
        style={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between', 
          cursor: 'pointer',
          userSelect: 'none'
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <FileSpreadsheet size={20} style={{ color: 'var(--primary-color, #10b981)' }} />
          <div>
            <h3 style={{ fontSize: '17px', fontWeight: '700', margin: 0 }}>
              📊 Ver Tabla de Datos Anuales (Latinoamérica 1970–2026)
            </h3>
            <span style={{ fontSize: '12.5px', color: 'var(--text-muted)' }}>
              Desglose exhaustivo de producción histórica, citación, vías de acceso abierto y distribución lingüística.
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
            <span>Descargar CSV</span>
          </button>
          {isOpen ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
        </div>
      </div>

      {/* Expanded Table */}
      {isOpen && (
        <div style={{ marginTop: '16px' }}>
          {processedData.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '30px', color: 'var(--text-muted)' }}>
              No hay series anuales disponibles.
            </div>
          ) : (
            <div style={{ overflowX: 'auto', maxHeight: '520px', border: '1px solid var(--border-color, rgba(255,255,255,0.08))', borderRadius: '8px' }}>
              <table className="table-custom" style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                <thead style={{ position: 'sticky', top: 0, zIndex: 3, backgroundColor: 'var(--card-bg, #1e222d)' }}>
                  <tr style={{ borderBottom: '2px solid var(--border-color, #333)' }}>
                    <th onClick={() => handleSort('year')} style={{ position: 'sticky', left: 0, zIndex: 4, backgroundColor: 'var(--card-bg, #1e222d)', cursor: 'pointer', padding: '10px 12px', textAlign: 'center', minWidth: '70px' }}>
                      Año {sortField === 'year' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('num_documents')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '95px', color: '#60a5fa' }}>
                      Documentos {sortField === 'num_documents' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('fwci_avg')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px', color: '#34d399' }}>
                      FWCI {sortField === 'fwci_avg' ? (sortAsc ? '▲' : '▼') : ''}
                    </th>
                    <th onClick={() => handleSort('pct_oa_total')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px' }}>
                      % OA Total
                    </th>
                    <th onClick={() => handleSort('pct_oa_diamond')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '95px', color: '#38bdf8' }}>
                      % OA Diamante
                    </th>
                    <th onClick={() => handleSort('pct_oa_gold')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px' }}>
                      % OA Gold
                    </th>
                    <th onClick={() => handleSort('pct_oa_green')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px' }}>
                      % OA Verde
                    </th>
                    <th onClick={() => handleSort('pct_oa_hybrid')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px' }}>
                      % OA Híbrido
                    </th>
                    <th onClick={() => handleSort('pct_oa_bronze')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px' }}>
                      % OA Bronce
                    </th>
                    <th onClick={() => handleSort('pct_oa_closed')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px' }}>
                      % Cerrado
                    </th>
                    <th onClick={() => handleSort('pct_lang_es')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px' }}>
                      % Español
                    </th>
                    <th onClick={() => handleSort('pct_lang_en')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px' }}>
                      % Inglés
                    </th>
                    <th onClick={() => handleSort('pct_lang_pt')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px' }}>
                      % Portugués
                    </th>
                    <th onClick={() => handleSort('pct_lang_fr')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px' }}>
                      % Francés
                    </th>
                    <th onClick={() => handleSort('pct_lang_de')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px' }}>
                      % Alemán
                    </th>
                    <th onClick={() => handleSort('pct_lang_it')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '75px' }}>
                      % Italiano
                    </th>
                    <th onClick={() => handleSort('avg_percentile')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '95px' }}>
                      Percentil Prom.
                    </th>
                    <th onClick={() => handleSort('pct_top_10')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '85px', color: '#f59e0b' }}>
                      % Top 10%
                    </th>
                    <th onClick={() => handleSort('pct_top_1')} style={{ textAlign: 'right', cursor: 'pointer', padding: '10px 10px', minWidth: '80px', color: '#ec4899' }}>
                      % Top 1%
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {processedData.map((d, idx) => (
                    <tr 
                      key={d.year} 
                      style={{ 
                        borderBottom: '1px solid var(--border-color, rgba(255,255,255,0.05))',
                        backgroundColor: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.015)' 
                      }}
                    >
                      <td style={{ position: 'sticky', left: 0, zIndex: 2, backgroundColor: 'var(--card-bg, #1e222d)', textAlign: 'center', fontWeight: 'bold', padding: '8px 10px' }}>
                        {d.year}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', fontWeight: '600', color: '#93c5fd' }}>
                        {d.num_documents.toLocaleString()}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', fontWeight: '600', color: d.fwci_avg >= 1.0 ? '#34d399' : '#fbbf24' }}>
                        {d.fwci_avg.toFixed(2)}
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_oa_total.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#7dd3fc', fontWeight: '600' }}>
                        {d.pct_oa_diamond.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_oa_gold.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_oa_green.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_oa_hybrid.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px' }}>
                        {d.pct_oa_bronze.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: d.pct_oa_closed > 50 ? '#f87171' : 'inherit' }}>
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
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#fbbf24', fontWeight: '600' }}>
                        {d.pct_top_10.toFixed(1)}%
                      </td>
                      <td style={{ textAlign: 'right', padding: '8px 10px', color: '#f472b6', fontWeight: '600' }}>
                        {d.pct_top_1.toFixed(2)}%
                      </td>
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
