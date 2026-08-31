import React, { useState, useEffect, useMemo } from 'react';
import api from '../api';
import { useAppStore } from '../store';
import { Download, Search, Filter, BookOpen, Layers3 } from 'lucide-react';

const LEVEL_CONFIG = [
  { id: 'domain', label: 'Dominio' },
  { id: 'field', label: 'Campo' },
  { id: 'subfield', label: 'Subcampo' },
];

export default function CountryThematicProfilesTable({ countryCode, countryName }) {
  const { setSelectedJournal, setActiveSection } = useAppStore();
  const [level, setLevel] = useState('domain');
  const [profilesData, setProfilesData] = useState({ columns: [], data: [] });
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [limit, setLimit] = useState(30);

  // Fetch thematic profile data
  useEffect(() => {
    if (!countryCode) return;
    setLoading(true);
    api.get(`/countries/${countryCode}/thematic-profiles?level=${level}`)
      .then(res => {
        setProfilesData(res.data || { columns: [], data: [] });
      })
      .catch(err => {
        console.error('Error fetching country thematic profiles:', err);
        setProfilesData({ columns: [], data: [] });
      })
      .finally(() => setLoading(false));
  }, [countryCode, level]);

  // Filtered and limited rows
  const filteredData = useMemo(() => {
    let list = profilesData.data || [];
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase().trim();
      list = list.filter(r => (r.Revista || '').toLowerCase().includes(q));
    }
    if (limit > 0) {
      list = list.slice(0, limit);
    }
    return list;
  }, [profilesData.data, searchQuery, limit]);

  // Export CSV
  const handleDownloadCsv = () => {
    const dataRows = profilesData.data || [];
    const cols = profilesData.columns || [];
    if (dataRows.length === 0 || cols.length === 0) return;

    const rowsCsv = dataRows.map(r => {
      return cols.map(c => {
        const val = r[c] ?? 0;
        if (typeof val === 'string') {
          return `"${val.replace(/"/g, '""')}"`;
        }
        return val;
      }).join(',');
    });

    const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [cols.join(','), ...rowsCsv].join('\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    link.setAttribute('download', `perfiles_tematicos_revistas_${countryCode}_${level}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const columns = profilesData.columns || [];
  const thematicCols = columns.filter(c => c !== 'Revista' && c !== 'Total');

  return (
    <div className="card" style={{ marginTop: '20px' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '14px' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Layers3 size={20} style={{ color: 'var(--primary-color, #3b82f6)' }} />
            <h3 style={{ fontSize: '18px', fontWeight: '700', margin: 0 }}>
              Análisis de Perfiles Temáticos de Revistas ({countryName || countryCode})
            </h3>
          </div>
          <span style={{ fontSize: '13px', color: 'var(--text-muted)' }}>
            Distribución de artículos por áreas temáticas en cada una de las revistas del país.
          </span>
        </div>

        {/* Level Selector Pills */}
        <div className="segmented-pills">
          {LEVEL_CONFIG.map(l => (
            <button
              key={l.id}
              className={`segmented-pill-btn ${level === l.id ? 'active' : ''}`}
              onClick={() => setLevel(l.id)}
            >
              {l.label}
            </button>
          ))}
        </div>
      </div>

      {/* Filter and Download Bar */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px', marginBottom: '14px', flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flex: '1 1 300px' }}>
          <div style={{ position: 'relative', width: '100%', maxWidth: '340px' }}>
            <Search size={16} style={{ position: 'absolute', left: '12px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
            <input
              type="text"
              className="input-search"
              placeholder="🔍 Buscar revista..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              style={{ paddingLeft: '36px', width: '100%', borderRadius: '8px' }}
            />
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <Filter size={15} style={{ color: 'var(--text-muted)' }} />
            <select
              className="select-custom"
              value={limit}
              onChange={e => setLimit(Number(e.target.value))}
              style={{ fontSize: '13px', padding: '6px 10px', borderRadius: '8px' }}
            >
              <option value={30}>Top 30</option>
              <option value={100}>Top 100</option>
              <option value={500}>Top 500</option>
              <option value={0}>Todas ({(profilesData.data || []).length})</option>
            </select>
          </div>
        </div>

        <button
          onClick={handleDownloadCsv}
          className="btn-secondary"
          style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px', padding: '7px 14px', borderRadius: '8px', cursor: 'pointer' }}
          disabled={!profilesData.data || profilesData.data.length === 0}
        >
          <Download size={15} />
          <span>Descargar CSV</span>
        </button>
      </div>

      {/* Table */}
      {loading ? (
        <div style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
          <div className="spinner" style={{ margin: '0 auto 10px' }} />
          <span>Cargando matriz de perfiles temáticos...</span>
        </div>
      ) : filteredData.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
          No se encontraron datos temáticos para esta selección.
        </div>
      ) : (
        <div style={{ overflowX: 'auto', maxHeight: '520px', border: '1px solid var(--border-color, rgba(255,255,255,0.08))', borderRadius: '8px' }}>
          <table className="table-custom" style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
            <thead style={{ position: 'sticky', top: 0, zIndex: 3, backgroundColor: 'var(--card-bg, #1e222d)' }}>
              <tr>
                <th style={{ position: 'sticky', left: 0, zIndex: 4, backgroundColor: 'var(--card-bg, #1e222d)', minWidth: '240px', textAlign: 'left', padding: '10px 14px', borderBottom: '2px solid var(--border-color, #333)' }}>
                  Revista
                </th>
                <th style={{ textAlign: 'right', padding: '10px 12px', minWidth: '90px', backgroundColor: 'rgba(59, 130, 246, 0.1)', color: '#60a5fa', fontWeight: 'bold', borderBottom: '2px solid var(--border-color, #333)' }}>
                  Total
                </th>
                {thematicCols.map(col => (
                  <th key={col} style={{ textAlign: 'right', padding: '10px 10px', minWidth: '95px', borderBottom: '2px solid var(--border-color, #333)' }}>
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filteredData.map((row, idx) => (
                <tr key={row.Revista || idx} style={{ borderBottom: '1px solid var(--border-color, rgba(255,255,255,0.05))', backgroundColor: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.015)' }}>
                  <td style={{ position: 'sticky', left: 0, zIndex: 2, backgroundColor: 'var(--card-bg, #1e222d)', fontWeight: '600', padding: '8px 14px', whiteSpace: 'nowrap', textOverflow: 'ellipsis', overflow: 'hidden', maxWidth: '280px' }} title={row.Revista}>
                    {row.Revista}
                  </td>
                  <td style={{ textAlign: 'right', padding: '8px 12px', fontWeight: 'bold', color: '#93c5fd', backgroundColor: 'rgba(59, 130, 246, 0.05)' }}>
                    {Number(row.Total || 0).toLocaleString()}
                  </td>
                  {thematicCols.map(col => {
                    const val = row[col] || 0;
                    return (
                      <td
                        key={col}
                        style={{
                          textAlign: 'right',
                          padding: '8px 10px',
                          color: val > 0 ? 'inherit' : 'var(--text-muted)',
                          fontVariantNumeric: 'tabular-nums'
                        }}
                      >
                        {val > 0 ? val.toLocaleString() : '—'}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div style={{ marginTop: '10px', fontSize: '12px', color: 'var(--text-muted)', display: 'flex', justifyContent: 'space-between' }}>
        <span>Mostrando {filteredData.length} de {(profilesData.data || []).length} revistas ordenadas por volumen total.</span>
        <span>Nivel: {LEVEL_CONFIG.find(l => l.id === level)?.label}</span>
      </div>
    </div>
  );
}
