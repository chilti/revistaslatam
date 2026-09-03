import { useTranslation } from '../i18n';
import React, { useState, useEffect, useMemo } from 'react';
import api from '../api';
import { Download, Search, Sparkles, Filter, Layers } from 'lucide-react';

const LEVEL_CONFIG = [
  { id: 'domain', label: 'Dominio' },
  { id: 'field', label: 'Campo' },
  { id: 'subfield', label: 'Subcampo' },
  { id: 'topic', label: 'Tópico' },
];

export default function ThematicEvolutionTable({ 
  countryCode = null, 
  countryName = null, 
  journalId = null,
  journalName = null,
  title = null, 
  subtitle = null 
}) {
  const { t } = useTranslation();
  const [level, setLevel] = useState('domain');
  const [rawData, setRawData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [limit, setLimit] = useState(30);

  // Fetch aggregated evolution data on level, countryCode or journalId change
  useEffect(() => {
    setLoading(true);
    let endpoint = `/regional/thematic-evolution?level=${level}`;
    if (journalId) {
      const cleanJid = journalId.includes('/') ? journalId.split('/').pop() : journalId;
      endpoint = `/journals/${encodeURIComponent(cleanJid)}/thematic-evolution?level=${level}`;
    } else if (countryCode) {
      endpoint = `/countries/${countryCode}/thematic-evolution?level=${level}`;
    }

    api.get(endpoint)
      .then(res => {
        setRawData(Array.isArray(res.data) ? res.data : []);
      })
      .catch(err => {
        console.error('Error fetching thematic evolution:', err);
        setRawData([]);
      })
      .finally(() => setLoading(false));
  }, [level, countryCode, journalId]);

  // Pivot data: Years as columns, Categories as rows
  const { years, pivotedRows, maxCellVal } = useMemo(() => {
    if (!rawData || rawData.length === 0) {
      return { years: [], pivotedRows: [], maxCellVal: 1 };
    }

    const yearSet = new Set();
    const map = {};

    rawData.forEach(item => {
      const yr = Number(item.year);
      const name = item.name || 'Sin Clasificación';
      const count = Number(item.num_documents || 0);

      if (yr >= 1985) {
        yearSet.add(yr);
        if (!map[name]) {
          map[name] = { name, total: 0, years: {} };
        }
        map[name].years[yr] = (map[name].years[yr] || 0) + count;
        map[name].total += count;
      }
    });

    const sortedYears = Array.from(yearSet).sort((a, b) => a - b);
    let rows = Object.values(map);

    // Calculate max cell value for color intensity
    let maxVal = 1;
    rows.forEach(r => {
      sortedYears.forEach(y => {
        const v = r.years[y] || 0;
        if (v > maxVal) maxVal = v;
      });
    });

    // Sort rows by total descending
    rows.sort((a, b) => b.total - a.total);

    return { years: sortedYears, pivotedRows: rows, maxCellVal: maxVal };
  }, [rawData]);

  // Filter and limit rows
  const filteredRows = useMemo(() => {
    let list = pivotedRows;
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase().trim();
      list = list.filter(r => r.name.toLowerCase().includes(q));
    }
    if (limit > 0) {
      list = list.slice(0, limit);
    }
    return list;
  }, [pivotedRows, searchQuery, limit]);

  // Export CSV
  const handleDownloadCsv = () => {
    if (pivotedRows.length === 0) return;
    const header = [LEVEL_CONFIG.find(l => l.id === level)?.label || 'Área', ...years, 'Total General'];
    const rowsCsv = pivotedRows.map(r => {
      const nameClean = `"${r.name.replace(/"/g, '""')}"`;
      const yearVals = years.map(y => r.years[y] || 0);
      return [nameClean, ...yearVals, r.total].join(',');
    });

    const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + [header.join(','), ...rowsCsv].join('\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    const cleanId = journalId ? (journalId.includes('/') ? journalId.split('/').pop() : journalId) : (countryCode || 'region');
    link.setAttribute('download', `evolucion_historica_perfiles_${cleanId}_${level}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  // Helper for cell background color heat map
  const getCellBg = (val) => {
    if (!val || val === 0) return 'transparent';
    const ratio = Math.min(1, Math.max(0.05, Math.sqrt(val / maxCellVal)));
    return `rgba(59, 130, 246, ${ratio.toFixed(3)})`;
  };

  const defaultTitle = journalName
    ? `Evolución Histórica de Perfiles de Conocimiento: ${journalName}`
    : (countryName 
      ? `Evolución Histórica de Perfiles de Conocimiento: ${countryName}`
      : (countryCode ? `Evolución Histórica de Perfiles de Conocimiento: ${countryCode}` : 'Evolución Histórica de Perfiles de Conocimiento: Región'));

  const defaultSubtitle = journalName
    ? `Tendencia temporal de producción de artículos en ${journalName} por área temática (1985–2026).`
    : (countryName
      ? `Tendencia temporal de producción de artículos en revistas de ${countryName} por área temática (1985–2026).`
      : 'Tendencia temporal de producción de artículos en revistas latinoamericanas por área temática (1985–2026).');

  return (
    <div className="card" style={{ marginTop: '24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '14px' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Layers size={20} style={{ color: 'var(--primary-color, #3b82f6)' }} />
            <h3 style={{ fontSize: '18px', fontWeight: '700', margin: 0 }}>
              {title || defaultTitle}
            </h3>
          </div>
          <span style={{ fontSize: '13px', color: 'var(--text-muted)' }}>
            {subtitle || defaultSubtitle}
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
          <div style={{ position: 'relative', width: '100%', maxWidth: '360px' }}>
            <Search size={16} style={{ position: 'absolute', left: '12px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
            <input
              type="text"
              className="input-search"
              placeholder={`🔍 Buscar ${LEVEL_CONFIG.find(l => l.id === level)?.label || 'área'}...`}
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
              <option value={30}>{t('thematic_table.top_30')}</option>
              <option value={100}>{t('thematic_table.top_100')}</option>
              <option value={500}>{t('thematic_table.top_500')}</option>
              <option value={0}>Todos ({pivotedRows.length})</option>
            </select>
          </div>
        </div>

        <button
          onClick={handleDownloadCsv}
          className="btn-secondary"
          style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px', padding: '7px 14px', borderRadius: '8px', cursor: 'pointer' }}
          disabled={pivotedRows.length === 0}
        >
          <Download size={15} />
          <span>{t('thematic_table.download_csv')}</span>
        </button>
      </div>

      {/* Table Display */}
      {loading ? (
        <div style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
          <div className="spinner" style={{ margin: '0 auto 10px' }} />
          <span>{t('thematic_table.loading_evolution')}</span>
        </div>
      ) : filteredRows.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
          No se encontraron datos para los filtros seleccionados.
        </div>
      ) : (
        <div style={{ overflowX: 'auto', maxHeight: '560px', border: '1px solid var(--border-color)', borderRadius: '8px' }}>
          <table className="table-custom" style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12.5px' }}>
            <thead style={{ position: 'sticky', top: 0, zIndex: 3, backgroundColor: 'var(--bg-card)' }}>
              <tr>
                <th style={{ position: 'sticky', left: 0, zIndex: 4, backgroundColor: 'var(--bg-card)', minWidth: '220px', textAlign: 'left', padding: '10px 14px', borderBottom: '2px solid var(--border-color)', color: 'var(--text-main)' }}>
                  {LEVEL_CONFIG.find(l => l.id === level)?.label || 'Área Temática'}
                </th>
                <th style={{ textAlign: 'right', padding: '10px 12px', minWidth: '100px', backgroundColor: 'var(--accent-primary-light)', color: 'var(--accent-primary)', fontWeight: 'bold', borderBottom: '2px solid var(--border-color)' }}>
                  Total
                </th>
                {years.map(y => (
                  <th key={y} style={{ textAlign: 'right', padding: '10px 8px', minWidth: '55px', borderBottom: '2px solid var(--border-color)', color: 'var(--text-main)' }}>
                    {y}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filteredRows.map((row, idx) => (
                <tr key={row.name} style={{ borderBottom: '1px solid var(--border-color)', backgroundColor: idx % 2 === 0 ? 'transparent' : 'rgba(2, 132, 199, 0.02)' }}>
                  <td style={{ position: 'sticky', left: 0, zIndex: 2, backgroundColor: 'var(--bg-card)', fontWeight: '600', padding: '8px 14px', whiteSpace: 'nowrap', textOverflow: 'ellipsis', overflow: 'hidden', maxWidth: '280px', color: 'var(--text-main)' }} title={row.name}>
                    {row.name}
                  </td>
                  <td style={{ textAlign: 'right', padding: '8px 12px', fontWeight: 'bold', color: 'var(--accent-primary)', backgroundColor: 'var(--accent-primary-light)' }}>
                    {row.total.toLocaleString()}
                  </td>
                  {years.map(y => {
                    const val = row.years[y] || 0;
                    const ratio = val / maxCellVal;
                    return (
                      <td
                        key={y}
                        style={{
                          textAlign: 'right',
                          padding: '8px 6px',
                          backgroundColor: getCellBg(val),
                          color: val > 0 ? (ratio > 0.65 ? '#ffffff' : 'var(--text-main)') : 'var(--text-muted)',
                          fontWeight: val > 0 && ratio > 0.3 ? '600' : 'normal',
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
        <span>{t('thematic_table.level_label')} {LEVEL_CONFIG.find(l => l.id === level)?.label} &bull; {filteredRows.length}/{pivotedRows.length}</span>
        <span>{t('thematic_table.heatmap_note')}</span>
      </div>
    </div>
  );
}
