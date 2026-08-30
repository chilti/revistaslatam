import React, { useState } from 'react';
import { useAppStore } from '../store';
import api from '../api';
import { X, Download, Trash2, FileText, CheckCircle2 } from 'lucide-react';

export default function DossierDrawer() {
  const { dossierItems, isDossierOpen, setDossierOpen, removeDossierItem, clearDossier } = useAppStore();
  const [reportTitle, setReportTitle] = useState('Dossier de Estudio - Revistas LATAM');
  const [downloading, setDownloading] = useState(false);

  if (!isDossierOpen) return null;

  const handleDownload = async (format = 'markdown') => {
    if (dossierItems.length === 0) return;
    setDownloading(true);
    try {
      const res = await api.post('/reports/generate', {
        title: reportTitle,
        items: dossierItems,
        format: format
      });

      if (format === 'markdown') {
        const blob = new Blob([res.data.content], { type: 'text/markdown;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `dossier_latam_${new Date().toISOString().slice(0, 10)}.md`;
        link.click();
      } else {
        const blob = new Blob([JSON.stringify(res.data, null, 2)], { type: 'application/json;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `dossier_latam_${new Date().toISOString().slice(0, 10)}.json`;
        link.click();
      }
    } catch (err) {
      console.error('Error exporting dossier:', err);
    } finally {
      setDownloading(false);
    }
  };

  return (
    <div style={{
      position: 'fixed',
      inset: 0,
      zIndex: 150,
      display: 'flex',
      justifyContent: 'flex-end',
      background: 'rgba(0, 0, 0, 0.4)',
      backdropFilter: 'blur(4px)'
    }}>
      <div style={{
        width: '460px',
        height: '100%',
        background: 'var(--bg-card)',
        borderLeft: '1px solid var(--border-color)',
        display: 'flex',
        flexDirection: 'column',
        boxShadow: '-10px 0 35px rgba(0, 0, 0, 0.25)',
        animation: 'slideIn 0.25s ease'
      }}>
        {/* Header */}
        <div style={{
          padding: '20px 24px',
          borderBottom: '1px solid var(--border-color)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <FileText size={18} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>Dossier de Estudio</h3>
            <span className="badge">{dossierItems.length}</span>
          </div>
          <button
            onClick={() => setDossierOpen(false)}
            style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: 'var(--text-muted)' }}
          >
            <X size={20} />
          </button>
        </div>

        {/* Body */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '20px 24px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
          <div>
            <label style={{ fontSize: '12px', fontWeight: '600', color: 'var(--text-muted)', marginBottom: '6px', display: 'block' }}>
              Título del Reporte:
            </label>
            <input
              type="text"
              value={reportTitle}
              onChange={(e) => setReportTitle(e.target.value)}
              style={{ width: '100%' }}
            />
          </div>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: '10px' }}>
            <span style={{ fontSize: '13px', fontWeight: '700' }}>Elementos Registrados:</span>
            {dossierItems.length > 0 && (
              <button
                onClick={clearDossier}
                style={{
                  background: 'transparent',
                  border: 'none',
                  color: 'var(--accent-danger)',
                  fontSize: '12px',
                  fontWeight: '600',
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '4px'
                }}
              >
                <Trash2 size={13} /> Vaciar
              </button>
            )}
          </div>

          {dossierItems.length === 0 ? (
            <div style={{
              textAlign: 'center',
              padding: '40px 20px',
              color: 'var(--text-muted)',
              background: 'var(--bg-input)',
              borderRadius: '10px',
              border: '1px dashed var(--border-color)',
              fontSize: '13px'
            }}>
              No has añadido elementos al dossier todavía. Haz clic en "📌 Guardar en Dossier" en cualquier gráfico o indicador para incluirlo.
            </div>
          ) : (
            dossierItems.map((item) => (
              <div
                key={item.key}
                style={{
                  background: 'var(--bg-input)',
                  border: '1px solid var(--border-color)',
                  borderRadius: '10px',
                  padding: '14px',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '6px'
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <span style={{ fontSize: '13px', fontWeight: '700', color: 'var(--text-main)' }}>
                    {item.title}
                  </span>
                  <button
                    onClick={() => removeDossierItem(item.key)}
                    style={{ background: 'transparent', border: 'none', color: 'var(--text-subtle)', cursor: 'pointer' }}
                  >
                    <X size={14} />
                  </button>
                </div>
                {item.context && (
                  <p style={{ fontSize: '11.5px', color: 'var(--text-muted)', lineHeight: 1.4 }}>
                    {item.context}
                  </p>
                )}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '4px' }}>
                  <span className="badge" style={{ fontSize: '10px' }}>{item.category}</span>
                  <CheckCircle2 size={13} color="var(--accent-success)" />
                </div>
              </div>
            ))
          )}
        </div>

        {/* Footer actions */}
        <div style={{
          padding: '20px 24px',
          borderTop: '1px solid var(--border-color)',
          display: 'flex',
          gap: '10px'
        }}>
          <button
            onClick={() => handleDownload('markdown')}
            disabled={dossierItems.length === 0 || downloading}
            style={{
              flex: 1,
              padding: '10px 16px',
              borderRadius: '8px',
              background: 'var(--accent-primary)',
              color: '#ffffff',
              border: 'none',
              fontWeight: '700',
              fontSize: '13px',
              cursor: dossierItems.length === 0 ? 'not-allowed' : 'pointer',
              opacity: dossierItems.length === 0 ? 0.5 : 1,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '6px',
              boxShadow: '0 2px 8px rgba(2, 132, 199, 0.3)'
            }}
          >
            <Download size={15} /> Exportar Markdown
          </button>
          <button
            onClick={() => handleDownload('json')}
            disabled={dossierItems.length === 0 || downloading}
            style={{
              padding: '10px 14px',
              borderRadius: '8px',
              background: 'var(--bg-input)',
              color: 'var(--text-main)',
              border: '1px solid var(--border-color)',
              fontWeight: '600',
              fontSize: '13px',
              cursor: dossierItems.length === 0 ? 'not-allowed' : 'pointer',
              opacity: dossierItems.length === 0 ? 0.5 : 1
            }}
          >
            JSON
          </button>
        </div>
      </div>
    </div>
  );
}
