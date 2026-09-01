import React, { useState } from 'react';
import { useAppStore } from '../store';
import api from '../api';
import { X, Download, Trash2, FileText, CheckCircle2, Copy, Bot, ClipboardCheck } from 'lucide-react';

// ─── Generador de texto Markdown para ChatGPT ──────────────────────────────
function buildMarkdownText(title, items) {
  const lines = [
    `# ${title}`,
    `> Generado desde Revistas LATAM — ${new Date().toLocaleDateString('es-MX', { year: 'numeric', month: 'long', day: 'numeric' })}`,
    '',
    '---',
    ''
  ];

  items.forEach((item, i) => {
    lines.push(`## ${i + 1}. ${item.title}`);
    if (item.category) lines.push(`**Categoría:** ${item.category}`);
    if (item.context)  lines.push(`\n${item.context}`);
    if (item.data) {
      if (Array.isArray(item.data) && item.data.length > 0) {
        const first = item.data[0];
        const keys  = Object.keys(first).slice(0, 8);
        lines.push('\n| ' + keys.join(' | ') + ' |');
        lines.push('| ' + keys.map(() => '---').join(' | ') + ' |');
        item.data.slice(0, 10).forEach(row => {
          lines.push('| ' + keys.map(k => String(row[k] ?? '—')).join(' | ') + ' |');
        });
        if (item.data.length > 10) lines.push(`\n_... y ${item.data.length - 10} registros más._`);
      }
    }
    lines.push('');
    lines.push('---');
    lines.push('');
  });

  return lines.join('\n');
}

export default function DossierDrawer() {
  const { dossierItems, isDossierOpen, setDossierOpen, removeDossierItem, clearDossier, requireAuth } = useAppStore();
  const [reportTitle, setReportTitle]   = useState('Contexto para IA - Revistas LATAM');
  const [downloading, setDownloading]   = useState(false);
  const [copied, setCopied]             = useState(false);
  const [expandedKey, setExpandedKey]   = useState(null);

  if (!isDossierOpen) return null;

  // ── Copiar al portapapeles para ChatGPT ─────────────────────────────────
  const handleCopyForChatGPT = () => {
    if (!requireAuth(() => handleCopyForChatGPT(), 'ai_context')) return;
    if (dossierItems.length === 0) return;
    const prefix = [
      'Analiza el siguiente paquete de contexto cienciométrico compilado desde Revistas LATAM.',
      'Los datos provienen de OpenAlex procesados con DuckDB.',
      'Proporciona un análisis crítico de las tendencias, fortalezas, brechas y recomendaciones estratégicas.',
      '',
      '---',
      ''
    ].join('\n');

    const text = prefix + buildMarkdownText(reportTitle, dossierItems);
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2500);
    });
  };

  // ── Exportar Markdown / JSON ─────────────────────────────────────────────
  const handleDownload = async (format = 'markdown') => {
    if (!requireAuth(() => handleDownload(format), 'ai_context')) return;
    if (dossierItems.length === 0) return;
    setDownloading(true);
    try {
      if (format === 'markdown') {
        const content = buildMarkdownText(reportTitle, dossierItems);
        const blob = new Blob([content], { type: 'text/markdown;charset=utf-8;' });
        const url  = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href  = url;
        link.download = `contexto_ia_latam_${new Date().toISOString().slice(0, 10)}.md`;
        link.click();
        URL.revokeObjectURL(url);
      } else {
        const payload = { title: reportTitle, generated: new Date().toISOString(), items: dossierItems };
        const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json;charset=utf-8;' });
        const url  = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href  = url;
        link.download = `contexto_ia_latam_${new Date().toISOString().slice(0, 10)}.json`;
        link.click();
        URL.revokeObjectURL(url);
      }
    } catch (err) {
      console.error('Error exporting context:', err);
    } finally {
      setDownloading(false);
    }
  };

  const canExport = dossierItems.length > 0 && !downloading;

  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 150,
      display: 'flex', justifyContent: 'flex-end',
      background: 'rgba(0, 0, 0, 0.45)',
      backdropFilter: 'blur(4px)'
    }}>
      <div style={{
        width: '480px', height: '100%',
        background: 'var(--bg-card)',
        borderLeft: '1px solid var(--border-color)',
        display: 'flex', flexDirection: 'column',
        boxShadow: '-12px 0 40px rgba(0, 0, 0, 0.3)',
        animation: 'slideIn 0.22s ease'
      }}>

        {/* ── Header ─────────────────────────────────────────────────────── */}
        <div style={{
          padding: '18px 22px', borderBottom: '1px solid var(--border-color)',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Bot size={18} color="var(--accent-primary)" />
            <h3 style={{ fontSize: '16px', fontWeight: '700' }}>Contexto para IA</h3>
            <span className="badge">{dossierItems.length}</span>
          </div>
          <button
            onClick={() => setDossierOpen(false)}
            style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: 'var(--text-muted)' }}
          >
            <X size={20} />
          </button>
        </div>

        {/* ── Body ───────────────────────────────────────────────────────── */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '18px 22px', display: 'flex', flexDirection: 'column', gap: '14px' }}>

          {/* Report title */}
          <div>
            <label style={{ fontSize: '11.5px', fontWeight: '700', color: 'var(--text-muted)', marginBottom: '5px', display: 'block', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
              Título del Paquete de Contexto:
            </label>
            <input
              type="text"
              value={reportTitle}
              onChange={e => setReportTitle(e.target.value)}
              style={{ width: '100%', fontSize: '13px' }}
            />
          </div>

          {/* Items header */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: '4px' }}>
            <span style={{ fontSize: '13px', fontWeight: '700' }}>
              Bloques de Datos Compilados ({dossierItems.length})
            </span>
            {dossierItems.length > 0 && (
              <button
                onClick={clearDossier}
                style={{
                  background: 'transparent', border: 'none', color: 'var(--accent-danger)',
                  fontSize: '12px', fontWeight: '600', cursor: 'pointer',
                  display: 'flex', alignItems: 'center', gap: '4px'
                }}
              >
                <Trash2 size={13} /> Vaciar
              </button>
            )}
          </div>

          {/* Items list */}
          {dossierItems.length === 0 ? (
            <div style={{
              textAlign: 'center', padding: '36px 20px',
              color: 'var(--text-muted)', background: 'var(--bg-input)',
              borderRadius: '10px', border: '1px dashed var(--border-color)',
              fontSize: '13px', lineHeight: 1.6
            }}>
              📌 Usa los botones <strong>"Guardar en Contexto IA"</strong> en cada gráfico o tabla para añadir indicadores y compilar el prompt aquí.
              <br /><br />
              Luego puedes exportarlo como Markdown o copiarlo directamente para analizar con ChatGPT.
            </div>
          ) : (
            dossierItems.map(item => (
              <div
                key={item.key}
                style={{
                  background: 'var(--bg-input)',
                  border: '1px solid var(--border-color)',
                  borderRadius: '10px',
                  overflow: 'hidden'
                }}
              >
                {/* Item header */}
                <div
                  style={{
                    padding: '12px 14px',
                    display: 'flex', alignItems: 'flex-start',
                    justifyContent: 'space-between', gap: '8px',
                    cursor: 'pointer'
                  }}
                  onClick={() => setExpandedKey(expandedKey === item.key ? null : item.key)}
                >
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '13px', fontWeight: '700', color: 'var(--text-main)', marginBottom: '3px' }}>
                      {item.title}
                    </div>
                    {item.context && (
                      <p style={{
                        fontSize: '11.5px', color: 'var(--text-muted)', lineHeight: 1.4,
                        overflow: 'hidden', textOverflow: 'ellipsis',
                        display: expandedKey === item.key ? 'block' : '-webkit-box',
                        WebkitLineClamp: expandedKey === item.key ? 'unset' : 2,
                        WebkitBoxOrient: 'vertical'
                      }}>
                        {item.context}
                      </p>
                    )}
                  </div>
                  <button
                    onClick={e => { e.stopPropagation(); removeDossierItem(item.key); }}
                    style={{ background: 'transparent', border: 'none', color: 'var(--text-subtle)', cursor: 'pointer', flexShrink: 0, marginTop: '2px' }}
                    title="Eliminar del Dossier"
                  >
                    <X size={14} />
                  </button>
                </div>

                {/* Expanded: data preview */}
                {expandedKey === item.key && item.data && Array.isArray(item.data) && item.data.length > 0 && (
                  <div style={{ padding: '0 14px 12px', borderTop: '1px solid var(--border-color)', overflowX: 'auto' }}>
                    <div style={{ fontSize: '10px', fontWeight: '700', textTransform: 'uppercase', color: 'var(--text-muted)', margin: '10px 0 6px' }}>
                      Vista previa de datos ({Math.min(item.data.length, 5)} de {item.data.length})
                    </div>
                    <table style={{ width: '100%', fontSize: '11px', borderCollapse: 'collapse' }}>
                      <thead>
                        <tr>
                          {Object.keys(item.data[0]).slice(0, 6).map(k => (
                            <th key={k} style={{ textAlign: 'left', padding: '3px 6px', color: 'var(--text-muted)', borderBottom: '1px solid var(--border-color)', fontWeight: '700' }}>
                              {k}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {item.data.slice(0, 5).map((row, i) => (
                          <tr key={i} style={{ borderBottom: '1px solid var(--border-color)' }}>
                            {Object.keys(item.data[0]).slice(0, 6).map(k => (
                              <td key={k} style={{ padding: '3px 6px', color: 'var(--text-main)' }}>
                                {String(row[k] ?? '—').slice(0, 20)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Item footer */}
                <div style={{ padding: '6px 14px 10px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span className="badge" style={{ fontSize: '10px' }}>{item.category}</span>
                  <CheckCircle2 size={13} color="var(--accent-success)" />
                </div>
              </div>
            ))
          )}
        </div>

        {/* ── Footer ─────────────────────────────────────────────────────── */}
        <div style={{
          padding: '16px 22px',
          borderTop: '1px solid var(--border-color)',
          display: 'flex', flexDirection: 'column', gap: '10px'
        }}>
          {/* ChatGPT copy button — highlighted */}
          <button
            onClick={handleCopyForChatGPT}
            disabled={!canExport}
            style={{
              width: '100%',
              padding: '11px 16px',
              borderRadius: '10px',
              background: copied
                ? 'linear-gradient(135deg, #10b981, #059669)'
                : 'linear-gradient(135deg, #6366f1, #8b5cf6)',
              color: '#ffffff',
              border: 'none',
              fontWeight: '700',
              fontSize: '13.5px',
              cursor: canExport ? 'pointer' : 'not-allowed',
              opacity: canExport ? 1 : 0.5,
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '8px',
              boxShadow: canExport ? '0 4px 15px rgba(99, 102, 241, 0.35)' : 'none',
              transition: 'all 0.25s ease'
            }}
          >
            {copied ? <ClipboardCheck size={16} /> : <Bot size={16} />}
            {copied ? '¡Copiado! Pega en ChatGPT' : '📋 Copiar para ChatGPT'}
          </button>

          {/* Export row */}
          <div style={{ display: 'flex', gap: '8px' }}>
            <button
              onClick={() => handleDownload('markdown')}
              disabled={!canExport}
              style={{
                flex: 1, padding: '9px 14px', borderRadius: '8px',
                background: 'var(--accent-primary)', color: '#ffffff',
                border: 'none', fontWeight: '700', fontSize: '12.5px',
                cursor: canExport ? 'pointer' : 'not-allowed',
                opacity: canExport ? 1 : 0.5,
                display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '5px',
                boxShadow: canExport ? '0 2px 8px rgba(2, 132, 199, 0.3)' : 'none'
              }}
            >
              <Download size={14} /> Markdown
            </button>
            <button
              onClick={() => handleDownload('json')}
              disabled={!canExport}
              style={{
                padding: '9px 14px', borderRadius: '8px',
                background: 'var(--bg-input)', color: 'var(--text-main)',
                border: '1px solid var(--border-color)',
                fontWeight: '600', fontSize: '12.5px',
                cursor: canExport ? 'pointer' : 'not-allowed',
                opacity: canExport ? 1 : 0.5
              }}
            >
              JSON
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
