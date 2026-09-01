import React, { useState, useMemo } from 'react';
import { useAppStore } from '../store';
import { 
  Bot, 
  Sparkles, 
  ChevronDown, 
  ChevronUp, 
  CheckSquare, 
  Square, 
  Copy, 
  ExternalLink, 
  Download, 
  BookmarkPlus, 
  FileText, 
  AlertTriangle, 
  CheckCircle2, 
  Info,
  RefreshCw,
  Eye,
  EyeOff
} from 'lucide-react';

const MAX_CHATGPT_URL_CHARS = 2000;

export default function PageDossierExpander({
  pageTitle = 'Análisis Cienciométrico',
  pageDescription = 'Selecciona los bloques de datos e indicadores de esta página para compilar el Contexto para IA o enviarlos a ChatGPT.',
  sections = [] // Array of { id, title, category, defaultChecked, buildDataText, rawData }
}) {
  const { addDossierItem, dossierItems, setDossierOpen, requireAuth } = useAppStore();
  
  // Open / Close Expander State (open by default so users see it, or toggleable)
  const [isOpen, setIsOpen] = useState(true);
  const [showPreview, setShowPreview] = useState(false);
  const [copied, setCopied] = useState(false);
  const [addedToDossier, setAddedToDossier] = useState(false);

  // Selected Section IDs
  const [selectedIds, setSelectedIds] = useState(() => {
    const initial = new Set();
    sections.forEach(s => {
      if (s.defaultChecked || s.id.includes('kpi') || s.id.includes('header') || s.id.includes('summary')) {
        initial.add(s.id);
      }
    });
    return initial;
  });

  const toggleSection = (id) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const selectAll = () => {
    setSelectedIds(new Set(sections.map(s => s.id)));
  };

  const selectNone = () => {
    setSelectedIds(new Set());
  };

  const selectDefault = () => {
    const next = new Set();
    sections.forEach(s => {
      if (s.defaultChecked || s.id.includes('kpi') || s.id.includes('header') || s.id.includes('summary')) {
        next.add(s.id);
      }
    });
    setSelectedIds(next);
  };

  // Build the compiled markdown string from selected sections
  const compiledMarkdown = useMemo(() => {
    if (selectedIds.size === 0) return '';

    const lines = [];
    lines.push(`# ${pageTitle}`);
    lines.push(`> Exportación de Inteligencia Científica — Revistas LATAM (${new Date().toLocaleDateString('es-MX', { year: 'numeric', month: 'long', day: 'numeric' })})`);
    lines.push('');
    lines.push('---');
    lines.push('');

    sections.forEach(s => {
      if (!selectedIds.has(s.id)) return;
      lines.push(`## 📌 ${s.title}`);
      if (s.category) lines.push(`*Categoría:* ${s.category}`);
      lines.push('');
      
      const content = typeof s.buildDataText === 'function' ? s.buildDataText() : (s.dataText || '');
      lines.push(content || '_Sin datos disponibles para esta sección._');
      lines.push('');
      lines.push('---');
      lines.push('');
    });

    return lines.join('\n');
  }, [sections, selectedIds, pageTitle]);

  // Full prompt for ChatGPT with system instructions
  const fullChatGptPrompt = useMemo(() => {
    if (!compiledMarkdown) return '';
    const systemInstructions = [
      `Eres un experto cienciométrico y analista de políticas científicas de la UNAM y de América Latina.`,
      `Analiza en profundidad el siguiente conjunto de datos empíricos de "${pageTitle}".`,
      `Los datos han sido calculados a partir de OpenAlex 2025 y procesados con motores OLAP (DuckDB).`,
      ``,
      `Por favor realiza:`,
      `1. Un resumen ejecutivo de los principales hallazgos y magnitudes.`,
      `2. Análisis de fortalezas, asimetrías y áreas de oportunidad identificadas en los indicadores (FWCI, Acceso Abierto Diamante, producción e impacto).`,
      `3. Recomendaciones estratégicas para editores, investigadores o tomadores de decisión.`,
      ``,
      `=== DATOS DEL ESTUDIO ===`,
      ``,
      `Compiled Markdown`,
      ``,
      compiledMarkdown
    ].join('\n');

    return systemInstructions;
  }, [compiledMarkdown, pageTitle]);

  // Length calculation
  const charCount = fullChatGptPrompt.length;
  const estimatedTokens = Math.round(charCount / 3.8);
  const exceedsUrlLimit = charCount > MAX_CHATGPT_URL_CHARS;
  const percentageOfLimit = Math.min(100, Math.round((charCount / MAX_CHATGPT_URL_CHARS) * 100));

  // Copy to clipboard
  const handleCopy = () => {
    if (!requireAuth(() => handleCopy(), 'ai_context')) return;
    if (!fullChatGptPrompt) return;
    navigator.clipboard.writeText(fullChatGptPrompt).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2500);
    });
  };

  // Open direct ChatGPT URL (if under limit)
  const handleOpenChatGPT = () => {
    if (!requireAuth('ai_context')) return;
    if (exceedsUrlLimit || !fullChatGptPrompt) return;
    const url = `https://chatgpt.com/?q=${encodeURIComponent(fullChatGptPrompt)}`;
    window.open(url, '_blank');
  };

  // Add all selected items to study dossier
  const handleSaveToDossier = () => {
    if (!requireAuth('ai_context')) return;
    if (selectedIds.size === 0) return;
    
    sections.forEach(s => {
      if (selectedIds.has(s.id)) {
        addDossierItem({
          key: `${s.id}_${Date.now().toString(36)}`,
          title: s.title,
          context: typeof s.buildDataText === 'function' ? s.buildDataText().slice(0, 300) : (s.dataText?.slice(0, 300) || ''),
          category: s.category || 'Análisis de Página',
          data: s.rawData || null
        });
      }
    });

    setAddedToDossier(true);
    setTimeout(() => setAddedToDossier(false), 2500);
  };

  // Download Markdown file
  const handleDownloadMarkdown = () => {
    if (!requireAuth('ai_context')) return;
    if (!compiledMarkdown) return;
    const blob = new Blob([compiledMarkdown], { type: 'text/markdown;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${pageTitle.toLowerCase().replace(/[^a-z0-9]+/g, '_')}_${new Date().toISOString().slice(0, 10)}.md`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="card" style={{
      marginTop: '32px',
      border: '1.5px solid var(--accent-primary)',
      boxShadow: '0 8px 30px rgba(2, 132, 199, 0.12)',
      borderRadius: '16px',
      overflow: 'hidden',
      padding: 0
    }}>
      {/* ── EXPANDER HEADER BAR ── */}
      <div 
        onClick={() => setIsOpen(!isOpen)}
        style={{
          padding: '18px 24px',
          background: 'linear-gradient(135deg, rgba(2, 132, 199, 0.08) 0%, rgba(14, 165, 233, 0.03) 100%)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          cursor: 'pointer',
          userSelect: 'none',
          borderBottom: isOpen ? '1px solid var(--border-color)' : 'none',
          transition: 'background 0.2s ease'
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <div style={{
            width: '36px',
            height: '36px',
            borderRadius: '10px',
            background: 'var(--accent-primary)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: '#ffffff',
            boxShadow: '0 4px 12px rgba(2, 132, 199, 0.35)'
          }}>
            <Bot size={20} />
          </div>

          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <h3 style={{ fontSize: '16px', fontWeight: '800', margin: 0, color: 'var(--text-main)' }}>
                Compilador de Contexto para IA (Envío a ChatGPT / LLMs)
              </h3>
              <span className="badge" style={{
                background: 'rgba(2, 132, 199, 0.15)',
                color: 'var(--accent-primary)',
                border: '1px solid var(--accent-primary)',
                fontSize: '11px'
              }}>
                {selectedIds.size} de {sections.length} secciones seleccionadas
              </span>
            </div>
            <p style={{ fontSize: '12px', color: 'var(--text-muted)', margin: '3px 0 0 0' }}>
              {pageDescription}
            </p>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          {/* Quick status pill */}
          <div style={{
            fontSize: '11.5px',
            fontWeight: '600',
            color: exceedsUrlLimit ? 'var(--accent-warning)' : 'var(--accent-success)',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            background: 'var(--bg-input)',
            padding: '5px 10px',
            borderRadius: '8px',
            border: '1px solid var(--border-color)'
          }}>
            <span>{charCount.toLocaleString()} caracteres</span>
            <span>•</span>
            <span>~{estimatedTokens.toLocaleString()} tokens</span>
          </div>

          <button
            style={{
              background: 'transparent',
              border: 'none',
              color: 'var(--text-muted)',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center'
            }}
          >
            {isOpen ? <ChevronUp size={22} /> : <ChevronDown size={22} />}
          </button>
        </div>
      </div>

      {/* ── EXPANDER CONTENT BODY ── */}
      {isOpen && (
        <div style={{ padding: '20px 24px', display: 'flex', flexDirection: 'column', gap: '20px' }}>
          {/* Quick Selection Toolbar */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
              <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Selección rápida:
              </span>
              <button 
                className="segmented-pill-btn" 
                onClick={selectAll}
                style={{ fontSize: '11.5px', padding: '4px 10px', height: 'auto' }}
              >
                ✓ Seleccionar Todos
              </button>
              <button 
                className="segmented-pill-btn" 
                onClick={selectDefault}
                style={{ fontSize: '11.5px', padding: '4px 10px', height: 'auto' }}
              >
                ⭐ Solo Indicadores Clave / Cabecera
              </button>
              <button 
                className="segmented-pill-btn" 
                onClick={selectNone}
                style={{ fontSize: '11.5px', padding: '4px 10px', height: 'auto' }}
              >
                ✗ Deseleccionar Todos
              </button>
            </div>

            <button
              onClick={() => setShowPreview(!showPreview)}
              className="btn-secondary"
              style={{ fontSize: '12px', padding: '5px 10px', display: 'flex', alignItems: 'center', gap: '5px' }}
            >
              {showPreview ? <EyeOff size={14} /> : <Eye size={14} />}
              {showPreview ? 'Ocultar Vista Previa' : 'Ver Vista Previa Markdown'}
            </button>
          </div>

          {/* Checklist of Page Sections */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))',
            gap: '10px',
            background: 'var(--bg-input)',
            padding: '16px',
            borderRadius: '12px',
            border: '1px solid var(--border-color)'
          }}>
            {sections.map(s => {
              const isChecked = selectedIds.has(s.id);
              return (
                <label
                  key={s.id}
                  style={{
                    display: 'flex',
                    alignItems: 'flex-start',
                    gap: '10px',
                    padding: '8px 12px',
                    borderRadius: '8px',
                    background: isChecked ? 'rgba(2, 132, 199, 0.08)' : 'transparent',
                    border: isChecked ? '1px solid var(--accent-primary)' : '1px solid transparent',
                    cursor: 'pointer',
                    userSelect: 'none',
                    transition: 'all 0.15s ease'
                  }}
                >
                  <input
                    type="checkbox"
                    checked={isChecked}
                    onChange={() => toggleSection(s.id)}
                    style={{
                      marginTop: '3px',
                      accentColor: 'var(--accent-primary)',
                      cursor: 'pointer',
                      width: '15px',
                      height: '15px'
                    }}
                  />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '12.5px', fontWeight: isChecked ? '700' : '500', color: 'var(--text-main)', lineHeight: 1.3 }}>
                      {s.title}
                    </div>
                    {s.category && (
                      <span style={{ fontSize: '10.5px', color: 'var(--text-muted)' }}>
                        {s.category}
                      </span>
                    )}
                  </div>
                </label>
              );
            })}
          </div>

          {/* ── CHARACTERS LENGTH METER & CAPACITY BAR ── */}
          <div style={{
            background: 'var(--bg-input)',
            borderRadius: '10px',
            padding: '12px 16px',
            border: '1px solid var(--border-color)',
            display: 'flex',
            flexDirection: 'column',
            gap: '8px'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '12px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span style={{ fontWeight: '700', color: 'var(--text-main)' }}>Medidor de Longitud para Envío Directo a ChatGPT:</span>
                <span style={{ color: 'var(--text-muted)' }}>
                  ({charCount.toLocaleString()} / {MAX_CHATGPT_URL_CHARS.toLocaleString()} caracteres recomendados para URL)
                </span>
              </div>
              <strong style={{ color: exceedsUrlLimit ? 'var(--accent-warning)' : 'var(--accent-success)' }}>
                {exceedsUrlLimit ? 'Límite URL Superado' : `${percentageOfLimit}% del límite URL`}
              </strong>
            </div>

            {/* Progress bar */}
            <div style={{
              width: '100%',
              height: '8px',
              background: 'var(--border-color)',
              borderRadius: '4px',
              overflow: 'hidden'
            }}>
              <div style={{
                width: `${Math.min(100, percentageOfLimit)}%`,
                height: '100%',
                background: exceedsUrlLimit
                  ? 'linear-gradient(90deg, #f59e0b, #ef4444)'
                  : 'linear-gradient(90deg, #10b981, #0284c7)',
                transition: 'width 0.3s ease, background 0.3s ease'
              }} />
            </div>

            {exceedsUrlLimit && (
              <div style={{
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                fontSize: '11.5px',
                color: 'var(--accent-warning)',
                marginTop: '2px'
              }}>
                <AlertTriangle size={14} flexShrink={0} />
                <span>
                  La selección actual ({charCount.toLocaleString()} caracteres) es muy extensa para abrirse directamente vía URL en el navegador. 
                  <strong> Usa el botón "📋 Copiar Texto para ChatGPT"</strong> y pégalo directamente en tu conversación de ChatGPT sin restricciones.
                </span>
              </div>
            )}
          </div>

          {/* ── MARKDOWN PREVIEW (COLLAPSIBLE) ── */}
          {showPreview && (
            <div style={{
              background: 'var(--bg-app)',
              border: '1px solid var(--border-color)',
              borderRadius: '10px',
              padding: '14px',
              maxHeight: '260px',
              overflowY: 'auto'
            }}>
              <div style={{ fontSize: '11px', fontWeight: '700', textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: '8px' }}>
                Vista Previa del Markdown Consolidado:
              </div>
              <pre style={{
                fontFamily: 'Fira Code, monospace',
                fontSize: '11.5px',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
                color: 'var(--text-main)',
                margin: 0,
                lineHeight: 1.5
              }}>
                {fullChatGptPrompt || 'No hay secciones seleccionadas.'}
              </pre>
            </div>
          )}

          {/* ── ACTION BUTTONS ── */}
          <div style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '12px',
            paddingTop: '8px'
          }}>
            {/* Left: Study Dossier Button */}
            <button
              onClick={handleSaveToDossier}
              disabled={selectedIds.size === 0}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '8px',
                padding: '10px 18px',
                borderRadius: '8px',
                background: addedToDossier ? 'var(--accent-success)' : 'var(--bg-input)',
                color: addedToDossier ? '#ffffff' : 'var(--text-main)',
                border: addedToDossier ? '1px solid var(--accent-success)' : '1px solid var(--border-color)',
                fontSize: '13px',
                fontWeight: '700',
                cursor: selectedIds.size === 0 ? 'not-allowed' : 'pointer',
                opacity: selectedIds.size === 0 ? 0.5 : 1,
                transition: 'all 0.2s ease',
                boxShadow: addedToDossier ? '0 4px 15px rgba(16, 185, 129, 0.3)' : 'none'
              }}
            >
              {addedToDossier ? <CheckCircle2 size={16} /> : <BookmarkPlus size={16} color="var(--accent-primary)" />}
              {addedToDossier ? '¡Guardado en Contexto IA!' : '📌 Guardar Selección en Contexto IA'}
            </button>

            {/* Right: ChatGPT & Export Action Buttons */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
              {/* Copy for ChatGPT Button */}
              <button
                onClick={handleCopy}
                disabled={selectedIds.size === 0}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '8px',
                  padding: '10px 18px',
                  borderRadius: '8px',
                  background: copied
                    ? 'linear-gradient(135deg, #10b981, #059669)'
                    : 'linear-gradient(135deg, #6366f1, #8b5cf6)',
                  color: '#ffffff',
                  border: 'none',
                  fontSize: '13px',
                  fontWeight: '700',
                  cursor: selectedIds.size === 0 ? 'not-allowed' : 'pointer',
                  opacity: selectedIds.size === 0 ? 0.5 : 1,
                  boxShadow: '0 4px 15px rgba(99, 102, 241, 0.35)',
                  transition: 'all 0.2s ease'
                }}
              >
                {copied ? <CheckCircle2 size={16} /> : <Copy size={16} />}
                {copied ? '¡Copiado para ChatGPT!' : '📋 Copiar Texto para ChatGPT'}
              </button>

              {/* Direct Open in ChatGPT (URL limited) */}
              <button
                onClick={handleOpenChatGPT}
                disabled={selectedIds.size === 0 || exceedsUrlLimit}
                title={exceedsUrlLimit ? 'Texto demasiado largo para URL directa. Usa "Copiar Texto para ChatGPT"' : 'Abrir prompt en ChatGPT en nueva pestaña'}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '6px',
                  padding: '10px 16px',
                  borderRadius: '8px',
                  background: exceedsUrlLimit ? 'var(--bg-input)' : 'var(--accent-primary)',
                  color: exceedsUrlLimit ? 'var(--text-subtle)' : '#ffffff',
                  border: exceedsUrlLimit ? '1px solid var(--border-color)' : 'none',
                  fontSize: '13px',
                  fontWeight: '600',
                  cursor: (selectedIds.size === 0 || exceedsUrlLimit) ? 'not-allowed' : 'pointer',
                  opacity: (selectedIds.size === 0 || exceedsUrlLimit) ? 0.5 : 1,
                  transition: 'all 0.2s ease'
                }}
              >
                <ExternalLink size={14} /> Abrir en ChatGPT
              </button>

              {/* Download Markdown */}
              <button
                onClick={handleDownloadMarkdown}
                disabled={selectedIds.size === 0}
                className="btn-secondary"
                style={{ fontSize: '13px', padding: '9px 14px', display: 'flex', alignItems: 'center', gap: '6px' }}
                title="Descargar archivo Markdown formateado"
              >
                <Download size={14} /> .MD
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
