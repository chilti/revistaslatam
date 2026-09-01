import React, { useEffect } from 'react';
import { useAppStore } from '../store';
import api from '../api';
import { X, Download, Trash2, CheckCircle2, Clock, AlertCircle, FileSpreadsheet, FileCode, Sparkles } from 'lucide-react';

export default function DownloadsDrawer() {
  const {
    exportJobs,
    isDownloadsOpen,
    setDownloadsOpen,
    updateExportJob,
    removeExportJob,
    clearCompletedJobs
  } = useAppStore();

  // Background Polling for all active jobs
  useEffect(() => {
    const activeJobs = exportJobs.filter(j => j.status === 'pending' || j.status === 'processing');
    if (activeJobs.length === 0) return;

    const interval = setInterval(async () => {
      for (const job of activeJobs) {
        try {
          const res = await api.get(`/exports/status/${job.id}`);
          if (res.data) {
            updateExportJob(job.id, res.data);
          }
        } catch (e) {
          // If server restarted or job lost
          console.warn(`Could not poll job ${job.id}:`, e);
        }
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [exportJobs, updateExportJob]);

  if (!isDownloadsOpen) return null;

  const completedCount = exportJobs.filter(j => j.status === 'completed').length;
  const activeCount = exportJobs.filter(j => j.status === 'processing' || j.status === 'pending').length;

  const handleDownloadFile = (job) => {
    const baseUrl = api.defaults.baseURL || '/api';
    const downloadUrl = `${baseUrl.replace(/\/$/, '')}/exports/download/${job.id}`;
    const link = document.createElement('a');
    link.href = downloadUrl;
    link.setAttribute('download', job.filename || `export_${job.id}.${job.format}`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 9999,
        background: 'rgba(0, 0, 0, 0.45)',
        backdropFilter: 'blur(4px)',
        display: 'flex',
        justifyContent: 'flex-end',
        transition: 'opacity 0.25s ease'
      }}
      onClick={(e) => {
        if (e.target === e.currentTarget) setDownloadsOpen(false);
      }}
    >
      <div
        style={{
          width: '100%',
          maxWidth: '480px',
          height: '100%',
          background: 'var(--bg-card)',
          borderLeft: '1px solid var(--border-color)',
          display: 'flex',
          flexDirection: 'column',
          boxShadow: '-8px 0 28px rgba(0, 0, 0, 0.25)',
          animation: 'slideInRight 0.25s ease'
        }}
      >
        {/* Drawer Header */}
        <div
          style={{
            padding: '20px 24px',
            borderBottom: '1px solid var(--border-color)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            background: 'var(--bg-surface)'
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div
              style={{
                width: '36px',
                height: '36px',
                borderRadius: '9px',
                background: 'linear-gradient(135deg, #10b981 0%, #059669 100%)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: '#ffffff',
                boxShadow: '0 4px 12px rgba(16, 185, 129, 0.35)'
              }}
            >
              <Download size={18} />
            </div>
            <div>
              <h2 style={{ fontSize: '16px', fontWeight: '800', margin: 0, lineHeight: 1.2 }}>
                Exportaciones y Descargas
              </h2>
              <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                {activeCount > 0
                  ? `⏳ ${activeCount} en proceso · ${completedCount} listas`
                  : `${completedCount} archivo(s) listos`}
              </span>
            </div>
          </div>

          <button
            onClick={() => setDownloadsOpen(false)}
            style={{
              background: 'transparent',
              border: 'none',
              cursor: 'pointer',
              color: 'var(--text-muted)',
              padding: '6px',
              borderRadius: '6px'
            }}
          >
            <X size={20} />
          </button>
        </div>

        {/* Action toolbar */}
        {exportJobs.length > 0 && (
          <div
            style={{
              padding: '10px 24px',
              background: 'var(--bg-input)',
              borderBottom: '1px solid var(--border-color)',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              fontSize: '12px'
            }}
          >
            <span style={{ color: 'var(--text-muted)', fontWeight: '500' }}>
              Registros generados con OpenAlex
            </span>
            {completedCount > 0 && (
              <button
                onClick={clearCompletedJobs}
                style={{
                  background: 'transparent',
                  border: 'none',
                  color: 'var(--text-muted)',
                  cursor: 'pointer',
                  fontSize: '12px',
                  textDecoration: 'underline'
                }}
              >
                Limpiar completados
              </button>
            )}
          </div>
        )}

        {/* Drawer Body / Jobs List */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '20px 24px' }}>
          {exportJobs.length === 0 ? (
            <div
              style={{
                textAlign: 'center',
                padding: '60px 20px',
                color: 'var(--text-muted)',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                gap: '12px'
              }}
            >
              <div
                style={{
                  width: '56px',
                  height: '56px',
                  borderRadius: '16px',
                  background: 'var(--bg-input)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: 'var(--text-muted)'
                }}
              >
                <Download size={26} />
              </div>
              <h3 style={{ fontSize: '15px', fontWeight: '700', margin: 0, color: 'var(--text-main)' }}>
                No hay exportaciones activas
              </h3>
              <p style={{ fontSize: '12.5px', maxWidth: '280px', margin: 0, lineHeight: 1.4 }}>
                En la vista de cualquier revista, haz clic en <strong>Exportar JSON (Full)</strong> o <strong>Exportar CSV</strong>. Podrás monitorear el progreso y descargarlos aquí.
              </p>
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
              {exportJobs.map((job) => {
                const isCompleted = job.status === 'completed';
                const isProcessing = job.status === 'processing' || job.status === 'pending';
                const isFailed = job.status === 'failed';

                return (
                  <div
                    key={job.id}
                    style={{
                      background: 'var(--bg-surface)',
                      border: `1px solid ${isCompleted ? 'rgba(16, 185, 129, 0.35)' : 'var(--border-color)'}`,
                      borderRadius: '12px',
                      padding: '16px',
                      boxShadow: isCompleted ? '0 4px 14px rgba(16, 185, 129, 0.08)' : 'none',
                      transition: 'all 0.2s ease'
                    }}
                  >
                    {/* Header line */}
                    <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '10px' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        {job.format === 'csv' ? (
                          <FileSpreadsheet size={18} color="#0284c7" />
                        ) : (
                          <FileCode size={18} color="#10b981" />
                        )}
                        <span
                          style={{
                            fontSize: '11px',
                            fontWeight: '700',
                            textTransform: 'uppercase',
                            padding: '2px 8px',
                            borderRadius: '6px',
                            background: job.format === 'csv' ? 'rgba(2, 132, 199, 0.15)' : 'rgba(16, 185, 129, 0.15)',
                            color: job.format === 'csv' ? '#0284c7' : '#10b981'
                          }}
                        >
                          {job.format === 'csv' ? 'CSV (88 cols)' : job.format.toUpperCase()}
                        </span>
                      </div>

                      <button
                        onClick={() => removeExportJob(job.id)}
                        title="Eliminar de la lista"
                        style={{
                          background: 'transparent',
                          border: 'none',
                          cursor: 'pointer',
                          color: 'var(--text-muted)',
                          padding: '2px'
                        }}
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>

                    {/* Title */}
                    <h4
                      style={{
                        fontSize: '13.5px',
                        fontWeight: '700',
                        margin: '8px 0 4px 0',
                        color: 'var(--text-main)',
                        lineHeight: 1.3
                      }}
                    >
                      {job.title || 'Exportación OpenAlex'}
                    </h4>

                    {/* Progress & States */}
                    {isProcessing && (
                      <div style={{ marginTop: '10px' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11.5px', color: 'var(--text-muted)', marginBottom: '4px' }}>
                          <span>⏳ Descargando registros...</span>
                          <strong style={{ color: 'var(--accent-primary)' }}>{job.pct ?? 0}%</strong>
                        </div>
                        <div
                          style={{
                            width: '100%',
                            height: '6px',
                            borderRadius: '3px',
                            background: 'var(--bg-input)',
                            overflow: 'hidden'
                          }}
                        >
                          <div
                            style={{
                              width: `${Math.min(100, Math.max(5, job.pct || 5))}%`,
                              height: '100%',
                              background: 'linear-gradient(90deg, #0284c7, #10b981)',
                              borderRadius: '3px',
                              transition: 'width 0.3s ease'
                            }}
                          />
                        </div>
                        {job.total > 0 && (
                          <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px', textAlign: 'right' }}>
                            {job.progress?.toLocaleString() || 0} / {job.total?.toLocaleString()} artículos
                          </div>
                        )}
                      </div>
                    )}

                    {isCompleted && (
                      <div style={{ marginTop: '12px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '12px', color: '#10b981', fontWeight: '600' }}>
                          <CheckCircle2 size={15} />
                          <span>Listo ({job.filesize_mb} MB)</span>
                        </div>

                        <button
                          className="btn-primary"
                          onClick={() => handleDownloadFile(job)}
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '6px',
                            fontSize: '12px',
                            padding: '6px 14px',
                            borderRadius: '6px'
                          }}
                        >
                          <Download size={13} /> Descargar
                        </button>
                      </div>
                    )}

                    {isFailed && (
                      <div style={{ marginTop: '8px', display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: '#ef4444' }}>
                        <AlertCircle size={15} />
                        <span>{job.error || 'Error en la exportación'}</span>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
