import React from 'react';
import { useAppStore } from '../store';
import { Sun, Moon, Sparkles, FileText, Download, Clock, Bot, LogOut, User, ExternalLink } from 'lucide-react';

export default function Navbar() {
  const {
    theme,
    setTheme,
    dossierItems,
    setDossierOpen,
    exportJobs,
    setDownloadsOpen,
    user,
    setLoginModalOpen,
    logout
  } = useAppStore();

  const activeJobs = exportJobs.filter(j => j.status === 'processing' || j.status === 'pending');
  const completedJobs = exportJobs.filter(j => j.status === 'completed');
  const latestActiveJob = activeJobs[0];

  return (
    <header style={{
      height: '64px',
      background: 'var(--bg-header)',
      borderBottom: '1px solid var(--border-color)',
      boxShadow: 'var(--header-shadow)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      padding: '0 32px',
      position: 'sticky',
      top: 0,
      zIndex: 40,
      transition: 'all 0.2s ease'
    }}>
      {/* Brand / Logo */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
        <div style={{
          width: '36px',
          height: '36px',
          borderRadius: '9px',
          background: 'linear-gradient(135deg, #0284c7 0%, #0369a1 100%)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: '#ffffff',
          fontWeight: '800',
          fontSize: '18px',
          boxShadow: '0 4px 12px rgba(2, 132, 199, 0.35)'
        }}>
          RL
        </div>
        <div>
          <h1 style={{ fontSize: '17px', fontWeight: '800', lineHeight: 1.1 }}>
            Revistas LATAM
          </h1>
          <span style={{ fontSize: '11px', color: 'var(--text-muted)', fontWeight: '500' }}>
            Con datos de OpenAlex
          </span>
        </div>
      </div>

      {/* Actions / Theme switcher / Dossier & Downloads & ORCID Auth buttons */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
        {/* Theme Segmented Switcher */}
        <div className="segmented-pills">
          <button
            className={`segmented-pill-btn ${theme === 'claro' ? 'active' : ''}`}
            onClick={() => setTheme('claro')}
            title="Tema Claro (Blanco)"
            style={{ display: 'flex', alignItems: 'center', gap: '5px' }}
          >
            <Sun size={14} />
            <span>Claro</span>
          </button>
          <button
            className={`segmented-pill-btn ${theme === 'oscuro' ? 'active' : ''}`}
            onClick={() => setTheme('oscuro')}
            title="Tema Oscuro (Dark)"
            style={{ display: 'flex', alignItems: 'center', gap: '5px' }}
          >
            <Moon size={14} />
            <span>Oscuro</span>
          </button>
          <button
            className={`segmented-pill-btn ${theme === 'navy' ? 'active' : ''}`}
            onClick={() => setTheme('navy')}
            title="Tema Azul Noche (Navy)"
            style={{ display: 'flex', alignItems: 'center', gap: '5px' }}
          >
            <Sparkles size={14} />
            <span>Navy</span>
          </button>
        </div>

        {/* Context for AI Button */}
        <button
          onClick={() => setDossierOpen(true)}
          title="Abrir panel de Contexto compilado para ChatGPT y agentes de IA"
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            padding: '7px 14px',
            borderRadius: '8px',
            background: dossierItems.length > 0 ? 'var(--accent-primary)' : 'var(--bg-input)',
            color: dossierItems.length > 0 ? '#ffffff' : 'var(--text-main)',
            border: '1px solid var(--border-color)',
            fontSize: '13px',
            fontWeight: '600',
            cursor: 'pointer',
            boxShadow: dossierItems.length > 0 ? '0 2px 8px rgba(2, 132, 199, 0.3)' : 'none',
            transition: 'all 0.2s ease'
          }}
        >
          <Bot size={15} />
          <span>Contexto para IA</span>
          {dossierItems.length > 0 && (
            <span style={{
              background: '#ffffff',
              color: 'var(--accent-primary)',
              borderRadius: '10px',
              padding: '1px 6px',
              fontSize: '11px',
              fontWeight: '800'
            }}>
              {dossierItems.length}
            </span>
          )}
        </button>

        {/* Background Downloads Button */}
        <button
          onClick={() => setDownloadsOpen(true)}
          title="Ver panel de exportaciones y descargas en segundo plano"
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            padding: '7px 14px',
            borderRadius: '8px',
            background: activeJobs.length > 0
              ? 'linear-gradient(135deg, rgba(2, 132, 199, 0.2), rgba(16, 185, 129, 0.2))'
              : (completedJobs.length > 0 ? 'rgba(16, 185, 129, 0.15)' : 'var(--bg-input)'),
            color: activeJobs.length > 0
              ? '#0284c7'
              : (completedJobs.length > 0 ? '#10b981' : 'var(--text-main)'),
            border: `1px solid ${activeJobs.length > 0 ? '#0284c7' : (completedJobs.length > 0 ? '#10b981' : 'var(--border-color)')}`,
            fontSize: '13px',
            fontWeight: '600',
            cursor: 'pointer',
            transition: 'all 0.2s ease'
          }}
        >
          <Download size={15} />
          <span>
            {activeJobs.length > 0
              ? `Descargas (${latestActiveJob?.pct || 0}%)`
              : 'Descargas'}
          </span>
          {completedJobs.length > 0 && activeJobs.length === 0 && (
            <span style={{
              background: '#10b981',
              color: '#ffffff',
              borderRadius: '10px',
              padding: '1px 6px',
              fontSize: '11px',
              fontWeight: '800'
            }}>
              {completedJobs.length}
            </span>
          )}
        </button>

        {/* ORCID Authentication Capsule / Login Button */}
        {user && user.orcid ? (
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            padding: '5px 12px 5px 8px',
            borderRadius: '20px',
            background: 'rgba(166, 206, 57, 0.12)',
            border: '1px solid rgba(166, 206, 57, 0.35)',
            fontSize: '12.5px'
          }}>
            <a
              href={`https://orcid.org/${user.orcid}`}
              target="_blank"
              rel="noreferrer"
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                color: 'var(--text-main)',
                textDecoration: 'none',
                fontWeight: '600'
              }}
              title={`Ver perfil de ORCID: ${user.orcid}`}
            >
              <span style={{
                background: '#a6ce39',
                color: '#fff',
                fontSize: '10.5px',
                fontWeight: '900',
                padding: '2px 5px',
                borderRadius: '4px'
              }}>
                iD
              </span>
              <span style={{ maxWidth: '140px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {user.name || user.orcid}
              </span>
            </a>
            <button
              onClick={logout}
              title="Cerrar sesión de ORCID"
              style={{
                background: 'none',
                border: 'none',
                color: 'var(--text-muted)',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                padding: '2px'
              }}
              onMouseEnter={(e) => e.currentTarget.style.color = '#f43f5e'}
              onMouseLeave={(e) => e.currentTarget.style.color = 'var(--text-muted)'}
            >
              <LogOut size={13} />
            </button>
          </div>
        ) : (
          <button
            onClick={() => setLoginModalOpen(true, 'general')}
            title="Iniciar sesión con ORCID iD"
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              padding: '7px 12px',
              borderRadius: '8px',
              background: 'rgba(166, 206, 57, 0.15)',
              color: '#a6ce39',
              border: '1px solid rgba(166, 206, 57, 0.4)',
              fontSize: '13px',
              fontWeight: '700',
              cursor: 'pointer',
              transition: 'all 0.2s ease'
            }}
          >
            <span style={{
              background: '#a6ce39',
              color: '#ffffff',
              fontSize: '11px',
              fontWeight: '900',
              padding: '1px 5px',
              borderRadius: '4px'
            }}>
              iD
            </span>
            <span>Conectar ORCID</span>
          </button>
        )}
      </div>
    </header>
  );
}

