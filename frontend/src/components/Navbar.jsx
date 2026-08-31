import React from 'react';
import { useAppStore } from '../store';
import { Sun, Moon, Sparkles, FileText, Database, ShieldCheck } from 'lucide-react';

export default function Navbar() {
  const { theme, setTheme, dossierItems, setDossierOpen } = useAppStore();

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
            Inteligencia Científica y Cartografía Topológica
          </span>
        </div>
      </div>

      {/* Actions / Theme switcher / Dossier button */}
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

        {/* Study Dossier Button */}
        <button
          onClick={() => setDossierOpen(true)}
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
          <FileText size={15} />
          <span>Dossier de Estudio</span>
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
      </div>
    </header>
  );
}
