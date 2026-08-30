import React from 'react';
import { useAppStore } from '../store';
import { 
  Globe2, 
  MapPin, 
  BookOpen, 
  Map, 
  Share2, 
  Info,
  ChevronRight
} from 'lucide-react';

const MENU_ITEMS = [
  { id: 'regional', label: 'Panorama Regional', icon: Globe2, desc: 'Indicadores macro y mapa coroplético' },
  { id: 'country', label: 'País', icon: MapPin, desc: 'Perfil cienciométrico por país' },
  { id: 'journal', label: 'Revista', icon: BookOpen, desc: 'Ficha técnica y artículos' },
  { id: 'maps', label: 'Mapas Semánticos', icon: Map, desc: 'Paisaje 2D GPU (Artículos & Revistas)' },
  { id: 'networks', label: 'Redes de Colaboración', icon: Share2, desc: 'Coautoría internacional y Sankey' },
  { id: 'about', label: 'Acerca de...', icon: Info, desc: 'Equipo, arquitectura y métodos' },
];

export default function Sidebar() {
  const { activeSection, setActiveSection } = useAppStore();

  return (
    <aside style={{
      width: '270px',
      background: 'var(--bg-sidebar)',
      borderRight: '1px solid var(--border-color)',
      padding: '24px 16px',
      display: 'flex',
      flexDirection: 'column',
      gap: '6px',
      flexShrink: 0,
      transition: 'all 0.2s ease'
    }}>
      <div style={{
        padding: '0 8px 12px 8px',
        fontSize: '11px',
        fontWeight: '700',
        color: 'var(--text-subtle)',
        textTransform: 'uppercase',
        letterSpacing: '0.06em'
      }}>
        Navegación Analítica
      </div>

      {MENU_ITEMS.map((item) => {
        const Icon = item.icon;
        const isActive = activeSection === item.id;

        return (
          <button
            key={item.id}
            onClick={() => setActiveSection(item.id)}
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              width: '100%',
              padding: '12px 14px',
              borderRadius: '10px',
              border: isActive ? '1px solid var(--accent-primary)' : '1px solid transparent',
              background: isActive ? 'var(--accent-primary-light)' : 'transparent',
              color: isActive ? 'var(--accent-primary)' : 'var(--text-main)',
              cursor: 'pointer',
              textAlign: 'left',
              transition: 'all 0.18s ease',
            }}
            onMouseEnter={(e) => {
              if (!isActive) {
                e.currentTarget.style.background = 'rgba(2, 132, 199, 0.04)';
              }
            }}
            onMouseLeave={(e) => {
              if (!isActive) {
                e.currentTarget.style.background = 'transparent';
              }
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              <Icon size={19} color={isActive ? 'var(--accent-primary)' : 'var(--text-muted)'} />
              <div>
                <div style={{ fontSize: '13.5px', fontWeight: isActive ? '700' : '600' }}>
                  {item.label}
                </div>
                <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '2px' }}>
                  {item.desc}
                </div>
              </div>
            </div>
            {isActive && <ChevronRight size={16} color="var(--accent-primary)" />}
          </button>
        );
      })}

      <div style={{ marginTop: 'auto', padding: '16px 12px', background: 'var(--bg-input)', borderRadius: '10px', border: '1px solid var(--border-color)' }}>
        <div style={{ fontSize: '11.5px', fontWeight: '700', color: 'var(--text-main)' }}>
          ⚡ DuckDB In-Memory Engine
        </div>
        <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px', lineHeight: 1.4 }}>
          3.63M Trabajos • 7,494 Revistas indexadas con latencia &lt; 15 ms.
        </div>
      </div>
    </aside>
  );
}
