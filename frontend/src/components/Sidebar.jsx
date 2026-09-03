import React from 'react';
import { useAppStore } from '../store';
import { useTranslation } from '../i18n';
import { 
  Globe2, 
  MapPin, 
  BookOpen, 
  Map, 
  Share2, 
  Info,
  ChevronRight,
  ChevronLeft,
  ShieldCheck
} from 'lucide-react';

const MENU_ITEMS = [
  { id: 'regional', labelKey: 'nav.regional', icon: Globe2, descKey: 'nav.regional_desc' },
  { id: 'country', labelKey: 'nav.country', icon: MapPin, descKey: 'nav.country_desc' },
  { id: 'journal', labelKey: 'nav.journal', icon: BookOpen, descKey: 'nav.journal_desc' },
  { id: 'maps', labelKey: 'nav.maps', icon: Map, descKey: 'nav.maps_desc' },
  { id: 'networks', labelKey: 'nav.networks', icon: Share2, descKey: 'nav.networks_desc' },
  { id: 'about', labelKey: 'nav.about', icon: Info, descKey: 'nav.about_desc' },
];

export default function Sidebar() {
  const { 
    activeSection, 
    setActiveSection, 
    user,
    sidebarCollapsed,
    toggleSidebar,
    mobileMenuOpen,
    setMobileMenuOpen
  } = useAppStore();
  const { t } = useTranslation();

  return (
    <>
      {/* Mobile Backdrop Overlay */}
      {mobileMenuOpen && (
        <div 
          className="mobile-overlay"
          onClick={() => setMobileMenuOpen(false)}
        />
      )}

      <aside 
        className={`sidebar-wrapper ${mobileMenuOpen ? 'mobile-open' : ''}`}
        style={{
          width: sidebarCollapsed ? '76px' : '270px',
          background: 'var(--bg-sidebar)',
          borderRight: '1px solid var(--border-color)',
          padding: sidebarCollapsed ? '20px 10px' : '20px 14px',
          display: 'flex',
          flexDirection: 'column',
          gap: '6px',
          overflowY: 'auto',
          overflowX: 'hidden',
          transition: 'width 0.25s cubic-bezier(0.4, 0, 0.2, 1), transform 0.3s ease'
        }}
      >
        {/* Header & Collapse Toggle Button */}
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: sidebarCollapsed ? 'center' : 'space-between',
          padding: sidebarCollapsed ? '0 0 12px 0' : '0 6px 12px 6px',
          borderBottom: '1px solid var(--border-color)',
          marginBottom: '8px'
        }}>
          {!sidebarCollapsed && (
            <span style={{
              fontSize: '11px',
              fontWeight: '700',
              color: 'var(--text-subtle)',
              textTransform: 'uppercase',
              letterSpacing: '0.06em',
              whiteSpace: 'nowrap'
            }}>
              {t('nav.section_title')}
            </span>
          )}

          {/* Desktop Toggle Button */}
          <button
            onClick={toggleSidebar}
            title={sidebarCollapsed ? t('nav.expand_btn') : t('nav.collapse_btn')}
            style={{
              background: 'var(--bg-input)',
              border: '1px solid var(--border-color)',
              color: 'var(--text-main)',
              borderRadius: '7px',
              width: '28px',
              height: '28px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
            onMouseEnter={(e) => e.currentTarget.style.borderColor = 'var(--accent-primary)'}
            onMouseLeave={(e) => e.currentTarget.style.borderColor = 'var(--border-color)'}
          >
            {sidebarCollapsed ? <ChevronRight size={15} /> : <ChevronLeft size={15} />}
          </button>
        </div>

        {/* Menu Items */}
        {MENU_ITEMS.map((item) => {
          const Icon = item.icon;
          const isActive = activeSection === item.id;
          const label = t(item.labelKey);
          const desc = t(item.descKey);

          return (
            <button
              key={item.id}
              onClick={() => setActiveSection(item.id)}
              title={sidebarCollapsed ? `${label} — ${desc}` : undefined}
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: sidebarCollapsed ? 'center' : 'space-between',
                width: '100%',
                padding: sidebarCollapsed ? '12px 0' : '10px 12px',
                borderRadius: '10px',
                border: isActive ? '1px solid var(--accent-primary)' : '1px solid transparent',
                background: isActive ? 'var(--accent-primary-light)' : 'transparent',
                color: isActive ? 'var(--accent-primary)' : 'var(--text-main)',
                cursor: 'pointer',
                textAlign: 'left',
                transition: 'all 0.18s ease',
                position: 'relative'
              }}
              onMouseEnter={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'rgba(2, 132, 199, 0.05)';
                }
              }}
              onMouseLeave={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'transparent';
                }
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px', minWidth: 0 }}>
                <Icon size={19} color={isActive ? 'var(--accent-primary)' : 'var(--text-muted)'} style={{ flexShrink: 0 }} />
                {!sidebarCollapsed && (
                  <div style={{ minWidth: 0, overflow: 'hidden' }}>
                    <div style={{ fontSize: '13px', fontWeight: isActive ? '700' : '600', whiteSpace: 'nowrap', textOverflow: 'ellipsis', overflow: 'hidden' }}>
                      {label}
                    </div>
                    <div style={{ fontSize: '10.5px', color: 'var(--text-muted)', marginTop: '1px', whiteSpace: 'nowrap', textOverflow: 'ellipsis', overflow: 'hidden' }}>
                      {desc}
                    </div>
                  </div>
                )}
              </div>
              {!sidebarCollapsed && isActive && <ChevronRight size={15} color="var(--accent-primary)" style={{ flexShrink: 0 }} />}
            </button>
          );
        })}

        {/* Administration Item (Only visible for Admins) */}
        {user && user.is_admin && (
          <div style={{ marginTop: '14px', borderTop: '1px solid var(--border-color)', paddingTop: '12px' }}>
            {!sidebarCollapsed && (
              <div style={{
                padding: '0 6px 8px 6px',
                fontSize: '10.5px',
                fontWeight: '700',
                color: '#6366f1',
                textTransform: 'uppercase',
                letterSpacing: '0.06em',
                display: 'flex',
                alignItems: 'center',
                gap: '5px'
              }}>
                <ShieldCheck size={13} />
                <span>{t('nav.admin')}</span>
              </div>
            )}

            <button
              onClick={() => setActiveSection('admin')}
              title={sidebarCollapsed ? `${t('nav.admin')} — ${t('nav.admin_desc')}` : undefined}
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: sidebarCollapsed ? 'center' : 'space-between',
                width: '100%',
                padding: sidebarCollapsed ? '12px 0' : '10px 12px',
                borderRadius: '10px',
                border: activeSection === 'admin' ? '1px solid #6366f1' : '1px solid rgba(99, 102, 241, 0.25)',
                background: activeSection === 'admin' ? 'rgba(99, 102, 241, 0.15)' : 'rgba(99, 102, 241, 0.05)',
                color: activeSection === 'admin' ? '#6366f1' : 'var(--text-main)',
                cursor: 'pointer',
                textAlign: 'left',
                transition: 'all 0.18s ease',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px', minWidth: 0 }}>
                <ShieldCheck size={19} color="#6366f1" style={{ flexShrink: 0 }} />
                {!sidebarCollapsed && (
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontSize: '13px', fontWeight: activeSection === 'admin' ? '700' : '600' }}>
                      {t('nav.admin')}
                    </div>
                    <div style={{ fontSize: '10.5px', color: 'var(--text-muted)', marginTop: '1px' }}>
                      {t('nav.admin_desc')}
                    </div>
                  </div>
                )}
              </div>
              {!sidebarCollapsed && activeSection === 'admin' && <ChevronRight size={15} color="#6366f1" />}
            </button>
          </div>
        )}
      </aside>
    </>
  );
}
