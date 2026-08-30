import React from 'react';

export default function KpiCard({ title, value, subtitle, icon: Icon, badge, color = 'var(--accent-primary)' }) {
  return (
    <div className="card" style={{
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'space-between',
      position: 'relative',
      overflow: 'hidden',
      padding: '18px 20px',
      gap: '8px'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span style={{ fontSize: '12px', fontWeight: '600', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
          {title}
        </span>
        {Icon && (
          <div style={{
            width: '32px',
            height: '32px',
            borderRadius: '8px',
            background: 'var(--bg-input)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: color
          }}>
            <Icon size={17} />
          </div>
        )}
      </div>

      <div style={{ fontSize: '26px', fontWeight: '800', fontFamily: 'Outfit', color: 'var(--text-main)', letterSpacing: '-0.03em' }}>
        {value !== undefined && value !== null ? value : '—'}
      </div>

      {(subtitle || badge) && (
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: '2px' }}>
          {subtitle && (
            <span style={{ fontSize: '11.5px', color: 'var(--text-muted)' }}>
              {subtitle}
            </span>
          )}
          {badge && (
            <span className="badge" style={{ fontSize: '10px' }}>
              {badge}
            </span>
          )}
        </div>
      )}
    </div>
  );
}
