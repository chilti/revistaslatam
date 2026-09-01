import React, { useState, useEffect, useMemo } from 'react';
import { useAppStore } from '../store';
import api from '../api';
import { 
  ShieldCheck, 
  Users, 
  Globe, 
  Activity, 
  Search, 
  ExternalLink, 
  RefreshCw, 
  Download, 
  Building2, 
  Calendar,
  Lock,
  UserCheck
} from 'lucide-react';

export default function AdminPage() {
  const { user } = useAppStore();
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState({
    total_users: 0,
    total_admins: 0,
    total_logins: 0,
    distinct_countries: 0,
    users: []
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [roleFilter, setRoleFilter] = useState('ALL'); // 'ALL' | 'admin' | 'user'

  const fetchUsers = async () => {
    setLoading(true);
    try {
      const res = await api.get('/auth/users');
      if (res.data) {
        setData(res.data);
      }
    } catch (err) {
      console.error('Error fetching registered users:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchUsers();
  }, []);

  const filteredUsers = useMemo(() => {
    let list = data.users || [];
    if (roleFilter !== 'ALL') {
      list = list.filter(u => roleFilter === 'admin' ? u.is_admin : !u.is_admin);
    }
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      list = list.filter(u => 
        (u.name && u.name.toLowerCase().includes(q)) ||
        (u.orcid && u.orcid.toLowerCase().includes(q)) ||
        (u.institution && u.institution.toLowerCase().includes(q)) ||
        (u.country && u.country.toLowerCase().includes(q))
      );
    }
    return list;
  }, [data.users, roleFilter, searchQuery]);

  const handleExportCSV = () => {
    if (!filteredUsers.length) return;
    const headers = ['ORCID', 'Nombre', 'Institucion', 'Pais', 'Rol', 'Inicios_Sesion', 'Primer_Acceso', 'Ultimo_Acceso'];
    const rows = filteredUsers.map(u => [
      `"${u.orcid}"`,
      `"${(u.name || '').replace(/"/g, '""')}"`,
      `"${(u.institution || '').replace(/"/g, '""')}"`,
      `"${u.country || ''}"`,
      `"${u.is_admin ? 'Administrador' : 'Investigador'}"`,
      u.login_count || 1,
      `"${u.first_login || ''}"`,
      `"${u.last_login || ''}"`
    ]);

    const csvContent = '\uFEFF' + [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `usuarios_orcid_revistaslatam_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // Si el usuario no tiene privilegios de administrador
  if (!user || !user.is_admin) {
    return (
      <div className="card" style={{ maxWidth: '600px', margin: '40px auto', textAlign: 'center', padding: '40px 24px' }}>
        <div style={{
          width: '56px',
          height: '56px',
          borderRadius: '50%',
          background: 'rgba(239, 68, 68, 0.15)',
          color: '#ef4444',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          margin: '0 auto 16px'
        }}>
          <Lock size={28} />
        </div>
        <h2 style={{ fontSize: '20px', fontWeight: '800', marginBottom: '8px', color: 'var(--text-main)' }}>
          Acceso Restringido a Administradores
        </h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', lineHeight: '1.5' }}>
          Esta sección contiene registros de auditoría y gestión de investigadores. Tu ORCID iD ({user?.orcid || 'No autenticado'}) no cuenta con privilegios de administrador configurados en el servidor.
        </p>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      {/* ── HEADER ── */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '4px' }}>
            <div style={{
              width: '36px',
              height: '36px',
              borderRadius: '9px',
              background: 'linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#ffffff',
              boxShadow: '0 4px 12px rgba(99, 102, 241, 0.35)'
            }}>
              <ShieldCheck size={22} />
            </div>
            <h1 style={{ fontSize: '22px', fontWeight: '800', color: 'var(--text-main)', margin: 0 }}>
              Panel de Administración y Registro de Investigadores
            </h1>
          </div>
          <p style={{ fontSize: '13px', color: 'var(--text-muted)', margin: 0 }}>
            Auditoría de usuarios autenticados vía ORCID OAuth 2.0, instituciones y métricas de acceso.
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <button
            onClick={fetchUsers}
            disabled={loading}
            className="btn-secondary"
            style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px', padding: '8px 14px' }}
            title="Recargar lista de usuarios"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            <span>Actualizar</span>
          </button>

          <button
            onClick={handleExportCSV}
            disabled={filteredUsers.length === 0}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              padding: '8px 16px',
              borderRadius: '8px',
              background: 'var(--accent-primary)',
              color: '#ffffff',
              border: 'none',
              fontSize: '13px',
              fontWeight: '700',
              cursor: filteredUsers.length === 0 ? 'not-allowed' : 'pointer',
              opacity: filteredUsers.length === 0 ? 0.6 : 1,
              boxShadow: '0 2px 8px rgba(2, 132, 199, 0.3)'
            }}
          >
            <Download size={14} />
            <span>Exportar CSV</span>
          </button>
        </div>
      </div>

      {/* ── KPI CARDS ── */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
        gap: '16px'
      }}>
        <div className="card" style={{ padding: '18px 20px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
            <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase' }}>
              Investigadores
            </span>
            <Users size={18} color="var(--accent-primary)" />
          </div>
          <div style={{ fontSize: '28px', fontWeight: '800', color: 'var(--text-main)' }}>
            {data.total_users.toLocaleString()}
          </div>
          <span style={{ fontSize: '11.5px', color: 'var(--text-muted)' }}>Registrados con ORCID</span>
        </div>

        <div className="card" style={{ padding: '18px 20px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
            <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase' }}>
              Administradores
            </span>
            <ShieldCheck size={18} color="#8b5cf6" />
          </div>
          <div style={{ fontSize: '28px', fontWeight: '800', color: '#8b5cf6' }}>
            {data.total_admins.toLocaleString()}
          </div>
          <span style={{ fontSize: '11.5px', color: 'var(--text-muted)' }}>Configurados en .env</span>
        </div>

        <div className="card" style={{ padding: '18px 20px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
            <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase' }}>
              Países
            </span>
            <Globe size={18} color="#10b981" />
          </div>
          <div style={{ fontSize: '28px', fontWeight: '800', color: '#10b981' }}>
            {data.distinct_countries}
          </div>
          <span style={{ fontSize: '11.5px', color: 'var(--text-muted)' }}>Orígenes institucionales</span>
        </div>

        <div className="card" style={{ padding: '18px 20px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
            <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase' }}>
              Inicios de Sesión
            </span>
            <Activity size={18} color="#f59e0b" />
          </div>
          <div style={{ fontSize: '28px', fontWeight: '800', color: '#f59e0b' }}>
            {data.total_logins.toLocaleString()}
          </div>
          <span style={{ fontSize: '11.5px', color: 'var(--text-muted)' }}>Sesiones registradas</span>
        </div>
      </div>

      {/* ── SEARCH & FILTER CONTROLS ── */}
      <div className="card" style={{ padding: '16px 20px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '14px' }}>
        {/* Search input */}
        <div style={{ position: 'relative', flex: '1', minWidth: '260px' }}>
          <Search size={16} style={{ position: 'absolute', left: '12px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
          <input
            type="text"
            placeholder="Buscar por nombre, ORCID, institución o país..."
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            style={{ width: '100%', paddingLeft: '36px', fontSize: '13px' }}
          />
        </div>

        {/* Role pills */}
        <div className="segmented-pills">
          <button
            className={`segmented-pill-btn ${roleFilter === 'ALL' ? 'active' : ''}`}
            onClick={() => setRoleFilter('ALL')}
          >
            Todos ({data.users.length})
          </button>
          <button
            className={`segmented-pill-btn ${roleFilter === 'admin' ? 'active' : ''}`}
            onClick={() => setRoleFilter('admin')}
          >
            Administradores ({data.total_admins})
          </button>
          <button
            className={`segmented-pill-btn ${roleFilter === 'user' ? 'active' : ''}`}
            onClick={() => setRoleFilter('user')}
          >
            Investigadores ({data.total_users - data.total_admins})
          </button>
        </div>
      </div>

      {/* ── USERS TABLE ── */}
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
            <thead>
              <tr style={{ background: 'var(--bg-input)', borderBottom: '1px solid var(--border-color)', textAlign: 'left' }}>
                <th style={{ padding: '12px 18px', fontWeight: '700', color: 'var(--text-muted)' }}>Investigador</th>
                <th style={{ padding: '12px 18px', fontWeight: '700', color: 'var(--text-muted)' }}>Institución</th>
                <th style={{ padding: '12px 18px', fontWeight: '700', color: 'var(--text-muted)' }}>País</th>
                <th style={{ padding: '12px 18px', fontWeight: '700', color: 'var(--text-muted)' }}>Rol</th>
                <th style={{ padding: '12px 18px', fontWeight: '700', color: 'var(--text-muted)', textAlign: 'center' }}>Inicios de Sesión</th>
                <th style={{ padding: '12px 18px', fontWeight: '700', color: 'var(--text-muted)' }}>Primer Acceso</th>
                <th style={{ padding: '12px 18px', fontWeight: '700', color: 'var(--text-muted)' }}>Último Acceso</th>
              </tr>
            </thead>
            <tbody>
              {filteredUsers.length === 0 ? (
                <tr>
                  <td colSpan={7} style={{ padding: '40px 20px', textAlign: 'center', color: 'var(--text-muted)' }}>
                    {loading ? 'Cargando usuarios registrados...' : 'No se encontraron investigadores con los filtros seleccionados.'}
                  </td>
                </tr>
              ) : (
                filteredUsers.map((u) => (
                  <tr key={u.orcid} style={{ borderBottom: '1px solid var(--border-color)', transition: 'background 0.15s ease' }}>
                    {/* Name & ORCID */}
                    <td style={{ padding: '14px 18px' }}>
                      <div style={{ fontWeight: '700', color: 'var(--text-main)', marginBottom: '3px' }}>
                        {u.name}
                      </div>
                      <a
                        href={`https://orcid.org/${u.orcid}`}
                        target="_blank"
                        rel="noreferrer"
                        style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: '4px',
                          color: '#a6ce39',
                          textDecoration: 'none',
                          fontSize: '11.5px',
                          fontWeight: '600'
                        }}
                      >
                        <span style={{
                          background: '#a6ce39',
                          color: '#fff',
                          fontSize: '9.5px',
                          fontWeight: '900',
                          padding: '1px 4px',
                          borderRadius: '3px'
                        }}>
                          iD
                        </span>
                        <span>{u.orcid}</span>
                        <ExternalLink size={11} />
                      </a>
                    </td>

                    {/* Institution */}
                    <td style={{ padding: '14px 18px', color: 'var(--text-main)' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                        <Building2 size={14} color="var(--text-muted)" flexShrink={0} />
                        <span style={{ maxWidth: '220px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={u.institution}>
                          {u.institution}
                        </span>
                      </div>
                    </td>

                    {/* Country */}
                    <td style={{ padding: '14px 18px' }}>
                      <span className="badge" style={{ fontSize: '11px' }}>
                        {u.country}
                      </span>
                    </td>

                    {/* Role */}
                    <td style={{ padding: '14px 18px' }}>
                      {u.is_admin ? (
                        <span style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: '4px',
                          background: 'rgba(99, 102, 241, 0.15)',
                          color: '#6366f1',
                          border: '1px solid rgba(99, 102, 241, 0.4)',
                          padding: '3px 8px',
                          borderRadius: '6px',
                          fontSize: '11px',
                          fontWeight: '700'
                        }}>
                          <ShieldCheck size={12} />
                          Administrador
                        </span>
                      ) : (
                        <span style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: '4px',
                          background: 'rgba(16, 185, 129, 0.12)',
                          color: '#10b981',
                          border: '1px solid rgba(16, 185, 129, 0.35)',
                          padding: '3px 8px',
                          borderRadius: '6px',
                          fontSize: '11px',
                          fontWeight: '600'
                        }}>
                          <UserCheck size={12} />
                          Investigador
                        </span>
                      )}
                    </td>

                    {/* Login Count */}
                    <td style={{ padding: '14px 18px', textAlign: 'center', fontWeight: '700', color: 'var(--text-main)' }}>
                      {u.login_count}
                    </td>

                    {/* First Login */}
                    <td style={{ padding: '14px 18px', fontSize: '11.5px', color: 'var(--text-muted)' }}>
                      {u.first_login}
                    </td>

                    {/* Last Login */}
                    <td style={{ padding: '14px 18px', fontSize: '11.5px', color: 'var(--text-muted)' }}>
                      {u.last_login}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
