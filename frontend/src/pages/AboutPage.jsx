import React, { useState } from 'react';
import { 
  Users, 
  Code, 
  Database, 
  Cpu, 
  Layers, 
  GitBranch, 
  ExternalLink, 
  Globe, 
  Terminal, 
  BookOpen, 
  ShieldCheck, 
  Share2,
  Quote,
  Copy,
  Check,
  Sparkles
} from 'lucide-react';
import { useTranslation } from '../i18n';

export default function AboutPage() {
  const { t } = useTranslation();
  const repoUrl = 'https://github.com/chilti/revistaslatam';
  const [copiedCitation, setCopiedCitation] = useState(false);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px', maxWidth: '1050px' }}>
      {/* Page Title */}
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: '800' }}>{t('about.title')}</h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
          {t('about.subtitle')}
        </p>
      </div>

      {/* ── GITHUB REPOSITORY & OPEN SCIENCE CARD ── */}
      <div className="card" style={{
        border: '1.5px solid var(--accent-primary)',
        background: 'linear-gradient(135deg, rgba(2, 132, 199, 0.06) 0%, rgba(99, 102, 241, 0.04) 100%)',
        boxShadow: '0 8px 25px rgba(2, 132, 199, 0.12)'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '14px', marginBottom: '16px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <div style={{
              width: '42px',
              height: '42px',
              borderRadius: '10px',
              background: 'var(--accent-primary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#ffffff',
              boxShadow: '0 4px 12px rgba(2, 132, 199, 0.35)'
            }}>
              <GitBranch size={22} />
            </div>
            <div>
              <h3 style={{ fontSize: '17px', fontWeight: '800', margin: 0 }}>
                {t('about.repo_title')}
              </h3>
              <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                {t('about.repo_subtitle')}
              </span>
            </div>
          </div>

          <a
            href={repoUrl}
            target="_blank"
            rel="noreferrer"
            className="btn-primary"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '8px',
              padding: '10px 18px',
              fontSize: '13px',
              fontWeight: '700',
              textDecoration: 'none',
              borderRadius: '8px',
              boxShadow: '0 4px 14px rgba(2, 132, 199, 0.3)'
            }}
          >
            <span>{t('about.repo_button')}</span>
            <ExternalLink size={15} />
          </a>
        </div>

        <div style={{ fontSize: '13px', color: 'var(--text-main)', lineHeight: 1.6, marginBottom: '16px' }}>
          <p>
            {t('about.repo_desc')}
          </p>
        </div>

        {/* Clone command block */}
        <div style={{
          background: 'var(--bg-app)',
          padding: '14px 18px',
          borderRadius: '10px',
          fontFamily: 'Fira Code, monospace',
          fontSize: '12.5px',
          border: '1px solid var(--border-color)',
          display: 'flex',
          flexDirection: 'column',
          gap: '6px'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px', color: 'var(--text-muted)', fontSize: '11px', fontWeight: '700', textTransform: 'uppercase' }}>
            <Terminal size={13} /> {t('about.clone_label')}
          </div>
          <div style={{ color: 'var(--accent-primary)', fontWeight: '600' }}>
            git clone https://github.com/chilti/revistaslatam.git
          </div>
        </div>

        {/* Official Deployments row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '12px', marginTop: '16px' }}>
          <div style={{
            background: 'var(--bg-input)',
            padding: '12px 16px',
            borderRadius: '8px',
            border: '1px solid var(--border-color)',
            display: 'flex',
            alignItems: 'center',
            gap: '10px'
          }}>
            <Globe size={18} color="var(--accent-primary)" />
            <div style={{ fontSize: '12px' }}>
              <div style={{ fontWeight: '700', color: 'var(--text-main)' }}>{t('about.main_server')}</div>
              <a 
                href="https://dinamica1.fciencias.unam.mx/revistaslatam/" 
                target="_blank" 
                rel="noreferrer"
                style={{ color: 'var(--accent-primary)', textDecoration: 'none', fontWeight: '600' }}
              >
                dinamica1.fciencias.unam.mx/revistaslatam/
              </a>
            </div>
          </div>

          <div style={{
            background: 'var(--bg-input)',
            padding: '12px 16px',
            borderRadius: '8px',
            border: '1px solid var(--border-color)',
            display: 'flex',
            alignItems: 'center',
            gap: '10px'
          }}>
            <Globe size={18} color="var(--accent-success)" />
            <div style={{ fontSize: '12px' }}>
              <div style={{ fontWeight: '700', color: 'var(--text-main)' }}>{t('about.mirror_server')}</div>
              <a 
                href="https://dinamica10.fciencias.unam.mx/revistaslatam/" 
                target="_blank" 
                rel="noreferrer"
                style={{ color: 'var(--accent-primary)', textDecoration: 'none', fontWeight: '600' }}
              >
                dinamica10.fciencias.unam.mx/revistaslatam/
              </a>
            </div>
          </div>
        </div>
      </div>

      {/* ── CITATION CARD (ZENODO DOI) ── */}
      <div className="card" style={{
        border: '1px solid var(--border-color)',
        background: 'var(--bg-card)',
        display: 'flex',
        flexDirection: 'column',
        gap: '14px'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '10px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div style={{
              width: '36px',
              height: '36px',
              borderRadius: '8px',
              background: 'rgba(2, 132, 199, 0.12)',
              color: 'var(--accent-primary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}>
              <Quote size={18} />
            </div>
            <div>
              <h3 style={{ fontSize: '16px', fontWeight: '700', margin: 0 }}>
                {t('about.citation_title')}
              </h3>
              <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                {t('about.citation_subtitle')}
              </span>
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <a
              href="https://doi.org/10.5281/zenodo.22679773"
              target="_blank"
              rel="noopener noreferrer"
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '6px',
                padding: '6px 12px',
                borderRadius: '6px',
                background: 'rgba(2, 132, 199, 0.1)',
                border: '1px solid rgba(2, 132, 199, 0.3)',
                color: 'var(--accent-primary)',
                textDecoration: 'none',
                fontSize: '12px',
                fontWeight: '700'
              }}
            >
              <span>DOI: 10.5281/zenodo.22679773</span>
              <ExternalLink size={13} />
            </a>

            <button
              onClick={() => {
                navigator.clipboard.writeText(t('about.citation_text'));
                setCopiedCitation(true);
                setTimeout(() => setCopiedCitation(false), 2500);
              }}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '6px',
                padding: '6px 14px',
                borderRadius: '6px',
                background: copiedCitation ? 'var(--accent-success)' : 'var(--bg-input)',
                border: '1px solid var(--border-color)',
                color: copiedCitation ? '#ffffff' : 'var(--text-main)',
                fontSize: '12px',
                fontWeight: '600',
                cursor: 'pointer',
                transition: 'all 0.2s ease'
              }}
            >
              {copiedCitation ? <Check size={14} /> : <Copy size={14} />}
              <span>{copiedCitation ? t('about.citation_copied') : t('about.citation_copy')}</span>
            </button>
          </div>
        </div>

        {/* Citation Box */}
        <div style={{
          background: 'var(--bg-input)',
          padding: '12px 16px',
          borderRadius: '8px',
          border: '1px solid var(--border-color)',
          fontSize: '13px',
          lineHeight: 1.6,
          color: 'var(--text-main)',
          fontStyle: 'italic'
        }}>
          "{t('about.citation_text')}"
        </div>
      </div>

      {/* Research Group */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <Users size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>{t('about.team_title')}</h3>
        </div>
        <div style={{ fontSize: '13.5px', color: 'var(--text-main)', lineHeight: 1.6 }}>
          <p style={{ fontWeight: '700', color: 'var(--accent-primary)', marginBottom: '12px' }}>
            {t('about.lab_name')}
          </p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px', padding: '10px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
              <div>
                <strong>Dr. José Luis Jiménez Andrade</strong> — <span style={{ color: 'var(--text-muted)' }}>{t('about.role_architecture_math')}</span>
                <div style={{ fontSize: '11.5px', color: 'var(--accent-primary)', marginTop: '2px', fontWeight: '500' }}>
                  {t('about.affil_fc_c3')}
                </div>
              </div>
              <a
                href="https://orcid.org/0000-0003-3453-7159"
                target="_blank"
                rel="noopener noreferrer"
                style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: '#a6ce39', background: 'rgba(166, 206, 57, 0.12)', border: '1px solid rgba(166, 206, 57, 0.3)', padding: '3px 8px', borderRadius: '6px', textDecoration: 'none', fontWeight: '600' }}
              >
                <span>🆔 ORCID: 0000-0003-3453-7159</span>
                <ExternalLink size={12} />
              </a>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px', padding: '10px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
              <div>
                <strong>Dr. Humberto Andrés Carrillo Calvet</strong> — <span style={{ color: 'var(--text-muted)' }}>{t('about.role_lead_researcher')}</span>
                <div style={{ fontSize: '11.5px', color: 'var(--accent-primary)', marginTop: '2px', fontWeight: '500' }}>
                  {t('about.affil_fc_c3')}
                </div>
              </div>
              <a
                href="https://orcid.org/0000-0003-3659-6769"
                target="_blank"
                rel="noopener noreferrer"
                style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: '#a6ce39', background: 'rgba(166, 206, 57, 0.12)', border: '1px solid rgba(166, 206, 57, 0.3)', padding: '3px 8px', borderRadius: '6px', textDecoration: 'none', fontWeight: '600' }}
              >
                <span>🆔 ORCID: 0000-0003-3659-6769</span>
                <ExternalLink size={12} />
              </a>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px', padding: '10px 14px', background: 'var(--bg-input)', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
              <div>
                <strong>Dr. Ricardo Arencibia Jorge</strong> — <span style={{ color: 'var(--text-muted)' }}>{t('about.role_scientometrics_specialist')}</span>
                <div style={{ fontSize: '11.5px', color: 'var(--accent-primary)', marginTop: '2px', fontWeight: '500' }}>
                  {t('about.affil_c3')}
                </div>
              </div>
              <a
                href="https://orcid.org/0000-0001-8907-2454"
                target="_blank"
                rel="noopener noreferrer"
                style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: '#a6ce39', background: 'rgba(166, 206, 57, 0.12)', border: '1px solid rgba(166, 206, 57, 0.3)', padding: '3px 8px', borderRadius: '6px', textDecoration: 'none', fontWeight: '600' }}
              >
                <span>🆔 ORCID: 0000-0001-8907-2454</span>
                <ExternalLink size={12} />
              </a>
            </div>
          </div>
        </div>
      </div>

      {/* Programming */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <Code size={18} color="var(--accent-success)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>{t('about.dev_title')}</h3>
        </div>
        <ul style={{ marginLeft: '20px', fontSize: '13.5px', display: 'flex', flexDirection: 'column', gap: '6px' }}>
          <li><strong>{t('about.dev_lead_name')}</strong> — {t('about.dev_lead_role')}</li>
          <li><strong>{t('about.dev_ai_name')}</strong> — {t('about.dev_ai_role')}</li>
        </ul>
      </div>

      {/* Architecture */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '14px' }}>
          <Cpu size={18} color="#f59e0b" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>{t('about.architecture_title')}</h3>
        </div>
        <div style={{
          background: 'var(--bg-input)',
          padding: '16px 20px',
          borderRadius: '10px',
          fontFamily: 'Fira Code, monospace',
          fontSize: '12px',
          lineHeight: 1.6,
          color: 'var(--text-main)',
          border: '1px solid var(--border-color)',
          overflowX: 'auto',
          display: 'flex',
          flexDirection: 'column',
          gap: '6px'
        }}>
          <div><strong>{t('about.arch_1_label')}</strong> {t('about.arch_1_desc')}</div>
          <div><strong>{t('about.arch_2_label')}</strong> {t('about.arch_2_desc')}</div>
          <div><strong>{t('about.arch_3_label')}</strong> {t('about.arch_3_desc')}</div>
          <div><strong>{t('about.arch_4_label')}</strong> {t('about.arch_4_desc')}</div>
          <div><strong>{t('about.arch_5_label')}</strong> {t('about.arch_5_desc')}</div>
        </div>
      </div>

      {/* Methodology Description */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <BookOpen size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            {t('about.methods_title')}
          </h3>
        </div>
        <div style={{ fontSize: '13px', lineHeight: 1.65, color: 'var(--text-muted)', display: 'flex', flexDirection: 'column', gap: '12px' }}>
          <p>
            {t('about.methods_p1')}
          </p>
          <p>
            {t('about.methods_p2')}
          </p>
        </div>
      </div>

      {/* Acknowledgments */}
      <div className="card" style={{
        background: 'linear-gradient(135deg, rgba(245, 158, 11, 0.04) 0%, rgba(2, 132, 199, 0.04) 100%)',
        border: '1px solid rgba(245, 158, 11, 0.2)'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' }}>
          <Sparkles size={18} color="#f59e0b" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            {t('about.ack_title')}
          </h3>
        </div>
        <div style={{ fontSize: '13px', lineHeight: 1.65, color: 'var(--text-main)' }}>
          <p>
            {t('about.ack_desc')}
          </p>
        </div>
      </div>
    </div>
  );
}
