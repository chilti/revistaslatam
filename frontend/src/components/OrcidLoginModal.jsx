import React, { useState } from 'react';
import { useAppStore } from '../store';
import api from '../api';
import { X, Lock, Sparkles, Download, Bot, ExternalLink, Loader2, CheckCircle } from 'lucide-react';
import { useTranslation } from '../i18n';

export default function OrcidLoginModal() {
  const { isLoginModalOpen, setLoginModalOpen, loginModalReason } = useAppStore();
  const { t } = useTranslation();
  const [loading, setLoading] = useState(false);

  if (!isLoginModalOpen) return null;

  const handleConnectOrcid = async () => {
    setLoading(true);
    try {
      const res = await api.get('/auth/orcid/url');
      if (res.data && res.data.auth_url) {
        window.location.href = res.data.auth_url;
      } else {
        alert(t('orcid.error_url'));
        setLoading(false);
      }
    } catch (err) {
      console.error('Error initiating ORCID OAuth:', err);
      alert(t('orcid.error_connect'));
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(0, 0, 0, 0.7)',
        backdropFilter: 'blur(6px)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 100,
        padding: '16px'
      }}
      onClick={() => setLoginModalOpen(false)}
    >
      <div
        style={{
          background: 'var(--bg-card)',
          border: '1px solid var(--border-color)',
          borderRadius: '16px',
          padding: '32px',
          maxWidth: '500px',
          width: '100%',
          boxShadow: '0 20px 40px rgba(0, 0, 0, 0.4)',
          position: 'relative',
          display: 'flex',
          flexDirection: 'column',
          gap: '20px'
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Close Button */}
        <button
          onClick={() => setLoginModalOpen(false)}
          style={{
            position: 'absolute',
            top: '16px',
            right: '16px',
            background: 'none',
            border: 'none',
            color: 'var(--text-muted)',
            cursor: 'pointer',
            padding: '4px'
          }}
        >
          <X size={20} />
        </button>

        {/* Header with ORCID Icon */}
        <div style={{ textAlign: 'center' }}>
          <div
            style={{
              width: '60px',
              height: '60px',
              borderRadius: '50%',
              backgroundColor: 'rgba(166, 206, 57, 0.15)',
              border: '2px solid #a6ce39',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              margin: '0 auto 16px',
              color: '#a6ce39',
              fontWeight: '800',
              fontSize: '24px',
              boxShadow: '0 0 20px rgba(166, 206, 57, 0.25)'
            }}
          >
            iD
          </div>

          <h2 style={{ fontSize: '20px', fontWeight: '800', color: 'var(--text-main)', marginBottom: '8px' }}>
            {t('orcid.modal_title')}
          </h2>

          <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', lineHeight: '1.5' }}>
            {loginModalReason === 'ai_context' ? (
              <>
                {t('orcid.reason_ai')}
              </>
            ) : loginModalReason === 'download_articles' ? (
              <>
                {t('orcid.reason_download')}
              </>
            ) : (
              <>
                {t('orcid.reason_general')}
              </>
            )}
          </p>
        </div>

        {/* Benefits list */}
        <div
          style={{
            background: 'var(--bg-input)',
            borderRadius: '10px',
            padding: '14px 16px',
            border: '1px solid var(--border-color)',
            fontSize: '12.5px',
            display: 'flex',
            flexDirection: 'column',
            gap: '8px'
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-main)' }}>
            <Bot size={15} color="var(--accent-primary)" />
            <span>{t('orcid.benefit_ai')}</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-main)' }}>
            <Download size={15} color="#10b981" />
            <span>{t('orcid.benefit_download')}</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-main)' }}>
            <CheckCircle size={15} color="#a6ce39" />
            <span>{t('orcid.benefit_secure')}</span>
          </div>
        </div>

        {/* Connect Button */}
        <button
          onClick={handleConnectOrcid}
          disabled={loading}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '10px',
            padding: '12px 20px',
            borderRadius: '10px',
            backgroundColor: '#a6ce39',
            color: '#ffffff',
            border: 'none',
            fontSize: '14px',
            fontWeight: '700',
            cursor: loading ? 'not-allowed' : 'pointer',
            boxShadow: '0 4px 14px rgba(166, 206, 57, 0.4)',
            transition: 'all 0.2s ease',
            opacity: loading ? 0.7 : 1
          }}
        >
          {loading ? (
            <>
              <Loader2 size={18} className="animate-spin" />
              <span>{t('orcid.connecting')}</span>
            </>
          ) : (
            <>
              <span style={{ fontWeight: '900', fontSize: '16px' }}>iD</span>
              <span>{t('orcid.connect_btn')}</span>
              <ExternalLink size={15} />
            </>
          )}
        </button>

        <p style={{ textAlign: 'center', fontSize: '11px', color: 'var(--text-dim)' }}>
          {t('orcid.legal_note')}
        </p>
      </div>
    </div>
  );
}
