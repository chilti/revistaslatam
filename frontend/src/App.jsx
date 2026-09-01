import React, { useEffect } from 'react';
import { useAppStore } from './store';
import api from './api';
import Navbar from './components/Navbar';
import Sidebar from './components/Sidebar';
import DossierDrawer from './components/DossierDrawer';
import DownloadsDrawer from './components/DownloadsDrawer';
import OrcidLoginModal from './components/OrcidLoginModal';
import ErrorBoundary from './components/ErrorBoundary';

import RegionalPage from './pages/RegionalPage';
import CountryPage from './pages/CountryPage';
import JournalPage from './pages/JournalPage';
import SemanticMapsPage from './pages/SemanticMapsPage';
import NetworksPage from './pages/NetworksPage';
import AboutPage from './pages/AboutPage';
import AdminPage from './pages/AdminPage';

export default function App() {
  const {
    activeSection,
    theme,
    setActiveSection,
    setSelectedCountry,
    setSelectedJournal,
    selectedCountry,
    selectedJournalId,
    setUser
  } = useAppStore();

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
  }, [theme]);

  // Handle ORCID OAuth callback (?code=...) on page load
  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      const code = params.get('code');
      if (code) {
        api.post('/auth/orcid/token', { code })
          .then(res => {
            if (res.data && res.data.orcid) {
              setUser({
                orcid: res.data.orcid,
                name: res.data.name,
                institution: res.data.institution || '',
                country: res.data.country || '',
                role: res.data.role || 'user',
                is_admin: !!res.data.is_admin,
                access_token: res.data.access_token,
                scope: res.data.scope
              });
            }
          })
          .catch(err => {
            console.error('Error exchanging ORCID code:', err);
          })
          .finally(() => {
            // Remove code from query string
            params.delete('code');
            const cleanUrl = params.toString() ? `${window.location.pathname}?${params.toString()}` : window.location.pathname;
            window.history.replaceState({}, '', cleanUrl);
          });
      }
    } catch (e) {
      console.error('ORCID callback error:', e);
    }
  }, []);

  // Listen for browser back / forward events (popstate)
  useEffect(() => {
    const handlePopState = () => {
      try {
        const params = new URLSearchParams(window.location.search);
        const section = params.get('section') || params.get('tab');
        const country = params.get('country') || params.get('country_code');
        const rawJournal = params.get('journal_id') || params.get('journal') || params.get('id');

        if (country) {
          setSelectedCountry(country.toUpperCase());
        }
        if (rawJournal) {
          const jId = rawJournal.startsWith('http') ? rawJournal : `https://openalex.org/${rawJournal.trim()}`;
          setSelectedJournal(jId);
        }
        if (section) {
          setActiveSection(section);
        }
      } catch (e) {
        // ignore
      }
    };

    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  const renderSection = () => {
    switch (activeSection) {
      case 'regional':
        return <RegionalPage />;
      case 'country':
        return <CountryPage />;
      case 'journal':
        return <JournalPage />;
      case 'maps':
        return <SemanticMapsPage />;
      case 'networks':
        return <NetworksPage />;
      case 'about':
        return <AboutPage />;
      case 'admin':
        return <AdminPage />;
      default:
        return <RegionalPage />;
    }
  };

  return (
    <div className="app-container">
      <Sidebar />
      <div className="main-content">
        <Navbar />
        <main className="page-body">
          <ErrorBoundary>
            {renderSection()}
          </ErrorBoundary>
        </main>
      </div>
      <DossierDrawer />
      <DownloadsDrawer />
      <OrcidLoginModal />
    </div>
  );
}

