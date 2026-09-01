import React, { useEffect } from 'react';
import { useAppStore } from './store';
import Navbar from './components/Navbar';
import Sidebar from './components/Sidebar';
import DossierDrawer from './components/DossierDrawer';
import DownloadsDrawer from './components/DownloadsDrawer';
import ErrorBoundary from './components/ErrorBoundary';

import RegionalPage from './pages/RegionalPage';
import CountryPage from './pages/CountryPage';
import JournalPage from './pages/JournalPage';
import SemanticMapsPage from './pages/SemanticMapsPage';
import NetworksPage from './pages/NetworksPage';
import AboutPage from './pages/AboutPage';

export default function App() {
  const { activeSection, theme, setActiveSection, setSelectedCountry, setSelectedJournal, selectedCountry, selectedJournalId } = useAppStore();

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
  }, [theme]);

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
    </div>
  );
}

