import React, { useEffect } from 'react';
import { useAppStore } from './store';
import Navbar from './components/Navbar';
import Sidebar from './components/Sidebar';
import DossierDrawer from './components/DossierDrawer';
import ErrorBoundary from './components/ErrorBoundary';

import RegionalPage from './pages/RegionalPage';
import CountryPage from './pages/CountryPage';
import JournalPage from './pages/JournalPage';
import SemanticMapsPage from './pages/SemanticMapsPage';
import NetworksPage from './pages/NetworksPage';
import AboutPage from './pages/AboutPage';

export default function App() {
  const { activeSection, theme } = useAppStore();

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
  }, [theme]);

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
    </div>
  );
}

