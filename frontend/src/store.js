import { create } from 'zustand';

// Helper to extract initial state from URL query parameters
const getInitialUrlState = () => {
  if (typeof window === 'undefined') {
    return {
      activeSection: 'regional',
      selectedCountry: 'MX',
      selectedJournalId: 'https://openalex.org/S2737081250',
      selectedJournalName: 'Estudios Demográficos y Urbanos'
    };
  }
  try {
    const params = new URLSearchParams(window.location.search);
    const section = params.get('section') || params.get('tab') || null;
    const country = params.get('country') || params.get('country_code') || null;
    const rawJournal = params.get('journal_id') || params.get('journal') || params.get('id') || null;

    let journalId = 'https://openalex.org/S2737081250';
    let journalName = 'Estudios Demográficos y Urbanos';
    let activeSection = 'regional';
    let selectedCountry = 'MX';

    if (country) {
      selectedCountry = country.toUpperCase();
    }

    if (rawJournal) {
      journalId = rawJournal.startsWith('http') ? rawJournal : `https://openalex.org/${rawJournal.trim()}`;
      journalName = '';
      activeSection = 'journal';
    } else if (country) {
      activeSection = section || 'country';
    } else if (section) {
      activeSection = section;
    }

    return { activeSection, selectedCountry, selectedJournalId: journalId, selectedJournalName: journalName };
  } catch (e) {
    return {
      activeSection: 'regional',
      selectedCountry: 'MX',
      selectedJournalId: 'https://openalex.org/S2737081250',
      selectedJournalName: 'Estudios Demográficos y Urbanos'
    };
  }
};

const initial = getInitialUrlState();

export const syncUrlParams = (activeSection, selectedCountry, selectedJournalId) => {
  if (typeof window === 'undefined') return;
  try {
    const params = new URLSearchParams();
    params.set('section', activeSection);
    if (activeSection === 'country' && selectedCountry) {
      params.set('country', selectedCountry);
    } else if (activeSection === 'journal' && selectedJournalId) {
      const cleanId = selectedJournalId.includes('/') ? selectedJournalId.split('/').pop() : selectedJournalId;
      params.set('journal_id', cleanId);
    }
    const newUrl = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, '', newUrl);
  } catch (e) {
    // Ignore history errors if any
  }
};

export const useAppStore = create((set, get) => ({
  // Navigation
  activeSection: initial.activeSection, // 'regional' | 'country' | 'journal' | 'maps' | 'networks' | 'about'
  setActiveSection: (section) => {
    set({ activeSection: section });
    const { selectedCountry, selectedJournalId } = get();
    syncUrlParams(section, selectedCountry, selectedJournalId);
  },

  // Theme: 'claro' | 'oscuro' | 'navy'
  theme: 'claro',
  setTheme: (theme) => {
    document.documentElement.setAttribute('data-theme', theme);
    set({ theme });
  },

  // Country Selection
  selectedCountry: initial.selectedCountry,
  setSelectedCountry: (country) => {
    set({ selectedCountry: country });
    const { activeSection, selectedJournalId } = get();
    syncUrlParams(activeSection, country, selectedJournalId);
  },

  // Journal Selection
  selectedJournalId: initial.selectedJournalId,
  selectedJournalName: initial.selectedJournalName,
  setSelectedJournal: (id, name = '') => {
    const normalizedId = id && !id.startsWith('http') ? `https://openalex.org/${id.trim()}` : id;
    set({ selectedJournalId: normalizedId, selectedJournalName: name || get().selectedJournalName });
    const { activeSection, selectedCountry } = get();
    syncUrlParams(activeSection, selectedCountry, normalizedId);
  },

  // Study Dossier (Exportable items)
  dossierItems: [],
  isDossierOpen: false,
  setDossierOpen: (open) => set({ isDossierOpen: open }),
  addDossierItem: (item) => {
    const current = get().dossierItems;
    if (!current.some((i) => i.key === item.key)) {
      set({ dossierItems: [...current, item] });
    }
  },
  removeDossierItem: (key) => {
    set({ dossierItems: get().dossierItems.filter((i) => i.key !== key) });
  },
  clearDossier: () => set({ dossierItems: [] }),

  // Background Export Downloads
  exportJobs: typeof window !== 'undefined' ? JSON.parse(localStorage.getItem('rl_export_jobs') || '[]') : [],
  isDownloadsOpen: false,
  setDownloadsOpen: (open) => set({ isDownloadsOpen: open }),
  addExportJob: (job) => {
    const current = get().exportJobs;
    const updated = [job, ...current.filter(j => j.id !== job.id)].slice(0, 30);
    set({ exportJobs: updated });
    try { localStorage.setItem('rl_export_jobs', JSON.stringify(updated)); } catch (e) {}
  },
  updateExportJob: (jobId, updates) => {
    const current = get().exportJobs;
    const updated = current.map(j => j.id === jobId ? { ...j, ...updates } : j);
    set({ exportJobs: updated });
    try { localStorage.setItem('rl_export_jobs', JSON.stringify(updated)); } catch (e) {}
  },
  removeExportJob: (jobId) => {
    const current = get().exportJobs;
    const updated = current.filter(j => j.id !== jobId);
    set({ exportJobs: updated });
    try { localStorage.setItem('rl_export_jobs', JSON.stringify(updated)); } catch (e) {}
  },
  clearCompletedJobs: () => {
    const current = get().exportJobs;
    const updated = current.filter(j => j.status !== 'completed' && j.status !== 'failed');
    set({ exportJobs: updated });
    try { localStorage.setItem('rl_export_jobs', JSON.stringify(updated)); } catch (e) {}
  },

  // ORCID Authentication
  user: typeof window !== 'undefined' && localStorage.getItem('rl_orcid_user')
    ? JSON.parse(localStorage.getItem('rl_orcid_user'))
    : null,
  isLoginModalOpen: false,
  loginModalReason: 'general', // 'general' | 'ai_context' | 'download_articles'
  setLoginModalOpen: (open, reason = 'general') => set({ isLoginModalOpen: open, loginModalReason: reason }),
  setUser: (user) => {
    set({ user });
    if (typeof window !== 'undefined') {
      try {
        if (user) {
          localStorage.setItem('rl_orcid_user', JSON.stringify(user));
        } else {
          localStorage.removeItem('rl_orcid_user');
        }
      } catch (e) {}
    }
  },
  logout: () => {
    set({ user: null });
    if (typeof window !== 'undefined') {
      try { localStorage.removeItem('rl_orcid_user'); } catch (e) {}
    }
  },
  requireAuth: (actionCallback, reason = 'general') => {
    const { user } = get();
    if (user && user.orcid) {
      if (typeof actionCallback === 'function') actionCallback();
      return true;
    } else {
      set({ isLoginModalOpen: true, loginModalReason: reason });
      return false;
    }
  }
}));



