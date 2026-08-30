import { create } from 'zustand';

export const useAppStore = create((set, get) => ({
  // Navigation
  activeSection: 'regional', // 'regional' | 'country' | 'journal' | 'maps' | 'networks' | 'about'
  setActiveSection: (section) => set({ activeSection: section }),

  // Theme: 'claro' | 'oscuro' | 'navy'
  theme: 'claro',
  setTheme: (theme) => {
    document.documentElement.setAttribute('data-theme', theme);
    set({ theme });
  },

  // Country Selection
  selectedCountry: 'MX',
  setSelectedCountry: (country) => set({ selectedCountry: country }),

  // Journal Selection
  selectedJournalId: 'https://openalex.org/S2737637841',
  selectedJournalName: 'Estudios Demográficos y Urbanos',
  setSelectedJournal: (id, name) => set({ selectedJournalId: id, selectedJournalName: name }),

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
}));
