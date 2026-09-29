/**
 * frontend/src/components/SuiteBar.jsx
 * Envoltura de la Franja del Ecosistema Científico TlachIA
 */

import React from 'react';
import { TlachiaSuiteBar } from '@tlachia/ecosystem-bar';
import { useAppStore } from '../store';

export function SuiteBar() {
  const language = useAppStore((state) => state.language);

  return (
    <TlachiaSuiteBar
      currentApp="revistaslatam"
      lang={language || 'es'}
    />
  );
}

export default SuiteBar;
