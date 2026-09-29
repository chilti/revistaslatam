/**
 * frontend/src/components/SuiteBar/SuiteBar.jsx
 * Envoltura de la Franja del Ecosistema Científico TlachIA
 */

import React from 'react';
import { TlachiaSuiteBar } from './TlachiaSuiteBar.jsx';
import { useAppStore } from '../../store';

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
