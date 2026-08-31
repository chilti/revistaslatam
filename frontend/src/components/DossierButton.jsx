import React from 'react';
import { PlusCircle, Check } from 'lucide-react';
import { useAppStore } from '../store';

/**
 * Botón reutilizable "Guardar en Dossier".
 * Muestra feedback visual de confirmación durante 2 s.
 *
 * Props:
 *   item  – { key, title, context, category, data }
 *   label – texto del botón (por defecto "Guardar")
 *   compact – si true, solo muestra el ícono
 */
export default function DossierButton({ item, label = 'Guardar', compact = false }) {
  const { addDossierItem, dossierItems } = useAppStore();
  const [saved, setSaved] = React.useState(false);

  const alreadySaved = dossierItems.some(d => d.key === item?.key);

  const handleClick = () => {
    if (!item || alreadySaved || saved) return;
    addDossierItem(item);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  const isActive = saved || alreadySaved;

  return (
    <button
      onClick={handleClick}
      title={isActive ? 'Ya en el Dossier' : `Guardar en Dossier: ${item?.title || ''}`}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '4px',
        fontSize: '11.5px',
        fontWeight: '600',
        padding: compact ? '5px 7px' : '5px 10px',
        borderRadius: '8px',
        border: isActive
          ? '1px solid var(--accent-success)'
          : '1px solid var(--border-color)',
        background: isActive
          ? 'rgba(16, 185, 129, 0.12)'
          : 'var(--bg-input)',
        color: isActive ? 'var(--accent-success)' : 'var(--text-muted)',
        cursor: isActive ? 'default' : 'pointer',
        transition: 'all 0.2s ease',
        whiteSpace: 'nowrap'
      }}
    >
      {isActive
        ? <Check size={12} />
        : <PlusCircle size={12} />}
      {!compact && (isActive ? 'Guardado' : label)}
    </button>
  );
}
