import React from 'react';

/**
 * Logotipo oficial vectorial de KnoMap (malla SOM hexagonal con spline neuronal fluido en degradado cian-azul-cobalto).
 */
export default function KnoMapLogo({ size = 16, style = {}, className = '' }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 120 120"
      fill="none"
      width={size}
      height={size}
      className={className}
      style={{ display: 'inline-block', verticalAlign: 'middle', flexShrink: 0, ...style }}
    >
      <defs>
        <linearGradient id="knomap-spline-grad" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#00f0ff" />
          <stop offset="50%" stopColor="#38bdf8" />
          <stop offset="100%" stopColor="#818cf8" />
        </linearGradient>
        <filter id="knomap-glow" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="2.5" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      {/* Hexágono SOM base */}
      <polygon
        points="60,15 99,37.5 99,82.5 60,105 21,82.5 21,37.5"
        stroke="#6366f1"
        strokeWidth="3.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.85"
      />

      {/* Malla interna SOM */}
      <path
        d="M60,15 L60,105 M21,37.5 L99,82.5 M21,82.5 L99,37.5"
        stroke="#38bdf8"
        strokeWidth="2"
        strokeDasharray="3,3"
        opacity="0.55"
      />

      {/* Hexágono interior concéntrico */}
      <polygon
        points="60,37.5 79.5,48.75 79.5,71.25 60,82.5 40.5,71.25 40.5,48.75"
        stroke="#818cf8"
        strokeWidth="2.5"
        opacity="0.75"
      />

      {/* Spline neuronal sinuoso KnoMap */}
      <path
        d="M30,50 C45,25 75,25 90,50 C105,75 15,75 30,100 C45,105 75,105 90,80"
        stroke="url(#knomap-spline-grad)"
        strokeWidth="7"
        strokeLinecap="round"
        fill="none"
        filter="url(#knomap-glow)"
      />

      {/* Nodos resplandecientes */}
      <circle cx="30" cy="50" r="4.5" fill="#a8f5ff" stroke="#00f0ff" strokeWidth="1.5" />
      <circle cx="90" cy="50" r="4.5" fill="#a8f5ff" stroke="#00f0ff" strokeWidth="1.5" />
      <circle cx="60" cy="60" r="5" fill="#ffffff" stroke="#00f0ff" strokeWidth="2" />
    </svg>
  );
}
