import React from 'react';
import Plot from 'react-plotly.js';
import { useAppStore } from '../store';

export default function PlotlyChart({ data = [], layout = {}, config = {}, style = {}, ...rest }) {
  const { theme } = useAppStore();

  const isDark = theme === 'oscuro' || theme === 'navy';
  const paperBg = theme === 'oscuro' ? '#0f172a' : theme === 'navy' ? '#071731' : '#ffffff';
  const plotBg = paperBg;
  const textColor = isDark ? '#f8fafc' : '#0f172a';
  const gridColor = isDark ? 'rgba(255, 255, 255, 0.08)' : '#f1f5f9';

  const defaultLayout = {
    autosize: true,
    paper_bgcolor: paperBg,
    plot_bgcolor: plotBg,
    font: {
      family: 'Plus Jakarta Sans, sans-serif',
      color: textColor,
      size: 12
    },
    margin: { l: 40, r: 20, t: 40, b: 40 },
    xaxis: {
      gridcolor: gridColor,
      zerolinecolor: gridColor,
      color: textColor
    },
    yaxis: {
      gridcolor: gridColor,
      zerolinecolor: gridColor,
      color: textColor
    },
    ...layout
  };

  const defaultConfig = {
    responsive: true,
    displayModeBar: true,
    displaylogo: false,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    ...config
  };

  return (
    <div style={{ width: '100%', height: '100%', minHeight: '350px', ...style }}>
      <Plot
        data={data}
        layout={defaultLayout}
        config={defaultConfig}
        useResizeHandler={true}
        style={{ width: '100%', height: '100%' }}
        {...rest}
      />
    </div>
  );
}
