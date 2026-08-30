import React from 'react';
import { Users, Code, Database, Cpu, Layers } from 'lucide-react';

export default function AboutPage() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '28px', maxWidth: '1000px' }}>
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: '800' }}>Acerca de...</h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
          Créditos, grupo de investigación y arquitectura técnica del sistema.
        </p>
      </div>

      {/* Research Group */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <Users size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>Grupo de Trabajo</h3>
        </div>
        <div style={{ fontSize: '13.5px', color: 'var(--text-main)', lineHeight: 1.6 }}>
          <p style={{ fontWeight: '700', color: 'var(--accent-primary)', marginBottom: '8px' }}>
            Complejidad, Cienciometría y Ciencia de la Ciencia
          </p>
          <ul style={{ marginLeft: '20px', display: 'flex', flexDirection: 'column', gap: '4px' }}>
            <li><strong>Dr. Humberto Andrés Carrillo Calvet</strong></li>
            <li><strong>Dr. Ricardo Arencibia Jorge</strong></li>
            <li><strong>Dr. José Luis Jiménez Andrade</strong></li>
          </ul>
        </div>
      </div>

      {/* Programming */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <Code size={18} color="var(--accent-success)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>Desarrollo y Programación</h3>
        </div>
        <ul style={{ marginLeft: '20px', fontSize: '13.5px', display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <li><strong>Dr. José Luis Jiménez Andrade</strong> — Arquitectura, ETL y Modelado Topológico</li>
          <li><strong>Antigravity con Gemini 3 Pro y Claude Sonnet 4.5</strong> — Pair Programming y Optimización DuckDB/WebGL</li>
        </ul>
      </div>

      {/* Architecture */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '14px' }}>
          <Cpu size={18} color="#f59e0b" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>Arquitectura del Sistema Desacoplado</h3>
        </div>
        <div style={{
          background: 'var(--bg-input)',
          padding: '16px',
          borderRadius: '8px',
          fontFamily: 'Fira Code, monospace',
          fontSize: '12px',
          lineHeight: 1.5,
          color: 'var(--text-main)',
          border: '1px solid var(--border-color)',
          overflowX: 'auto'
        }}>
          <div>1. Capa de Datos: OpenAlex Snapshot + PostgreSQL + ClickHouse local (569M trabajos).</div>
          <div>2. Motor OLAP: DuckDB embebido con tablas Parquet columnares (3.63M trabajos LATAM / 7,494 revistas).</div>
          <div>3. Backend: FastAPI asíncrono con compresión Gzip, CORS y respuestas &lt; 20 ms.</div>
          <div>4. Frontend: React 18 + Vite + Plotly.js + Shaders WebGL GPU a 60 FPS.</div>
        </div>
      </div>

      {/* Methodology Description */}
      <div className="card">
        <h3 style={{ fontSize: '16px', fontWeight: '700', marginBottom: '12px' }}>
          Descripción Detallada de Componentes
        </h3>
        <div style={{ fontSize: '13px', lineHeight: 1.65, color: 'var(--text-muted)', display: 'flex', flexDirection: 'column', gap: '12px' }}>
          <p>
            El sistema implementa un pipeline de inteligencia científica para cartografiar el ecosistema de revistas académicas en América Latina. A través de modelos neuronales de lenguaje y reducción topológica no lineal (UMAP), se proyecta la variedad semántica pura del conocimiento regional sin sesgos institucionales ni geopolíticos.
          </p>
          <p>
            Las métricas de citación e impacto normalizado por campo (**FWCI**), la clasificación en percentiles mundiales (**Top 1%** y **Top 10%**) y el seguimiento exhaustivo de las vías de **Acceso Abierto Diamante y Dorado** permiten una evaluación integral y transparente de las publicaciones académicas iberoamericanas.
          </p>
        </div>
      </div>
    </div>
  );
}
