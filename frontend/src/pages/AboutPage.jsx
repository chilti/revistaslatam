import React from 'react';
import { 
  Users, 
  Code, 
  Database, 
  Cpu, 
  Layers, 
  GitBranch, 
  ExternalLink, 
  Globe, 
  Terminal, 
  BookOpen, 
  ShieldCheck, 
  Share2 
} from 'lucide-react';

export default function AboutPage() {
  const repoUrl = 'https://github.com/chilti/revistaslatam';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px', maxWidth: '1050px' }}>
      {/* Page Title */}
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: '800' }}>Acerca de Revistas LATAM</h2>
        <p style={{ fontSize: '13.5px', color: 'var(--text-muted)', marginTop: '2px' }}>
          Créditos, grupo de investigación, repositorio oficial de código abierto y arquitectura técnica.
        </p>
      </div>

      {/* ── GITHUB REPOSITORY & OPEN SCIENCE CARD ── */}
      <div className="card" style={{
        border: '1.5px solid var(--accent-primary)',
        background: 'linear-gradient(135deg, rgba(2, 132, 199, 0.06) 0%, rgba(99, 102, 241, 0.04) 100%)',
        boxShadow: '0 8px 25px rgba(2, 132, 199, 0.12)'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '14px', marginBottom: '16px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <div style={{
              width: '42px',
              height: '42px',
              borderRadius: '10px',
              background: 'var(--accent-primary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#ffffff',
              boxShadow: '0 4px 12px rgba(2, 132, 199, 0.35)'
            }}>
              <GitBranch size={22} />
            </div>
            <div>
              <h3 style={{ fontSize: '17px', fontWeight: '800', margin: 0 }}>
                Repositorio Oficial en GitHub
              </h3>
              <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                Código abierto, pipelines ETL reproducibles y documentación técnica
              </span>
            </div>
          </div>

          <a
            href={repoUrl}
            target="_blank"
            rel="noreferrer"
            className="btn-primary"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '8px',
              padding: '10px 18px',
              fontSize: '13px',
              fontWeight: '700',
              textDecoration: 'none',
              borderRadius: '8px',
              boxShadow: '0 4px 14px rgba(2, 132, 199, 0.3)'
            }}
          >
            <span>Ver Repositorio en GitHub</span>
            <ExternalLink size={15} />
          </a>
        </div>

        <div style={{ fontSize: '13px', color: 'var(--text-main)', lineHeight: 1.6, marginBottom: '16px' }}>
          <p>
            El proyecto <strong>Revistas LATAM</strong> es un desarrollo de ciencia abierta enfocado en la evaluación cienciométrica de las revistas académicas de América Latina e Iberoamérica. Todos los scripts de recolección, pipelines de cálculo, modelos de reducción topológica (UMAP) y el código de la plataforma web están disponibles públicamente:
          </p>
        </div>

        {/* Clone command block */}
        <div style={{
          background: 'var(--bg-app)',
          padding: '14px 18px',
          borderRadius: '10px',
          fontFamily: 'Fira Code, monospace',
          fontSize: '12.5px',
          border: '1px solid var(--border-color)',
          display: 'flex',
          flexDirection: 'column',
          gap: '6px'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px', color: 'var(--text-muted)', fontSize: '11px', fontWeight: '700', textTransform: 'uppercase' }}>
            <Terminal size={13} /> Clonación del Repositorio:
          </div>
          <div style={{ color: 'var(--accent-primary)', fontWeight: '600' }}>
            git clone https://github.com/chilti/revistaslatam.git
          </div>
        </div>

        {/* Official Deployments row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '12px', marginTop: '16px' }}>
          <div style={{
            background: 'var(--bg-input)',
            padding: '12px 16px',
            borderRadius: '8px',
            border: '1px solid var(--border-color)',
            display: 'flex',
            alignItems: 'center',
            gap: '10px'
          }}>
            <Globe size={18} color="var(--accent-primary)" />
            <div style={{ fontSize: '12px' }}>
              <div style={{ fontWeight: '700', color: 'var(--text-main)' }}>Servidor Principal:</div>
              <a 
                href="https://dinamica1.fciencias.unam.mx/revistaslatam/" 
                target="_blank" 
                rel="noreferrer"
                style={{ color: 'var(--accent-primary)', textDecoration: 'none', fontWeight: '600' }}
              >
                dinamica1.fciencias.unam.mx/revistaslatam/
              </a>
            </div>
          </div>

          <div style={{
            background: 'var(--bg-input)',
            padding: '12px 16px',
            borderRadius: '8px',
            border: '1px solid var(--border-color)',
            display: 'flex',
            alignItems: 'center',
            gap: '10px'
          }}>
            <Globe size={18} color="var(--accent-success)" />
            <div style={{ fontSize: '12px' }}>
              <div style={{ fontWeight: '700', color: 'var(--text-main)' }}>Servidor Espejo:</div>
              <a 
                href="https://dinamica10.fciencias.unam.mx/revistaslatam/" 
                target="_blank" 
                rel="noreferrer"
                style={{ color: 'var(--accent-primary)', textDecoration: 'none', fontWeight: '600' }}
              >
                dinamica10.fciencias.unam.mx/revistaslatam/
              </a>
            </div>
          </div>
        </div>
      </div>

      {/* Research Group */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <Users size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>Grupo de Trabajo</h3>
        </div>
        <div style={{ fontSize: '13.5px', color: 'var(--text-main)', lineHeight: 1.6 }}>
          <p style={{ fontWeight: '700', color: 'var(--accent-primary)', marginBottom: '8px' }}>
            Complejidad, Cienciometría y Ciencia de la Ciencia — Facultad de Ciencias, UNAM
          </p>
          <ul style={{ marginLeft: '20px', display: 'flex', flexDirection: 'column', gap: '6px' }}>
            <li><strong>Dr. Humberto Andrés Carrillo Calvet</strong> — Investigador Titular</li>
            <li><strong>Dr. Ricardo Arencibia Jorge</strong> — Especialista Cienciométrico</li>
            <li><strong>Dr. José Luis Jiménez Andrade</strong> — Arquitectura y Modelado Matemático</li>
          </ul>
        </div>
      </div>

      {/* Programming */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <Code size={18} color="var(--accent-success)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>Desarrollo, ETL e Ingeniería de Software</h3>
        </div>
        <ul style={{ marginLeft: '20px', fontSize: '13.5px', display: 'flex', flexDirection: 'column', gap: '6px' }}>
          <li><strong>Dr. José Luis Jiménez Andrade</strong> — Arquitectura del Sistema, Pipelines ETL y Modelado Topológico</li>
          <li><strong>Antigravity con Gemini 3 Pro y Claude Sonnet 4.5</strong> — Pair Programming, Optimización DuckDB, Canvas 2D y WebGL GPU</li>
        </ul>
      </div>

      {/* Architecture */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '14px' }}>
          <Cpu size={18} color="#f59e0b" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>Arquitectura del Sistema Desacoplado (2.0)</h3>
        </div>
        <div style={{
          background: 'var(--bg-input)',
          padding: '16px 20px',
          borderRadius: '10px',
          fontFamily: 'Fira Code, monospace',
          fontSize: '12px',
          lineHeight: 1.6,
          color: 'var(--text-main)',
          border: '1px solid var(--border-color)',
          overflowX: 'auto',
          display: 'flex',
          flexDirection: 'column',
          gap: '6px'
        }}>
          <div><strong>1. Capa de Datos:</strong> OpenAlex Snapshot Global + PostgreSQL + ClickHouse local (569M trabajos, 337M autores).</div>
          <div><strong>2. Motor Analítico OLAP:</strong> DuckDB embebido con almacenamiento columnar Parquet (3.63M trabajos LATAM / 7,494 revistas).</div>
          <div><strong>3. Backend REST:</strong> FastAPI asíncrono con compresión Gzip, endpoints analíticos &lt; 15 ms y servicio de archivos estáticos.</div>
          <div><strong>4. Frontend SPA:</strong> React 18 + Vite + Plotly.js + Canvas 2D Heatmaps + Shaders WebGL GPU a 60 FPS.</div>
          <div><strong>5. Inteligencia Artificial:</strong> Exportador estructurado de Dossier de Estudio para integración fluida con ChatGPT / LLMs.</div>
        </div>
      </div>

      {/* Methodology Description */}
      <div className="card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <BookOpen size={18} color="var(--accent-primary)" />
          <h3 style={{ fontSize: '16px', fontWeight: '700' }}>
            Metodología Cienciométrica y Soberanía Editorial
          </h3>
        </div>
        <div style={{ fontSize: '13px', lineHeight: 1.65, color: 'var(--text-muted)', display: 'flex', flexDirection: 'column', gap: '12px' }}>
          <p>
            El sistema implementa un pipeline de inteligencia científica para cartografiar el ecosistema de revistas académicas en América Latina. A través de modelos neuronales de lenguaje y reducción topológica no lineal (UMAP), se proyecta la variedad semántica pura del conocimiento regional sin sesgos institucionales ni geopolíticos.
          </p>
          <p>
            Las métricas de citación e impacto normalizado por campo (<strong>FWCI</strong>), la clasificación en percentiles mundiales (<strong>Top 1%</strong> y <strong>Top 10%</strong>) y el seguimiento exhaustivo de las vías de <strong>Acceso Abierto Diamante y Dorado</strong> permiten una evaluación integral, justa y transparente de las publicaciones académicas iberoamericanas.
          </p>
        </div>
      </div>
    </div>
  );
}
