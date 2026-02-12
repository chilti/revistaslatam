-- Índices para optimizar las consultas del data collector
-- Ejecutar con: psql -U postgres -d openalex -f create_indexes.sql

\timing on

-- Índices en works_primary_location (crítico para encontrar works por journal)
CREATE INDEX IF NOT EXISTS idx_wpl_source_id 
ON openalex.works_primary_location(source_id);

CREATE INDEX IF NOT EXISTS idx_wpl_work_id 
ON openalex.works_primary_location(work_id);

-- Índices en works_authorships (para encontrar autores e instituciones)
CREATE INDEX IF NOT EXISTS idx_wa_work_id 
ON openalex.works_authorships(work_id);

CREATE INDEX IF NOT EXISTS idx_wa_institution_id 
ON openalex.works_authorships(institution_id);

-- Índice en institutions (para filtrar por país)
CREATE INDEX IF NOT EXISTS idx_inst_country 
ON openalex.institutions(country_code);

-- Índices en works_concepts (para agregar conceptos)
CREATE INDEX IF NOT EXISTS idx_wc_work_id 
ON openalex.works_concepts(work_id);

-- Índices en works_topics (si existe)
CREATE INDEX IF NOT EXISTS idx_wt_work_id 
ON openalex.works_topics(work_id);

-- Índices en works_open_access (si existe)
CREATE INDEX IF NOT EXISTS idx_woa_work_id 
ON openalex.works_open_access(work_id);

-- Índice en works (para consultas generales)
CREATE INDEX IF NOT EXISTS idx_works_id 
ON openalex.works(id);

CREATE INDEX IF NOT EXISTS idx_works_pub_year 
ON openalex.works(publication_year);

-- Índice en sources
CREATE INDEX IF NOT EXISTS idx_sources_id 
ON openalex.sources(id);

-- Actualizar estadísticas para el optimizador de consultas
ANALYZE openalex.works;
ANALYZE openalex.works_primary_location;
ANALYZE openalex.works_authorships;
ANALYZE openalex.institutions;
ANALYZE openalex.works_concepts;
ANALYZE openalex.sources;

\echo 'Índices creados exitosamente'
