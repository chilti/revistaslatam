-- 1. Crear la tabla de agregación institucional
-- Optimizada con SummingMergeTree para colapsar métricas bibliométricas
CREATE TABLE IF NOT EXISTS summing_subfield_inst_metrics
(
    `subfield` String,
    `year` UInt16,
    `institution_id` String,
    `topic` String,
    `source_id` String,
    `doc_count` UInt64,
    `fwci_sum` Float64,
    `percentile_sum` Float64,
    -- Excelencia
    `top_1_sum` UInt64,
    `top_10_sum` UInt64,
    `top_25_sum` UInt64,
    -- Impacto y Colaboración
    `citations_sum` UInt64,
    `intl_collab_count` UInt64,
    -- Alineación y Financiamiento
    `sdg_count` UInt64,
    `award_count` UInt64,
    -- Tipología y Acceso
    `review_count` UInt64,
    `gold_count` UInt64,
    `diamond_count` UInt64,
    `green_count` UInt64,
    `hybrid_count` UInt64,
    `bronze_count` UInt64,
    `closed_count` UInt64,
    -- Idiomas
    `lang_en` UInt64,
    `lang_es` UInt64,
    `lang_pt` UInt64
)
ENGINE = SummingMergeTree
ORDER BY (subfield, year, institution_id, topic, source_id)
SETTINGS index_granularity = 8192;

-- 2. Población inicial de la tabla
-- Nota: Usamos arrayJoin para 'explotar' las instituciones.
-- Cada institución en un artículo recibe crédito completo (Full Counting).
INSERT INTO summing_subfield_inst_metrics
SELECT 
    subfield,
    publication_year as year,
    arrayJoin(institution_ids) as institution_id,
    topic,
    source_id,
    count() as doc_count,
    sum(fwci) as fwci_sum,
    sum(percentile) as percentile_sum,
    -- Excelencia
    sum(toUInt64(is_top_1)) as top_1_sum,
    sum(toUInt64(is_top_10)) as top_10_sum,
    sum(toUInt64(percentile >= 0.75)) as top_25_sum,
    -- Impacto y Colaboración
    sum(cited_by_count) as citations_sum,
    sum(if(length(all_country_codes) > 1, 1, 0)) as int_collab_count,
    -- Alineación y Financiamiento
    sum(if(length(sdg_ids) > 0, 1, 0)) as sdg_count,
    sum(if(length(awards) > 0, 1, 0)) as award_count,
    -- Tipología y Acceso
    sum(toUInt64(type = 'review')) as review_count,
    sum(toUInt64(oa_status = 'gold')) as gold_count,
    sum(toUInt64(oa_status = 'diamond')) as diamond_count,
    sum(toUInt64(oa_status = 'green')) as green_count,
    sum(toUInt64(oa_status = 'hybrid')) as hybrid_count,
    sum(toUInt64(oa_status = 'bronze')) as bronze_count,
    sum(toUInt64(oa_status = 'closed')) as closed_count,
    -- Idiomas
    sum(toUInt64(language = 'en')) as lang_en,
    sum(toUInt64(language = 'es')) as lang_es,
    sum(toUInt64(language = 'pt')) as lang_pt
FROM works
WHERE subfield != '' 
  AND publication_year >= 1900
  AND NOT empty(institution_ids)
GROUP BY subfield, year, institution_id, topic, source_id;
