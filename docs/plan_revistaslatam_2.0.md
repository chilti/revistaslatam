# Plan de Implementacion: RevistasLATAM 2.0
## Plataforma Integral de Cienciometria para America Latina

**Documento:** docs/plan_revistaslatam_2.0.md
**Proyecto:** C:\Users\jlja\Documents\Proyectos\revistaslatam
**Fecha:** 2026-08-22
**Version:** 1.0

---

## Vision General

RevistasLATAM 2.0 pasa de un dashboard bibliometrico a una plataforma cienciometrica integral para las revistas latinoamericanas. La estrategia reutiliza codigo y metodologias del ecosistema de proyectos existente, evitando duplicacion y maximizando coherencia metodologica.

### Ecosistema de Codigo Reutilizable

| Proyecto Fuente | Componentes que se Jalan |
|---|---|
| revistaslatam/src/performance_metrics.py | MetricsAccumulator, FWCI, OA breakdown |
| revistaslatam/pipeline_revistaslatam/calculate_umap.py | UMAP 2D para paises y revistas |
| PLmetrix-Lab-2.0/backend/app/ | lotka.py, bradford.py, growth.py, price.py, zipf.py |
| newLabSOM/engine/semantic_engine.py | UMAP GPU-aware, TF-IDF, stopwords ES+EN |
| newLabSOM/engine/bibliometrics_parser.py | Parsers WoS/Scopus/OpenAlex |
| Topics/fronts/structural/leiden_detector.py | Leiden + Salton >= 0.1 |
| Topics/fronts/semantic/embeddings.py | SPECTER2 batch inference, cache Parquet |
| Topics/src/viz_bibliometrics.py | Plotly: mapas, Sankey, redes |
| sos-mcp-services/services/revistaslatam/ | MCP get_journal_impact_profile, compare_journals_benchmarking |
| sos-mcp-services/shared/ | clickhouse.py, neo4j_client.py, qdrant_client.py |
| revistaslatam/nomic-embed/ | Modelo nomic-embed-text-v1.5 ya descargado |

---

## Prioridades de Implementacion

    FASE 1 --- Enriquecimiento Bibliometrico-Cienciometrico   [PRIORIDAD MAXIMA]
    FASE 2 --- Mapas Semanticos e Inteligencia Topologica      [PRIORIDAD ALTA]
    FASE 3 --- Asistente Cientifico IA                        [PRIORIDAD ALTA]
    FASE 4 --- Trabajo Futuro (Editorial, Perfiles, API)       [PENDIENTE]

---

## FASE 1: Enriquecimiento Bibliometrico-Cienciometrico

Secciones cubiertas: S.5 (Indicadores bibliometricos), S.10 parcial, S.4 parcial.

### 1.1 Nuevos Indicadores de Impacto por Revista

Todos se integran al MetricsAccumulator existente en src/performance_metrics.py.

**H-index de Revista**
Cuantos articulos h han recibido al menos h citas:
  h = max{ i : citas[i] >= i }   (vector ordenado descendente, 1-indexed)
Fuente de datos: campo cited_by_count ya en el Parquet de works.

**G-index de Revista**
Mayor entero g tal que los g articulos mas citados suman al menos g^2 citas:
  g = max{ g : SUM(citas[1..g]) >= g^2 }

**M-index (H-index temporal)**
  m = h / T   donde T es la antiguedad en anos de la revista.
Normaliza el impacto por edad editorial. Util para comparar revistas jovenes vs. establecidas.

**PageRank de Revista en Grafo de Citaciones**
Importancia de la revista en el grafo de citaciones inter-revistas.
Fuente: campo referenced_works en OpenAlex -> join con journal_id.
Codigo: networkx.pagerank() ya disponible en ecosistema Topics.

**Eigenfactor Score (Abierto)**
Alternativa libre al Eigenfactor propietario (Bergstrom 2007).
Calculo: PageRank sobre matriz de citaciones inter-revistas normalizada por referencias por articulo.
Referencia: West JD et al. (2010). PLOS ONE. DOI: 10.1371/journal.pone.0010937

**Relative Citation Ratio (RCR) y Field Citation Ratio (FCR)**
- FCR: citas normalizadas por promedio del campo y ano (equivalente a fwci en OpenAlex).
- RCR (NIH): se aproxima con informacion de Topics de OpenAlex.

**Citas Normalizadas por Campo y Ano (CNCA)**
  CNCA_j = (1/N_j) * SUM_i ( citas_i / media_campo_ano_i )
Equivalente al CNCI de InCites con datos abiertos. Se extiende directamente desde FWCI en performance_metrics.py.

**H-index Agregado por Pais / Campo / Institucion**
H-index sobre todos los articulos de un pais, campo cientifico o tipo de institucion.

### 1.2 Indicadores de Internacionalizacion

**Diversidad Institucional (Indice de Shannon)**
  H = -SUM p_i * log(p_i)   donde p_i = proporcion de articulos de la institucion i.

**Porcentaje de Autores Extranjeros**
  %_ext = (autores_afiliacion_extranjera / total_autores) * 100
Fuente: authorships.institutions.country_code en OpenAlex.

**Indice de Colaboracion Internacional**
Proporcion de articulos con al menos un coautor de un pais diferente al de la revista.

**Diversidad Geografica de Referencias**
Distribucion de paises de los trabajos citados (endogamia vs. apertura).

### 1.3 Indicadores de Ciencia Abierta Expandidos

Ademas del desglose actual (Gold, Diamond, Green, Hybrid, Bronze, Closed):
- % articulos con datos abiertos (has_fulltext + enlace a repositorio)
- % preprints (best_oa_location.version = "submittedVersion")
- Desglose de licencias CC: CC-BY / CC-BY-SA / CC-BY-NC / CC-BY-NC-ND
- % articulos con codigo abierto (enlace GitHub/Zenodo)

### 1.4 Indicadores de Produccion Expandidos

| Indicador | Descripcion | Fuente |
|---|---|---|
| Numero de autores | Por articulo y por periodo | OpenAlex authorships |
| Numero de instituciones | Distintas por periodo | OpenAlex authorships.institutions |
| Numero de paises | Distintos por periodo | OpenAlex |
| Referencias promedio | Promedio de refs por articulo | OpenAlex referenced_works_count |
| Edad media de referencias | Antiguedad promedio de las citas | OpenAlex referenced_works + publication_year |
| Price Index | % referencias < 5 anos | PLmetrix-Lab-2.0/backend/app/price.py (reutilizar) |

### 1.5 Archivos a Crear / Modificar

**[MODIFY] src/performance_metrics.py**
- Anadir: compute_h_index(citation_vector), compute_g_index(citation_vector), compute_m_index(h, age_years)
- Ampliar MetricsAccumulator: autores unicos, instituciones, paises, edad de referencias
- Anadir Shannon Diversity Index

**[NEW] src/citation_indices.py**
- compute_pagerank_journals(works_df, journals_df) -> pd.DataFrame
- compute_eigenfactor(citation_matrix) -> pd.DataFrame
- compute_cnca(works_df, field_year_means_df) -> float
- Dependencia: networkx

**[NEW] src/openness_metrics.py**
- compute_oa_detailed(works_df) -> dict
- compute_license_breakdown(works_df) -> dict
- compute_preprint_rate(works_df) -> float

**[MODIFY] pipeline_revistaslatam/precompute_metrics.py**
- Incorporar llamadas a los nuevos modulos

**[MODIFY] pipeline_revistaslatam/transform_metrics.py**
- Transformacion y normalizacion de nuevos indicadores al esquema de BD

---

## FASE 2: Mapas Semanticos e Inteligencia Topologica

Secciones cubiertas: S.10 (UMAP interactivos, hexbin, redes, Sankey, geografico), S.6 (Grafo parcial).

### 2.1 Mapa Semantico Enriquecido de Revistas

El pipeline UMAP actual (calculate_umap.py) usa solo 6 indicadores numericos. Se enriquece con:

**Embeddings Semanticos de Revistas**
Cada revista se representa por un embedding desde:
- Titulos y resumenes de sus ultimos 500 articulos (concatenados)
- Codificacion: nomic-embed-text-v1.5 (ya descargado en revistaslatam/nomic-embed/)
- Journal-level mean pooling

Codigo reutilizable:
- newLabSOM/engine/semantic_engine.py: embedding y GPU-awareness
- Topics/fronts/semantic/embeddings.py: gestion de cache en Parquet

**UMAP Multimodal (Hibrido Numerico + Semantico)**
  X_final = [ alpha * X_biblio_normalized | (1-alpha) * X_semantic_reduced ]
  alpha = 0.4 (40% numerico, 60% semantico)
  Parametros UMAP: n_components=2, n_neighbors=15, metric='cosine', min_dist=0.1
Codigo: newLabSOM/engine/semantic_engine.py: reduce_with_umap()

**Deteccion de Comunidades Tematicas**
Algoritmo de Leiden sobre grafo KNN del espacio UMAP.
Codigo: Topics/fronts/structural/leiden_detector.py: leiden_detect(graph, resolution=1.0)

**Mapa Hexagonal (Hexbin) de Revistas**
Densidad de revistas en espacio semantico coloreada por FWCI, OA Diamante.
Herramienta: Plotly go.Densitymapbox / go.Histogram2dContour

### 2.2 Redes de Colaboracion Internacional

**Grafo de Coautoria Pais-Pais**
- Nodos: paises LATAM (y mundo como contexto)
- Aristas: articulos con coautores de cada pais; peso = num. articulos
- Visualizacion: Choropleth + grafo de arcos curvos (Plotly)
- Codigo: Topics/src/viz_bibliometrics.py

**Red Editorial Institucion-Revista**
- Hubs institucionales: universidades que editan multiples revistas de alto impacto

**Grafo de Citacion Inter-Revistas (Sankey de Disciplinas)**
- Flujo de citas entre disciplinas (OpenAlex Topics level-0)
- Sankey: origen = disciplina citante, destino = disciplina citada
- Herramienta: Plotly go.Sankey

### 2.3 Grafo de Conocimiento en Neo4j

Reutiliza sos-mcp-services/shared/neo4j_client.py.

Esquema de nodos:
  (:Revista {id, issn, nombre, pais, campo, fwci_avg, h_index})
  (:Autor {orcid, nombre, pais, h_index})
  (:Institucion {ror, nombre, pais, tipo})
  (:Pais {iso2, nombre, region})
  (:Campo {id_openalex, nombre, nivel})
  (:Articulo {doi, titulo, ano, fwci, citas})

Relaciones clave:
  (:Articulo)-[:PUBLICADO_EN]->(:Revista)
  (:Autor)-[:ESCRIBIO]->(:Articulo)
  (:Autor)-[:AFILIADO_A]->(:Institucion)
  (:Institucion)-[:UBICADA_EN]->(:Pais)
  (:Articulo)-[:PERTENECE_A]->(:Campo)
  (:Revista)-[:EDITADA_POR]->(:Institucion)
  (:Articulo)-[:CITA]->(:Articulo)

Consultas de alto valor (ejemplos del S.6 de Sugerencias.md):
- Que revistas mexicanas publican biologia molecular de Brasil?
- Que editor participa en mas de 3 revistas indexadas en Scopus?
- Que universidades editan revistas con mayor impacto en ciencias sociales?

### 2.4 Busqueda Semantica de Revistas

Flujo:
  Query usuario -> embedding nomic-embed -> ANN en Qdrant -> re-ranking FWCI+H-index -> resultados

Codigo reutilizable:
- sos-mcp-services/shared/qdrant_client.py
- sos-mcp-services/services/sinapsisai/tools/graph_tools.py: patron de busqueda semantica

### 2.5 Archivos a Crear / Modificar

**[NEW] src/semantic_journals.py**
- generate_journal_embeddings(journals_df, works_df, model_path) -> np.ndarray
- build_hybrid_umap_space(biblio_features, semantic_embeddings, alpha=0.4) -> np.ndarray
- detect_journal_communities(umap_coords, resolution=1.0) -> pd.Series

**[NEW] src/network_analysis.py**
- build_country_coauthorship_graph(works_df) -> nx.Graph
- build_journal_citation_network(works_df) -> nx.DiGraph
- compute_sankey_discipline_flows(works_df) -> dict

**[NEW] src/knowledge_graph.py**
- ingest_journals_to_neo4j(journals_df, neo4j_client)
- ingest_works_to_neo4j(works_df, neo4j_client)
- query_journal_community(journal_id, cypher_template) -> list

**[NEW] pipeline_revistaslatam/build_semantic_map.py**
Pipeline: carga works -> genera embeddings -> UMAP -> Leiden -> guarda en Parquet.

**[NEW] pipeline_revistaslatam/build_networks.py**
Pipeline: coautoria pais-pais, red institucion-revista, Sankey disciplinas.

**[MODIFY] dashboard.py**
- Tabs nuevos: "Mapa Semantico", "Redes de Colaboracion", "Busqueda Semantica"
- Mapa UMAP interactivo: color=FWCI, tamano=volumen, tooltip=nombre
- Mapa hexagonal de densidad
- Grafo de coautoria con slider de ano

---

## FASE 3: Asistente Cientifico IA

Seccion cubierta: S.7 completo de Sugerencias.md.

### 3.1 Asistente de Lenguaje Natural

Chatbot integrado en el dashboard con RAG sobre Neo4j + Qdrant + ClickHouse.

Arquitectura:
  Pregunta usuario
    -> Clasificacion de intencion: busqueda | comparacion | recomendacion | estadistica
    -> Recuperacion de contexto:
        * Busqueda semantica en Qdrant (embeddings de revistas)
        * Consulta Cypher en Neo4j (relaciones estructuradas)
        * Consulta SQL en ClickHouse (metricas agregadas)
    -> Generacion de respuesta con LLM
    -> Respuesta en espanol con citas a revistas especificas

Ejemplos de preguntas resolubles:
- "Que revista latinoamericana es adecuada para publicar sobre bibliometria?"
  -> busqueda semantica + filtro OA + ranking FWCI
- "Que revistas aceptan articulos en espanol sin APC?"
  -> filtro estructurado Neo4j
- "Que revistas son similares a Scientometrics en LATAM?"
  -> nearest neighbors en espacio UMAP semantico

Codigo reutilizable:
- sos-mcp-services/services/sinapsisai/: patron RAG completo con Neo4j + Qdrant
- sos-mcp-services/services/revistaslatam/: herramientas MCP de revistas ya implementadas

### 3.2 Motor de Recomendaciones de Revistas

Variables de entrada: titulo+resumen del manuscrito, pais del autor, idioma preferido,
nivel de impacto objetivo, preferencia OA, restriccion de APC.

Algoritmo:
1. Embedding semantico del manuscrito con nomic-embed-text-v1.5
2. ANN en Qdrant: top-20 revistas mas similares tematicamente
3. Re-ranking con funcion de utilidad multicriterio:
   Score(r) = w1*simcos + w2*fwci_norm + w3*oa_bonus + w4*lang_match - w5*apc_penalty
4. Estimacion de probabilidad de aceptacion (naive Bayes sobre campo y pais del autor)

### 3.3 Comparador Inteligente de Revistas

El usuario selecciona 2-5 revistas y obtiene comparacion automatica estructurada.

| Dimension | Indicadores |
|---|---|
| Impacto | H-index, FWCI, % Top 10%, PageRank |
| Internacionalizacion | % autores extranjeros, Shannon diversity, colaboracion intl. |
| Ciencia Abierta | Tipo OA, licencia, politica preprints |
| Produccion | Volumen articulos, crecimiento anual, periodicidad |
| Tematica | Distancia coseno en espacio UMAP, overlap de temas |
| Editorial | Tiempo de revision estimado, indexaciones |

Salida: Tabla comparativa + radar chart + texto narrativo generado por LLM.
Codigo reutilizable: sos-mcp-services/services/revistaslatam/tools/journals_tools.py

### 3.4 Resumenes Automaticos de Revistas

Perfil narrativo generado por LLM para cada revista:
- Cobertura tematica principal (top-5 Topics de OpenAlex)
- Posicion en el ecosistema LATAM (cluster semantico + vecinas mas proximas)
- Tendencia de impacto (creciente / estable / decreciente)
- Fortaleza en ciencia abierta
Pre-generado offline, almacenado en BD.

### 3.5 Archivos a Crear / Modificar

**[NEW] src/assistant/**
  intent_classifier.py     - Clasificar intencion de pregunta
  context_retriever.py     - Orquestador RAG (Qdrant + Neo4j + ClickHouse)
  response_generator.py    - Generacion con LLM + referencias
  recommendation_engine.py - Motor de recomendacion multicriterio

**[NEW] src/auto_summaries.py**
- generate_journal_summary(journal_id, metrics_dict, llm_client) -> str
- Pipeline batch para todas las revistas

**[MODIFY] dashboard.py**
- Componente de chat: st.chat_message, st.chat_input
- Widget comparador: multiselect de revistas + boton "Comparar"
- Widget de recomendacion: formulario de manuscrito + resultados rankeados

---

## FASE 4: Trabajo Futuro (Editorial, Perfiles, API Publica)

### 4.1 Modelo de Datos Editorial Enriquecido (S.3 de Sugerencias.md)
- Entidades: Editoriales, Editores, Comites Editoriales
- Campos: APC, politica preprints, politica datos abiertos, politica IA, tiempo de revision
- Integracion: DOAJ, Crossref, Latindex, ROAD, ORCID, ROR

### 4.2 Indice Integral de Calidad Editorial (IICE)
Contribucion cientifica original propuesta en la recomendacion estrategica:
- Calidad editorial (criterios tipo Latindex)
- Impacto bibliometrico (OpenAlex)
- Internacionalizacion
- Ciencia abierta
- Transparencia editorial
- Uso de identificadores persistentes (ISSN, DOI, ORCID, ROR)
- Adopcion de IA y datos abiertos

### 4.3 Perfiles Automaticos (S.12)
Paginas de perfil para: revistas, editoriales, universidades, investigadores, paises, disciplinas.
Cada perfil integra: indicadores, produccion, evolucion historica, relaciones, analisis comparativos.

### 4.4 API REST Publica (S.8)
Endpoints abiertos: listado de revistas, indicadores, series historicas, articulos, autores,
instituciones, recomendaciones, consultas semanticas, descarga masiva.

### 4.5 Descarga de Datos (S.9)
Exportacion periodica en CSV, JSON, Parquet, DuckDB, SQLite.

### 4.6 Versionado y Trazabilidad (S.11)
Historial: cambios de ISSN, nombre, editorial, politicas, indexaciones, metricas.

---

## Resumen General de Archivos

| Fase | Archivos Nuevos | Archivos Modificados |
|---|---|---|
| Fase 1 | src/citation_indices.py, src/openness_metrics.py | src/performance_metrics.py, pipeline_revistaslatam/precompute_metrics.py, pipeline_revistaslatam/transform_metrics.py |
| Fase 2 | src/semantic_journals.py, src/network_analysis.py, src/knowledge_graph.py, pipeline_revistaslatam/build_semantic_map.py, pipeline_revistaslatam/build_networks.py | dashboard.py |
| Fase 3 | src/assistant/ (4 modulos), src/auto_summaries.py | dashboard.py |
| Fase 4 | api/, profiles/, modelo de datos editorial | Pipeline, base de datos |

---

## Nuevas Dependencias (requirements.txt)

Fase 1:
  networkx>=3.0          - PageRank, Eigenfactor (scikit-learn ya presente)

Fase 2:
  leidenalg>=0.10        - Deteccion de comunidades
  igraph>=0.11           - Grafo para Leiden
  sentence-transformers>=2.7  - Para nomic-embed inference
  qdrant-client>=1.9     - Busqueda vectorial (via sos-mcp-services)
  neo4j>=5.0             - Cliente Neo4j (via sos-mcp-services)

Fase 3:
  openai>=1.0            - Acceso a LLM (compatible Ollama/OpenAI/LM Studio)

---

## Infraestructura Requerida

| Servicio | Uso | Estado |
|---|---|---|
| ClickHouse (VPN UNAM) | Works de OpenAlex ~569M, cache de embeddings | Existente |
| Neo4j (VPN UNAM) | Grafo de conocimiento de revistas | Existente (sinapsisai) |
| Qdrant (local/Docker) | Busqueda semantica de revistas | Disponible via sos-mcp-services |
| nomic-embed (local) | Embeddings offline sin costo de API | Ya descargado en nomic-embed/ |
| LLM (LM Studio / API) | Asistente y generacion de resumenes | Configurable |

---

## Referencias Metodologicas

- Hirsch, J.E. (2005). An index to quantify scientific research output. PNAS, 102(46), 16569-16572. DOI: 10.1073/pnas.0507655102
- Egghe, L. (2006). Theory and practise of the g-index. Scientometrics, 69(1), 131-152. DOI: 10.1007/s11192-006-0144-7
- West, J.D. et al. (2010). Big Macs and Eigenfactor Scores. PLOS ONE, 5(11), e10937. DOI: 10.1371/journal.pone.0010937
- Hutchins, B.I. et al. (2016). Relative Citation Ratio (RCR). PLOS Biology, 14(9), e1002541. DOI: 10.1371/journal.pbio.1002541
- Waltman, L. & Van Eck, N.J. (2012). Publication-level classification system. JASIST, 63(12), 2378-2392. DOI: 10.1002/asi.22748
- McInnes, L. et al. (2018). UMAP. JOSS. DOI: 10.21105/joss.00861
- Traag, V.A. et al. (2019). From Louvain to Leiden. Scientific Reports, 9, 5233. DOI: 10.1038/s41598-019-41695-z
- Bergstrom, C.T. et al. (2008). Eigenfactor: Measuring value and prestige of scholarly journals. CRL News, 69(5), 314-316.
