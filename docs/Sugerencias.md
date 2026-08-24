
Especificación de Requerimientos
Revistas LATAM 2.0
Plataforma Abierta para el Ecosistema Científico Latinoamericano

1. Objetivo General

Transformar Revistas LATAM de un sistema de análisis bibliométrico basado en OpenAlex a una plataforma integral de información científica para América Latina, capaz de funcionar como la infraestructura abierta de referencia para investigadores, editores, universidades, agencias de financiamiento y organismos gubernamentales.

La plataforma deberá integrar información editorial, bibliométrica, institucional y semántica, proporcionando servicios de consulta, análisis, interoperabilidad y recomendación mediante inteligencia artificial.

2. Objetivos específicos

La plataforma deberá permitir:

localizar cualquier revista científica latinoamericana;
conocer toda su información editorial;
conocer su impacto científico;
analizar su evolución histórica;
conocer su posición dentro del ecosistema científico latinoamericano;
comparar revistas;
recomendar revistas para publicar;
ofrecer servicios mediante API abierta;
servir como fuente de datos para sistemas externos;
incorporar agentes inteligentes especializados.
3. Ampliación del modelo de datos

Actualmente la unidad principal es la revista.

Se deberá evolucionar hacia un modelo relacional compuesto por múltiples entidades.

Revistas

Agregar:

ISSN impreso
ISSN electrónico
DOI institucional
URL oficial
URL OJS
Año de creación
Estado (activa/inactiva)
Periodicidad
Idiomas
APC
Moneda del APC
Política de exención
Licencia Creative Commons
Política de IA
Política de preprints
Política de datos abiertos
Política de revisión abierta
Tiempo promedio de revisión
Tiempo hasta publicación
Tasa estimada de aceptación
País
Estado
Ciudad
Coordenadas geográficas
Editoriales

Registrar

nombre
país
ROR
GRID
Wikidata
ORCID del editor responsable
información de contacto
Instituciones

Registrar

universidad
centro de investigación
facultad
departamento
ROR
GRID
OpenAlex Institution
país
ciudad
Editores

Registrar

ORCID
afiliación
país
especialidad
producción científica
otras revistas donde participa
Comités editoriales

Registrar

miembros
país
institución
ORCID
diversidad internacional
porcentaje nacional/internacional
4. Integración con fuentes externas

La plataforma deberá sincronizar automáticamente con:

OpenAlex

Crossref

DOAJ

SciELO

Redalyc

Latindex

ROAD

ROR

ORCID

Crossmark

OpenAIRE

Zenodo

PKP/OJS

Dimensions (cuando sea posible)

OpenCitations

Wikidata

5. Indicadores bibliométricos

Además de los indicadores actuales deberán calcularse:

Producción

Número de artículos

Número de autores

Número de instituciones

Número de países

Número de referencias

Edad media de los artículos

Impacto

Eigenfactor abierto

Article Influence

PageRank

H-index

G-index

M-index

Citas normalizadas

Altmetrics

Field Citation Ratio

Relative Citation Ratio

Internacionalización

Diversidad institucional

Diversidad geográfica

Porcentaje de autores extranjeros

Porcentaje de revisores extranjeros

Idiomas

Colaboración internacional

Ciencia abierta

Datos abiertos

Código abierto

Preprints

OA Diamante

OA Gold

OA Verde

OA Híbrido

Licencias

Calidad editorial

Tiempo de revisión

Tiempo de publicación

Periodicidad

Puntualidad

Antigüedad

Cumplimiento de buenas prácticas

6. Grafo de conocimiento

La información deberá almacenarse además como un grafo.

Las entidades incluirán

Revistas

Autores

Instituciones

Editoriales

Países

Campos científicos

Temas

Artículos

Financiamiento

Repositorios

Proyectos

Organizaciones

Las relaciones deberán permitir consultas complejas.

Ejemplos:

¿Qué revistas mexicanas publican principalmente artículos de biología molecular escritos por investigadores brasileños?

¿Qué editor participa en más de tres revistas indexadas en Scopus?

¿Qué universidades editan revistas con mayor impacto en ciencias sociales?

7. Inteligencia Artificial
   Asistente científico

La plataforma deberá responder preguntas mediante lenguaje natural.

Ejemplos

¿Qué revista latinoamericana es adecuada para publicar un artículo sobre bibliometría?

¿Qué revistas aceptan artículos en español sin APC?

¿Qué revistas similares existen a Scientometrics en Latinoamérica?

Motor de recomendaciones

Deberá recomendar revistas considerando

tema

idioma

impacto

tiempo de revisión

acceso abierto

probabilidad de aceptación

historial del investigador

Búsqueda semántica

La búsqueda deberá funcionar mediante embeddings además de búsqueda textual.

Resúmenes automáticos

Cada revista deberá tener un resumen generado automáticamente.

Comparador inteligente

El usuario podrá seleccionar varias revistas y obtener una comparación automática de fortalezas y debilidades.

8. API pública

La plataforma deberá ofrecer una API REST.

Entre otros servicios

Listado de revistas

Información editorial

Indicadores

Series históricas

Temáticas

Artículos

Autores

Instituciones

Recomendaciones

Consultas semánticas

Descarga masiva

9. Descarga de datos

Se deberán ofrecer descargas en

CSV

JSON

Parquet

DuckDB

SQLite

con versiones periódicas.

10. Visualizaciones

Agregar nuevas visualizaciones.

Mapa de colaboración

Redes editoriales

Grafos de citación

Sankey entre disciplinas

Redes institución–revista

Evolución temática

Mapas UMAP interactivos

Mapas hexagonales

Timeline editorial

Análisis geográfico

11. Versionado

Toda modificación deberá conservar historial.

Cambios de ISSN

Cambio de editorial

Cambio de nombre

Cambio de políticas

Cambio de indexaciones

Cambio de métricas

12. Perfiles

La plataforma deberá generar automáticamente perfiles para:

revistas;
editoriales;
universidades;
investigadores;
países;
disciplinas científicas.

Cada perfil integrará indicadores, producción, evolución histórica, relaciones y análisis comparativos.

13. Interoperabilidad

La plataforma deberá utilizar identificadores persistentes siempre que existan (ISSN, DOI, ORCID, ROR, OpenAlex ID, Crossref ID, Wikidata, etc.) y ofrecer exportación mediante formatos estándar para facilitar su integración con sistemas externos.

14. Visión a largo plazo

Revistas LATAM evolucionará desde un dashboard bibliométrico hacia una infraestructura científica abierta para América Latina, equivalente a un OpenAlex especializado en revistas científicas latinoamericanas, enriquecido con inteligencia artificial, un grafo de conocimiento y servicios abiertos para investigación, evaluación científica y políticas públicas.

Mi recomendación estratégica

Hay un aspecto que añadiría porque encaja muy bien con tu experiencia en cienciometría: incorporar un módulo de evaluación editorial. Mientras Latindex se centra en criterios editoriales y tu plataforma en indicadores bibliométricos, podrías crear un Índice Integral de Calidad Editorial (IICE) que combine:

Calidad editorial (criterios tipo Latindex).
Impacto bibliométrico (OpenAlex).
Internacionalización.
Ciencia abierta.
Transparencia editorial.
Visibilidad e interoperabilidad.
Uso de identificadores persistentes.
Adopción de IA y datos abiertos.

Ese índice sería una contribución científica original y convertiría a Revistas LATAM no solo en una base de datos, sino también en una herramienta para la evaluación y mejora de las revistas científicas de la región.
