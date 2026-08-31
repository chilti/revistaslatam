# 📋 Inventario Exhaustivo de Indicadores, Gráficos y Módulos
### Revistas LATAM — Matriz de Migración a FastAPI + React / Vite

Este inventario documenta el 100% de los componentes analíticos, indicadores calculados, gráficos interactivos, motores de renderizado, tablas y filtros presentes en el sistema para garantizar una migración exhaustiva y sin omisiones.

---

## 1. Panorama Regional (Latinoamérica)

### 📊 Indicadores y KPIs Macro
| Indicador | Fuente de Cálculo | Descripción |
| :--- | :--- | :--- |
| **Revistas Indexadas** | `latin_american_journals.parquet` | Conteo total de revistas latinoamericanas (7,494). |
| **Total Artículos** | `latin_american_works.parquet` | Suma de artículos publicados (3,632,625). |
| **FWCI Promedio** | `metrics_latam_period.parquet` | Impacto de citas ponderado por campo regional (0.56). |
| **% OA Diamante** | `metrics_latam_period.parquet` | Proporción de publicaciones sin cobro por APC (~67.0%). |
| **% OA Total** | `metrics_latam_period.parquet` | Suma de Gold, Diamond, Green, Hybrid, Bronze (~92.0%). |

### 🗺️ Gráficos y Visualizaciones
1. **Mapa Coroplético Regional por Indicador**:
   - **Motor**: Plotly `choropleth` (coordenadas ISO-3 de 20 países LATAM, proyección `natural earth`, centrado en $[-5, -70]$).
   - **Selector de 15 Indicadores**:
     - *Producción e Impacto*: Número de Revistas, Artículos, FWCI Promedio, % Top 10%, % Top 1%.
     - *Ciencia Abierta*: % OA Diamante, % OA Total, % OA Gold, % OA Verde, % OA Híbrido, % OA Bronce, % Cerrado.
     - *Multilingüismo*: % Idioma Español, % Idioma Inglés, % Idioma Portugués.
   - **Interactividad**: Tooltip interactivo con código, nombre, valor del indicador, revistas y artículos + Tabla descargable.

2. **Tarjetas Comparativas de Desempeño Temporal**:
   - **Periodo Histórico vs Periodo Reciente (2021-2025)**:
     - Documentos, FWCI Promedio, % Top 10%, % Top 1%, Percentil Promedio Normalizado.

3. **Gráficos Circulares de Distribución**:
   - **Acceso Abierto**: Pie chart (Gold, Diamond, Green, Hybrid, Bronze, Closed).
   - **Idiomas de Publicación**: Pie chart (Español, Inglés, Portugués, Francés, Alemán, Italiano, Otros).

4. **Sunburst Temático Regional (4 Niveles de Profundidad)**:
   - **Jerarquía**: Dominio $\rightarrow$ Campo $\rightarrow$ Subcampo $\rightarrow$ Tópico.
   - **Tamaño de nodo**: Volumen de artículos (`count_recent` o `count_full`).
   - **Selector de Indicador de Color (10 métricas)**: FWCI (2021-2025), Percentil (2021-2025), % Top 1%, % Top 10%, % OA Gold (2021-2025), y sus equivalentes del periodo completo.
   - **Escala de Color**: Normalización tricromática anclada en 1.0 (Media Mundial) para FWCI.
   - **Toggle**: Sincronización de volumen con/sin "Sin Clasificación".

5. **Tablas de Perfiles Temáticos**:
   - Pestañas: **Dominio**, **Campo**, **Subcampo** cruzadas contra los 20 países y el Total Regional LATAM con conteo y porcentaje (`share`).

6. **Evolución Histórica de Perfiles de Conocimiento**:
   - Matriz temporal de crecimiento disciplinar año con año.

7. **Series Temporales Anuales (1970–2026)**:
   - Línea de Documentos Publicados por año.
   - Línea de FWCI Promedio con línea guía en $y = 1.0$ (Promedio Mundial).
   - Líneas de Artículos Altamente Citados (% Top 10% y % Top 1%).
   - Líneas de Tipos de Acceso Abierto (Gold, Green, Hybrid).
   - Tabla expandible con datos crudos y formateados.

8. **Tablas Comparativas de Países (Rankings)**:
   - Pestaña **Periodo Completo** y Pestaña **Periodo Reciente (2021-2025)** ordenables por cualquiera de sus 19 columnas.

9. **Trayectorias Globales en Espacio UMAP (2000–2025)**:
   - Curvas de spline y markers comparando la evolución multidimensional de cada país contra la referencia **Iberoamérica (LATAM)** con tabla enriquecida.

10. **Radares de Desempeño Multidimensional por País**:
    - Grilla 3x3 de gráficos radar normalizados $[0, 1]$ comparando Periodo Completo vs Reciente (2021-2025) en FWCI, Percentil, Top 10%, Top 1% y OA Diamante.

11. **Mapa UMAP 2D de Similitud entre Países**:
    - Scatter 2D de países según 7 dimensiones con tabla de atributos.

12. **Explorador Dinámico Scatter Plot de Revistas**:
    - Selector de Periodo (Reciente vs Completo).
    - Selectores dinámicos para Eje X y Eje Y entre 12 variables con coloreado por país.

13. **Tablas de Indicadores Anuales Suavizados**:
    - Pestañas: Datos Crudos, Media Móvil $w=3$ y Media Móvil $w=5$.

---

## 2. Nivel País

### 📊 Indicadores y Métricas del País
- **Básicos**: Revistas, Artículos.
- **Fila 1 (Impacto y Citación)**: Documentos, FWCI Promedio, % Top 10%, % Top 1%, Percentil Promedio Normalizado.
- **Fila 2 (Ciencia Abierta y Visibilidad)**: % OA Diamante, % OA Gold, % OA Verde, % en Scopus, % en DOAJ.
- **Fila 3 (Distribución Lingüística)**: % Español, % Inglés, % Portugués, % Otros Idiomas, % Autoría Doméstica.
- **Comparativa Reciente (2021-2025)**: Documentos, FWCI, % Top 10%, % Top 1%, Percentil.

### 🗺️ Gráficos y Visualizaciones
1. **Trayectoria de Desempeño UMAP (País vs LATAM 2000–2025)** con anotaciones anuales.
2. **Mapa UMAP 2D de Similitud entre Revistas del País**: Identificación de clusters disciplinares locales.
3. **Huella Semántica y Evolución Temporal de Artículos del País**: Proyección de artículos sobre el paisaje regional coloreados por año de publicación (escala Turbo).
4. **Explorador Dinámico Scatter Plot de Revistas del País**: Ejes X/Y configurables, estadísticas descriptivas (Media, Mediana, Desv. Est., Min, Max) y correlación de Pearson.
5. **Distribución de Acceso Abierto e Idiomas** (Pie charts).
6. **Sunburst Temático del País (4 Niveles)** con selector de métricas.
7. **Tablas de Perfiles Temáticos de Revistas del País** (Dominio, Campo, Subcampo).
8. **Evolución Histórica de Perfiles de Conocimiento del País**.
9. **Series Temporales Anuales**: Documentos, FWCI (línea 1.0), Top 10%/1%, Acceso Abierto y tablas cruda / $w=3$ / $w=5$.

---

## 3. Nivel Revista

### 📊 Metadatos y Métricas de la Revista
- **Ficha Técnica**: Nombre oficial, ISSN-L, País, Editorial, Enlace a OpenAlex, Homepage, Badge de Comunidad Temática.
- **Fila 1 (Producción y Citación)**: Total Documentos, Total Citas, FWCI Promedio (o 2-Year Mean Citedness), Índice H.
- **Fila 2 (Cienciometría Avanzada y Red)**: Índice i10, PageRank de Citas (‰), Eigenfactor Score (%), Percentil Promedio.
- **Fila 3 (Ciencia Abierta e Indexaciones)**: % OA Diamante, % OA Dorado, Flag En DOAJ (Sí/No), Flag En Scopus (Sí/No).
- **Periodo Reciente (2021-2025)**: Documentos, FWCI, % Top 10%, % Top 1%, Percentil, % Autoría Doméstica.

### 🗺️ Gráficos y Visualizaciones
1. **Sunburst Temático de la Revista (4 Niveles)**: Dominio $\rightarrow$ Campo $\rightarrow$ Subcampo $\rightarrow$ Tópico.
2. **Evolución Histórica de Perfiles de Conocimiento de la Revista**.
3. **Distribución de Acceso Abierto e Idiomas** (Pie charts).
4. **Radar de Desempeño Reciente (2021-2025)**: FWCI, Percentil Norm., Top 10%, Top 1%, OA Diamante, Autoría Doméstica.
5. **Series Temporales Anuales de la Revista (Últimos 20 años)** y tabla histórica completa.
6. **Listado Detallado de Artículos con DuckDB**:
   - Paginación y búsqueda de artículos específicos de la revista.
   - Filtro múltiple por año de publicación.
   - Columnas: Título, Año, DOI clicable, Citas, FWCI.
7. **Foco Temático y Deriva Longitudinal de la Revista**:
   - Proyección de los artículos de la revista sobre el paisaje regional.
   - Coloreado cronológico por año de publicación con escala continua Turbo.
   - Métrica calculada de **Dispersión Semántica** $\sqrt{\sigma_x^2 + \sigma_y^2}$ e interpretación de foco editorial vs deriva.
8. **Trayectoria UMAP Multidimensional de la Revista vs su País (2000–2025)** con tablas cruda, $w=3$ y $w=5$.
9. **Explorador Dinámico Scatter Plot de Artículos de la Revista**:
   - Ejes X/Y configurables (FWCI, Percentil, Citas, Top 1%, Top 10%, Autoría Doméstica).
   - Muestreo inteligente (1,000 puntos o total).
   - Estadísticas descriptivas completas y correlación de Pearson.

---

## 4. 🗺️ Mapas Semánticos

### 🌌 Pestaña: Artículos Académicos
- **Motor Principal**: ⚡ WebGL acelerado por GPU (capacidad: 30K, 50K o 100K+ artículos a 60 FPS).
- **Motor Secundario**: 📊 Plotly Scatter 2D interactivo.
- **Selectores y Filtros**:
  - *Variable de Color*: Año de Publicación (Gradiente Turbo continuo), Comunidad Temática (paleta 15 colores), FWCI (Viridis $[0, 3.0]$), Uniforme.
  - *Filtro por País*: Todos los 20 países LATAM.
  - *Filtro por Comunidad Temática*: Todas las macro-comunidades identificadas.
  - *Límite de Muestra*: 30,000 / 50,000 / Todos.
  - *Métrica de Tamaño de Burbuja*: Citas Totales, FWCI, Uniforme.
- **Interactividad WebGL**:
  - Zoom fluido con rueda del ratón (centrado en cursor).
  - Paneo con arrastre (`mousedown` + `mousemove`).
  - Botón Recentrar.
  - Tooltip HUD flotante con Título, Autores, Revista, Año, País, Comunidad, Citas, FWCI y Acceso Abierto.
  - **🖱️ Clic Derecho**: Apertura directa del artículo en OpenAlex en pestaña nueva.
- **Documentación Metodológica**: Explicación completa del pipeline (Semántica Pura Título+Resumen, Stopwords Trilingües, Nomic Embed v2, UMAP Cosine 2D y clustering).

### 🎯 Pestaña: Revistas
- **Motor Principal**: ⚡ WebGL acelerado por GPU (7,494 revistas completas).
- **Motor Secundario**: 📊 Plotly Scatter 2D.
- **Selectores y Filtros**:
  - *Variable de Color*: Comunidad Temática, FWCI Promedio, Índice H, PageRank Citas, % OA Diamante, País.
  - *Filtro por Comunidad Temática* y *Filtro por País*.
  - *Métrica de Tamaño de Burbuja*: Total Artículos, Citas Totales, FWCI Promedio, Índice H, Uniforme.
- **Interactividad WebGL**:
  - Tooltip HUD con Editorial, País, Artículos, Citas, FWCI, H-Index, % OA Diamante y Badges verdes de indexación (`✓ DOAJ`, `✓ SciELO`, `✓ Scopus`).
  - Clic derecho para abrir la fuente en OpenAlex.
- **Documentación Metodológica**: Explicación del espacio híbrido multimodal (60% Semántica + 40% Indicadores Cienciométricos) y baricentro geométrico de artículos.

---

## 5. 🌐 Redes de Colaboración Internacional y Flujos

1. **🌍 Red de Coautoría Internacional**:
   - Mapa geográfico interactivo (`Scattergeo` de Plotly) con coordenadas de nodos de países y arcos de colaboración ponderados por volumen de coautoría.
   - Tabla de pares de países con conteo de artículos en colaboración.

2. **🔀 Diagrama Sankey Interdisciplinar**:
   - Flujo continuo de volumen de producción científica: Dominio $\rightarrow$ Campo $\rightarrow$ Subcampo.

---

## 6. Módulos Transversales

1. **Módulo de Temas**:
   - `☀️ Claro (Blanco)`: Fondo blanco `#ffffff`, `gl.clearColor(1,1,1,1)`, tipografía oscura y bordes sutiles.
   - `🌙 Oscuro (Dark)`: Fondo grafito `#0f172a`, `gl.clearColor(0.059, 0.090, 0.165, 1)`.
   - `🌌 Azul Noche (Navy)`: Fondo azul zafiro `#071731`, `gl.clearColor(0.027, 0.090, 0.192, 1)`.

2. **Módulo Study Dossier / Exportador de Reportes**:
   - Captura de cualquier gráfico o tabla analizada mediante `register_exportable`.
   - Drawer interactivo para seleccionar hallazgos y exportar en Markdown estructurado o JSON.

3. **Sección Acerca de...**:
   - Grupo de investigación, créditos, diagrama de arquitectura Mermaid y descripción detallada del flujo ETL y analítico.
