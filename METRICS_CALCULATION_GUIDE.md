# Guía de Cálculo de Métricas - Script Optimizado Paralelo

## Tabla de Contenidos

1. [Visión General](#visión-general)
2. [Métricas de Rendimiento Científico](#métricas-de-rendimiento-científico)
3. [Métricas de Acceso Abierto](#métricas-de-acceso-abierto)
4. [Métricas de Indexación de Revistas](#métricas-de-indexación-de-revistas)
5. [Niveles de Agregación](#niveles-de-agregación)
6. [Proceso de Cálculo Paralelo](#proceso-de-cálculo-paralelo)
7. [Ejemplos Detallados](#ejemplos-detallados)

---

## Visión General

El script `precompute_metrics_parallel_optimized.py` calcula métricas bibliométricas para revistas latinoamericanas a tres niveles:

- **LATAM**: Todas las revistas de América Latina
- **País**: Revistas agrupadas por país
- **Revista**: Métricas individuales por revista

Para cada nivel, se calculan:
- **Métricas anuales**: Por cada año en el rango de datos
- **Métricas de período**: Agregado de todo el período (ej. 2021-2025)

---

## Métricas de Rendimiento Científico

### 1. **Número de Documentos** (`num_documents`)

**Definición**: Cantidad total de artículos publicados.

**Cálculo**:
```python
num_documents = len(works_df)
```

**Ejemplo**:
- Revista X publicó 150 artículos en 2023
- `num_documents = 150`

---

### 2. **FWCI Promedio** (`fwci_avg`)

**Definición**: Field-Weighted Citation Impact promedio. Mide el impacto de citación normalizado por campo.

**Interpretación**:
- `FWCI = 1.0`: Impacto promedio mundial
- `FWCI > 1.0`: Por encima del promedio
- `FWCI < 1.0`: Por debajo del promedio

**Cálculo**:
```python
# 1. Convertir valores a numérico
fwci_values = pd.to_numeric(works_df['fwci'], errors='coerce')

# 2. Rellenar NaN con 0 (artículos sin FWCI)
fwci_values = fwci_values.fillna(0)

# 3. Calcular promedio
fwci_avg = fwci_values.sum() / num_documents

# 4. Redondear a 2 decimales
fwci_avg = round(fwci_avg, 2)
```

**Ejemplo**:
```
Artículos: [FWCI: 2.5, FWCI: NaN, FWCI: 3.0, FWCI: 1.5]

Paso 1: [2.5, NaN, 3.0, 1.5]
Paso 2: [2.5, 0, 3.0, 1.5]
Paso 3: (2.5 + 0 + 3.0 + 1.5) / 4 = 1.75
Paso 4: 1.75
```

**Nota Importante**: Los artículos sin FWCI (NaN) se cuentan como 0, lo que hace el cálculo más conservador.

---

### 3. **% Top 10%** (`pct_top_10`)

**Definición**: Porcentaje de artículos en el top 10% más citados de su campo.

**Cálculo**:
```python
# 1. Rellenar NaN con False
top_10_values = works_df['is_in_top_10_percent'].fillna(False)

# 2. Convertir a booleano
top_10_values = top_10_values.astype(bool)

# 3. Contar True y calcular porcentaje
pct_top_10 = (top_10_values.sum() / num_documents) * 100

# 4. Redondear a 2 decimales
pct_top_10 = round(pct_top_10, 2)
```

**Ejemplo**:
```
100 artículos totales
15 artículos en top 10%

pct_top_10 = (15 / 100) * 100 = 15.00%
```

---

### 4. **% Top 1%** (`pct_top_1`)

**Definición**: Porcentaje de artículos en el top 1% más citados de su campo.

**Cálculo**: Idéntico a Top 10%, pero usando `is_in_top_1_percent`.

```python
top_1_values = works_df['is_in_top_1_percent'].fillna(False).astype(bool)
pct_top_1 = (top_1_values.sum() / num_documents) * 100
pct_top_1 = round(pct_top_1, 2)
```

**Ejemplo**:
```
100 artículos totales
3 artículos en top 1%

pct_top_1 = (3 / 100) * 100 = 3.00%
```

---

### 5. **Percentil Promedio** (`avg_percentile`)

**Definición**: Percentil de citación promedio normalizado por campo.

**Interpretación**:
- `percentile = 50`: Mediana mundial
- `percentile > 50`: Por encima de la mediana
- `percentile < 50`: Por debajo de la mediana

**Cálculo**:
```python
# 1. Convertir a numérico
percentile_values = pd.to_numeric(
    works_df['citation_normalized_percentile'], 
    errors='coerce'
)

# 2. Rellenar NaN con 0
percentile_values = percentile_values.fillna(0)

# 3. Calcular promedio
avg_percentile = percentile_values.sum() / num_documents

# 4. Redondear a 2 decimales
avg_percentile = round(avg_percentile, 2)
```

**Ejemplo**:
```
Artículos: [percentil: 75, percentil: NaN, percentil: 60, percentil: 85]

Paso 1: [75, NaN, 60, 85]
Paso 2: [75, 0, 60, 85]
Paso 3: (75 + 0 + 60 + 85) / 4 = 55.0
Paso 4: 55.00
```

---

## Métricas de Acceso Abierto

### 6-10. **Porcentajes por Tipo de OA**

**Definición**: Distribución de artículos según su tipo de acceso abierto.

**Tipos de OA**:
- **Gold** (`pct_oa_gold`): Publicado en revista completamente OA
- **Green** (`pct_oa_green`): Depositado en repositorio
- **Hybrid** (`pct_oa_hybrid`): OA en revista de suscripción
- **Bronze** (`pct_oa_bronze`): Gratis en sitio del editor, sin licencia clara
- **Closed** (`pct_oa_closed`): No disponible en acceso abierto

**Cálculo**:
```python
# 1. Contar artículos por tipo de OA
oa_counts = works_df['oa_status'].value_counts()

# 2. Calcular porcentaje para cada tipo
total = len(works_df)
pct_oa_gold = (oa_counts.get('gold', 0) / total) * 100
pct_oa_green = (oa_counts.get('green', 0) / total) * 100
pct_oa_hybrid = (oa_counts.get('hybrid', 0) / total) * 100
pct_oa_bronze = (oa_counts.get('bronze', 0) / total) * 100
pct_oa_closed = (oa_counts.get('closed', 0) / total) * 100

# 3. Redondear a 2 decimales
pct_oa_gold = round(pct_oa_gold, 2)
# ... (igual para los demás)
```

**Ejemplo**:
```
Total: 100 artículos

Distribución:
- Gold: 40 artículos
- Green: 20 artículos
- Hybrid: 10 artículos
- Bronze: 5 artículos
- Closed: 25 artículos

Resultados:
pct_oa_gold = (40/100) * 100 = 40.00%
pct_oa_green = (20/100) * 100 = 20.00%
pct_oa_hybrid = (10/100) * 100 = 10.00%
pct_oa_bronze = (5/100) * 100 = 5.00%
pct_oa_closed = (25/100) * 100 = 25.00%

Total OA = 40 + 20 + 10 + 5 = 75%
```

---

## Métricas de Indexación de Revistas

Estas métricas solo se calculan a nivel de **país** y **LATAM**, no por revista individual.

### 11. **Número de Revistas** (`num_journals`)

**Definición**: Cantidad total de revistas.

**Cálculo**:
```python
num_journals = len(journals_df)
```

---

### 12. **% Scopus** (`pct_scopus`)

**Definición**: Porcentaje de revistas indexadas en Scopus.

**Cálculo**:
```python
# Contar revistas indexadas en Scopus
scopus_count = journals_df.apply(
    lambda x: safe_get(x, 'is_indexed_in_scopus', default=False), 
    axis=1
).sum()

# Calcular porcentaje
pct_scopus = (scopus_count / num_journals) * 100
pct_scopus = round(pct_scopus, 2)
```

**Ejemplo**:
```
Total revistas: 50
Indexadas en Scopus: 30

pct_scopus = (30 / 50) * 100 = 60.00%
```

---

### 13. **% CORE** (`pct_core`)

**Definición**: Porcentaje de revistas en CORE (Computing Research and Education).

**Cálculo**: Idéntico a Scopus, usando `is_core`.

```python
core_count = journals_df.apply(
    lambda x: safe_get(x, 'is_core', default=False), 
    axis=1
).sum()

pct_core = (core_count / num_journals) * 100
pct_core = round(pct_core, 2)
```

---

### 14. **% DOAJ** (`pct_doaj`)

**Definición**: Porcentaje de revistas en Directory of Open Access Journals.

**Cálculo**: Idéntico a Scopus, usando `is_in_doaj`.

```python
doaj_count = journals_df.apply(
    lambda x: safe_get(x, 'is_in_doaj', default=False), 
    axis=1
).sum()

pct_doaj = (doaj_count / num_journals) * 100
pct_doaj = round(pct_doaj, 2)
```

---

## Niveles de Agregación

### Nivel 1: LATAM (Toda América Latina)

**Datos de entrada**:
- Todas las revistas latinoamericanas
- Todos los artículos de esas revistas

**Métricas calculadas**:
- Todas las métricas de rendimiento científico (1-5)
- Todas las métricas de OA (6-10)
- Todas las métricas de indexación (11-14)

**Código**:
```python
# Filtrar artículos del año
year_works = works_df[works_df['publication_year'] == year]

# Calcular métricas
metrics = calculate_performance_metrics_from_df(year_works)
metrics['year'] = year

# Agregar métricas de revistas
journal_metrics = {
    'num_journals': len(journals_df),
    'pct_scopus': ...,
    'pct_core': ...,
    'pct_doaj': ...
}
```

---

### Nivel 2: País

**Datos de entrada**:
- Revistas de un país específico
- Artículos de esas revistas

**Proceso**:
```python
# 1. Obtener revistas del país
country_journals = journals_df[journals_df['country_code'] == country_code]
journal_ids = country_journals['id'].tolist()

# 2. Filtrar artículos de esas revistas
country_works = works_df[works_df['journal_id'].isin(journal_ids)]

# 3. Calcular métricas por año
for year in range(start_year, end_year + 1):
    year_works = country_works[country_works['publication_year'] == year]
    metrics = calculate_performance_metrics_from_df(year_works)
    metrics['year'] = year
    metrics['country_code'] = country_code
```

**Métricas calculadas**: Todas (1-14)

---

### Nivel 3: Revista Individual

**Datos de entrada**:
- Una revista específica
- Artículos de esa revista

**Proceso**:
```python
# 1. Filtrar artículos de la revista
journal_works = works_df[works_df['journal_id'] == journal_id]

# 2. Calcular métricas por año
for year in range(start_year, end_year + 1):
    year_works = journal_works[journal_works['publication_year'] == year]
    metrics = calculate_performance_metrics_from_df(year_works)
    metrics['year'] = year
    metrics['journal_id'] = journal_id
```

**Métricas calculadas**: Solo rendimiento y OA (1-10), NO indexación

---

## Proceso de Cálculo Paralelo

### Arquitectura

```
┌─────────────────────────────────────────────────────────┐
│                    Proceso Principal                     │
│  - Carga metadatos                                       │
│  - Calcula métricas LATAM (secuencial)                  │
│  - Coordina procesamiento paralelo                      │
└─────────────────────────────────────────────────────────┘
                            │
                            ├─────────────────────────────┐
                            ▼                             ▼
                ┌──────────────────────┐      ┌──────────────────────┐
                │   Worker Process 1   │      │   Worker Process N   │
                │  - Carga datos       │      │  - Carga datos       │
                │  - Procesa países    │ ...  │  - Procesa países    │
                │  - Procesa revistas  │      │  - Procesa revistas  │
                └──────────────────────┘      └──────────────────────┘
```

### Paso 1: Inicialización de Workers

Cada proceso worker ejecuta `init_worker()` una sola vez:

```python
def init_worker(works_file, journals_file, start_year, end_year):
    global _works_df, _journals_df, _start_year, _end_year
    
    # Cargar datos en memoria del worker
    _works_df = pd.read_parquet(works_file)
    _journals_df = pd.read_parquet(journals_file)
    _start_year = start_year
    _end_year = end_year
```

**Ventaja**: Los datos se cargan **una vez por worker**, no en cada tarea.

---

### Paso 2: Procesamiento por Chunks

Para evitar saturar la memoria, se procesan items en lotes:

```python
# Dividir países en chunks
chunk_size = max(1, len(countries) // 4)  # 4 batches

for i in range(0, len(countries), chunk_size):
    chunk = countries[i:i + chunk_size]
    
    # Crear pool de workers para este chunk
    with Pool(processes=num_cores, 
              initializer=init_worker,
              initargs=(works_file, journals_file, start_year, end_year)) as pool:
        results = pool.map(process_country_worker, chunk)
    
    # Pool se destruye, liberando memoria
```

**Ventaja**: Memoria se libera entre chunks.

---

### Paso 3: Worker Functions

Cada worker procesa un item (país o revista):

```python
def process_country_worker(country_code):
    global _works_df, _journals_df, _start_year, _end_year
    
    # 1. Filtrar datos del país
    country_journals = _journals_df[_journals_df['country_code'] == country_code]
    journal_ids = country_journals['id'].tolist()
    country_works = _works_df[_works_df['journal_id'].isin(journal_ids)]
    
    # 2. Calcular métricas anuales
    annual_data = []
    for year in range(_start_year, _end_year + 1):
        year_works = country_works[country_works['publication_year'] == year]
        metrics = calculate_performance_metrics_from_df(year_works)
        metrics['year'] = year
        metrics['country_code'] = country_code
        annual_data.append(metrics)
    
    # 3. Calcular métricas de período
    period_works = country_works[
        (country_works['publication_year'] >= _start_year) & 
        (country_works['publication_year'] <= _end_year)
    ]
    period_metrics = calculate_performance_metrics_from_df(period_works)
    
    return country_code, pd.DataFrame(annual_data), period_metrics
```

---

### Paso 4: Procesamiento Incremental

El script detecta qué ya fue calculado:

```python
# Cargar métricas existentes
existing_country_period = load_existing_metrics(cache_dir, 'country_period')

# Determinar qué países procesar
if existing_country_period is not None and not args.force:
    existing_ids = set(existing_country_period['country_code'].unique())
    countries_to_process = [c for c in countries if c not in existing_ids]
else:
    countries_to_process = countries

# Solo procesar los nuevos
if len(countries_to_process) > 0:
    results = process_in_chunks(countries_to_process, ...)
    
    # Combinar con existentes
    new_metrics = pd.concat(results)
    all_metrics = pd.concat([existing_country_period, new_metrics])
```

---

## Ejemplos Detallados

### Ejemplo 1: Cálculo Completo para una Revista

**Datos**:
- Revista: "Revista Mexicana de Física"
- Año: 2023
- Artículos: 50

**Distribución de artículos**:
```
FWCI:
- 10 artículos: FWCI = 2.0
- 15 artículos: FWCI = 1.5
- 20 artículos: FWCI = 1.0
- 5 artículos: FWCI = NaN (sin datos)

Top 10%:
- 8 artículos en top 10%

Top 1%:
- 2 artículos en top 1%

Percentil:
- 10 artículos: percentil 80
- 15 artículos: percentil 60
- 20 artículos: percentil 50
- 5 artículos: percentil NaN

OA Status:
- 30 Gold
- 10 Green
- 5 Hybrid
- 5 Closed
```

**Cálculos**:

1. **num_documents** = 50

2. **fwci_avg**:
   ```
   = (10×2.0 + 15×1.5 + 20×1.0 + 5×0) / 50
   = (20 + 22.5 + 20 + 0) / 50
   = 62.5 / 50
   = 1.25
   ```

3. **pct_top_10**:
   ```
   = (8 / 50) × 100
   = 16.00%
   ```

4. **pct_top_1**:
   ```
   = (2 / 50) × 100
   = 4.00%
   ```

5. **avg_percentile**:
   ```
   = (10×80 + 15×60 + 20×50 + 5×0) / 50
   = (800 + 900 + 1000 + 0) / 50
   = 2700 / 50
   = 54.00
   ```

6. **OA percentages**:
   ```
   pct_oa_gold = (30/50) × 100 = 60.00%
   pct_oa_green = (10/50) × 100 = 20.00%
   pct_oa_hybrid = (5/50) × 100 = 10.00%
   pct_oa_bronze = (0/50) × 100 = 0.00%
   pct_oa_closed = (5/50) × 100 = 10.00%
   ```

**Resultado final**:
```python
{
    'num_documents': 50,
    'fwci_avg': 1.25,
    'pct_top_10': 16.00,
    'pct_top_1': 4.00,
    'avg_percentile': 54.00,
    'pct_oa_gold': 60.00,
    'pct_oa_green': 20.00,
    'pct_oa_hybrid': 10.00,
    'pct_oa_bronze': 0.00,
    'pct_oa_closed': 10.00,
    'year': 2023,
    'journal_id': 'https://openalex.org/S123456'
}
```

---

### Ejemplo 2: Agregación a Nivel País

**Datos**:
- País: México
- Revistas: 100
- Artículos totales (2023): 5,000

**Indexación de revistas**:
- 60 revistas en Scopus
- 40 revistas en CORE
- 80 revistas en DOAJ

**Cálculos adicionales** (además de métricas de rendimiento):

1. **num_journals** = 100

2. **pct_scopus**:
   ```
   = (60 / 100) × 100
   = 60.00%
   ```

3. **pct_core**:
   ```
   = (40 / 100) × 100
   = 40.00%
   ```

4. **pct_doaj**:
   ```
   = (80 / 100) × 100
   = 80.00%
   ```

---

## Validación de Resultados

### Verificaciones Automáticas

El script valida:

1. **Suma de OA = 100%**:
   ```python
   total_oa = pct_oa_gold + pct_oa_green + pct_oa_hybrid + pct_oa_bronze + pct_oa_closed
   assert abs(total_oa - 100.0) < 0.1  # Tolerancia por redondeo
   ```

2. **Valores en rangos válidos**:
   ```python
   assert 0 <= pct_top_10 <= 100
   assert 0 <= pct_top_1 <= 100
   assert 0 <= avg_percentile <= 100
   assert fwci_avg >= 0
   ```

3. **Top 1% ≤ Top 10%**:
   ```python
   assert pct_top_1 <= pct_top_10
   ```

---

## Archivos de Salida

### Estructura de Archivos

```
data/cache/
├── metrics_latam_annual.parquet      # Métricas anuales LATAM
├── metrics_latam_period.parquet      # Métricas período LATAM
├── metrics_country_annual.parquet    # Métricas anuales por país
├── metrics_country_period.parquet    # Métricas período por país
├── metrics_journal_annual.parquet    # Métricas anuales por revista
└── metrics_journal_period.parquet    # Métricas período por revista
```

### Esquema de Datos

**Annual (anuales)**:
```
Columnas:
- year (int): Año
- country_code / journal_id: Identificador
- num_documents (int)
- fwci_avg (float)
- pct_top_10 (float)
- pct_top_1 (float)
- avg_percentile (float)
- pct_oa_gold (float)
- pct_oa_green (float)
- pct_oa_hybrid (float)
- pct_oa_bronze (float)
- pct_oa_closed (float)
- [Solo país/LATAM] num_journals (int)
- [Solo país/LATAM] pct_scopus (float)
- [Solo país/LATAM] pct_core (float)
- [Solo país/LATAM] pct_doaj (float)
```

**Period (período)**:
```
Columnas:
- period (str): Ej. "2021-2025"
- country_code / journal_id: Identificador
- [Mismas métricas que annual]
```

---

## Notas Técnicas

### Manejo de Valores Faltantes

1. **FWCI y Percentil**: NaN → 0
   - Razón: Artículos sin datos se consideran sin impacto
   - Hace el cálculo más conservador

2. **Top 10% y Top 1%**: NaN → False
   - Razón: Si no está marcado, no está en el top

3. **OA Status**: Ausente → 'closed'
   - Razón: Por defecto se asume cerrado

### Precisión Numérica

- Todos los porcentajes: 2 decimales
- FWCI y percentil: 2 decimales
- Redondeo estándar (0.5 → 1)

### Rendimiento

**Tiempos estimados** (1000 revistas, 100k artículos):
- LATAM: ~30 segundos
- Países (18): ~5 minutos
- Revistas (1000): ~45 minutos

**Con procesamiento incremental**:
- +10 revistas nuevas: ~30 segundos
- +2 países nuevos: ~30 segundos

---

## Referencias

- **OpenAlex**: Fuente de datos bibliométricos
- **FWCI**: [Scopus Metrics Guidebook](https://www.elsevier.com/solutions/scopus/how-scopus-works/metrics)
- **Percentiles**: Normalización por campo y año de publicación
- **OA Types**: [PLOS OA Spectrum](https://plos.org/open-science/open-access/)
