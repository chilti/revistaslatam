# Resumen de Cambios: Métricas de Indexación a Nivel Revista

## Objetivo
Agregar métricas de indexación (Scopus, CORE, DOAJ) a nivel de revista individual en todos los scripts de precálculo.

## Cambios Realizados

### 1. **precompute_metrics_parallel_optimized.py** ✅

**Función modificada**: `process_journal_worker()`

**Cambios**:
- Agregado acceso a `_journals_df` en variables globales
- Extracción de metadatos de indexación de la revista:
  ```python
  journal_indexing = {
      'is_scopus': bool(is_scopus),
      'is_core': bool(is_core),
      'is_doaj': bool(is_doaj)
  }
  ```
- Agregado `metrics.update(journal_indexing)` a métricas anuales y de período

**Resultado**: Cada registro de métricas de revista ahora incluye `is_scopus`, `is_core`, `is_doaj` como valores booleanos.

---

### 2. **precompute_metrics_parallel.py** ✅

**Función modificada**: `process_journal_parallel()`

**Cambios**:
- Actualizada firma de función para recibir `journals_df`:
  ```python
  def process_journal_parallel(args):
      journal_id, works_df, journals_df, start_year, end_year = args
  ```
- Extracción de metadatos de indexación (igual que optimizado)
- Agregado `metrics.update(journal_indexing)` a métricas anuales y de período

**Función modificada**: Preparación de argumentos en `main()`
- Actualizado `journal_args` para incluir `journals_df`:
  ```python
  journal_args = [
      (journal_id, works_df, journals_df, start_year, end_year)
      for journal_id in journal_ids
  ]
  ```

**Resultado**: Script paralelo básico ahora también incluye métricas de indexación.

---

### 3. **src/performance_metrics.py** ✅

**Función modificada**: `calculate_journal_metrics_chunked()`

**Cambios**:
- Actualizada firma para recibir `journals_df`:
  ```python
  def calculate_journal_metrics_chunked(works_filepath, journals_df, journal_id, start_year=None, end_year=None):
  ```
- Validación de existencia de revista
- Extracción de metadatos de indexación
- Agregado `metrics.update(journal_indexing)` a métricas anuales y de período

**Función modificada**: `compute_and_cache_all_metrics()`
- Actualizada llamada para pasar `journals_df`:
  ```python
  annual, period = calculate_journal_metrics_chunked(works_filepath, journals_df, journal_id, start_year, end_year)
  ```

**Resultado**: Script original también incluye métricas de indexación.

---

### 4. **METRICS_CALCULATION_GUIDE.md** ✅

**Secciones actualizadas**:

#### a) Métricas de Indexación de Revistas
- Actualizada introducción para indicar que se calculan en **todos los niveles**
- Agregadas subsecciones para cada métrica (Scopus, CORE, DOAJ):
  - **A nivel LATAM/País**: Porcentajes (`pct_scopus`, `pct_core`, `pct_doaj`)
  - **A nivel Revista**: Booleanos (`is_scopus`, `is_core`, `is_doaj`)

#### b) Nivel 3: Revista Individual
- Actualizado proceso para mostrar extracción de metadatos de indexación
- Actualizada lista de métricas calculadas:
  ```
  ✅ Rendimiento científico (1-5)
  ✅ Acceso abierto (6-10)
  ✅ Indexación (12-14): is_scopus, is_core, is_doaj
  ❌ NO se incluye: num_journals
  ```

#### c) Esquema de Datos
- Separado en tres esquemas distintos:
  - **LATAM Annual/Period**: Con `pct_scopus`, `pct_core`, `pct_doaj`
  - **Country Annual/Period**: Con `pct_scopus`, `pct_core`, `pct_doaj`
  - **Journal Annual/Period**: Con `is_scopus`, `is_core`, `is_doaj` (booleanos)

#### d) Ejemplo Detallado
- Agregado paso 7 con valores de indexación de ejemplo
- Actualizado resultado final para incluir:
  ```python
  'is_scopus': True,
  'is_core': False,
  'is_doaj': True
  ```

---

## Resumen de Métricas por Nivel

### Nivel LATAM
| Métrica | Tipo | Descripción |
|---------|------|-------------|
| `num_journals` | int | Número total de revistas |
| `pct_scopus` | float | % revistas en Scopus |
| `pct_core` | float | % revistas en CORE |
| `pct_doaj` | float | % revistas en DOAJ |
| + Métricas de rendimiento (1-5) | | |
| + Métricas de OA (6-10) | | |

### Nivel País
| Métrica | Tipo | Descripción |
|---------|------|-------------|
| `num_journals` | int | Número de revistas del país |
| `pct_scopus` | float | % revistas en Scopus |
| `pct_core` | float | % revistas en CORE |
| `pct_doaj` | float | % revistas en DOAJ |
| + Métricas de rendimiento (1-5) | | |
| + Métricas de OA (6-10) | | |

### Nivel Revista (NUEVO ✨)
| Métrica | Tipo | Descripción |
|---------|------|-------------|
| `is_scopus` | **bool** | ¿Está en Scopus? |
| `is_core` | **bool** | ¿Está en CORE? |
| `is_doaj` | **bool** | ¿Está en DOAJ? |
| + Métricas de rendimiento (1-5) | | |
| + Métricas de OA (6-10) | | |

---

## Validación

### Scripts Modificados
- ✅ `precompute_metrics_parallel_optimized.py`
- ✅ `precompute_metrics_parallel.py`
- ✅ `src/performance_metrics.py`

### Documentación Actualizada
- ✅ `METRICS_CALCULATION_GUIDE.md`

### Archivos de Salida Afectados
Los siguientes archivos Parquet ahora incluirán las nuevas columnas:
- `data/cache/metrics_journal_annual.parquet` - Ahora con `is_scopus`, `is_core`, `is_doaj`
- `data/cache/metrics_journal_period.parquet` - Ahora con `is_scopus`, `is_core`, `is_doaj`

---

## Próximos Pasos

### Para el Usuario

1. **Ejecutar script optimizado** para generar nuevas métricas:
   ```bash
   python precompute_metrics_parallel_optimized.py --force
   ```

2. **Verificar archivos de salida**:
   ```python
   import pandas as pd
   
   # Cargar métricas de revistas
   journal_annual = pd.read_parquet('data/cache/metrics_journal_annual.parquet')
   
   # Verificar nuevas columnas
   print(journal_annual.columns)
   # Debe incluir: is_scopus, is_core, is_doaj
   
   # Ver ejemplo
   print(journal_annual[['journal_id', 'year', 'is_scopus', 'is_core', 'is_doaj']].head())
   ```

3. **Actualizar dashboard** (si es necesario) para mostrar las nuevas métricas de indexación a nivel revista.

---

## Notas Técnicas

### Manejo de Valores Faltantes
```python
is_scopus = safe_get(journal_info, 'is_indexed_in_scopus', default=False)
```
- Si el campo no existe, se asume `False`
- Todos los valores se convierten explícitamente a `bool()`

### Consistencia entre Scripts
Los tres scripts ahora:
1. Extraen los mismos campos de indexación
2. Usan la misma lógica de conversión a booleano
3. Agregan las métricas de la misma manera

### Compatibilidad hacia Atrás
- Los archivos existentes **no** incluirán estas columnas
- Se requiere ejecutar con `--force` para regenerar con las nuevas columnas
- El dashboard debe manejar la ausencia de estas columnas en datos antiguos

---

## Ejemplo de Uso en Dashboard

```python
import pandas as pd
import streamlit as st

# Cargar métricas de revista
journal_metrics = pd.read_parquet('data/cache/metrics_journal_period.parquet')

# Filtrar revistas indexadas en Scopus
scopus_journals = journal_metrics[journal_metrics['is_scopus'] == True]

st.write(f"Revistas en Scopus: {len(scopus_journals)}")

# Crear badge de indexación
def indexing_badge(row):
    badges = []
    if row['is_scopus']:
        badges.append('🔵 Scopus')
    if row['is_core']:
        badges.append('🟢 CORE')
    if row['is_doaj']:
        badges.append('🟡 DOAJ')
    return ' | '.join(badges) if badges else '⚪ No indexada'

journal_metrics['indexing'] = journal_metrics.apply(indexing_badge, axis=1)
```

---

## Conclusión

✅ **Todos los scripts de precálculo ahora calculan métricas de indexación a nivel revista**

Las métricas se representan como:
- **Porcentajes** a nivel LATAM/País (agregado)
- **Booleanos** a nivel Revista (individual)

Esto permite análisis más detallados y visualizaciones más ricas en el dashboard.
