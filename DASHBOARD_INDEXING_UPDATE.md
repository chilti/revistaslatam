# Actualización del Dashboard: Métricas de Indexación

## Resumen

Se agregó una nueva sección de **Indexación de la Revista** en la vista de detalle de revista del dashboard, que muestra visualmente si la revista está indexada en Scopus, CORE y/o DOAJ.

---

## Cambios Realizados

### Archivo Modificado
- ✅ `dashboard.py`

### Sección Modificada
- **Nivel**: Revista (líneas 386-507)
- **Ubicación**: Después del gráfico de "Distribución de Acceso Abierto"

---

## Implementación

### Código Agregado

```python
# Indexing status
st.markdown("#### Indexación de la Revista")

# Create badges for indexing
indexing_badges = []
if period_data.get('is_scopus', False):
    indexing_badges.append("🔵 **Scopus**")
if period_data.get('is_core', False):
    indexing_badges.append("🟢 **CORE**")
if period_data.get('is_doaj', False):
    indexing_badges.append("🟡 **DOAJ**")

if indexing_badges:
    st.markdown(" | ".join(indexing_badges))
else:
    st.markdown("⚪ No indexada en bases de datos principales")
```

### Lógica

1. **Extrae datos** de `period_data` (métricas de período de la revista)
2. **Verifica** cada campo booleano: `is_scopus`, `is_core`, `is_doaj`
3. **Crea badges** con emojis de colores para cada indexación activa
4. **Muestra** los badges separados por `|` o un mensaje si no está indexada

---

## Visualización

### Ejemplo 1: Revista Indexada en Scopus y DOAJ

```
#### Indexación de la Revista
🔵 **Scopus** | 🟡 **DOAJ**
```

### Ejemplo 2: Revista Indexada en las Tres Bases

```
#### Indexación de la Revista
🔵 **Scopus** | 🟢 **CORE** | 🟡 **DOAJ**
```

### Ejemplo 3: Revista No Indexada

```
#### Indexación de la Revista
⚪ No indexada en bases de datos principales
```

---

## Comparación de Secciones del Dashboard

### Nivel LATAM (Región)
| Sección | Contenido |
|---------|-----------|
| KPIs Básicos | Revistas Indexadas, Total Artículos |
| Mapa Regional | Choropleth con indicadores seleccionables |
| Indicadores de Desempeño | Documentos, FWCI, Top 10%, Top 1%, Percentil |
| Distribución OA | Gráfico de pastel |
| **Indexación** | **% Scopus, % CORE, % DOAJ** (porcentajes) |
| Tendencias Anuales | Documentos, FWCI, Top%, OA por año |

### Nivel País
| Sección | Contenido |
|---------|-----------|
| KPIs Básicos | Revistas, Artículos |
| Top Revistas | Tabla de las 10 más citadas |
| Indicadores de Desempeño | Documentos, FWCI, Top 10%, Top 1%, Percentil |
| Distribución OA | Gráfico de pastel |
| **Indexación** | **% Scopus, % CORE, % DOAJ** (porcentajes) |
| Tendencias Anuales | Documentos, FWCI, Top%, OA por año |

### Nivel Revista (ACTUALIZADO ✨)
| Sección | Contenido |
|---------|-----------|
| Header | Nombre, ISSN, URL |
| Métricas Básicas | Total Documentos, Citas, Impacto 2yr, Índice H |
| Indicadores de Desempeño | Documentos, FWCI, Top 10%, Top 1%, Percentil |
| Distribución OA | Gráfico de pastel |
| **Indexación** | **🔵 Scopus \| 🟢 CORE \| 🟡 DOAJ** (badges) ← **NUEVO** |
| Tendencias Anuales | Documentos, FWCI, Top%, OA por año |

---

## Beneficios

### 1. **Consistencia Visual**
Ahora las tres secciones (LATAM, País, Revista) muestran información de indexación:
- **LATAM/País**: Porcentajes agregados
- **Revista**: Estado individual con badges visuales

### 2. **Información Rápida**
Los usuarios pueden ver de un vistazo si una revista está indexada en las principales bases de datos.

### 3. **Diseño Intuitivo**
- ✅ Emojis de colores para fácil identificación
- ✅ Formato de badges profesional
- ✅ Mensaje claro cuando no hay indexación

### 4. **Datos Completos**
Aprovecha las nuevas métricas `is_scopus`, `is_core`, `is_doaj` agregadas en los scripts de precálculo.

---

## Flujo de Datos

```
Datos de Origen (journals_df)
    ↓
    is_indexed_in_scopus, is_core, is_in_doaj
    ↓
Scripts de Precálculo
    ↓
    is_scopus, is_core, is_doaj (bool)
    ↓
metrics_journal_period.parquet
    ↓
Dashboard (load_cached_metrics)
    ↓
period_data.get('is_scopus', False)
    ↓
Badges Visuales: 🔵 🟢 🟡
```

---

## Pruebas Recomendadas

### 1. Verificar Datos
```python
import pandas as pd

# Cargar métricas de revista
journal_period = pd.read_parquet('data/cache/metrics_journal_period.parquet')

# Verificar columnas de indexación
print(journal_period[['journal_id', 'is_scopus', 'is_core', 'is_doaj']].head())

# Contar revistas por indexación
print(f"Scopus: {journal_period['is_scopus'].sum()}")
print(f"CORE: {journal_period['is_core'].sum()}")
print(f"DOAJ: {journal_period['is_doaj'].sum()}")
```

### 2. Ejecutar Dashboard
```bash
streamlit run dashboard.py
```

### 3. Navegar a Sección Revista
1. Seleccionar "Revista" en la barra lateral
2. Filtrar por país
3. Seleccionar una revista
4. Verificar que aparezca la sección "Indexación de la Revista"
5. Confirmar que los badges coincidan con los datos

---

## Casos de Uso

### Investigador
> "Quiero saber si esta revista está indexada en Scopus antes de enviar mi artículo"

**Solución**: Navegar a la revista y ver los badges de indexación inmediatamente.

### Administrador de Biblioteca
> "Necesito comparar la indexación de revistas latinoamericanas"

**Solución**: 
- Ver porcentajes agregados en nivel LATAM/País
- Ver estado individual en nivel Revista

### Evaluador de Calidad
> "Debo verificar la calidad de las publicaciones de un investigador"

**Solución**: Revisar las revistas donde publica y verificar sus badges de indexación.

---

## Próximos Pasos Sugeridos

### 1. Filtros por Indexación
Agregar filtros en la vista de País para mostrar solo revistas indexadas:
```python
indexing_filter = st.multiselect(
    "Filtrar por indexación",
    ["Scopus", "CORE", "DOAJ"]
)
```

### 2. Tabla Comparativa
Agregar una tabla que compare todas las revistas del país por indexación:
```python
st.dataframe(
    country_journals[['display_name', 'is_scopus', 'is_core', 'is_doaj']]
)
```

### 3. Estadísticas de Indexación
Agregar gráficos de barras mostrando cuántas revistas tienen cada tipo de indexación:
```python
fig = px.bar(
    x=['Scopus', 'CORE', 'DOAJ'],
    y=[scopus_count, core_count, doaj_count],
    title='Revistas por Base de Datos'
)
```

---

## Notas Técnicas

### Compatibilidad hacia Atrás
El código usa `.get()` con valor por defecto `False`:
```python
period_data.get('is_scopus', False)
```

Esto asegura que:
- ✅ Funciona con datos nuevos (con las columnas)
- ✅ Funciona con datos antiguos (sin las columnas)
- ✅ No genera errores si falta la columna

### Manejo de Valores Nulos
Los valores booleanos se extraen directamente:
- `True` → Muestra badge
- `False` → No muestra badge
- `None` / ausente → Tratado como `False`

---

## Resumen Visual

```
┌─────────────────────────────────────────────────┐
│         Dashboard - Nivel Revista               │
├─────────────────────────────────────────────────┤
│                                                 │
│  📊 Indicadores de Desempeño                    │
│  ┌─────┬─────┬─────┬─────┬─────┐              │
│  │ Doc │FWCI │Top10│Top1 │Perc │              │
│  └─────┴─────┴─────┴─────┴─────┘              │
│                                                 │
│  📈 Distribución de Acceso Abierto             │
│  [Gráfico de Pastel]                           │
│                                                 │
│  🔍 Indexación de la Revista      ← NUEVO ✨   │
│  🔵 Scopus | 🟡 DOAJ                           │
│                                                 │
│  📊 Tendencias Anuales                         │
│  [Gráficos de línea]                           │
│                                                 │
└─────────────────────────────────────────────────┘
```

---

## Conclusión

✅ **Dashboard actualizado** con sección de indexación a nivel revista

✅ **Consistencia** entre los tres niveles de análisis

✅ **Visualización intuitiva** con badges de colores

✅ **Datos completos** aprovechando las nuevas métricas

El dashboard ahora proporciona una vista completa de la indexación de revistas en todos los niveles de análisis.
