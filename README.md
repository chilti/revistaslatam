# Análisis Bibliométrico (Revistas LATAM)

Este proyecto es una plataforma integral de recolección, procesamiento y visualización de datos bibliométricos a gran escala, impulsada por la API de OpenAlex. Su objetivo principal es evaluar el impacto y la evolución de la producción científica a través de indicadores avanzados (como FWCI e Índice H) y representaciones visuales interactivas. Se centra en un análisis detallado del impacto y alcance de las revistas científicas latinoamericanas, gestionado a través de `dashboard.py` y los pipelines especializados en `pipeline_revistaslatam/`.

## 🌐 Despliegue
El sistema se encuentra desplegado y accesible en:
- **Servidor Principal**: [https://dinamica1.fciencias.unam.mx/revistaslatam/](https://dinamica1.fciencias.unam.mx/revistaslatam/)
- **Servidor Espejo**: [https://dinamica10.fciencias.unam.mx/revistaslatam/](https://dinamica10.fciencias.unam.mx/revistaslatam/)



## 🚀 Funcionalidades

- **Descarga Masiva**: Obtiene datos de miles de revistas latinoamericanas y sus artículos.
- **Procesamiento Inteligente**: Calcula indicadores como FWCI (Field-Weighted Citation Impact), percentiles de citas, Índice H, y más.
- **Dashboard Interactivo**: Visualización de datos con Streamlit y Plotly.
  - Análisis por Región, País y Revista.
  - Gráficos de impacto, redes de colaboración (futuro), y evolución temporal.

## 🛠️ Instalación

1.  **Clonar el repositorio**:
    ```bash
    git clone https://github.com/chilti/revistas_latam.git
    cd revistas_latam
    ```

2.  **Crear un entorno virtual** (recomendado):
    ```bash
    python -m venv .venv
    # Windows:
    .venv\Scripts\activate
    # Linux/Mac:
    source .venv/bin/activate
    ```

3.  **Instalar dependencias**:
    ```bash
    pip install -r requirements.txt
    ```

## 📊 Arquitectura y Uso

- **Dashboard:** `dashboard.py`
- **Pipelines:** Scripts ubicados en la carpeta `pipeline_revistaslatam/` (y scripts legacy en la raíz o en `src/`).

---

### Uso

#### 1. Recolección de Datos
El script `data_collector.py` (o equivalentes en `pipeline_revistaslatam/`) descarga la información desde OpenAlex.
*Nota: La primera ejecución puede tardar varias horas dependiendo del volumen de datos.*

```bash
python src/data_collector.py
```
Esto generará archivos `.parquet` en la carpeta `data/`.

### 2. Precalcular Indicadores (Recomendado)
Después de descargar los artículos, ejecuta el script de precálculo para acelerar el dashboard.

#### Opción A: Script Optimizado Paralelo (Recomendado)
Para máquinas con múltiples cores y grandes volúmenes de datos:

```bash
python precompute_metrics_parallel_optimized.py
```

**Características**:
- ✅ **Procesamiento paralelo**: Usa múltiples cores para acelerar el cálculo
- ✅ **Procesamiento incremental**: Solo calcula métricas para revistas/países nuevos
- ✅ **Optimizado para memoria**: Procesa en chunks para evitar saturar la RAM
- ✅ **Recuperación de errores**: Continúa donde se quedó si falla

**Opciones**:
- `--force`: Recalcula todas las métricas desde cero (ignora cache)

**Ejemplo**:
```bash
# Primera ejecución - calcula todo
python precompute_metrics_parallel_optimized.py

# Ejecuciones posteriores - solo procesa lo nuevo
python precompute_metrics_parallel_optimized.py

# Forzar recálculo completo
python precompute_metrics_parallel_optimized.py --force
```

**Documentación detallada**:
- 📖 [Guía de Cálculo de Métricas](METRICS_CALCULATION_GUIDE.md) - Explicación detallada de cómo se calcula cada métrica
- 📖 [Procesamiento Incremental](INCREMENTAL_PROCESSING.md) - Cómo funciona el modo incremental
- 📖 [Correcciones de Cálculo](CALCULATION_FIXES.md) - Validación de consistencia con script original

#### Opción B: Script Original (Más Simple)
Para datasets pequeños o primera vez:

```bash
python precompute_metrics.py
```

**Opciones**:
- `--force`: Forzar recálculo aunque exista caché válido

---

**Métricas calculadas** (ambos scripts):

- FWCI (Field-Weighted Citation Impact)
- Percentiles de citas
- % Top 10% (artículos altamente citados)
- % Artículos en acceso abierto
- % Revistas indexadas en Scopus, CORE, DOAJ

Los resultados se guardan en `data/cache/` y el dashboard los cargará automáticamente.

**Opciones:**
- `python precompute_metrics.py --force`: Forzar recálculo aunque exista caché válido

### 3. Ejecutar el Dashboard
Para visualizar los indicadores de las revistas latinoamericanas:

```bash
streamlit run dashboard.py
```




## 📂 Estructura del Proyecto

### Dashboards
- `dashboard.py`: Aplicación principal (Streamlit) para **Revistas LATAM**.

### Pipelines
- `pipeline_revistaslatam/`: Scripts originales y legacy para el procesamiento de **Revistas LATAM**.

### Otros Archivos y Módulos Legacy
- `precompute_metrics*.py`: Scripts para precalcular indicadores en la versión LATAM.
- `src/`: Módulos de lógica originariamente usados por LATAM (`data_collector.py`, `data_processor.py`, `performance_metrics.py`).
- `data/`: Almacenamiento de datos y caché local.

### 📚 Documentación

- **[METRICS_CALCULATION_GUIDE.md](METRICS_CALCULATION_GUIDE.md)**: Guía completa y detallada de cómo se calcula cada métrica
  - Definiciones de todas las métricas
  - Fórmulas y ejemplos paso a paso
  - Proceso de cálculo paralelo
  - Validación de resultados
  
- **[INCREMENTAL_PROCESSING.md](INCREMENTAL_PROCESSING.md)**: Documentación del procesamiento incremental
  - Cómo funciona el modo incremental
  - Ventajas y casos de uso
  - Comparación de rendimiento
  
- **[CALCULATION_FIXES.md](CALCULATION_FIXES.md)**: Validación de consistencia con script original
  - Correcciones aplicadas
  - Verificación de equivalencia


## 📝 Notas
- Este proyecto utiliza `pyalex` para interactuar con OpenAlex.
- Los datos complejos se almacenan como cadenas JSON dentro de archivos Parquet para máxima compatibilidad.

---
Desarrollado para el análisis de la ciencia en Latinoamérica. 🌎
