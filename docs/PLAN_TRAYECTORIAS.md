# Plan de Implementación: Visualización de Trayectorias de Desempeño

Este documento detalla las fases para implementar la visualización de trayectorias (Journal vs. País vs. Iberoamérica) en el dashboard, utilizando suavizado exponencial y proyección UMAP.

## Fase 1: Preparación de Datos y Algoritmos
**Objetivo:** Crear un script dedicado que procese las métricas anuales, aplique suavizado y genere las coordenadas de proyección.

### Tareas:
1.  **Crear script `pipeline/process_trajectories.py`**:
    *   **Entrada:** Leer los archivos `metrics_journal_annual.parquet`, `metrics_country_annual.parquet` y `metrics_latam_annual.parquet` generados previamente.
    *   **Procesamiento - Suavizado:**
        *   Aplicar medias móviles con ponderación exponencial.
        *   **Variante 1 (Suave):** `.rolling(3, min_periods=1, win_type='exponential', center=True).mean(tau=1)`.
        *   **Variante 2 (Intenso):** `.rolling(5, min_periods=1, win_type='exponential', center=True).mean(tau=1)`.
        *   Variables a suavizar: `num_documents`, `fwci_avg`, `avg_percentile`, `pct_top_1`, `pct_top_10`.
    *   **Procesamiento - Proyección (UMAP):**
        *   Estandarizar datos (Z-score).
        *   Reducir dimensionalidad de 5 indicadores a 2 coordenadas (X, Y) usando UMAP.
    *   **Salida:** Guardar cuatro archivos en `data/cache/`:
        *   `trajectory_data_raw.parquet`: Datos originales combinados.
        *   `trajectory_data_smoothed.parquet`: Datos suavizados (w=3).
        *   `trajectory_data_smoothed_w5.parquet`: Datos suavizados (w=5).
        *   `trajectory_coordinates.parquet`: Coordenadas X, Y lista para graficar (basado en w=3).

2.  **Dependencias:**
    *   Asegurar que `scipy` (para suavizado exponencial) y `umap-learn` (para proyección) estén instalados.

## Fase 2: Integración en el Dashboard
**Objetivo:** Visualizar los datos pre-calculados en `dashboard.py`.

### Tareas:
1.  **Localizar sección:** Identificar el punto de inserción después del gráfico "Sunburst".
2.  **Carga de datos:**
    *   Leer `trajectory_coordinates.parquet` para el gráfico.
    *   Leer `trajectory_data_raw.parquet` y `trajectory_data_smoothed.parquet` para las tablas.
3.  **Gráfico Interactivo (Plotly):**
    *   Filtrar datos para: Revista Seleccionada, País de la Revista, e Iberoamérica (referencia).
    *   Trazar líneas suavizadas (`line_shape='spline'`) conectando los años.
    *   Añadir marcadores por año.
4.  **Tablas de Datos:**
    *   Usar `st.expander` para ocultar/mostrar detalles.
    *   Crear pestañas (`st.tabs`): "Datos Crudos", "Suavizado (w=3)" y "Suavizado (w=5)".

## Fase 3: Automatización del Pipeline
**Objetivo:** Integrar el nuevo proceso en el flujo de actualización diario.

### Tareas:
1.  **Actualizar `run_pipeline.py`**:
    *   Añadir un "Paso 4" al final del script.
    *   Ejecutar `pipeline/process_trajectories.py` después de que `transform_metrics.py` haya terminado exitosamente.
    *   Manejar posibles errores (ej. si falta la librería `umap-learn`, el pipeline no debe romperse catastróficamente, solo saltar este paso).

## Fase 4: Ejecución y Verificación
**Objetivo:** Validar que todo funcione en conjunto.

### Pasos de Ejecución:
1.  Instalar librerías:
    ```bash
    pip install scipy umap-learn
    ```
2.  Ejecutar pipeline completo (para generar los parquets):
    ```bash
    python run_pipeline.py
    ```
3.  Iniciar Dashboard:
    ```bash
    streamlit run dashboard.py
    ```

### Criterios de Éxito:
*   [ ] El archivo `trajectory_coordinates.parquet` existe y tiene tamaño > 0.
*   [ ] El dashboard muestra el nuevo gráfico de trayectorias.
*   [ ] Las curvas del gráfico se ven suaves (no picos bruscos).
*   [ ] Las tablas de datos coinciden con los puntos del gráfico.
