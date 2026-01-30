# Revistas LATAM (Análisis Bibliométrico)

Sistema de recolección y análisis de datos bibliométricos para revistas latinoamericanas indexadas, utilizando la API de OpenAlex.

## 🚀 Funcionalidades

- **Descarga Masiva**: Obtiene datos de miles de revistas latinoamericanas y sus artículos.
- **Procesamiento Inteligente**: Calcula indicadores como FWCI (Field-Weighted Citation Impact), percentiles de citas, Índice H, y más.
- **Dashboard Interactivo**: Visualización de datos con Streamlit y Plotly.
  - Análisis por Región, País y Revista.
  - Gráficos de impacto, redes de colaboración (futuro), y evolución temporal.

## 🛠️ Instalación

1.  **Clonar el repositorio**:
    ```bash
    git clone https://github.com/usuario/revistas_latam.git
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

## 📊 Uso

### 1. Recolección de Datos
El script `data_collector.py` descarga la información desde OpenAlex.
*Nota: La primera ejecución puede tardar varias horas dependiendo del volumen de datos.*

```bash
python src/data_collector.py
```
Esto generará archivos `.parquet` en la carpeta `data/`.

### 2. Ejecutar el Dashboard
Para visualizar los indicadores:

```bash
streamlit run dashboard.py
```

## 📂 Estructura del Proyecto

- `dashboard.py`: Aplicación principal (Streamlit).
- `src/`: Módulos de lógica.
  - `data_collector.py`: Interacción con API OpenAlex y guardado incremental.
  - `data_processor.py`: Limpieza y cálculo de KPIs generales.
  - `performance_metrics.py`: Cálculo avanzado de métricas (Normalización, Percentiles).
- `data/`: Almacenamiento de datos (ignorado en git por tamaño).

## 📝 Notas
- Este proyecto utiliza `pyalex` para interactuar con OpenAlex.
- Los datos complejos se almacenan como cadenas JSON dentro de archivos Parquet para máxima compatibilidad.

---
Desarrollado para el análisis de la ciencia en Latinoamérica. 🌎
