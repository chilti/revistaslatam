# Procesamiento Incremental de Métricas

## Descripción

El script `precompute_metrics_parallel_optimized.py` ahora soporta **procesamiento incremental**, lo que significa que:

- ✅ Solo calcula métricas para países y revistas **nuevas**
- ✅ Combina automáticamente resultados nuevos con existentes
- ✅ Ahorra tiempo y recursos al no recalcular todo
- ✅ Permite recuperación de errores sin empezar desde cero

## Uso

### Modo Incremental (por defecto)

```bash
python precompute_metrics_parallel_optimized.py
```

**Comportamiento:**
- Detecta qué países/revistas ya tienen métricas calculadas
- Solo procesa los que faltan
- Combina resultados nuevos con existentes
- Muestra estadísticas de lo que se procesó vs. lo que ya existía

**Ejemplo de salida:**
```
📊 Country metrics (chunked processing)...
  ℹ️  Found existing metrics for 15 countries
  📝 Processing 3 new countries...
  Processing 3 countries in 1 chunks of 3...
    Chunk 1/1: processing 3 countries... ✓ (12.3s)
  ✓ Combined 3 new countries with 15 existing
  ✓ Saved country period metrics: 18 total countries
```

### Modo Force (recalcular todo)

```bash
python precompute_metrics_parallel_optimized.py --force
```

**Comportamiento:**
- Ignora métricas existentes
- Recalcula **todo** desde cero
- Sobrescribe archivos existentes

**Cuándo usar `--force`:**
- Cambió la lógica de cálculo de métricas
- Los datos fuente fueron actualizados (nuevos works para revistas existentes)
- Sospechas que hay errores en métricas existentes
- Quieres asegurar consistencia total

## Ventajas del Modo Incremental

### 1. **Ahorro de Tiempo**
Si solo agregaste 10 revistas nuevas a una base de 1000:
- **Modo incremental**: Procesa solo 10 revistas (~1% del tiempo)
- **Modo force**: Procesa todas las 1000 revistas (100% del tiempo)

### 2. **Ahorro de Memoria**
Menos items a procesar = menos workers necesarios = menos uso de RAM

### 3. **Recuperación de Errores**
Si el script falla a mitad (por ejemplo, se quedó sin RAM):
- Al ejecutar de nuevo, continúa donde se quedó
- No pierde el progreso ya realizado

### 4. **Flujo de Trabajo Iterativo**
Puedes agregar datos gradualmente:
```bash
# Día 1: Procesar países iniciales
python precompute_metrics_parallel_optimized.py

# Día 2: Agregar más países, solo procesa los nuevos
python precompute_metrics_parallel_optimized.py

# Día 3: Agregar más revistas, solo procesa las nuevas
python precompute_metrics_parallel_optimized.py
```

## Archivos de Métricas

El script genera/actualiza estos archivos en `data/.cache/`:

| Archivo | Contenido |
|---------|-----------|
| `metrics_latam_annual.parquet` | Métricas anuales de LATAM (siempre se recalcula) |
| `metrics_latam_period.parquet` | Métricas de período de LATAM (siempre se recalcula) |
| `metrics_country_annual.parquet` | Métricas anuales por país (incremental) |
| `metrics_country_period.parquet` | Métricas de período por país (incremental) |
| `metrics_journal_annual.parquet` | Métricas anuales por revista (incremental) |
| `metrics_journal_period.parquet` | Métricas de período por revista (incremental) |

> **Nota**: Las métricas de LATAM siempre se recalculan porque son rápidas y dependen de todos los datos.

## Optimizaciones de Memoria

Además del procesamiento incremental, el script incluye:

1. **Inicialización de workers**: Cada proceso carga datos una sola vez
2. **Procesamiento por chunks**: Procesa en lotes pequeños para controlar memoria
3. **Uso conservador de cores**: Máximo 8 cores (25% del total)
4. **Liberación de memoria**: Elimina DataFrames del proceso principal después de usarlos

## Ejemplos de Uso

### Escenario 1: Primera ejecución
```bash
# Primera vez - procesa todo
python precompute_metrics_parallel_optimized.py
```

### Escenario 2: Agregar nuevos datos
```bash
# Después de agregar nuevas revistas/países
# Solo procesa los nuevos
python precompute_metrics_parallel_optimized.py
```

### Escenario 3: Actualización de datos existentes
```bash
# Si actualizaste works de revistas existentes
# Usa --force para recalcular todo
python precompute_metrics_parallel_optimized.py --force
```

### Escenario 4: Recuperación de error
```bash
# Si el script falló a mitad por falta de RAM
# Al ejecutar de nuevo, continúa donde se quedó
python precompute_metrics_parallel_optimized.py
```

## Comparación de Rendimiento

### Ejemplo con 1000 revistas

| Escenario | Revistas a procesar | Tiempo estimado | Uso de RAM |
|-----------|---------------------|-----------------|------------|
| Primera ejecución | 1000 | 60 min | ~8 GB |
| +50 revistas nuevas (incremental) | 50 | ~3 min | ~8 GB |
| +50 revistas nuevas (force) | 1000 | 60 min | ~8 GB |

### Ejemplo con 18 países

| Escenario | Países a procesar | Tiempo estimado |
|-----------|-------------------|-----------------|
| Primera ejecución | 18 | 5 min |
| +2 países nuevos (incremental) | 2 | ~30 seg |
| +2 países nuevos (force) | 18 | 5 min |

## Troubleshooting

### "All X items already processed"
**Causa**: Todas las métricas ya fueron calculadas.

**Solución**: 
- Si agregaste nuevos datos, verifica que estén en los archivos fuente
- Si quieres recalcular, usa `--force`

### Métricas inconsistentes
**Causa**: Datos fuente cambiaron pero métricas no se recalcularon.

**Solución**: Usa `--force` para recalcular todo

### Sigue quedándose sin memoria
**Solución**: Edita el script y reduce el número de cores:
```python
# Línea ~293
num_cores = min(4, max(1, int(total_cores * 0.125)))  # Solo 4 cores
```

## Ayuda

Para ver todas las opciones disponibles:
```bash
python precompute_metrics_parallel_optimized.py --help
```
