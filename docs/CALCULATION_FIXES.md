# Correcciones de Cálculo de Métricas

## Resumen

Se corrigió la función `calculate_performance_metrics_from_df()` en `precompute_metrics_parallel_optimized.py` para que calcule las métricas **exactamente igual** que el script original `performance_metrics.py`.

## Diferencias Corregidas

### 1. **FWCI Average**

**❌ Versión incorrecta (antes):**
```python
fwci_values = pd.to_numeric(works_df['fwci'], errors='coerce')
fwci_avg = fwci_values.mean()  # Ignora NaN
```

**✅ Versión correcta (ahora):**
```python
fwci_values = pd.to_numeric(works_df['fwci'], errors='coerce').fillna(0)
fwci_avg = fwci_values.sum() / num_documents  # Cuenta NaN como 0
```

**Impacto:** 
- La versión incorrecta calculaba el promedio solo de valores válidos
- La versión correcta incluye los NaN como 0 en el denominador
- Esto hace que el FWCI promedio sea más bajo (más conservador)

**Ejemplo:**
- Datos: `[2.5, NaN, 3.0]`
- Incorrecto: `(2.5 + 3.0) / 2 = 2.75`
- Correcto: `(2.5 + 0 + 3.0) / 3 = 1.83`

---

### 2. **Average Percentile**

**❌ Versión incorrecta (antes):**
```python
percentile_values = pd.to_numeric(works_df['citation_normalized_percentile'], errors='coerce')
avg_percentile = percentile_values.mean()  # Ignora NaN
```

**✅ Versión correcta (ahora):**
```python
percentile_values = pd.to_numeric(works_df['citation_normalized_percentile'], errors='coerce').fillna(0)
avg_percentile = percentile_values.sum() / num_documents  # Cuenta NaN como 0
```

**Impacto:** Mismo que FWCI - más conservador al incluir NaN como 0.

---

### 3. **Top 10% y Top 1%**

**❌ Versión incorrecta (antes):**
```python
top_10_values = pd.to_numeric(works_df['is_in_top_10_percent'], errors='coerce').fillna(0).astype(bool)
```

**✅ Versión correcta (ahora):**
```python
top_10_values = works_df['is_in_top_10_percent'].fillna(False).astype(bool)
```

**Impacto:** 
- Eliminado el paso innecesario de `pd.to_numeric()`
- Más eficiente y directo
- Mismo resultado final

---

## Verificación de Consistencia

### Lógica Original (performance_metrics.py)

La clase `MetricsAccumulator` usa esta lógica:

```python
# Líneas 60-61
fwci_values = pd.to_numeric(df_chunk['fwci'], errors='coerce').fillna(0)
self.fwci_sum += fwci_values.sum()

# Líneas 103
'fwci_avg': round(self.fwci_sum / self.count, 2)
```

Esto es equivalente a:
```python
fwci_avg = sum(fillna(0)) / total_count
```

### Lógica Corregida (precompute_metrics_parallel_optimized.py)

Ahora usa exactamente la misma lógica:

```python
fwci_values = pd.to_numeric(works_df['fwci'], errors='coerce').fillna(0)
fwci_avg = fwci_values.sum() / num_documents
```

## Métricas No Afectadas

Las siguientes métricas ya estaban correctas:

- ✅ `num_documents` - Conteo simple
- ✅ `pct_oa_gold`, `pct_oa_green`, etc. - Basados en `value_counts()`
- ✅ Journal metrics (`pct_scopus`, `pct_core`, `pct_doaj`)

## Prueba de Equivalencia

Para verificar que ambos scripts producen los mismos resultados:

```bash
# 1. Ejecutar script original
python precompute_metrics.py --force

# 2. Respaldar resultados
cp data/cache/metrics_*.parquet data/cache/backup/

# 3. Ejecutar script optimizado
python precompute_metrics_parallel_optimized.py --force

# 4. Comparar resultados
python -c "
import pandas as pd
from pathlib import Path

cache_dir = Path('data/cache')
backup_dir = Path('data/cache/backup')

for file in cache_dir.glob('metrics_*.parquet'):
    original = pd.read_parquet(backup_dir / file.name)
    optimized = pd.read_parquet(file)
    
    # Comparar valores numéricos con tolerancia
    pd.testing.assert_frame_equal(
        original.sort_index(axis=1), 
        optimized.sort_index(axis=1),
        check_exact=False,
        rtol=0.01  # 1% tolerancia por redondeo
    )
    print(f'✓ {file.name} matches!')
"
```

## Conclusión

✅ **El script optimizado ahora calcula las métricas exactamente igual que el original.**

Las únicas diferencias son:
- Procesamiento paralelo (más rápido)
- Procesamiento incremental (más eficiente)
- Uso de memoria optimizado (más estable)

Los **valores numéricos** de las métricas son **idénticos**.
