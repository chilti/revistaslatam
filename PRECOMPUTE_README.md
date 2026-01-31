# Precálculo de Métricas - Versiones Disponibles

## 📊 Versiones del Script

### 1. `precompute_metrics.py` - Versión Estándar (Chunk-based)

**Uso recomendado**: Computadoras con RAM limitada (< 16 GB)

**Características**:
- ✅ Procesa datos en chunks (50,000 filas a la vez)
- ✅ Uso mínimo de RAM (~500 MB)
- ✅ Funciona en cualquier máquina
- ⏱️ Tiempo estimado: 2-4 horas para dataset completo

**Ejecución**:
```bash
python precompute_metrics.py
```

---

### 2. `precompute_metrics_parallel.py` - Versión Paralela (RAM-optimized)

**Uso recomendado**: Servidores con mucha RAM (> 32 GB) y múltiples núcleos

**Características**:
- ✅ Carga TODO el dataset a RAM (una sola vez)
- ✅ Paraleliza por país y revista usando todos los núcleos
- ✅ Speedup: 10-20x más rápido
- ⚡ Tiempo estimado: 10-30 minutos para dataset completo

**Requisitos**:
- RAM: ~8-10 GB disponibles (para dataset de 3.7 GB)
- CPU: Múltiples núcleos (aprovecha todos los disponibles)

**Ejecución**:
```bash
python precompute_metrics_parallel.py
```

**Salida esperada**:
```
🖥️  Detected 16 CPU cores
⚙️  Loading data to RAM...
  → Loading journals...
    ✓ 1,234 journals loaded
  → Loading works (this may take a minute)...
    ✓ 702,641 works loaded
  ✓ Data loaded in 45.2 seconds

📊 LATAM metrics...
  ✓ LATAM metrics completed in 12.3s

📊 Country metrics (using 16 cores)...
  Processing 19 countries in parallel...
  ✓ Country metrics completed in 34.5s

📊 Journal metrics (using 16 cores)...
  Processing 1,234 journals in parallel...
  ✓ Journal metrics completed in 156.7s

✅ ALL METRICS COMPUTED SUCCESSFULLY!
Total time: 248.7s (4.1 minutes)
Speedup: ~16x faster than sequential processing
```

---

### 3. `precompute_country_metrics.py` - Solo Países

**Uso recomendado**: Testing rápido o cuando solo necesitas métricas de países

**Características**:
- ✅ Solo calcula métricas de países (no journals individuales)
- ✅ Mucho más rápido que el completo
- ⏱️ Tiempo estimado: 30-60 minutos

**Ejecución**:
```bash
python precompute_country_metrics.py
```

---

## 🎯 ¿Cuál usar?

### En tu servidor (128 GB RAM, múltiples núcleos):
```bash
python precompute_metrics_parallel.py
```
**Razón**: Aprovecha toda la RAM y núcleos disponibles. Será 10-20x más rápido.

### En laptop/PC local (< 16 GB RAM):
```bash
python precompute_metrics.py
```
**Razón**: Usa chunks para no saturar la memoria.

### Para testing rápido:
```bash
python precompute_country_metrics.py
```
**Razón**: Solo países, mucho más rápido.

---

## 📁 Archivos Generados

Todos los scripts generan los mismos archivos en `data/cache/`:

```
data/cache/
├── metrics_latam_annual.parquet      # Métricas anuales LATAM
├── metrics_latam_period.parquet      # Métricas periodo LATAM
├── metrics_country_annual.parquet    # Métricas anuales por país
├── metrics_country_period.parquet    # Métricas periodo por país
├── metrics_journal_annual.parquet    # Métricas anuales por revista
└── metrics_journal_period.parquet    # Métricas periodo por revista
```

---

## ⚡ Comparación de Rendimiento

| Script | RAM Usada | Tiempo (3.7GB dataset) | Núcleos Usados |
|--------|-----------|------------------------|----------------|
| `precompute_metrics.py` | ~500 MB | 2-4 horas | 1 |
| `precompute_metrics_parallel.py` | ~8 GB | 10-30 min | Todos |
| `precompute_country_metrics.py` | ~2 GB | 30-60 min | 1 |

---

## 🔧 Troubleshooting

### Error: "MemoryError" o "Killed"
**Solución**: Usa `precompute_metrics.py` (versión chunk-based)

### Proceso muy lento en servidor
**Solución**: Verifica que estés usando `precompute_metrics_parallel.py`

### Solo necesito actualizar países
**Solución**: Usa `precompute_country_metrics.py` para ahorrar tiempo

---

## 📊 Monitoreo en Servidor

Para ver el uso de recursos mientras corre:
```bash
# En otra terminal
watch -n 2 'top -b -n 1 | head -20'

# O para ver solo Python
watch -n 2 'ps aux | grep python'
```
