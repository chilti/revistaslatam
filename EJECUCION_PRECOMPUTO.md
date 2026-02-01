# Guía de Ejecución del Precálculo de Métricas

## ⚠️ Problema Identificado

El script paralelo con 24-32 núcleos **satura el servidor** y crea procesos zombies que no completan.

## ✅ Solución Implementada

**Configuración actualizada: 50% de núcleos (16 de 32)**

```python
# En precompute_metrics_parallel.py línea 202
num_cores = max(1, int(total_cores * 0.5))  # 50% = 16 núcleos
```

## 🚀 Cómo Ejecutar Correctamente

### Opción 1: Escritorio Remoto (Recomendado para primera vez)
```bash
# Desde escritorio remoto
cd /mnt/expansion/desplegados/revistaslatam
python3 precompute_metrics_parallel.py
```

**Ventajas:**
- Ves el progreso en tiempo real
- Puedes monitorear recursos
- Fácil de interrumpir si hay problemas

### Opción 2: Con nohup (Para dejar corriendo)
```bash
# Ejecutar en background
nohup python3 precompute_metrics_parallel.py > precompute.log 2>&1 &

# Ver progreso
tail -f precompute.log

# Ver PID del proceso
ps aux | grep precompute_metrics_parallel
```

### Opción 3: Con screen (Más flexible)
```bash
# Crear sesión
screen -S metrics

# Dentro de screen
python3 precompute_metrics_parallel.py

# Desconectar: Ctrl+A, luego D
# Reconectar: screen -r metrics
```

## 📊 Tiempo Estimado (16 núcleos)

- **Carga de datos**: ~30 segundos
- **LATAM**: ~15-20 segundos
- **Países (19)**: ~1-2 minutos
- **Revistas (7,715)**: ~10-20 minutos

**Total: 15-25 minutos**

## 🔍 Monitorear Recursos

En otra terminal SSH:

```bash
# Ver uso de CPU y RAM
htop

# O más simple
top

# Ver procesos Python
watch -n 2 'ps aux | grep python | head -20'

# Ver uso de RAM
free -h
```

## ✅ Verificar Archivos Generados

```bash
# Después de completar
ls -lh data/cache/

# Deberías ver:
# metrics_latam_annual.parquet
# metrics_latam_period.parquet
# metrics_country_annual.parquet
# metrics_country_period.parquet
# metrics_journal_annual.parquet
# metrics_journal_period.parquet
```

## 🛑 Si Necesitas Detener el Proceso

```bash
# Encontrar PID
ps aux | grep precompute_metrics_parallel

# Matar proceso (reemplaza PID)
kill -9 <PID>

# Verificar que se detuvo
ps aux | grep precompute_metrics_parallel
```

## 🔧 Ajustar Núcleos Manualmente

Si 16 núcleos aún es mucho, edita línea 202:

```python
# 25% de núcleos (8 de 32)
num_cores = max(1, int(total_cores * 0.25))

# 33% de núcleos (10-11 de 32)
num_cores = max(1, int(total_cores * 0.33))
```

## 📝 Notas Importantes

1. **Primera ejecución**: Usa escritorio remoto para ver que todo funciona
2. **RAM suficiente**: 128 GB es más que suficiente para los 3.7 GB de datos
3. **Disco externo**: El I/O puede ser lento, pero con datos en RAM no es problema
4. **Procesos zombies**: Si aparecen, mata el proceso principal y reinicia
5. **Archivos parciales**: Si se interrumpe, borra `data/cache/*` antes de reiniciar

## 🎯 Recomendación Final

**Para tu servidor (32 núcleos, 128 GB RAM):**
- Usa **16 núcleos (50%)** - Balance perfecto
- Ejecuta desde **escritorio remoto** la primera vez
- Tiempo total: **~20 minutos**
- Deja SSH disponible para monitoreo

Si 16 núcleos aún causa problemas, baja a 8 (25%).
