# Computer Vision Aggregation - Troubleshooting Guide

## Mejoras Implementadas

### 1. Logging Robusto
- **Archivo de logs**: `aggregation_service.log` y `aggregation_main.log`
- **Logs detallados** de cada paso del proceso
- **Identificación de errores** específicos en S3, API, y procesamiento

### 2. Manejo de Errores Mejorado
- **Reintento automático** (3 intentos) cuando faltan archivos
- **Backoff exponencial** entre reintentos
- **Validación de archivos** descargados (tamaño > 0)
- **Continuación del servicio** aunque fallen mensajes individuales

### 3. Diagnóstico de Archivos Faltantes
- **Verificación detallada** de archivos S3 faltantes
- **Análisis de patrones** en fallas de descarga
- **Detección de problemas** de bucket/conectividad

## Cómo Usar

### Ejecutar el Servicio Mejorado
```bash
cd ~/Proyects/Indexador/pyton-computer-vision/computer_vision_aggregation
python main.py
```

### Verificar Archivos Faltantes
```bash
# Verificar qué archivos faltan en S3
python check_missing_files.py

# Ver archivos recientes subidos
python check_missing_files.py --list
```

### Monitorear Logs
```bash
# Ver logs en tiempo real
tail -f aggregation_service.log

# Ver errores específicos
grep ERROR aggregation_service.log

# Ver archivos faltantes
grep "Missing.*files" aggregation_service.log
```

## Resolución de Problemas Comunes

### Problema: "Missing X files. Not all chunks are ready yet."

**Causa**: Algunos chunks del procesamiento de video no se están subiendo a S3.

**Solución**:
1. **Verificar el servicio de procesamiento de video**:
   ```bash
   # En el servidor CUDA, verificar que el servicio esté corriendo
   ps aux | grep python
   ```

2. **Verificar logs del servicio de procesamiento**:
   ```bash
   # Buscar errores de CUDA o memoria
   grep -i "cuda\|memory\|error" computer_vision_demos/logs/
   ```

3. **Usar el script de diagnóstico**:
   ```bash
   python check_missing_files.py
   ```

### Problema: "S3 operation failed; code: NoSuchKey"

**Causa**: El archivo resultado no fue subido por el servicio de procesamiento.

**Solución**:
1. **Verificar conectividad S3**:
   ```bash
   # Probar conexión a MinIO
   curl http://your-server:9000/health
   ```

2. **Revisar logs del procesamiento de video** para ver si hay errores durante la subida

3. **Re-procesar el video** si es necesario

### Problema: CUDA Out of Memory

**Causa**: El servidor CUDA se queda sin memoria GPU durante el procesamiento.

**Solución**:
1. **Reducir el batch size** en el procesamiento de video
2. **Procesar chunks más pequeños**
3. **Reiniciar el servicio** para limpiar memoria GPU:
   ```bash
   # Limpiar memoria GPU
   nvidia-smi --gpu-reset
   ```

## Logs Importantes a Revigar

### En aggregation_service.log:
```
# Buscar archivos faltantes
grep "Missing.*files" aggregation_service.log

# Buscar errores S3
grep "S3 error" aggregation_service.log

# Buscar reintentos
grep "Retrying" aggregation_service.log
```

### En el servidor CUDA (computer_vision_demos):
```
# Buscar errores de subida
grep "upload.*error" logs/

# Buscar problemas de memoria
grep -i "cuda\|memory" logs/

# Verificar que se suban los archivos JSON
grep "uploaded.*results.json" logs/
```

## Métricas de Monitoreo

El servicio mejorado ahora reporta:
- **Tiempo de procesamiento** por mensaje
- **Número de reintentos** necesarios
- **Archivos descargados vs esperados**
- **Chunks disponibles vs necesarios**
- **Errores específicos** con códigos S3

## Configuración de Ambiente

Asegúrate de que las variables de ambiente estén configuradas:

```bash
# .env file
IP_ADDRESS=192.168.1.252
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
BACKEND_EMAIL=admin@example.com
BACKEND_PASSWORD=Adminpassword1@
```

## Contacto para Soporte

Si el problema persiste después de seguir esta guía:
1. **Recopilar logs** de los últimos 30 minutos
2. **Ejecutar** `check_missing_files.py`
3. **Documentar** el comportamiento específico observado