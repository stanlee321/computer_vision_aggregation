# 🚀 Instrucciones para Ejecutar en el Servidor Remoto (kipustec-B650EGTQ)

## ✅ Paso 1: Actualizar el código
```bash
cd /home/kipustec/Proyects/Indexador/pyton-computer-vision/computer_vision_aggregation
git pull origin dev
```

## ✅ Paso 2: Verificar que Kafka esté funcionando
```bash
# Verificar contenedores
docker ps | grep kafka

# Ver logs de Kafka
docker logs kafka --tail 10

# Test de conexión local
telnet localhost 9092
# (Presiona Ctrl+C para salir)
```

## ✅ Paso 3: Ejecutar el servicio de agregación
```bash
# Ejecutar el nuevo script optimizado para servidor remoto
python run_remote_server.py
```

## 🎯 Lo que deberías ver:
```
🚀 Starting Computer Vision Aggregation Service (REMOTE SERVER MODE)
🧪 Testing service connectivity...
✅ Kafka (localhost:9092): OK
✅ MinIO (localhost:9000): OK
✅ API (localhost:8003): OK
✅ Backend (localhost:3001): OK
🔧 Service Configuration:
  📡 Kafka: ['localhost:9092']
✅ Application initialized successfully
🔄 Creating Kafka consumer (attempt 1)...
✅ Starting consumer loop for topic: video-general-results
📨 Waiting for messages... (Ctrl+C to stop)
```

## 🔧 Si hay problemas:

### Problema: Kafka no responde
```bash
docker-compose restart kafka
# Esperar 10 segundos
docker logs kafka
```

### Problema: Servicios no disponibles
```bash
docker ps
docker-compose up -d
```

### Problema: Puerto ocupado o error de conexión
```bash
netstat -tlnp | grep :9092
sudo pkill -f python  # Matar procesos Python anteriores
```

## 🎉 Ventajas del Nuevo Sistema:

1. **Auto-reconexión**: Si se pierde la conexión a Kafka, se reconecta automáticamente
2. **Sin timeouts**: El consumer no se cierra por falta de mensajes
3. **Logs informativos**: Emojis y mensajes claros del estado
4. **Configuración optimizada**: Para el entorno Docker en el servidor
5. **Test automático**: Verifica todos los servicios antes de iniciar

## 📊 Para detener el servicio:
- Presiona `Ctrl+C` una vez (el sistema hará shutdown limpio)
- Si no responde: `Ctrl+Z` luego `kill %1`

## 🆘 Troubleshooting Rápido:

Si el servicio no funciona, ejecuta en orden:
```bash
1. docker ps | grep kafka
2. docker logs kafka --tail 20
3. telnet localhost 9092
4. python run_remote_server.py
```

¡El servicio ahora debería mantenerse activo permanentemente! 🚀