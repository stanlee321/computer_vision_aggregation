# 🖥️ Comandos para Ejecutar en la PC Remota (192.168.1.252)

## 🚀 Opción 1: Script Automático (Recomendado)

### Copiar y ejecutar script completo:
```bash
# 1. Crear el archivo en la PC remota
cat > kafka_fix.sh << 'EOF'
[Aquí pegarías todo el contenido de remote_kafka_fix.sh]
EOF

# 2. Hacer ejecutable y correr
chmod +x kafka_fix.sh
./kafka_fix.sh
```

## 🔍 Opción 2: Comandos Individuales

### **PASO 1: Verificar Docker**
```bash
# Verificar si Docker está corriendo
sudo systemctl status docker

# Si no está corriendo, iniciarlo
sudo systemctl start docker

# Verificar versión
docker --version
docker-compose --version
```

### **PASO 2: Verificar Contenedores**
```bash
# Ver todos los contenedores
docker ps

# Ver específicamente Kafka
docker ps | grep kafka

# Ver contenedores parados también
docker ps -a | grep kafka

# Ver logs de Kafka
docker logs kafka --tail 20
```

### **PASO 3: Verificar Puertos**
```bash
# Verificar qué está usando el puerto 9092
netstat -tlnp | grep :9092
# O si no tienes netstat:
ss -tlnp | grep :9092

# Test de conexión local
telnet localhost 9092
# O
timeout 3 bash -c "</dev/tcp/localhost/9092" && echo "Kafka responde" || echo "Kafka no responde"
```

### **PASO 4: Verificar Recursos del Sistema**
```bash
# Espacio en disco
df -h

# Memoria
free -h

# Carga del sistema
uptime

# Espacio usado por Docker
docker system df
```

### **PASO 5: Verificar Firewall**
```bash
# Estado del firewall
sudo ufw status

# Permitir puerto 9092 (si está bloqueado)
sudo ufw allow 9092

# Verificar reglas iptables
sudo iptables -L | grep 9092
```

## 🔧 Fixes Comunes

### **Fix 1: Reiniciar Kafka**
```bash
# Opción A: Con docker-compose (si tienes el archivo)
docker-compose restart kafka

# Opción B: Contenedor directo
docker restart kafka

# Verificar que reinició
docker ps | grep kafka
```

### **Fix 2: Reiniciar Todos los Servicios**
```bash
# Parar todos los servicios
docker-compose down

# Iniciar todos los servicios
docker-compose up -d

# Ver el progreso
docker-compose logs -f kafka
```

### **Fix 3: Limpiar Docker (si hay problemas de espacio)**
```bash
# Limpiar contenedores parados
docker container prune -f

# Limpiar imágenes sin usar
docker image prune -f

# Limpiar todo el sistema
docker system prune -f

# Verificar espacio liberado
docker system df
```

### **Fix 4: Recrear Kafka Completamente**
```bash
# Parar Kafka
docker-compose stop kafka

# Eliminar contenedor
docker rm kafka

# Eliminar volumen (CUIDADO: esto borra datos)
docker volume rm $(docker volume ls | grep kafka | awk '{print $2}')

# Recrear
docker-compose up -d kafka

# Ver logs
docker logs -f kafka
```

## 🚨 Comandos de Emergencia

### **Si nada funciona, ejecuta EN ORDEN:**

```bash
# 1. Parar todo
docker-compose down

# 2. Limpiar completamente
docker system prune -f --volumes

# 3. Reiniciar Docker
sudo systemctl restart docker

# 4. Esperar un momento
sleep 10

# 5. Iniciar servicios
docker-compose up -d

# 6. Verificar estado
docker ps
docker logs kafka

# 7. Test final
telnet localhost 9092
```

## 📊 Verificación Final

### **Comandos para confirmar que todo funciona:**
```bash
# 1. Kafka corriendo
docker ps | grep kafka | grep "Up"

# 2. Puerto accesible
netstat -tlnp | grep :9092

# 3. Conexión local funciona
timeout 5 bash -c "</dev/tcp/localhost/9092" && echo "✅ OK" || echo "❌ FAIL"

# 4. Test desde la red externa (tu Mac)
# En tu Mac ejecuta: telnet 192.168.1.252 9092
```

## 📝 Información para Debug

### **Recopilar información para soporte:**
```bash
# Crear reporte completo
{
    echo "=== HOSTNAME ==="
    hostname
    echo "=== IP ADDRESS ==="
    hostname -I
    echo "=== DOCKER STATUS ==="
    sudo systemctl status docker
    echo "=== CONTAINERS ==="
    docker ps -a
    echo "=== KAFKA LOGS ==="
    docker logs kafka --tail 50
    echo "=== PORTS ==="
    netstat -tlnp | grep :9092
    echo "=== DISK SPACE ==="
    df -h
    echo "=== MEMORY ==="
    free -h
    echo "=== FIREWALL ==="
    sudo ufw status
} > kafka_debug_report.txt

# Ver el reporte
cat kafka_debug_report.txt
```

---

## 🎯 **Pasos Recomendados:**

1. **Ejecuta primero:** `docker ps | grep kafka`
2. **Si no hay Kafka:** `docker-compose up -d kafka`
3. **Si hay errores:** `docker logs kafka --tail 20`
4. **Test conectividad:** `telnet localhost 9092`
5. **Si falla:** Ejecuta el script completo `kafka_fix.sh`

**¡La mayoría se arregla con `docker-compose restart kafka`!** 🚀