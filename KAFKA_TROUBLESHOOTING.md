# 🔧 Kafka Remote Troubleshooting Guide

## 🚀 Quick Start

### 1. **Ejecutar Diagnóstico Local**
```bash
cd ~/Proyects/Indexador/pyton-computer-vision/computer_vision_aggregation
python kafka_remote_diagnosis.py
```

### 2. **Ejecutar Diagnóstico en Servidor Remoto**
```bash
# Opción A: Copiar y ejecutar script
scp fix_kafka_remote.sh user@192.168.1.252:/tmp/
ssh user@192.168.1.252 'bash /tmp/fix_kafka_remote.sh'

# Opción B: Ejecutar directamente vía SSH
ssh user@192.168.1.252 'bash -s' < fix_kafka_remote.sh
```

## 🔍 Diagnósticos Paso a Paso

### **A. Verificar Conectividad de Red**

**En tu máquina local:**
```bash
# Test 1: Ping básico
ping 192.168.1.252

# Test 2: Puerto específico
telnet 192.168.1.252 9092
# o
nc -zv 192.168.1.252 9092

# Test 3: Múltiples puertos
nmap -p 9092,9000,8003,3001 192.168.1.252
```

### **B. Verificar Estado de Docker** (En servidor remoto)

```bash
# SSH al servidor
ssh user@192.168.1.252

# Verificar Docker
docker ps | grep kafka
docker logs kafka --tail 50

# Verificar Docker Compose
docker-compose ps
```

### **C. Comandos de Diagnóstico Rápido** (En servidor remoto)

```bash
# Estado del contenedor Kafka
docker inspect kafka | grep -A 10 -B 10 "NetworkSettings"

# Puerto binding
netstat -tlnp | grep :9092
# o
ss -tlnp | grep :9092

# Recursos del sistema
df -h          # Espacio en disco
free -h        # Memoria
docker system df  # Espacio usado por Docker
```

## 🛠️ Soluciones Comunes

### **Problema 1: Kafka Container No Ejecutando**
```bash
# Verificar estado
docker ps -a | grep kafka

# Reiniciar servicios
docker-compose down
docker-compose up -d

# Ver logs de error
docker logs kafka
```

### **Problema 2: Puerto 9092 No Accesible**
```bash
# Verificar firewall
sudo ufw status
sudo ufw allow 9092

# Verificar binding
docker port kafka

# Test local
telnet localhost 9092
```

### **Problema 3: Docker Network Issues**
```bash
# Recrear network
docker-compose down
docker network prune -f
docker-compose up -d

# Inspeccionar network
docker network ls
docker network inspect secvision-network
```

### **Problema 4: Configuración de Kafka**
```bash
# Verificar configuración del contenedor
docker exec kafka cat /opt/bitnami/kafka/config/server.properties | grep listeners

# Debería mostrar algo como:
# listeners=BROKER://kafka:9092
# advertised.listeners=BROKER://192.168.0.59:9092
```

### **Problema 5: Recursos Insuficientes**
```bash
# Limpiar Docker
docker system prune -f
docker volume prune -f

# Verificar espacio
df -h
free -h

# Reiniciar Docker daemon
sudo systemctl restart docker
```

## 🚨 Fixes de Emergencia

### **Fix 1: Reinicio Completo**
```bash
# En el servidor (como root o con sudo)
docker-compose down
sudo systemctl restart docker
docker-compose up -d
docker logs kafka -f
```

### **Fix 2: Kafka Container Recreate**
```bash
# Forzar recreación
docker-compose down
docker rm -f kafka
docker volume rm $(docker volume ls -q | grep kafka) 2>/dev/null || true
docker-compose up -d kafka
```

### **Fix 3: Network Reset**
```bash
# Reset completo de red
docker-compose down
docker network rm secvision-network 2>/dev/null || true
docker system prune -f --volumes
docker-compose up -d
```

## 📊 Verificación Final

### **Test de Funcionamiento:**
```bash
# En tu máquina local (Mac)
cd ~/Proyects/Indexador/pyton-computer-vision/computer_vision_aggregation
python test_complete_flow.py

# Si falla, verificar con:
telnet 192.168.1.252 9092
```

### **Verificar Topics:**
```bash
# En servidor remoto
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Crear topic si no existe
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic video-general-results --partitions 1 --replication-factor 1
```

## 🆘 Checklist de Emergencia

Ejecuta esto **en orden** si nada funciona:

```bash
# En servidor remoto
1. sudo systemctl status docker
2. docker ps
3. docker-compose down && docker-compose up -d
4. docker logs kafka
5. sudo ufw allow 9092
6. sudo systemctl restart docker
7. docker system prune -f
8. docker-compose up -d --force-recreate
```

## 📞 Contacto para Ayuda

Si necesitas ayuda remota:
1. Ejecuta: `python kafka_remote_diagnosis.py`
2. Ejecuta en servidor: `bash fix_kafka_remote.sh > kafka_diagnosis.log 2>&1`
3. Envía el output de ambos comandos

---

**¡La mayoría de problemas se resuelven con `docker-compose down && docker-compose up -d`!** 🚀