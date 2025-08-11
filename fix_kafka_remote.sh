#!/bin/bash
# Kafka Remote Fix Script
# Run this script on the remote server (192.168.1.252)

echo "🔧 Starting Kafka Fix Script on $(hostname)"
echo "============================================"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to print status
print_status() {
    if [ $? -eq 0 ]; then
        echo "✅ $1"
    else
        echo "❌ $1"
    fi
}

# 1. Check Docker
echo ""
echo "1️⃣ Checking Docker..."
docker --version
print_status "Docker is available"

# 2. Check Docker Compose
echo ""
echo "2️⃣ Checking Docker Compose..."
if command_exists docker-compose; then
    docker-compose --version
    print_status "Docker Compose is available"
else
    echo "⚠️  docker-compose not found, trying docker compose"
    docker compose version
    print_status "Docker Compose (plugin) is available"
fi

# 3. Check current containers
echo ""
echo "3️⃣ Current Docker containers:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 4. Check Kafka specifically
echo ""
echo "4️⃣ Kafka container status:"
kafka_running=$(docker ps --filter "name=kafka" --format "{{.Names}}" | grep kafka)
if [ -n "$kafka_running" ]; then
    echo "✅ Kafka container is running: $kafka_running"
    
    # Check Kafka logs
    echo ""
    echo "📋 Recent Kafka logs:"
    docker logs kafka --tail 10
else
    echo "❌ Kafka container is not running"
    
    # Try to start Kafka
    echo ""
    echo "🚀 Attempting to start Kafka..."
    if [ -f "docker-compose.yml" ]; then
        docker-compose up -d kafka
        print_status "Started Kafka with docker-compose"
    elif [ -f "docker-compose.yaml" ]; then
        docker-compose -f docker-compose.yaml up -d kafka
        print_status "Started Kafka with docker-compose.yaml"
    else
        echo "⚠️  docker-compose.yml not found in current directory"
        echo "Please navigate to the project directory and run:"
        echo "  docker-compose up -d kafka"
    fi
fi

# 5. Check port binding
echo ""
echo "5️⃣ Checking port 9092 binding..."
if command_exists netstat; then
    netstat -tlnp | grep :9092
    print_status "Port 9092 status (netstat)"
elif command_exists ss; then
    ss -tlnp | grep :9092
    print_status "Port 9092 status (ss)"
else
    echo "⚠️  Neither netstat nor ss available"
fi

# 6. Test local Kafka connection
echo ""
echo "6️⃣ Testing local Kafka connection..."
if command_exists telnet; then
    timeout 3 telnet localhost 9092 </dev/null
    print_status "Local Kafka connection test"
elif command_exists nc; then
    timeout 3 nc -zv localhost 9092
    print_status "Local Kafka connection test (nc)"
else
    echo "⚠️  Neither telnet nor nc available for connection test"
fi

# 7. Check system resources
echo ""
echo "7️⃣ System resources:"
echo "Disk space:"
df -h | head -5

echo ""
echo "Memory:"
free -h

echo ""
echo "Load:"
uptime

# 8. Check firewall
echo ""
echo "8️⃣ Firewall status:"
if command_exists ufw; then
    sudo ufw status | head -10
    
    # Check if port 9092 is allowed
    if sudo ufw status | grep -q 9092; then
        echo "✅ Port 9092 is allowed in UFW"
    else
        echo "⚠️  Port 9092 not explicitly allowed in UFW"
        echo "💡 You might need to run: sudo ufw allow 9092"
    fi
else
    echo "⚠️  UFW not available"
fi

# 9. Docker network inspection
echo ""
echo "9️⃣ Docker network inspection:"
kafka_container_id=$(docker ps --filter "name=kafka" --format "{{.ID}}" | head -1)
if [ -n "$kafka_container_id" ]; then
    echo "Kafka container network settings:"
    docker inspect $kafka_container_id | grep -A 5 -B 5 "NetworkMode\|Ports" || true
else
    echo "⚠️  No Kafka container found for network inspection"
fi

# 10. Suggested fixes
echo ""
echo "🔧 SUGGESTED FIXES:"
echo "=================="

if [ -z "$kafka_running" ]; then
    echo "❌ Kafka is not running:"
    echo "  1. Navigate to your docker-compose directory"
    echo "  2. Run: docker-compose up -d"
    echo "  3. Check logs: docker logs kafka"
fi

echo ""
echo "🔧 Common fixes to try:"
echo "  1. Restart all services: docker-compose down && docker-compose up -d"
echo "  2. Check Docker daemon: sudo systemctl status docker"
echo "  3. Restart Docker: sudo systemctl restart docker"
echo "  4. Allow port in firewall: sudo ufw allow 9092"
echo "  5. Check available space: df -h"
echo "  6. Prune unused containers: docker system prune -f"

echo ""
echo "🌐 Network troubleshooting:"
echo "  1. Test from localhost: telnet localhost 9092"
echo "  2. Test from remote: telnet $(hostname -I | awk '{print $1}') 9092"
echo "  3. Check Docker port mapping: docker port kafka"

echo ""
echo "============================================"
echo "✅ Kafka diagnostic completed on $(hostname)"
echo "💡 Send this output to diagnose the issue"