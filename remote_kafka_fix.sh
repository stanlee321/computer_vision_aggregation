#!/bin/bash
# Script para ejecutar DIRECTAMENTE en la PC remota (192.168.1.252)
# Copia este archivo al servidor y ejecútalo allí

clear
echo "🔧 KAFKA DIAGNOSTIC & FIX SCRIPT"
echo "================================="
echo "Hostname: $(hostname)"
echo "IP: $(hostname -I | awk '{print $1}')"
echo "Date: $(date)"
echo "================================="

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para imprimir con colores
print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }

# Función para ejecutar comandos con logging
run_cmd() {
    echo -e "\n${BLUE}🔍 Running: $1${NC}"
    eval $1
    if [ $? -eq 0 ]; then
        print_success "Command completed successfully"
    else
        print_error "Command failed"
    fi
}

echo ""
echo "1️⃣ CHECKING DOCKER STATUS"
echo "========================="

# Verificar si Docker está corriendo
if systemctl is-active --quiet docker; then
    print_success "Docker daemon is running"
    docker --version
else
    print_error "Docker daemon is not running"
    echo "🔧 Trying to start Docker..."
    sudo systemctl start docker
    sleep 3
    if systemctl is-active --quiet docker; then
        print_success "Docker started successfully"
    else
        print_error "Failed to start Docker"
        exit 1
    fi
fi

echo ""
echo "2️⃣ CHECKING DOCKER CONTAINERS"
echo "============================="

# Listar contenedores
run_cmd "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"

# Verificar específicamente Kafka
kafka_status=$(docker ps --filter "name=kafka" --format "{{.Names}}\t{{.Status}}")
if [ -n "$kafka_status" ]; then
    print_success "Kafka container found: $kafka_status"
else
    print_error "Kafka container is not running"
    
    # Buscar contenedor parado
    stopped_kafka=$(docker ps -a --filter "name=kafka" --format "{{.Names}}\t{{.Status}}")
    if [ -n "$stopped_kafka" ]; then
        print_warning "Found stopped Kafka container: $stopped_kafka"
    fi
fi

echo ""
echo "3️⃣ CHECKING KAFKA LOGS"
echo "======================"

if docker ps --filter "name=kafka" --format "{{.Names}}" | grep -q kafka; then
    print_info "Recent Kafka logs:"
    docker logs kafka --tail 20
else
    print_warning "Cannot show Kafka logs - container not running"
fi

echo ""
echo "4️⃣ CHECKING NETWORK PORTS"
echo "========================="

# Verificar puerto 9092
port_check=$(netstat -tlnp 2>/dev/null | grep :9092 || ss -tlnp 2>/dev/null | grep :9092)
if [ -n "$port_check" ]; then
    print_success "Port 9092 is bound:"
    echo "$port_check"
else
    print_error "Port 9092 is not bound"
fi

# Test local connection
print_info "Testing local Kafka connection..."
timeout 3 bash -c "</dev/tcp/localhost/9092" 2>/dev/null
if [ $? -eq 0 ]; then
    print_success "Local Kafka connection works"
else
    print_error "Cannot connect to local Kafka"
fi

echo ""
echo "5️⃣ CHECKING SYSTEM RESOURCES"
echo "============================"

print_info "Disk space:"
df -h | head -5

print_info "Memory usage:"
free -h

print_info "System load:"
uptime

print_info "Docker system usage:"
docker system df 2>/dev/null || print_warning "Docker system df not available"

echo ""
echo "6️⃣ CHECKING DOCKER COMPOSE"
echo "=========================="

# Buscar docker-compose.yml
compose_files=(
    "docker-compose.yml"
    "docker-compose.yaml" 
    "../docker-compose.yml"
    "../../docker-compose.yml"
    "/opt/docker-compose.yml"
    "$HOME/docker-compose.yml"
)

compose_file=""
for file in "${compose_files[@]}"; do
    if [ -f "$file" ]; then
        compose_file="$file"
        print_success "Found docker-compose file: $file"
        break
    fi
done

if [ -z "$compose_file" ]; then
    print_warning "No docker-compose.yml found in common locations"
    print_info "Please navigate to your docker-compose directory"
else
    print_info "Docker Compose status:"
    docker-compose -f "$compose_file" ps 2>/dev/null || print_warning "Could not run docker-compose ps"
fi

echo ""
echo "7️⃣ FIREWALL CHECK"
echo "================="

# UFW check
if command -v ufw >/dev/null 2>&1; then
    ufw_status=$(sudo ufw status 2>/dev/null)
    if echo "$ufw_status" | grep -q "Status: active"; then
        print_info "UFW is active"
        if echo "$ufw_status" | grep -q "9092"; then
            print_success "Port 9092 is allowed in UFW"
        else
            print_warning "Port 9092 is not explicitly allowed in UFW"
        fi
        echo "$ufw_status" | head -10
    else
        print_info "UFW is inactive"
    fi
else
    print_info "UFW not available"
fi

echo ""
echo "🔧 AUTOMATED FIXES"
echo "=================="

echo ""
read -p "🤔 Do you want to try automated fixes? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    
    echo "🔧 FIX 1: Allow port 9092 in firewall"
    if command -v ufw >/dev/null 2>&1; then
        sudo ufw allow 9092
        print_success "Port 9092 allowed in UFW"
    fi
    
    echo ""
    echo "🔧 FIX 2: Restart Kafka container"
    if [ -n "$compose_file" ]; then
        print_info "Restarting Kafka with docker-compose..."
        docker-compose -f "$compose_file" restart kafka
        print_success "Kafka restart command sent"
        sleep 5
    else
        print_info "Restarting Kafka container directly..."
        docker restart kafka 2>/dev/null && print_success "Kafka restarted" || print_warning "Could not restart kafka container"
    fi
    
    echo ""
    echo "🔧 FIX 3: Check if restart worked"
    sleep 3
    new_kafka_status=$(docker ps --filter "name=kafka" --format "{{.Names}}\t{{.Status}}")
    if [ -n "$new_kafka_status" ]; then
        print_success "Kafka is now running: $new_kafka_status"
        
        # Test connection again
        sleep 2
        timeout 3 bash -c "</dev/tcp/localhost/9092" 2>/dev/null
        if [ $? -eq 0 ]; then
            print_success "✨ SUCCESS! Kafka is now accessible locally"
        else
            print_warning "Kafka container is running but port not accessible yet (may need a few more seconds)"
        fi
    else
        print_error "Kafka is still not running"
        
        echo ""
        echo "🔧 FIX 4: Nuclear option - restart all containers"
        read -p "🚨 Try restarting ALL containers? (y/n): " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            if [ -n "$compose_file" ]; then
                print_info "Stopping all containers..."
                docker-compose -f "$compose_file" down
                print_info "Starting all containers..."
                docker-compose -f "$compose_file" up -d
                print_success "All containers restarted"
                
                sleep 10
                print_info "Final status check..."
                docker ps --filter "name=kafka" --format "{{.Names}}\t{{.Status}}"
            fi
        fi
    fi
fi

echo ""
echo "📋 FINAL STATUS REPORT"
echo "====================="

# Final checks
final_kafka=$(docker ps --filter "name=kafka" --format "{{.Names}}")
final_port=$(netstat -tlnp 2>/dev/null | grep :9092 || ss -tlnp 2>/dev/null | grep :9092)

if [ -n "$final_kafka" ] && [ -n "$final_port" ]; then
    print_success "🎉 KAFKA IS RUNNING AND PORT IS ACCESSIBLE!"
    echo ""
    print_info "Test from your Mac with:"
    echo "  telnet $(hostname -I | awk '{print $1}') 9092"
    echo ""
    print_info "Or run your Python aggregation service"
    
else
    print_error "❌ KAFKA IS STILL NOT WORKING PROPERLY"
    echo ""
    echo "🆘 MANUAL STEPS TO TRY:"
    echo "1. Check Docker logs: docker logs kafka"
    echo "2. Check disk space: df -h"  
    echo "3. Restart Docker daemon: sudo systemctl restart docker"
    echo "4. Check for port conflicts: sudo lsof -i :9092"
    echo "5. Recreate containers: docker-compose down && docker-compose up -d"
fi

echo ""
echo "📧 SEND THIS OUTPUT TO SUPPORT IF NEEDED"
echo "========================================"
echo "Hostname: $(hostname)"
echo "IP: $(hostname -I | awk '{print $1}')"
echo "Date: $(date)"
echo "Kafka Status: $(docker ps --filter 'name=kafka' --format '{{.Status}}' || echo 'Not running')"
echo "Port 9092: $(netstat -tlnp 2>/dev/null | grep :9092 >/dev/null && echo 'Bound' || echo 'Not bound')"

echo ""
echo "✅ Diagnostic completed!"