#!/usr/bin/env python3
"""
Script de verificación rápida para el servidor remoto
Ejecutar ANTES de run_remote_server.py para verificar que todo esté OK
"""

import socket
import subprocess
import json
import time
import sys

def test_port(host, port, service_name):
    """Test if a port is accessible"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✅ {service_name} ({host}:{port}): ACCESIBLE")
            return True
        else:
            print(f"❌ {service_name} ({host}:{port}): NO ACCESIBLE")
            return False
    except Exception as e:
        print(f"❌ {service_name} ({host}:{port}): ERROR - {e}")
        return False

def check_docker_containers():
    """Check Docker container status"""
    try:
        # Try JSON format first, fallback to table format
        result = subprocess.run(['docker', 'ps', '--format', 'json'], 
                              capture_output=True, text=True, timeout=10)
        
        containers = []
        if result.returncode == 0:
            # Parse JSON format
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    try:
                        container = json.loads(line)
                        containers.append(container)
                    except:
                        pass
        
        # Fallback to table format if JSON failed or empty
        if not containers:
            result = subprocess.run(['docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                print("❌ No se pudo ejecutar 'docker ps'")
                return False
            
            # Parse table format (skip header)
            lines = result.stdout.strip().split('\n')[1:] if result.stdout.strip() else []
            for line in lines:
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        containers.append({
                            'Names': parts[0].strip(),
                            'Status': parts[1].strip(),
                            'Ports': parts[2].strip() if len(parts) > 2 else ''
                        })
        
        print(f"\n🐳 CONTENEDORES DOCKER ({len(containers)} activos):")
        
        key_services = ['kafka', 'minio', 'postgres', 'redis']
        found_services = {}
        
        for container in containers:
            name = container.get('Names', 'unknown')
            status = container.get('Status', 'unknown')
            ports = container.get('Ports', '')
            
            # Check if it's a key service
            for service in key_services:
                if service in name.lower():
                    found_services[service] = {'name': name, 'status': status, 'ports': ports}
                    status_icon = "✅" if "Up" in status else "❌"
                    print(f"{status_icon} {service.upper()}: {name} ({status})")
                    break
        
        # Check missing services
        missing = [s for s in key_services if s not in found_services]
        if missing:
            print(f"⚠️  Servicios faltantes: {', '.join(missing)}")
            
        return len(found_services) >= 3  # At least 3 key services running
        
    except Exception as e:
        print(f"❌ Error verificando Docker: {e}")
        return False

def check_kafka_topics():
    """Check if Kafka topics exist"""
    try:
        # Try to list Kafka topics
        result = subprocess.run([
            'docker', 'exec', 'kafka', 
            'kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--list'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            print(f"\n📋 TOPICS DE KAFKA ({len(topics)}):")
            
            required_topics = ['video-general-results', 'video-input-general', 'video-chunks']
            found_topics = []
            
            for topic in topics:
                if topic.strip():
                    found_topics.append(topic.strip())
                    required = "✅" if topic.strip() in required_topics else "ℹ️"
                    print(f"{required} {topic.strip()}")
            
            missing_topics = [t for t in required_topics if t not in found_topics]
            if missing_topics:
                print(f"⚠️  Topics faltantes: {', '.join(missing_topics)}")
                
            return True
        else:
            print("❌ No se pudieron listar los topics de Kafka")
            return False
            
    except Exception as e:
        print(f"❌ Error verificando topics: {e}")
        return False

def main():
    print("🔍 VERIFICACIÓN DEL SERVIDOR REMOTO")
    print("=" * 50)
    
    # Test 1: Docker containers
    print("\n1️⃣ VERIFICANDO CONTENEDORES DOCKER:")
    docker_ok = check_docker_containers()
    
    # Test 2: Network connectivity
    print("\n2️⃣ VERIFICANDO CONECTIVIDAD DE RED:")
    services = {
        'Kafka': ('localhost', 9092),
        'MinIO': ('localhost', 9000),
        'API': ('localhost', 8003),
        'Backend': ('localhost', 3001),
        'PostgreSQL': ('localhost', 5432)
    }
    
    network_results = {}
    for service_name, (host, port) in services.items():
        network_results[service_name] = test_port(host, port, service_name)
    
    # Test 3: Kafka topics
    print("\n3️⃣ VERIFICANDO TOPICS DE KAFKA:")
    if network_results.get('Kafka', False):
        topics_ok = check_kafka_topics()
    else:
        print("❌ Kafka no accesible, no se pueden verificar topics")
        topics_ok = False
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 RESUMEN DE VERIFICACIÓN:")
    
    docker_status = "✅ OK" if docker_ok else "❌ PROBLEMAS"
    kafka_status = "✅ OK" if network_results.get('Kafka', False) else "❌ NO ACCESIBLE"
    minio_status = "✅ OK" if network_results.get('MinIO', False) else "❌ NO ACCESIBLE"
    api_status = "✅ OK" if network_results.get('API', False) else "❌ NO ACCESIBLE"
    
    print(f"🐳 Docker:     {docker_status}")
    print(f"📡 Kafka:      {kafka_status}")
    print(f"🗄️ MinIO:      {minio_status}")
    print(f"🌐 API:        {api_status}")
    
    # Final recommendation
    critical_services = ['Kafka', 'MinIO', 'API']
    all_critical_ok = all(network_results.get(service, False) for service in critical_services)
    
    if docker_ok and all_critical_ok:
        print("\n🎉 ¡TODO ESTÁ LISTO!")
        print("✅ Puedes ejecutar: python run_remote_server.py")
    else:
        print("\n⚠️  HAY PROBLEMAS QUE RESOLVER:")
        if not docker_ok:
            print("🔧 Ejecuta: docker-compose up -d")
        if not network_results.get('Kafka', False):
            print("🔧 Ejecuta: docker-compose restart kafka")
        print("🔧 Luego ejecuta este script de nuevo")
    
    print("=" * 50)

if __name__ == "__main__":
    main()