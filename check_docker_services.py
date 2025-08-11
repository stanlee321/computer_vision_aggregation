#!/usr/bin/env python3
"""
Check Docker services status
"""

import subprocess
import socket
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_docker_containers():
    """Check which Docker containers are running"""
    try:
        result = subprocess.run(['docker', 'ps', '--format', 'json'], 
                              capture_output=True, text=True, check=True)
        
        containers = []
        for line in result.stdout.strip().split('\n'):
            if line:
                containers.append(json.loads(line))
        
        logger.info("🐳 Running Docker containers:")
        for container in containers:
            name = container.get('Names', 'unknown')
            image = container.get('Image', 'unknown')
            ports = container.get('Ports', 'none')
            logger.info(f"  📦 {name} ({image}) - {ports}")
        
        return containers
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to get Docker containers: {e}")
        return []
    except FileNotFoundError:
        logger.error("❌ Docker not found. Is Docker installed?")
        return []

def check_port_accessibility():
    """Check if Docker services are accessible"""
    services = [
        ("Kafka", "localhost", 9092),
        ("MinIO API", "localhost", 9000), 
        ("MinIO Console", "localhost", 9001),
        ("Kafka UI", "localhost", 8081),
        ("API Service", "localhost", 8003),
        ("Main App", "localhost", 3001),
        ("Postgres", "localhost", 5432),
        ("Redis", "localhost", 6379),
    ]
    
    logger.info("🔍 Checking service accessibility:")
    accessible_services = []
    
    for service_name, host, port in services:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                logger.info(f"  ✅ {service_name} ({host}:{port}) - Accessible")
                accessible_services.append((service_name, host, port))
            else:
                logger.warning(f"  ❌ {service_name} ({host}:{port}) - Not accessible")
                
        except Exception as e:
            logger.error(f"  💥 {service_name} ({host}:{port}) - Error: {e}")
    
    return accessible_services

def suggest_actions():
    """Suggest actions based on findings"""
    logger.info("\n💡 RECOMMENDATIONS:")
    
    containers = check_docker_containers()
    accessible = check_port_accessibility()
    
    kafka_running = any('kafka' in c.get('Names', '').lower() for c in containers)
    kafka_accessible = any('Kafka' in s[0] for s in accessible)
    
    if not containers:
        logger.info("1. Start Docker services:")
        logger.info("   docker-compose up -d")
        
    elif kafka_running and not kafka_accessible:
        logger.info("1. Kafka is running but not accessible:")
        logger.info("   docker logs kafka")
        logger.info("   docker-compose restart kafka")
        
    elif not kafka_running:
        logger.info("1. Kafka container is not running:")
        logger.info("   docker-compose up -d kafka")
        
    if kafka_accessible:
        logger.info("2. ✅ Kafka is accessible, run aggregation service:")
        logger.info("   python run_with_docker.py")
    else:
        logger.info("2. Fix Kafka connectivity first")

def main():
    logger.info("🔍 Checking Docker services for Computer Vision system")
    suggest_actions()

if __name__ == "__main__":
    main()