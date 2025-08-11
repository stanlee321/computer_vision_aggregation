#!/usr/bin/env python3
"""
Complete Kafka connectivity diagnosis
"""

import os
import sys
import socket
import time
import subprocess
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_network_connectivity(host, port):
    """Check if we can reach the host:port"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.error(f"Network error: {e}")
        return False

def check_ping(host):
    """Check if host responds to ping"""
    try:
        result = subprocess.run(['ping', '-c', '3', host], 
                              capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False

def check_dns_resolution(host):
    """Check if hostname resolves"""
    try:
        socket.gethostbyname(host)
        return True
    except Exception:
        return False

def diagnose_kafka_connectivity():
    """Complete Kafka connectivity diagnosis"""
    
    SERVER_IP = os.getenv("IP_ADDRESS", "192.168.1.252")
    KAFKA_PORT = 9092
    
    logger.info(f"🔍 Diagnosing Kafka connectivity to {SERVER_IP}:{KAFKA_PORT}")
    
    # Test 1: DNS Resolution
    logger.info("1️⃣ Testing DNS resolution...")
    if check_dns_resolution(SERVER_IP):
        logger.info(f"✅ DNS resolution for {SERVER_IP} successful")
    else:
        logger.error(f"❌ DNS resolution failed for {SERVER_IP}")
        return False
    
    # Test 2: Ping connectivity
    logger.info("2️⃣ Testing ping connectivity...")
    if check_ping(SERVER_IP):
        logger.info(f"✅ Ping to {SERVER_IP} successful")
    else:
        logger.error(f"❌ Ping to {SERVER_IP} failed")
        logger.warning("⚠️  Host may be unreachable or blocking ping")
    
    # Test 3: Port connectivity
    logger.info("3️⃣ Testing Kafka port connectivity...")
    if check_network_connectivity(SERVER_IP, KAFKA_PORT):
        logger.info(f"✅ Port {KAFKA_PORT} on {SERVER_IP} is reachable")
    else:
        logger.error(f"❌ Port {KAFKA_PORT} on {SERVER_IP} is not reachable")
        logger.error("💡 Possible causes:")
        logger.error("   - Kafka server is down")
        logger.error("   - Firewall blocking port 9092")
        logger.error("   - Kafka not listening on all interfaces")
        logger.error("   - Network connectivity issues")
        return False
    
    # Test 4: Other common ports
    logger.info("4️⃣ Testing other services...")
    services = [
        ("MinIO", 9000),
        ("API", 8003),
        ("Backend", 3001),
    ]
    
    for service_name, port in services:
        if check_network_connectivity(SERVER_IP, port):
            logger.info(f"✅ {service_name} ({port}) is reachable")
        else:
            logger.warning(f"⚠️  {service_name} ({port}) is not reachable")
    
    logger.info("✅ Basic connectivity tests completed")
    return True

def suggest_solutions():
    """Suggest solutions based on the diagnosis"""
    
    logger.info("\n💡 SUGGESTED SOLUTIONS:")
    
    logger.info("1. Check Kafka server status:")
    logger.info("   ssh user@192.168.1.252")
    logger.info("   sudo systemctl status kafka")
    logger.info("   sudo systemctl status zookeeper")
    
    logger.info("\n2. Restart Kafka if needed:")
    logger.info("   sudo systemctl restart zookeeper")
    logger.info("   sudo systemctl restart kafka")
    
    logger.info("\n3. Check Kafka configuration:")
    logger.info("   sudo cat /opt/kafka/config/server.properties | grep listeners")
    logger.info("   # Should have: listeners=PLAINTEXT://0.0.0.0:9092")
    
    logger.info("\n4. Check firewall:")
    logger.info("   sudo ufw status")
    logger.info("   sudo ufw allow 9092")
    
    logger.info("\n5. Check Kafka logs:")
    logger.info("   sudo journalctl -u kafka -f")
    logger.info("   tail -f /opt/kafka/logs/server.log")

def main():
    logger.info("🚀 Starting Kafka Connectivity Diagnosis")
    
    try:
        success = diagnose_kafka_connectivity()
        
        if not success:
            suggest_solutions()
            sys.exit(1)
        else:
            logger.info("🎉 All connectivity tests passed!")
            logger.info("💭 Kafka should be reachable. The issue might be:")
            logger.info("   - Kafka server configuration")
            logger.info("   - Topic doesn't exist yet")
            logger.info("   - Kafka authentication/authorization")
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.info("❌ Diagnosis interrupted")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Diagnosis failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()