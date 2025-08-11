#!/usr/bin/env python3
"""
Run aggregation service ON the remote server (kipustec-B650EGTQ)
This script is designed to run directly on the server where Docker containers are running
"""

import os
import sys
import logging
import socket
from pathlib import Path
from dotenv import load_dotenv

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from libs.core import Application

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('aggregation_remote.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def detect_kafka_on_server():
    """Detect Kafka configuration when running ON the remote server"""
    
    # When running on the server, try localhost first (Docker containers)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        
        if result == 0:
            logger.info("✅ Found Kafka on localhost:9092 (Docker containers)")
            return 'localhost:9092'
    except Exception as e:
        logger.debug(f"localhost test failed: {e}")
    
    # Try the server's own IP
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)  
        result = sock.connect_ex(('192.168.1.252', 9092))
        sock.close()
        
        if result == 0:
            logger.info("✅ Found Kafka on 192.168.1.252:9092")
            return '192.168.1.252:9092'
    except Exception as e:
        logger.debug(f"192.168.1.252 test failed: {e}")
    
    # Default fallback
    logger.warning("⚠️  Using fallback: localhost:9092")
    return 'localhost:9092'

def test_services():
    """Test connectivity to required services"""
    services = {
        'Kafka': ('localhost', 9092),
        'MinIO': ('localhost', 9000),
        'API': ('localhost', 8003),
        'Backend': ('localhost', 3001)
    }
    
    logger.info("🧪 Testing service connectivity...")
    results = {}
    
    for service_name, (host, port) in services.items():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                logger.info(f"✅ {service_name} ({host}:{port}): OK")
                results[service_name] = True
            else:
                logger.error(f"❌ {service_name} ({host}:{port}): FAILED")
                results[service_name] = False
        except Exception as e:
            logger.error(f"❌ {service_name} ({host}:{port}): ERROR - {e}")
            results[service_name] = False
    
    return results

def main():
    logger.info("🚀 Starting Computer Vision Aggregation Service (REMOTE SERVER MODE)")
    logger.info("=" * 70)
    
    # Test services first
    service_results = test_services()
    
    # Check critical services
    if not service_results.get('Kafka', False):
        logger.error("💥 CRITICAL: Kafka is not accessible!")
        logger.info("🔧 Try these commands:")
        logger.info("  docker ps | grep kafka")
        logger.info("  docker-compose restart kafka")
        logger.info("  docker logs kafka")
        sys.exit(1)
    
    # Detect Kafka broker
    kafka_broker = detect_kafka_on_server()
    
    # Configuration for REMOTE SERVER
    config = {
        'server_ip': 'localhost',  # Services are on localhost when running on server
        'minio_access_key': 'minio',
        'minio_secret_key': 'minio123',
        'brokers': [kafka_broker],
        'api_base_url': 'http://localhost:8003',
        'topic_input': 'video-general-results',
        'topic_output': 'video-processed',
        'bucket_name': 'video-chunks',
        'output_folder': './temp_processing',
        'backend_email': 'admin@gmail.com',
        'backend_password': '123456789',
        'backend_base_url': 'http://localhost:3001'
    }
    
    # Log configuration
    logger.info("🔧 Service Configuration:")
    logger.info(f"  📡 Kafka: {config['brokers']}")
    logger.info(f"  🗄️  MinIO: {config['server_ip']}:9000")
    logger.info(f"  🌐 API: {config['api_base_url']}")
    logger.info(f"  🏢 Backend: {config['backend_base_url']}")
    logger.info(f"  📂 Topic: {config['topic_input']} -> {config['topic_output']}")
    
    # Create output directory
    os.makedirs(config['output_folder'], exist_ok=True)
    
    try:
        logger.info("🏗️  Initializing application...")
        app = Application(**config)
        logger.info("✅ Application initialized successfully")
        
        logger.info("🔄 Starting consumer loop...")
        logger.info("   Press Ctrl+C to stop")
        app.run(offset='latest')
        
    except KeyboardInterrupt:
        logger.info("🛑 Service stopped by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()