#!/usr/bin/env python3
"""
Run aggregation service with Docker networking support
"""

import os
import sys
import logging
import socket
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def detect_environment():
    """Detect if we're running with Docker services"""
    
    # Check if Docker Kafka is accessible on localhost
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        
        if result == 0:
            logger.info("✅ Detected Docker Kafka on localhost:9092")
            return 'docker'
    except:
        pass
    
    # Check if remote Kafka is accessible
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        sock.settimeout(3)
        result = sock.connect_ex(('192.168.1.252', 9092))
        sock.close()
        
        if result == 0:
            logger.info("✅ Detected remote Kafka on 192.168.1.252:9092")
            return 'remote'
    except:
        pass
    
    logger.warning("⚠️  No Kafka detected, defaulting to Docker mode")
    return 'docker'

def setup_environment(mode):
    """Setup environment variables based on detected mode"""
    
    if mode == 'docker':
        # Override environment variables for Docker
        os.environ['IP_ADDRESS'] = 'localhost'
        os.environ['KAFKA_BROKERS'] = 'localhost:9092'
        os.environ['MINIO_HOST'] = 'localhost'
        os.environ['API_HOST'] = 'localhost'
        logger.info("🐳 Using Docker networking (localhost)")
        
    elif mode == 'remote':
        # Use existing .env configuration
        load_dotenv()
        logger.info("🌐 Using remote networking (192.168.1.252)")
    
    # Ensure MinIO credentials are loaded
    if not os.getenv('MINIO_ACCESS_KEY'):
        load_dotenv()

def main():
    """Main function with auto-detection"""
    
    logger.info("🔍 Auto-detecting environment...")
    
    # Detect environment
    mode = detect_environment()
    
    # Setup environment
    setup_environment(mode)
    
    # Import and run application
    try:
        from libs.core import Application
        
        # Get configuration
        server_ip = os.getenv('IP_ADDRESS', 'localhost')
        minio_key = os.getenv('MINIO_ACCESS_KEY')
        minio_secret = os.getenv('MINIO_SECRET_KEY')
        
        if not all([minio_key, minio_secret]):
            logger.error("❌ Missing MinIO credentials")
            sys.exit(1)
        
        # Application settings
        brokers = [f'{server_ip}:9092']
        
        logger.info(f"📡 Connecting to Kafka: {brokers}")
        logger.info(f"🗄️  MinIO server: {server_ip}:9000")
        logger.info(f"🌐 API server: {server_ip}:8003")
        
        # Create application
        app = Application(
            server_ip=server_ip,
            brokers=brokers,
            minio_access_key=minio_key,
            minio_secret_key=minio_secret,
            api_base_url=f"http://{server_ip}:8003",
            backend_email=os.getenv("BACKEND_EMAIL", "admin@example.com"),
            backend_password=os.getenv("BACKEND_PASSWORD", "Adminpassword1@"),
            backend_base_url=f"http://{server_ip}:3001", 
            topic_input="video-general-results",
            topic_output="video-fine-detections",
            bucket_name="my-bucket",
            output_folder="./tmp"
        )
        
        logger.info("✅ Application initialized successfully")
        logger.info("🔄 Waiting for messages... (Ctrl+C to stop)")
        
        # Run the application
        app.run(offset='latest')
        
    except KeyboardInterrupt:
        logger.info("⏹️  Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()