#!/usr/bin/env python3
"""
Clean runner for aggregation service - minimal logging
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Set up minimal logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# Completely silence Kafka internal logs
kafka_loggers = [
    'kafka', 'kafka.conn', 'kafka.client', 'kafka.coordinator', 
    'kafka.consumer', 'kafka.producer', 'kafka.cluster',
    'kafka.consumer.subscription_state', 'kafka.coordinator.consumer',
    'kafka.consumer.fetcher', 'kafka.metrics'
]

for logger_name in kafka_loggers:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)
    logging.getLogger(logger_name).disabled = True

logger = logging.getLogger(__name__)

def main():
    """Main function with clean output"""
    
    logger.info("🚀 Starting Computer Vision Aggregation Service (Clean Mode)")
    
    # Import and run the main application
    try:
        from libs.core import Application
        
        # Configuration
        SERVER_IP = os.getenv("IP_ADDRESS")
        minio_key = os.getenv("MINIO_ACCESS_KEY")
        minio_secret = os.getenv("MINIO_SECRET_KEY")
        
        if not all([SERVER_IP, minio_key, minio_secret]):
            logger.error("❌ Missing required environment variables")
            sys.exit(1)
        
        # Application settings
        API_BASE_URL = f"http://{SERVER_IP}:8003"
        BACKEND_EMAIL = os.getenv("BACKEND_EMAIL", "admin@example.com")
        BACKEND_PASSWORD = os.getenv("BACKEND_PASSWORD", "Adminpassword1@")
        BACKEND_BASE_URL = f"http://{SERVER_IP}:3001"
        TOPIC_INPUT = "video-general-results"
        TOPIC_OUTPUT = "video-fine-detections"
        brokers = [f'{SERVER_IP}:9092']
        BUCKET_NAME = "my-bucket"
        WORKING_FOLDER = "./tmp"
        
        logger.info(f"📡 Connecting to Kafka at {SERVER_IP}:9092")
        logger.info(f"📥 Listening to topic: {TOPIC_INPUT}")
        logger.info(f"🗂️  Working with bucket: {BUCKET_NAME}")
        
        # Create working directory
        os.makedirs(WORKING_FOLDER, exist_ok=True)
        
        # Initialize application
        app = Application(
            server_ip=SERVER_IP,
            brokers=brokers,
            minio_access_key=minio_key, 
            minio_secret_key=minio_secret,
            api_base_url=API_BASE_URL,
            backend_email=BACKEND_EMAIL,
            backend_password=BACKEND_PASSWORD,
            backend_base_url=BACKEND_BASE_URL,
            topic_input=TOPIC_INPUT,
            topic_output=TOPIC_OUTPUT,
            bucket_name=BUCKET_NAME,
            output_folder=WORKING_FOLDER
        )
        
        logger.info("✅ Application initialized")
        logger.info("🔄 Waiting for messages... (Press Ctrl+C to stop)")
        
        # Run the application
        app.run(offset='latest')
        
    except KeyboardInterrupt:
        logger.info("⏹️  Shutting down gracefully...")
        sys.exit(0)
    except ImportError as e:
        logger.error(f"❌ Import error: {e}")
        logger.error("💡 Make sure all dependencies are installed: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()