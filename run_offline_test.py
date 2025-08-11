#!/usr/bin/env python3
"""
Offline test mode - simulate message processing without Kafka
"""

import os
import json
import logging
import time
from libs.core import Application
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_mock_message():
    """Create a mock message for testing"""
    return {
        'video_id': '019896c6-b5b6-736d-88bb-5216a5b8ea77',
        'fps': 25,
        'info_path': '019896c6-b5b6-736d-88bb-5216a5b8ea77/019896fd-be98-7220-be3f-22efcef982c7/test_chunk_1_of_2_results.json',
        'job_id': '019896fd-be98-7220-be3f-22efcef982c7'
    }

class MockMessage:
    def __init__(self, value):
        self.value = value

def main():
    logger.info("🧪 Starting Offline Test Mode")
    
    try:
        # Configuration
        SERVER_IP = os.getenv("IP_ADDRESS", "192.168.1.252")
        minio_key = os.getenv("MINIO_ACCESS_KEY")
        minio_secret = os.getenv("MINIO_SECRET_KEY")
        
        if not all([minio_key, minio_secret]):
            logger.error("❌ Missing MinIO credentials")
            return
        
        # Application settings (without Kafka)
        app = Application(
            server_ip=SERVER_IP,
            brokers=[],  # Empty brokers for offline mode
            minio_access_key=minio_key, 
            minio_secret_key=minio_secret,
            api_base_url=f"http://{SERVER_IP}:8003",
            backend_email="admin@example.com",
            backend_password="Adminpassword1@",
            backend_base_url=f"http://{SERVER_IP}:3001",
            topic_input="video-general-results",
            topic_output="video-fine-detections",
            bucket_name="my-bucket",
            output_folder="./tmp"
        )
        
        logger.info("✅ Application initialized in offline mode")
        
        # Create mock message
        mock_message_data = create_mock_message()
        mock_message = MockMessage(mock_message_data)
        
        logger.info(f"📝 Processing mock message: {json.dumps(mock_message_data, indent=2)}")
        
        # Process the mock message
        app.process_message(mock_message)
        
        logger.info("✅ Mock message processing completed")
        
    except Exception as e:
        logger.error(f"💥 Error in offline test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()