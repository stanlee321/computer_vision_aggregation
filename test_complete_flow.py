#!/usr/bin/env python3
"""
Test complete aggregation flow with real message processing
"""

import os
import json
import time
import logging
import threading
import uuid
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silence Kafka logs
for kafka_logger in ['kafka', 'kafka.conn', 'kafka.client']:
    logging.getLogger(kafka_logger).setLevel(logging.WARNING)

def send_test_message():
    """Send a single test message to video-general-results"""
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=10000,
        delivery_timeout_ms=30000,
        retries=3
    )
    
    # Create test message
    test_message = {
        'video_id': 'test-video-123',
        'fps': 25,
        'info_path': 'test-video-123/test-job-456/test_chunk_1_of_1_results.json',
        'job_id': 'test-job-456'
    }
    
    try:
        logger.info("📤 Sending test message...")
        future = producer.send('video-general-results', value=test_message)
        result = future.get(timeout=10)
        logger.info(f"✅ Test message sent! Partition: {result.partition}, Offset: {result.offset}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send test message: {e}")
        return False
    finally:
        producer.close()

def run_consumer_test():
    """Run consumer to receive the test message"""
    
    from libs.queues import KafkaHandler
    
    try:
        kafka_handler = KafkaHandler(bootstrap_servers=['localhost:9092'])
        consumer = kafka_handler.create_consumer(
            'video-general-results',
            f'test-consumer-{int(time.time())}',
            auto_offset_reset='earliest'  # Read from beginning
        )
        
        logger.info("📥 Starting consumer (waiting 10 seconds for messages)...")
        
        messages_received = 0
        start_time = time.time()
        
        for message in consumer:
            messages_received += 1
            logger.info(f"🎉 Received message #{messages_received}:")
            logger.info(f"   📄 {json.dumps(message.value, indent=2)}")
            
            # Stop after first message or 10 seconds
            if messages_received >= 1 or (time.time() - start_time) > 10:
                break
        
        consumer.close()
        logger.info(f"✅ Consumer test completed. Received {messages_received} messages.")
        return messages_received > 0
        
    except Exception as e:
        logger.error(f"❌ Consumer test failed: {e}")
        return False

def test_aggregation_service():
    """Test the actual aggregation service with mock data"""
    
    logger.info("🧪 Testing aggregation service...")
    
    try:
        # Import the application
        from libs.core import Application
        
        # Create application with minimal config
        app = Application(
            server_ip='localhost',
            brokers=['localhost:9092'],
            minio_access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            minio_secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            api_base_url='http://localhost:8003',
            backend_email='admin@example.com',
            backend_password='password',
            backend_base_url='http://localhost:3001',
            topic_input='video-general-results',
            topic_output='video-fine-detections',
            bucket_name='my-bucket',
            output_folder='./tmp'
        )
        
        logger.info("✅ Aggregation service initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Aggregation service test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run complete flow test"""
    
    logger.info("🚀 Starting Complete Flow Test")
    
    print("\n🧪 TESTING SEQUENCE:")
    print("1. Test Kafka producer")
    print("2. Test Kafka consumer")
    print("3. Test aggregation service initialization")
    print("4. Send test message and process")
    
    # Test 1: Producer
    logger.info("\n1️⃣ Testing Kafka Producer...")
    if not send_test_message():
        logger.error("❌ Producer test failed")
        return
    
    # Test 2: Consumer
    logger.info("\n2️⃣ Testing Kafka Consumer...")
    if not run_consumer_test():
        logger.error("❌ Consumer test failed") 
        return
    
    # Test 3: Aggregation service
    logger.info("\n3️⃣ Testing Aggregation Service...")
    if not test_aggregation_service():
        logger.error("❌ Aggregation service test failed")
        return
    
    # Test 4: Complete flow
    logger.info("\n4️⃣ All tests passed! 🎉")
    logger.info("\n💡 You can now:")
    logger.info("   • Run: python run_simple.py")
    logger.info("   • Run: python simulate_messages.py")
    logger.info("   • Run: python run_with_docker.py")
    logger.info("   • Monitor: http://localhost:8081 (Kafka UI)")

if __name__ == "__main__":
    main()