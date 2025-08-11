#!/usr/bin/env python3
"""
Simulate messages to test the aggregation service
"""

import json
import time
import logging
import uuid
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silence Kafka logs
for kafka_logger in ['kafka', 'kafka.conn', 'kafka.client']:
    logging.getLogger(kafka_logger).setLevel(logging.ERROR)

def create_mock_messages():
    """Create mock messages for testing"""
    
    video_id = '019896c6-b5b6-736d-88bb-5216a5b8ea77'
    job_id = f'job-{uuid.uuid4()}'
    
    # Simulate messages from computer_vision_demos to aggregation
    messages = []
    
    # Message 1: First chunk processed
    messages.append({
        'video_id': video_id,
        'fps': 25,
        'info_path': f'{video_id}/{job_id}/test_chunk_1_of_2_results.json',
        'job_id': job_id
    })
    
    # Message 2: Second chunk processed  
    messages.append({
        'video_id': video_id,
        'fps': 25,
        'info_path': f'{video_id}/{job_id}/test_chunk_2_of_2_results.json',
        'job_id': job_id
    })
    
    return messages

def create_producer():
    """Create Kafka producer with minimal config"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=10000,
        delivery_timeout_ms=30000,
        retries=3,
        acks='all'
    )
    return producer

def send_messages_gradually(messages, topic='video-general-results', delay=5):
    """Send messages with delay to simulate real processing"""
    
    producer = create_producer()
    
    try:
        for i, message in enumerate(messages, 1):
            logger.info(f"📤 Sending message {i}/{len(messages)} to '{topic}':")
            logger.info(f"   📄 {json.dumps(message, indent=2)}")
            
            future = producer.send(topic, value=message)
            result = future.get(timeout=10)
            
            logger.info(f"✅ Message {i} sent successfully")
            logger.info(f"   📍 Partition: {result.partition}, Offset: {result.offset}")
            
            if i < len(messages):
                logger.info(f"⏳ Waiting {delay} seconds before next message...")
                time.sleep(delay)
    
    except Exception as e:
        logger.error(f"❌ Error sending messages: {e}")
    finally:
        producer.close()

def create_mock_result_files():
    """Create mock result files that the aggregation service might need"""
    
    import os
    from minio import Minio
    
    # This would create mock files in MinIO, but for now just log what we would do
    logger.info("💡 In a real scenario, you would need:")
    logger.info("   1. Mock JSON result files in MinIO")
    logger.info("   2. Mock video chunks for processing")
    logger.info("   3. API responses for task management")
    
def main():
    """Main simulation function"""
    
    logger.info("🎭 Starting Message Simulation for Aggregation Service")
    
    # Create mock messages
    messages = create_mock_messages()
    
    logger.info(f"📝 Created {len(messages)} mock messages")
    
    # Ask user what to do
    print("\n🤔 Choose an option:")
    print("1. Send messages gradually (5 second delay)")
    print("2. Send all messages at once")
    print("3. Send one message for testing")
    print("4. Just show messages (don't send)")
    
    try:
        choice = input("\nEnter choice (1-4): ").strip()
        
        if choice == '1':
            send_messages_gradually(messages, delay=5)
        elif choice == '2':
            send_messages_gradually(messages, delay=0)
        elif choice == '3':
            send_messages_gradually([messages[0]], delay=0)
        elif choice == '4':
            for i, msg in enumerate(messages, 1):
                logger.info(f"📄 Message {i}: {json.dumps(msg, indent=2)}")
        else:
            logger.info("❌ Invalid choice")
            return
        
        logger.info("\n✅ Simulation completed!")
        logger.info("💡 Now run the aggregation service to process these messages:")
        logger.info("   python run_simple.py")
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Simulation interrupted")
    except Exception as e:
        logger.error(f"💥 Simulation error: {e}")

if __name__ == "__main__":
    main()