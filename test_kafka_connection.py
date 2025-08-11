#!/usr/bin/env python3
"""
Test Kafka connection for the aggregation service
"""

import os
import sys
import time
import logging
from libs.queues import KafkaHandler
from dotenv import load_dotenv

load_dotenv()

# Configure logging to be minimal for testing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Silence Kafka logs completely for this test
logging.getLogger('kafka').setLevel(logging.CRITICAL)
logging.getLogger('kafka.conn').setLevel(logging.CRITICAL)
logging.getLogger('kafka.client').setLevel(logging.CRITICAL)
logging.getLogger('kafka.coordinator').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)

def test_kafka_connection():
    """Test Kafka connection without verbose logging"""
    
    SERVER_IP = os.getenv("IP_ADDRESS")
    if not SERVER_IP:
        logger.error("❌ IP_ADDRESS environment variable not set")
        return False
    
    brokers = [f'{SERVER_IP}:9092']
    topic_input = "video-general-results"
    
    logger.info(f"🔍 Testing Kafka connection to {brokers}")
    logger.info(f"📥 Topic: {topic_input}")
    
    try:
        # Initialize Kafka handler
        kafka_handler = KafkaHandler(bootstrap_servers=brokers)
        logger.info("✅ Kafka handler initialized")
        
        # Create consumer with short timeout
        logger.info("🔌 Creating consumer...")
        consumer = kafka_handler.create_consumer(
            topic_input, 
            'test-group-' + str(int(time.time())), 
            auto_offset_reset='latest'
        )
        logger.info("✅ Consumer created successfully")
        
        # Test connection by trying to get partitions
        logger.info("📊 Checking topic partitions...")
        partitions = consumer.partitions_for_topic(topic_input)
        if partitions is not None:
            logger.info(f"✅ Found {len(partitions)} partitions for topic '{topic_input}'")
        else:
            logger.warning(f"⚠️  Topic '{topic_input}' may not exist yet")
        
        # Close consumer
        consumer.close()
        logger.info("✅ Connection test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Kafka connection test failed: {e}")
        return False

def main():
    logger.info("🚀 Starting Kafka Connection Test")
    
    success = test_kafka_connection()
    
    if success:
        logger.info("🎉 All tests passed! Aggregation service should connect properly.")
        sys.exit(0)
    else:
        logger.error("💥 Connection test failed. Check Kafka server and network.")
        sys.exit(1)

if __name__ == "__main__":
    main()