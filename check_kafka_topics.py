#!/usr/bin/env python3
"""
Check Kafka topics and messages
"""

import json
import logging
import sys
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silence Kafka internal logs
for kafka_logger in ['kafka', 'kafka.conn', 'kafka.client', 'kafka.coordinator']:
    logging.getLogger(kafka_logger).setLevel(logging.ERROR)

def list_kafka_topics():
    """List all available Kafka topics"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            request_timeout_ms=10000
        )
        
        metadata = admin_client.describe_cluster()
        topics = admin_client.list_topics()
        
        logger.info(f"📋 Available topics: {list(topics)}")
        return list(topics)
        
    except Exception as e:
        logger.error(f"❌ Failed to list topics: {e}")
        return []

def check_topic_messages(topic, max_messages=5):
    """Check recent messages in a topic"""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Get current offset
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            logger.warning(f"⚠️  No partitions found for topic '{topic}'")
            return []
        
        # Check for recent messages
        logger.info(f"🔍 Checking recent messages in '{topic}'...")
        messages = []
        
        try:
            for message in consumer:
                messages.append(message.value)
                logger.info(f"📨 Recent message: {json.dumps(message.value, indent=2)}")
                if len(messages) >= max_messages:
                    break
        except Exception as e:
            logger.info(f"ℹ️  No recent messages in '{topic}' (timeout)")
        
        consumer.close()
        return messages
        
    except Exception as e:
        logger.error(f"❌ Error checking topic '{topic}': {e}")
        return []

def main():
    """Main function to check Kafka status"""
    
    logger.info("🔍 Checking Kafka Topics and Messages")
    
    # List all topics
    topics = list_kafka_topics()
    
    if not topics:
        logger.error("❌ No topics found or Kafka not accessible")
        sys.exit(1)
    
    # Check specific topics we care about
    important_topics = [
        'video-input-general',
        'video-chunks', 
        'video-general-results'
    ]
    
    for topic in important_topics:
        if topic in topics:
            logger.info(f"✅ Topic '{topic}' exists")
            messages = check_topic_messages(topic, max_messages=3)
            logger.info(f"📊 Found {len(messages)} recent messages in '{topic}'")
        else:
            logger.warning(f"⚠️  Topic '{topic}' does not exist")
    
    # Suggest next steps
    logger.info("\n💡 NEXT STEPS:")
    
    if 'video-general-results' in topics:
        logger.info("1. ✅ Target topic exists, try the simple runner:")
        logger.info("   python run_simple.py")
    else:
        logger.info("1. ❌ Target topic 'video-general-results' missing")
        logger.info("   - Check if computer_vision_demos is running")
        logger.info("   - Check if it's sending messages to the right topic")
    
    logger.info("2. Monitor Kafka UI: http://localhost:8081")
    logger.info("3. Check producer services are running")

if __name__ == "__main__":
    main()