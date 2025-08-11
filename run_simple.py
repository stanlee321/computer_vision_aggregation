#!/usr/bin/env python3
"""
Simple runner with minimal Kafka configuration
"""

import os
import sys
import logging
import json
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Configure minimal logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Silence Kafka logs
for logger_name in ['kafka', 'kafka.conn', 'kafka.client', 'kafka.coordinator']:
    logging.getLogger(logger_name).setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

class SimpleKafkaHandler:
    """Simplified Kafka handler with minimal configuration"""
    
    def __init__(self, bootstrap_servers):
        from kafka import KafkaConsumer, KafkaProducer
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        
    def create_consumer(self, topic, group_id, auto_offset_reset='latest'):
        """Create consumer with minimal, working configuration"""
        from kafka import KafkaConsumer
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            # Minimal timeouts that work
            session_timeout_ms=10000,      # 10 seconds
            request_timeout_ms=30000,      # 30 seconds (> session_timeout)
            heartbeat_interval_ms=3000,    # 3 seconds (< session_timeout/3)
            consumer_timeout_ms=5000       # 5 seconds
        )
        return consumer
    
    def create_producer(self):
        """Create producer with minimal configuration"""
        from kafka import KafkaProducer
        
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                request_timeout_ms=30000,
                retries=3,
                acks='all'
            )
        return self.producer
    
    def produce_message(self, topic, message):
        """Send message with simple error handling"""
        try:
            producer = self.create_producer()
            future = producer.send(topic, value=message)
            producer.flush()
            logger.info(f"✅ Message sent to {topic}")
        except Exception as e:
            logger.error(f"❌ Error sending message: {e}")

def main():
    """Main function with simplified configuration"""
    
    logger.info("🚀 Starting Simple Aggregation Service")
    
    try:
        # Simple configuration
        server_ip = 'localhost'
        brokers = [f'{server_ip}:9092']
        topic_input = "video-general-results"
        
        logger.info(f"📡 Connecting to Kafka: {brokers}")
        logger.info(f"📥 Topic: {topic_input}")
        
        # Create simple Kafka handler
        kafka_handler = SimpleKafkaHandler(bootstrap_servers=brokers)
        
        # Test connection
        logger.info("🔍 Testing connection...")
        consumer = kafka_handler.create_consumer(
            topic_input, 
            'simple-aggregator-test', 
            auto_offset_reset='latest'
        )
        logger.info("✅ Consumer created successfully")
        
        # Simple message loop
        logger.info("🔄 Waiting for messages... (Ctrl+C to stop)")
        message_count = 0
        
        for message in consumer:
            message_count += 1
            logger.info(f"📨 Message #{message_count}: {message.value}")
            
            # Here you would process the message
            # For now, just log it
            
        consumer.close()
        
    except KeyboardInterrupt:
        logger.info("⏹️  Shutting down...")
    except Exception as e:
        logger.error(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()