#!/usr/bin/env python3
"""
Script to clean old tasks from API and Kafka queues
"""
import os
import requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from dotenv import load_dotenv
import json
import time

load_dotenv()

SERVER_IP = os.getenv("IP_ADDRESS")
API_BASE_URL = f"http://{SERVER_IP}:8000"
KAFKA_BROKERS = [f'{SERVER_IP}:9092']

class TaskCleaner:
    def __init__(self):
        self.api_base_url = API_BASE_URL
        self.brokers = KAFKA_BROKERS
        
    def clean_api_tasks(self, video_id=None, status='pending'):
        """Clean old tasks from API"""
        print("\n=== Cleaning API Tasks ===")
        
        try:
            # Get all pending tasks
            if video_id:
                url = f"{self.api_base_url}/items/video_id/{video_id}/?status={status}"
            else:
                url = f"{self.api_base_url}/items/"
            
            response = requests.get(url)
            if response.status_code == 200:
                tasks = response.json()
                print(f"Found {len(tasks)} tasks in API")
                
                # Delete or update old tasks
                deleted_count = 0
                for task in tasks:
                    if 'status' in task and task['status'] == 'pending':
                        task_id = task.get('id')
                        if task_id:
                            # Update task status to 'cancelled' or delete it
                            update_response = requests.put(
                                f"{self.api_base_url}/items/{task_id}",
                                json={"status": "cancelled"}
                            )
                            if update_response.status_code == 200:
                                deleted_count += 1
                                print(f"Updated task {task_id} to cancelled")
                            else:
                                # Try to delete if update fails
                                delete_response = requests.delete(f"{self.api_base_url}/items/{task_id}")
                                if delete_response.status_code == 200:
                                    deleted_count += 1
                                    print(f"Deleted task {task_id}")
                
                print(f"Cleaned {deleted_count} tasks from API")
            else:
                print(f"Failed to get tasks from API: {response.status_code}")
                
        except Exception as e:
            print(f"Error cleaning API tasks: {e}")
    
    def consume_and_clear_topic(self, topic_name, max_messages=1000):
        """Consume all messages from a topic to clear it"""
        print(f"\n=== Clearing Topic: {topic_name} ===")
        
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.brokers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f'cleaner-{topic_name}-{int(time.time())}',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                consumer_timeout_ms=5000  # 5 second timeout
            )
            
            message_count = 0
            for message in consumer:
                message_count += 1
                print(f"Consumed message {message_count}: {message.key}")
                
                if message_count >= max_messages:
                    print(f"Reached max messages limit ({max_messages})")
                    break
            
            consumer.close()
            print(f"Consumed {message_count} messages from {topic_name}")
            
        except Exception as e:
            print(f"Error clearing topic {topic_name}: {e}")
    
    def reset_consumer_group_offset(self, topic_name, group_id):
        """Reset consumer group offset to latest"""
        print(f"\n=== Resetting Consumer Group: {group_id} for topic: {topic_name} ===")
        
        try:
            from kafka import KafkaConsumer
            
            # Create a consumer to reset offset
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.brokers,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset='latest'
            )
            
            # Subscribe and seek to end
            consumer.subscribe([topic_name])
            consumer.poll(timeout_ms=1000)
            consumer.seek_to_end()
            consumer.commit()
            consumer.close()
            
            print(f"Reset consumer group {group_id} to latest offset")
            
        except Exception as e:
            print(f"Error resetting consumer group: {e}")
    
    def clean_all(self, video_id=None):
        """Clean everything"""
        print("=" * 50)
        print("TASK CLEANER - Cleaning old tasks and queues")
        print("=" * 50)
        
        # Clean API tasks
        self.clean_api_tasks(video_id)
        
        # Clear Kafka topics
        topics_to_clear = [
            'video-general-results',
            'video-chunks',
            'video-results',
            'video-fine-detections'
        ]
        
        for topic in topics_to_clear:
            self.consume_and_clear_topic(topic)
        
        print("\n" + "=" * 50)
        print("Cleaning completed!")
        print("=" * 50)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean old tasks from API and Kafka')
    parser.add_argument('--video-id', help='Specific video ID to clean')
    parser.add_argument('--api-only', action='store_true', help='Only clean API tasks')
    parser.add_argument('--kafka-only', action='store_true', help='Only clean Kafka queues')
    parser.add_argument('--topic', help='Specific topic to clear')
    
    args = parser.parse_args()
    
    cleaner = TaskCleaner()
    
    if args.api_only:
        cleaner.clean_api_tasks(args.video_id)
    elif args.kafka_only:
        if args.topic:
            cleaner.consume_and_clear_topic(args.topic)
        else:
            topics = ['video-general-results', 'video-chunks', 'video-results', 'video-fine-detections']
            for topic in topics:
                cleaner.consume_and_clear_topic(topic)
    else:
        cleaner.clean_all(args.video_id)

if __name__ == "__main__":
    main()