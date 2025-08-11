#!/usr/bin/env python3
"""
Test script that cleans old tasks and sends a new message to the aggregator
"""
import os
import json
import time
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

SERVER_IP = os.getenv("IP_ADDRESS")
API_BASE_URL = f"http://{SERVER_IP}:8000"
KAFKA_BROKERS = [f'{SERVER_IP}:9092']

def clean_old_tasks(video_id):
    """Clean old pending tasks for a video_id"""
    print(f"\n=== Cleaning old tasks for video_id: {video_id} ===")
    
    try:
        # Get all pending tasks for this video
        url = f"{API_BASE_URL}/items/video_id/{video_id}/?status=pending"
        response = requests.get(url)
        
        if response.status_code == 200:
            tasks = response.json()
            print(f"Found {len(tasks)} pending tasks")
            
            # Mark all old tasks as cancelled
            for task in tasks:
                task_id = task.get('id')
                if task_id:
                    update_response = requests.put(
                        f"{API_BASE_URL}/items/{task_id}",
                        json={"status": "cancelled"}
                    )
                    if update_response.status_code == 200:
                        print(f"Cancelled task {task_id}")
                    else:
                        print(f"Failed to cancel task {task_id}")
        else:
            print(f"No pending tasks found or error: {response.status_code}")
            
    except Exception as e:
        print(f"Error cleaning tasks: {e}")

def consume_old_messages(topic, timeout=5):
    """Consume and discard old messages from a topic"""
    print(f"\n=== Consuming old messages from {topic} ===")
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'cleaner-{int(time.time())}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=timeout * 1000
        )
        
        count = 0
        for message in consumer:
            count += 1
            print(f"Consumed old message {count}")
        
        consumer.close()
        print(f"Consumed {count} old messages")
        
    except Exception as e:
        print(f"Error consuming old messages: {e}")

def send_test_message():
    """Send a test message to the aggregator"""
    print("\n=== Sending test message ===")
    
    # Test data - adjust these values according to your needs
    video_id = "019896c6-b5b6-736d-88bb-5216a5b8ea77"
    job_id = "0198977a-3e85-7084-ba5e-034e62cf8a00"
    
    # First, clean old tasks
    clean_old_tasks(video_id)
    
    # Consume old messages from the topic
    consume_old_messages("video-general-results", timeout=3)
    
    # Create the test message
    test_message = {
        "video_id": video_id,
        "job_id": job_id,
        "fps": 25,
        "info_path": f"{video_id}/{job_id}/complete_info.json"  # This should be a valid path
    }
    
    print(f"Message to send: {json.dumps(test_message, indent=2)}")
    
    # Send the message
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        future = producer.send('video-general-results', value=test_message)
        record_metadata = future.get(timeout=10)
        
        print(f"\nMessage sent successfully!")
        print(f"Topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")
        
    except KafkaError as e:
        print(f"Failed to send message: {e}")
    
    finally:
        producer.close()
    
    print("\n" + "=" * 50)
    print("Test completed!")
    print("The aggregator should now process only this message")
    print("=" * 50)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Send test message after cleaning old tasks')
    parser.add_argument('--video-id', default="019896c6-b5b6-736d-88bb-5216a5b8ea77", 
                       help='Video ID to use')
    parser.add_argument('--job-id', default="0198977a-3e85-7084-ba5e-034e62cf8a00",
                       help='Job ID to use')
    parser.add_argument('--clean-only', action='store_true',
                       help='Only clean, do not send new message')
    
    args = parser.parse_args()
    
    if args.clean_only:
        clean_old_tasks(args.video_id)
        consume_old_messages("video-general-results", timeout=5)
    else:
        # Clean and send
        send_test_message()

if __name__ == "__main__":
    main()