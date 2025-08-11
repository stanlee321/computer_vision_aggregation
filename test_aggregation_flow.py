#!/usr/bin/env python3
"""
Test script for the complete aggregation flow
This will send the correct messages for the existing chunks in MinIO
"""

import os
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

# Configuration
SERVER_IP = os.getenv("IP_ADDRESS")
KAFKA_BROKERS = [f'{SERVER_IP}:9092']
TOPIC = "video-general-results"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL = f"{SERVER_IP}:9000"
BUCKET_NAME = "my-bucket"

# Video details (from your MinIO screenshot)
VIDEO_ID = "019896c6-b5b6-736d-88bb-5216a5b8ea77"
JOB_ID = "0198977a-3e85-7084-ba5e-034e62cf8a00"
FPS = 25

def verify_files_in_minio():
    """Verify that the required files exist in MinIO"""
    print("\n" + "="*60)
    print("VERIFYING FILES IN MINIO")
    print("="*60)
    
    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Expected files based on your MinIO screenshot
    expected_files = [
        f"{VIDEO_ID}/{JOB_ID}/tmpe0lw1b1y_chunk_1_of_2_results.json",
        f"{VIDEO_ID}/{JOB_ID}/tmpe0lw1b1y_chunk_2_of_2_results.json",
        f"{VIDEO_ID}/{JOB_ID}/tmpe0lw1b1y_chunk_1_of_2_annotated.mp4",
        f"{VIDEO_ID}/{JOB_ID}/tmpe0lw1b1y_chunk_2_of_2_annotated.mp4",
    ]
    
    all_exist = True
    for file_path in expected_files:
        try:
            client.stat_object(BUCKET_NAME, file_path)
            print(f"✅ Found: {file_path}")
        except:
            print(f"❌ Missing: {file_path}")
            all_exist = False
    
    return all_exist

def send_chunk_messages():
    """Send messages for both chunks"""
    
    # Messages for the two chunks that exist
    messages = [
        {
            "video_id": VIDEO_ID,
            "fps": FPS,
            "info_path": f"{VIDEO_ID}/{JOB_ID}/tmpe0lw1b1y_chunk_1_of_2_results.json",
            "job_id": JOB_ID
        },
        {
            "video_id": VIDEO_ID,
            "fps": FPS,
            "info_path": f"{VIDEO_ID}/{JOB_ID}/tmpe0lw1b1y_chunk_2_of_2_results.json",
            "job_id": JOB_ID
        }
    ]
    
    print("\n" + "="*60)
    print("SENDING KAFKA MESSAGES")
    print("="*60)
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    success_count = 0
    for i, msg in enumerate(messages, 1):
        print(f"\nSending chunk {i} message:")
        print(json.dumps(msg, indent=2))
        
        try:
            future = producer.send(TOPIC, value=msg)
            record_metadata = future.get(timeout=10)
            print(f"✅ Sent successfully! Offset: {record_metadata.offset}")
            success_count += 1
            
            # Small delay between messages
            if i < len(messages):
                time.sleep(0.5)
                
        except KafkaError as e:
            print(f"❌ Failed to send: {e}")
    
    producer.close()
    
    return success_count == len(messages)

def main():
    """Main test flow"""
    
    print("\n" + "="*60)
    print("AGGREGATION FLOW TEST")
    print("="*60)
    print(f"Video ID: {VIDEO_ID}")
    print(f"Job ID: {JOB_ID}")
    print(f"Topic: {TOPIC}")
    print("="*60)
    
    # Step 1: Verify files exist
    if not verify_files_in_minio():
        print("\n⚠️  WARNING: Some files are missing in MinIO")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return
    
    # Step 2: Send messages
    print("\nReady to send messages for aggregation.")
    response = input("Send messages? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    if send_chunk_messages():
        print("\n" + "="*60)
        print("✅ SUCCESS - Messages sent!")
        print("="*60)
        print("\nThe aggregator should now:")
        print("1. Receive both messages")
        print("2. Download the JSON results")
        print("3. Download the annotated videos")
        print("4. Combine everything into final output")
        print("5. Upload the aggregated results")
        print("\nCheck the aggregator logs to monitor progress.")
    else:
        print("\n❌ Failed to send all messages")

if __name__ == "__main__":
    main()