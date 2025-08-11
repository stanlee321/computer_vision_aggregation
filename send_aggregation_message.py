#!/usr/bin/env python3
"""
Script to send aggregation message after all chunks are processed
This sends a single message to trigger aggregation of all chunks
"""

import os
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

# Configuration
SERVER_IP = os.getenv("IP_ADDRESS")
KAFKA_BROKERS = [f'{SERVER_IP}:9092']
TOPIC = "video-general-results"  # The topic that aggregator listens to

# MinIO configuration
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL = f"{SERVER_IP}:9000"
BUCKET_NAME = "my-bucket"

# ============================================================
# CONFIGURE YOUR VIDEO DETAILS HERE
# ============================================================
VIDEO_ID = "019896c6-b5b6-736d-88bb-5216a5b8ea77"
JOB_ID = "0198977a-3e85-7084-ba5e-034e62cf8a00"
FPS = 25
TOTAL_CHUNKS = 2  # Change this based on your video chunks

# This path doesn't need to be a specific chunk - just a valid path for the aggregator
# The aggregator will find all chunks based on video_id and job_id
INFO_PATH = f"{VIDEO_ID}/{JOB_ID}/aggregation_trigger.json"
# ============================================================

def check_chunks_exist():
    """Check if all chunks exist in MinIO"""
    print("\n" + "="*60)
    print("CHECKING CHUNKS IN MINIO")
    print("="*60)
    
    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # List all objects in the job directory
    objects = client.list_objects(
        BUCKET_NAME,
        prefix=f"{VIDEO_ID}/{JOB_ID}/",
        recursive=True
    )
    
    found_chunks = []
    for obj in objects:
        if "_results.json" in obj.object_name:
            found_chunks.append(obj.object_name)
            print(f"✅ Found: {obj.object_name}")
    
    print(f"\nTotal chunks found: {len(found_chunks)}")
    
    # Check if we have all expected chunks
    if len(found_chunks) < TOTAL_CHUNKS:
        print(f"⚠️  WARNING: Expected {TOTAL_CHUNKS} chunks but found {len(found_chunks)}")
        print("Some chunks might still be processing...")
        return False, found_chunks
    
    return True, found_chunks

def send_aggregation_message():
    """Send the aggregation trigger message"""
    
    # First check if all chunks exist
    all_ready, chunks = check_chunks_exist()
    
    if not all_ready:
        print("\n" + "="*60)
        print("❌ NOT ALL CHUNKS ARE READY")
        print("="*60)
        print("Please wait for all chunks to finish processing before sending aggregation message")
        response = input("\nDo you want to send anyway? (y/n): ")
        if response.lower() != 'y':
            return False
    
    # Create the aggregation message
    message = {
        "video_id": VIDEO_ID,
        "fps": FPS,
        "info_path": INFO_PATH,
        "job_id": JOB_ID
    }
    
    print("\n" + "="*60)
    print("SENDING AGGREGATION MESSAGE")
    print("="*60)
    print(f"Topic: {TOPIC}")
    print(f"Message:")
    print(json.dumps(message, indent=2))
    print("="*60)
    
    # Send the message
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        future = producer.send(TOPIC, value=message)
        record_metadata = future.get(timeout=10)
        
        print("\n✅ Aggregation message sent successfully!")
        print(f"Topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")
        print("\nThe aggregator should now process all chunks and create the final output")
        return True
        
    except KafkaError as e:
        print(f"\n❌ Failed to send message: {e}")
        return False
    
    finally:
        producer.close()

def main():
    """Main function"""
    
    print("\n" + "="*60)
    print("AGGREGATION MESSAGE SENDER")
    print("="*60)
    print(f"\nVideo ID: {VIDEO_ID}")
    print(f"Job ID: {JOB_ID}")
    print(f"Expected chunks: {TOTAL_CHUNKS}")
    print("="*60)
    
    # Check and send
    if send_aggregation_message():
        print("\n" + "="*60)
        print("✅ SUCCESS")
        print("="*60)
        print("The aggregator will now:")
        print("1. Download all chunk results")
        print("2. Combine them into a single dataset")
        print("3. Create the final annotated video")
        print("4. Upload the complete results")
    else:
        print("\n" + "="*60)
        print("❌ FAILED")
        print("="*60)

if __name__ == "__main__":
    main()