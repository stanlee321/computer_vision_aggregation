#!/usr/bin/env python3
"""
Script to send individual chunk messages as the demos processor would
Use this to simulate the demos processor sending messages for each chunk
"""

import os
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

# Configuration
SERVER_IP = os.getenv("IP_ADDRESS")
KAFKA_BROKERS = [f'{SERVER_IP}:9092']
TOPIC = "video-general-results"  # The topic that aggregator listens to

# ============================================================
# CONFIGURE YOUR CHUNKS HERE
# Based on your log, you have 2 chunks:
# ============================================================
CHUNKS = [
    {
        "video_id": "019896c6-b5b6-736d-88bb-5216a5b8ea77",
        "fps": 25,
        "info_path": "019896c6-b5b6-736d-88bb-5216a5b8ea77/0198977a-3e85-7084-ba5e-034e62cf8a00/tmpe0lw1b1y_chunk_1_of_2_results.json",
        "job_id": "0198977a-3e85-7084-ba5e-034e62cf8a00"
    },
    {
        "video_id": "019896c6-b5b6-736d-88bb-5216a5b8ea77",
        "fps": 25,
        "info_path": "019896c6-b5b6-736d-88bb-5216a5b8ea77/0198977a-3e85-7084-ba5e-034e62cf8a00/tmpe0lw1b1y_chunk_2_of_2_results.json",
        "job_id": "0198977a-3e85-7084-ba5e-034e62cf8a00"
    }
]
# ============================================================

def send_chunk_message(chunk_data, chunk_num):
    """Send a single chunk message"""
    
    print(f"\n{'='*60}")
    print(f"SENDING CHUNK {chunk_num} MESSAGE")
    print(f"{'='*60}")
    print(json.dumps(chunk_data, indent=2))
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        future = producer.send(TOPIC, value=chunk_data)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Chunk {chunk_num} sent successfully!")
        print(f"   Offset: {record_metadata.offset}")
        return True
        
    except KafkaError as e:
        print(f"❌ Failed to send chunk {chunk_num}: {e}")
        return False
    
    finally:
        producer.close()

def main():
    """Main function"""
    
    print("\n" + "="*60)
    print("CHUNK MESSAGE SENDER")
    print("="*60)
    print(f"Will send {len(CHUNKS)} chunk messages")
    print("="*60)
    
    # Ask for confirmation
    print("\nChunks to send:")
    for i, chunk in enumerate(CHUNKS, 1):
        print(f"\nChunk {i}:")
        print(f"  Video ID: {chunk['video_id']}")
        print(f"  Job ID: {chunk['job_id']}")
        print(f"  Info Path: {chunk['info_path']}")
    
    response = input("\nSend all chunk messages? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    # Send each chunk message
    success_count = 0
    for i, chunk in enumerate(CHUNKS, 1):
        if send_chunk_message(chunk, i):
            success_count += 1
        
        # Small delay between messages
        if i < len(CHUNKS):
            print(f"\nWaiting 1 second before next chunk...")
            time.sleep(1)
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Successfully sent: {success_count}/{len(CHUNKS)} chunks")
    
    if success_count == len(CHUNKS):
        print("\n✅ All chunks sent!")
        print("\nNOTE: The aggregator will wait until it receives messages")
        print("for ALL chunks before starting the aggregation process.")
        print("\nIf you sent these messages multiple times, the aggregator")
        print("might process them multiple times. Use clean_tasks.py to")
        print("clear old messages if needed.")
    else:
        print("\n⚠️  Some chunks failed to send")
        print("The aggregator won't be able to process until all chunks are available")

if __name__ == "__main__":
    main()