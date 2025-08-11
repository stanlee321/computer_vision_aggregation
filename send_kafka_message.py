#!/usr/bin/env python3
"""
Script to manually send a Kafka message to the aggregator
Copy the JSON from the demos log and paste it in the message_data variable
"""

import os
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

# Configuration
SERVER_IP = os.getenv("IP_ADDRESS")
KAFKA_BROKERS = [f'{SERVER_IP}:9092']
TOPIC = "video-general-results"  # The topic that aggregator listens to

# ============================================================
# PASTE YOUR MESSAGE HERE (copy from demos log)
# ============================================================
message_data = {
  "video_id": "",
  "fps": 0,
  "info_path": "",
  "job_id": ""
}
# ============================================================

def send_message():
    """Send the message to Kafka"""
    
    # Validate message
    if not message_data.get("video_id"):
        print("ERROR: video_id is empty! Please paste the message from demos log.")
        return
    
    if not message_data.get("job_id"):
        print("ERROR: job_id is empty! Please paste the message from demos log.")
        return
    
    if not message_data.get("info_path"):
        print("ERROR: info_path is empty! Please paste the message from demos log.")
        return
    
    print("\n" + "="*60)
    print("SENDING MESSAGE TO KAFKA")
    print("="*60)
    print(f"Topic: {TOPIC}")
    print(f"Brokers: {KAFKA_BROKERS}")
    print("\nMessage:")
    print(json.dumps(message_data, indent=2))
    print("="*60)
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        # Send the message
        future = producer.send(TOPIC, value=message_data)
        
        # Wait for send to complete
        record_metadata = future.get(timeout=10)
        
        print("\n✅ Message sent successfully!")
        print(f"Topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")
        print("="*60)
        
    except KafkaError as e:
        print(f"\n❌ Failed to send message: {e}")
        return False
    
    finally:
        producer.close()
    
    return True

def main():
    """Main function"""
    
    print("\n" + "="*60)
    print("KAFKA MESSAGE SENDER FOR AGGREGATOR")
    print("="*60)
    print("\nInstructions:")
    print("1. Run the demos processor and look for the log:")
    print("   'KAFKA MESSAGE TO SEND TO TOPIC: video-results'")
    print("2. Copy the JSON message from the log")
    print("3. Paste it in the message_data variable in this script")
    print("4. Run this script to send the message to aggregator")
    print("="*60)
    
    # Check if message is configured
    if not message_data.get("video_id"):
        print("\n⚠️  WARNING: Message is empty!")
        print("Please edit this script and paste the message from demos log")
        print("in the message_data variable (lines 22-27)")
        return
    
    # Ask for confirmation
    print("\nMessage to send:")
    print(json.dumps(message_data, indent=2))
    
    response = input("\nDo you want to send this message? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    # Send the message
    if send_message():
        print("\n✅ Success! The aggregator should now process this message.")
    else:
        print("\n❌ Failed to send message. Please check the logs.")

if __name__ == "__main__":
    main()