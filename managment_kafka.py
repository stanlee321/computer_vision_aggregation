from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, KafkaProducer

import json

def list_topics(kafka_address):
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_address,
        client_id='my-client'
    )
    
    topics = admin_client.list_topics()
    print(f"Available topics: {topics}")
    admin_client.close()
    return topics

def consume_messages(kafka_address, topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_address,
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Set to False to manually control when to commit the offsets
        group_id='list-messages-group-1',
        consumer_timeout_ms=5000  # Increase the timeout to give the consumer more time to pull messages
    )

    print(f"Messages in topic '{topic}':")
    try:
        for message in consumer:
            print(f"Offset: {message.offset}, Key: {message.key}, Value: {message.value.decode('utf-8')}")
            # Manually commit the offset after processing the message
            consumer.commit()
    except Exception as e:
        print(f"Error consuming messages from {topic}: {e}")
    finally:
        consumer.close()

def send_message_to_topic(kafka_address: str, topic: str, message: dict):
    producer = KafkaProducer(
        bootstrap_servers=[kafka_address],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        future = producer.send(topic, value=message)
        result = future.get(timeout=10)
        print(f"Message sent successfully to {topic}: {message}")
    except Exception as e:
        print(f"Failed to send message to {topic}. Error: {e}")
    finally:
        producer.close()
        
        

def delete_kafka_topics(kafka_address, topics):
    # Initialize Kafka Admin Client
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_address,
        client_id='admin-client'
    )

    try:
        # Delete the specified topics
        admin_client.delete_topics(topics=topics)
        print(f"Successfully deleted topics: {topics}")
    except Exception as e:
        print(f"Failed to delete topics {topics}. Error: {e}")
    finally:
        # Close the admin client connection
        admin_client.close()


if __name__ == '__main__':
    kafka_address = '192.168.1.16:9092'
    topics = list_topics(kafka_address)
   
    # delete topics 
    
    delete_kafka_topics(kafka_address, topics=topics)
    
    # topic_name = 'video-chunks'
    # sample_message = {
    #     "video_id": "a29615c3-9227-496e-b688-839ad828c898",
    #     "info_path": "some/path/to/resource.json",
    #     "status": "processed"
    # }

    # send_message_to_topic(kafka_address, topic_name, sample_message)

    # for topic in topics:
    #      consume_messages(kafka_address, topic)
