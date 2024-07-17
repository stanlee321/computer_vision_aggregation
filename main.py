from libs.queues import KafkaHandler
from minio import Minio

if __name__ == "__main__":
    # Initialize Kafka handler
    kafka_handler = KafkaHandler(bootstrap_servers=['192.168.1.12:9093'])
        
    client = Minio("0.0.0.0:9000",  # Replace with your MinIO storage address
        access_key = "minioadmin",   # Replace with your access key
        secret_key = "minioadmin",    # Replace with your secret key
        secure = False
    )
    
    bucket_name = "my-bucket"
    
    topic_input = 'RESULTS'
    topic_group = 'results-group'
    
    # # Create Kafka consumer
    consumer = kafka_handler.create_consumer(topic_input, topic_group)

    for message in consumer:
        print(f"Consumed message: {message.value}")
        file : str = message.value
        filename = file.split('/')[-1]
        # Process data
        client.fget_object(bucket_name, file, f'./data/outputs/{filename}')
