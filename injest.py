from libs.queues import KafkaHandler
import json
from minio import Minio
import os
from typing import List
from libs.utils import list_files_in_folder


def load_demo_data(host_folder: str) -> List[dict]:
    # List files in output/{asset_id} dir
    data_dict = []
    files = os.listdir(host_folder)
    for file in files:
        if file.endswith('.json'):
            with open(host_folder + "/" + file, 'r') as f:
                data_dict.append(json.load(f))
    return data_dict
                
            
if __name__ == '__main__':
        
           
    print("Startingg...")
    kafka_address = '192.168.1.12:9093'

    kafka_handler = KafkaHandler(bootstrap_servers=[kafka_address])
        
    client = Minio("0.0.0.0:9000",  # Replace with your MinIO storage address
        access_key = "minioadmin",   # Replace with your access key
        secret_key = "minioadmin",    # Replace with your secret key
        secure = False
    )
    
    bucket_name = "my-bucket"
    test_vide_id = '5049e5f6-ec91-4afb-b2f5-a63a991a7993'
    
    # load json files from ../data folder
    data_folder = "./data/inputs/"
    files =  list_files_in_folder(client, bucket_name, test_vide_id)        
    
    print("Consuming... ")

    for data in files:
        print("Sending...")
        # Create a producer and send a message
        kafka_handler.produce_message('RESULTS', data)
        
    # # Create a consumer and consume messages
    # consumer = kafka_handler.create_consumer('profiles_2', 'profiles-group')
    # res = kafka_handler.consume_messages(consumer, process_message_callback=lambda x: print(x))
