import os
import json
from libs.queues import KafkaHandler
from typing import List
from libs.api import ApiClient

     
if __name__ == '__main__':
        
    


    print("Startingg...")

    kafka_address = '192.168.1.16:9092'
    kafka_handler = KafkaHandler(bootstrap_servers=[kafka_address])
    api_client = ApiClient("http://127.0.0.1:8000")

    bucket_name = "my-bucket"
    topic_results = 'video-results'

    chunks  = [1, 2, 3, 4, 5]

    for index in chunks:
        print("Sending...")
        video_id = "05a2c75b-7d1e-4466-9e96-c8609cd576ad"

        remote_path = f"{video_id}/resumed_maq_ch2_main_20210207170000_20210207180000_chunk_{index}_of_{len(chunks)}_results.json"
        # new_item = {
        #     "remote_path": remote_path ,
        #     "video_id": video_id,
        #     "status": "pending",
        #     "original_video": "some video url",
        #     "kind": "ground",
        #     "fps": 25,
        # }
        
        # response = api_client.create_item(new_item)
        # if response.status_code != 200:
        #     print(f"Failed to create item. Status Code: {response.status_code}. Response: {response.text}")
        #     continue
        
        # Create a producer and send a message
        kafka_handler.produce_message(topic_results, {
                    "video_id": video_id,
                    "info_path": remote_path,
                    })
        
        # print(f"Message sent: {new_item}")
    # # Create a consumer and consume messages
    # consumer = kafka_handler.create_consumer('profiles_2', 'profiles-group')
    # res = kafka_handler.consume_messages(consumer, process_message_callback=lambda x: print(x))
