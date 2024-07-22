from libs.queues import KafkaHandler
from minio import Minio
from uuid import uuid1
import pandas as pd
from db.pandas_crud import PandasCRUD
import json
import random
import time
from typing import Any, Optional, Dict
import os
import re


# Some Pandas CRUD class
# pandas_crud = PandasCRUD()


def generate_unique_id():
    random.seed(time.time())  # Seed random number generator with current time
    new_id = random.randint(1, 999999999)
    return new_id

def extract_chunk_value(filename):
    match = re.search(r'chunk_(\d+)_', filename)
    if match:
        return int(match.group(1))
    else:
        return -1
    
    

def check_task_exists(video_id:str, chunk:str, df: pd.DataFrame) -> bool:
    # Get
    value = df[(df['video_id'] == video_id) & (df['chunk_id'] == chunk)].empty
    
    print("Comparation : ", value)
    return not value

        
def create_pandas_data_task(data_file_path: str, video_id: str) -> Any:
    
    with open(data_file_path, 'r') as file:
        input_data = json.load(file)    

    print("Input file is ...", data_file_path)
    print("Video id : ", video_id)
    
    remote_data_path = video_id  + "/"  + data_file_path.split("/")[-1]
    remote_vide_path = video_id + '/' + input_data['annotated_video']
    chunk_id = extract_chunk_value(data_file_path)
    
    data = {
        "id": generate_unique_id(),
        "video_id" : video_id,
        "status": "pending",
        "chunk_id" : chunk_id,
        "total_frames": input_data['total_frames'],
        "fps": input_data['fps'],
        "remote_data_path": remote_data_path,
        "annotated_video_path":  remote_vide_path
    }
    
    return data

# Function to append new task to the tasks DataFrame
def append_to_tasks_df(task_data: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
    new_df = pd.DataFrame([task_data])
    return pd.concat([df, new_df], ignore_index=True)
    
    
def read_tasks(video_id: str, chunk_id: Optional[int], df: pd.DataFrame) -> pd.DataFrame:
    if chunk_id is not None:
        result = df[(df['video_id'] == video_id) & (df['chunk'] == chunk_id)]
    else:
        result = df[df['video_id'] == video_id]
    return result

# Function to read or create a DataFrame
def read_or_create_dataframe(file_path: str) -> pd.DataFrame:
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
    else:
        df = pd.DataFrame(columns=[
            'id', 'video_id', 'status', 'chunk_id', 'total_frames', 'fps', 
            'remote_data_path', 'annotated_video_path'
        ])
        df.to_csv(file_path, index=False)
    return df



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

    data_df_path = "./data/outputs/data.csv"
    tasks_df = read_or_create_dataframe(data_df_path)
    
    
    for message in consumer:
        print(f"Consumed message: {message.value}")
        file : str = message.value
        
        video_id = file.split('/')[0]
        filename = file.split('/')[-1]
        
        
        # Process data
        output_file_path =  f'./data/outputs/{filename}'
        client.fget_object(bucket_name, file, output_file_path)
        chunk_id = extract_chunk_value(output_file_path)
        
        # Save to the tasks table
        if check_task_exists(video_id, chunk_id, tasks_df):
            print(f"Task with video_id: {video_id} and chunk: {chunk_id} already exists.")
            continue
        
        # tasks_df = read_tasks(video_id, chunk_id, tasks_df)
        new_task_data = create_pandas_data_task(output_file_path, video_id)
        tasks_df = append_to_tasks_df(new_task_data, tasks_df)
        
        print(tasks_df)
        print("done updated tasks_df ...", tasks_df.shape)
        
        # Save the dataframe to disk
        tasks_df.to_csv(data_df_path, index=False)