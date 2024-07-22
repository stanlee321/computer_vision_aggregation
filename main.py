import os
import re
import json
import random
import time
import pandas as pd
from minio import Minio
from typing import Any, Optional, Dict, Union

from libs.video_handler import VideoHandler
from libs.clean_data import ProcessData
from libs.queues import KafkaHandler
from db.pandas_crud import PandasCRUD



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

        
def create_pandas_data_task(data_file_path: str, video_id: str, status: str) -> Any:
    
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
        "status": status,
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
def read_or_create_dataframe(file_path: str, status : Union[str,None]) -> pd.DataFrame:
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        if status is not None:
            return df[df['status'] == status]
        return df
    else:
        df = pd.DataFrame(columns=[
            'id', 'video_id', 'status', 'chunk_id', 'total_frames', 'fps', 
            'remote_data_path', 'annotated_video_path'
        ])
        df.to_csv(file_path, index=False)
    return df



def create_join_video(video_id: str, 
                      df: pd.DataFrame, 
                      minio_client: Minio, 
                      bucket_name: str,
                      video_handler: VideoHandler) -> str:
    
    # download all the images from the s3_path    
    df_videos = df.drop_duplicates(subset=['annotated_video'], keep='first')

    # Join videos
    video_output_path_remote = video_handler.process(video_id, df_videos, minio_client, bucket_name)
    
    return video_output_path_remote
    


def create_json_data(df: pd.DataFrame, video_output_path_remote: str) -> str:
    additional_data = {
        'annotated_video': video_output_path_remote,
        'original_video': 'https://s3.amazonaws.com/groundtruth-ai/5049e5f6-ec91-4afb-b2f5-a63a991a7993.mp4'
    }
    
    full_data_local_path = f'{output_folder}output_json_timestamp.json'
    
    # remove duplicates
    df = data_handler.remove_duplicates(df, conditions= ['frame_number', 'class', 'track_id'])
    
    # Create aggregated data
    data_handler.create_aggregated_data(df, 
                                        keep_columns=['timestamp', 'class', 'name', 'track_id', 's3_path'], 
                                        output_path=full_data_local_path, additional_data=additional_data)
    
    return full_data_local_path
    

def process_data(data_path: str, data_handler: ProcessData, video_id: Union[str, None]) -> pd.DataFrame:


    df = data_handler.read_input_data(data_path)
    df = data_handler.join_chunks(df, video_id=video_id)
    df = data_handler.create_annotations(df)
    
    # Join frame numbers
    df = data_handler.join_frames(df)
    
    # Create timestamps
    return data_handler.create_timestamps(df, fps=30)


if __name__ == "__main__":
    
    print("Starting ...")
    
    bucket_name = "my-bucket"
    
    topic_input = 'RESULTS'
    topic_group = 'results-group'
    
    
    # Initialize Kafka handler
    kafka_handler = KafkaHandler(bootstrap_servers=['192.168.1.12:9093'])
        
    client_minio = Minio("0.0.0.0:9000",  # Replace with your MinIO storage address
        access_key = "minioadmin",   # Replace with your access key
        secret_key = "minioadmin",    # Replace with your secret key
        secure = False
    )
    
    # Join VIDEO
    output_folder = './tmp/'
    video_handler = VideoHandler( output_folder = output_folder)
    
    data_handler = ProcessData(
        minio_client=client_minio,
        bucket=bucket_name
    )
    
    

    # # Create Kafka consumer
    consumer = kafka_handler.create_consumer(topic_input, topic_group)


    
    for message in consumer:
        
        data_df_path = "./data/outputs/data.csv"
        tasks_df = read_or_create_dataframe(data_df_path, status='pending')
    
        print(f"Consumed message: {message.value}")
        file : str = message.value
        
        video_id = file.split('/')[0]
        filename = file.split('/')[-1]
        
        
        # Process data
        output_file_path =  f'./data/outputs/{filename}'
        client_minio.fget_object(bucket_name, file, output_file_path)
        chunk_id = extract_chunk_value(output_file_path)
        
        # Save to the tasks table
        if check_task_exists(video_id, chunk_id, tasks_df):
            print(f"Task with video_id: {video_id} and chunk: {chunk_id} already exists.")
            df_to_work = tasks_df[tasks_df['status'] == 'pending']
            if df_to_work.empty:
                print("No pending tasks")
                continue
        
        new_task_data = create_pandas_data_task(output_file_path, video_id, status='pending')
        tasks_df = append_to_tasks_df(new_task_data, tasks_df)
        
        tasks_df.to_csv(data_df_path, index=False)

        # Filter only pending tasks
        df_to_work = tasks_df[tasks_df['status'] == 'pending']

        # Process data
        df = process_data(data_path = data_df_path, data_handler=data_handler, video_id=video_id)
        remote_video_path = create_join_video(video_id, df, client_minio, bucket_name, video_handler=video_handler)
        local_data_path = create_json_data(df, remote_video_path)
    
        print("Data saved to", local_data_path)
        
        # Update columns with "pending" to "done" in the tasks_df
        tasks_df.loc[tasks_df['status'] == 'pending', 'status'] = 'done'
        
        
        tasks_df.to_csv(data_df_path, index=False)
