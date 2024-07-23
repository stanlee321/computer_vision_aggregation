import os
import re
import json
import random
import time
import pandas as pd
from minio import Minio
from minio.error import S3Error
from typing import Any, Optional, Dict, Union, List, Tuple

from libs.video_handler import VideoHandler
from libs.clean_data import ProcessData

 

def list_files_in_folder(minio_client: Minio, bucket_name, prefix):
    try:
        print(f"Listing objects in bucket {bucket_name} with prefix {prefix}")
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        data = []
        for obj in objects:
            if obj.object_name.endswith('.json'):
                data.append(obj.object_name)
        return data
    except S3Error as e:
        print(f"Error listing objects in bucket {bucket_name} with prefix {prefix}: {e}")

def list_buckets_and_objects(minio_client: Minio):
    try:
        for bucket in minio_client.list_buckets():
            print(f"Bucket: {bucket.name}")
            for item in minio_client.list_objects(bucket.name, recursive=True):
                print(f"Object: {item.object_name}")
    except S3Error as e:
        print(f"Error occurred while listing buckets/objects: {e}")





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
    


def create_json_data(df: pd.DataFrame, 
                     video_output_path_remote: str, 
                     conditions: Union[List[str], None] = ['frame_number', 'class', 'track_id'],
                     output_name: str = 'output_json_timestamp.json',
                     output_folder: str = './tmp/',
                     data_handler: ProcessData = None,
                     ) -> Tuple[str, dict]:
    additional_data = {
        'annotated_video': video_output_path_remote,
        'original_video': 'https://s3.amazonaws.com/groundtruth-ai/5049e5f6-ec91-4afb-b2f5-a63a991a7993.mp4'
    }
    
    full_data_local_path = f'{output_folder}{output_name}'
    
    # remove duplicates
    df = data_handler.remove_duplicates(df, conditions= conditions)
    
    # Create aggregated data
    data = data_handler.create_aggregated_data(df, 
                                        keep_columns=['timestamp', 'class', 'name', 'track_id', 's3_path'], 
                                        output_path=full_data_local_path, additional_data=additional_data)
    
    return full_data_local_path, data
    

def process_data(data_path: str, data_handler: ProcessData, video_id: Union[str, None]) -> pd.DataFrame:


    df = data_handler.read_input_data(data_path)
    df = data_handler.join_chunks(df, video_id=video_id)
    df = data_handler.create_annotations(df)
    
    # Join frame numbers
    df = data_handler.join_frames(df)
    
    # Create timestamps
    return data_handler.create_timestamps(df, fps=30)

def put_to_redis(
                video_id: str,
                json_data: Union[Dict[str, Any], None] = None,
                local_data_path : Union[str, None] = None,
                redis_client: Any = None
                 ) -> str:
    
    if local_data_path:
        # Load data from local path to json_data
        with open(local_data_path, 'r') as file:
            json_data = json.load(file)
    
    # Save to Redis
    key = f"video:{video_id}_label:complete"
    
    redis_client.set_value(key, json.dumps(json_data))
    
    print(f"Data saved to Redis with key: {key}")
    
    return key
    