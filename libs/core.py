import os
import re
import pandas as pd
from typing import Tuple, List

from libs.queues import KafkaHandler
from libs.redis_service import RedisClient
from libs.api import ApiClient
from libs.utils import (
    put_to_redis,
)

from libs.video_handler import VideoHandler
from libs.clean_data import ProcessData

from minio import Minio

class Application:
    def __init__(self, server_ip: str, minio_access_key: str, minio_secret_key: str, brokers: List[str], api_base_url: str):
        self.bucket_name = "my-bucket"
        self.topic_input = 'video-results'
        self.topic_fine_detections = 'fine-detections'
        self.topic_group = 'results-group'
        self.output_folder = './tmp'
        self.data_handler = ProcessData()
        self.kafka_handler = KafkaHandler(bootstrap_servers=brokers)

        self.client_minio = Minio(f"{server_ip}:9000",
                                    access_key= minio_access_key,
                                    secret_key= minio_secret_key,
                                    secure=False)
        self.video_handler = VideoHandler(output_folder=self.output_folder)
        self.redis_client = RedisClient(host=f'{server_ip}', port=6379, password="Secret.")
        
        self.api_client = ApiClient(api_base_url)
        self.workdir = self.output_folder
        
        self.fps = None
        self.original_video = None
        self.df_tasks = None
                
    def create_filenames(self, video_id: str, file:str):
        filename = file.split('/')[-1]
        self.workdir = f'{self.output_folder}/{video_id}'
        output_file_path = f'{self.workdir}/{filename}'
        return output_file_path
    def get_working_data_file(self, chunk_number, max_chunks):
        return os.path.join(self.workdir, f'output_{chunk_number}_{max_chunks}.csv')

    @staticmethod
    def extract_chunk_and_annotated(filename: str) -> dict:
        # Use regular expressions to find the chunk and annotated values
        chunk_match = re.search(r'chunk_(\d+)', filename)
        total_chunks_match = re.search(r'of_(\d+)', filename)
        
        if chunk_match  and total_chunks_match:
            chunk_value = int(chunk_match.group(1))
            total_chunks_match = int(total_chunks_match.group(1))
            return {'chunk': chunk_value, 'total_chunks': total_chunks_match}
        else:
            raise ValueError("Chunk or annotated value not found in the filename")
        
    def create_main_tasks(self, video_id, remote_path)-> Tuple[pd.DataFrame, pd.DataFrame, str]:
         # Cre`ate filenames
        output_file_path = self.create_filenames(video_id, remote_path)

        # Download file
        self.client_minio.fget_object(self.bucket_name, remote_path, output_file_path)
        
        # Get id of the chunk

        previous_tasks = self.api_client.get_by_video_id(video_id, status = 'pending')
        if previous_tasks.status_code != 200:
            print("No pending tasks found", previous_tasks.text)
            return
            
        self.df_tasks = self.data_handler.create_pandas_data(tasks = previous_tasks.json())
        
        # SET the fps
        self.fps = self.df_tasks['fps'].iloc[0]
        self.original_video = self.df_tasks['original_video'].iloc[0]
        
        self.workdir = os.path.join(self.output_folder, video_id)
        task_files = self.data_handler.download_remote_files(self.df_tasks, 
                                                               self.client_minio, 
                                                               self.bucket_name, 
                                                               self.workdir,
                                                               video_id = video_id
                                                               
                                                               )       
        return task_files

    def create_main_dataframe(self, output_files: List[str], fps: int)-> pd.DataFrame:
        df = self.data_handler.join_chunks(tasks_dir=output_files)
        df = self.data_handler.create_annotations(df)
        df = self.data_handler.join_frames(df)
        df = self.data_handler.create_timestamps(df, fps=fps)
        
        # extract the class from the class_id array as the first element
        df["class"] = df["class_id"].apply(lambda x: x)
        df["track_id"] = df["tracker_id"].apply(lambda x: x)
        df['confidence'] =df["confidence"].apply(lambda x:  x)
        
        # expand xyxy "[[1294.099853515625, 786.7964477539062, 2145.39892578125, 1426.97998046875]]" to  box.x1,box.y1,box.x2,box.y2
        # First combert xyxy string to array
        df['xyxy'] = df['xyxy'].apply(lambda x: x)
        df['box.x1'] = df['xyxy'].apply(lambda x: x[0][0] if x else 0)
        df['box.y1'] = df['xyxy'].apply(lambda x: x[0][1] if x else 0)
        df['box.x2'] = df['xyxy'].apply(lambda x: x[0][2] if x else 0)
        df['box.y2'] = df['xyxy'].apply(lambda x: x[0][3] if x else 0)
        
        df['name'] = df['data.class_name'].apply(lambda x: x)
        
        return df
        
    def process_message(self, message):
        
        # print(f"Consumed message: {message.value}")
        
        _message_input = message.value
        remote_path: str = _message_input['info_path']
        video_id: str = _message_input['video_id']
        
        tasks_list = self.create_main_tasks(video_id, remote_path)
        
        print("Tasks list: ", tasks_list)
        # Chunks
        # chunk_data = self.extract_chunk_and_annotated(remote_path)
        # chunk_number = chunk_data['chunk']
        # working_data_file  = self.get_working_data_file(chunk_number, max_chunks)

        # max_chunks = chunk_data['total_chunks']
        if tasks_list is None:
            return

        df = self.create_main_dataframe(tasks_list, self.fps)

        chunk_number = (df['chunk'].iloc[-1]).max()
        max_chunks = df['total_chunks'].iloc[0]

        print(chunk_number)
        print(max_chunks)


        working_data_file  = self.get_working_data_file(chunk_number, max_chunks)
        # if chunk_number > 1:
        #     print("Chunk number > 1")
        #     if os.path.exists(working_data_file):
        #         print("File exists")
        #         df = self.data_handler.create_pandas_data(working_data_file)
        #     else:
        #         print("File does not exist")
        #         # Load previous data chunks and join with this new one
        #         working_data_file_previous  = self.get_working_data_file(chunk_number-1, max_chunks)
        #         df_prev = self.data_handler.create_pandas_data(working_data_file_previous)
                
        #         # Create the new dataframe
        #         df_new = self.create_main_dataframe(tasks_list, self.fps)
                
        #         df = pd.concat([df_prev, df_new])          
        # else:
            
        df.to_csv(working_data_file, index=False)
        

        # video_id = "a29615c3-9227-496e-b688-839ad828c898"
        df = pd.read_csv(working_data_file)
        print("saving data to ...", working_data_file )
        
        # Process the data
        remote_video_path = self.data_handler.create_join_video(
            video_id, 
            df, 
            minio_client = self.client_minio, 
            bucket_name= self.bucket_name, 
            video_handler =self.video_handler)
        
        local_file_results = self.data_handler.set_filenames(
            video_id=video_id, results_file_name = 'output_json_timestamp.json')
        
        local_file_results_full = self.data_handler.set_filenames(
            video_id=video_id, results_file_name = 'output_json_timestamp_full.json')
  
        
        json_data_full =  self.data_handler.create_json_data(
                                             df, 
                                             annotated_video = remote_video_path, 
                                             conditions = ['frame_number', 'class_id'], 
                                             output_path = local_file_results_full, 
                                             original_video = self.original_video,
                                             keep_columns = None)

        # Save json_data_full to json file
        complete_data_path = os.path.join(self.workdir, 'complete_data.json')
        key_data_full = put_to_redis(video_id, json_data_full, redis_client =  self.redis_client, label="complete", local_data_path = complete_data_path)

        # Upload complete data to minio
        self.client_minio.fput_object(self.bucket_name, f'{video_id}/complete_data.json', complete_data_path)
        
        
        # Send the data to the next topic
        self.kafka_handler.produce_message(self.topic_fine_detections, 
                                            { 
                                               "full_data": key_data_full
                                            })
        
        # update tasks by id in df['id']
        ids_to_update = self.df_tasks['id'].tolist()
        update_data = {"status": "done"}
        for id in ids_to_update:
            self.api_client.update_item_status(id, update_data)
            
    def run(self):
        print("Consuming topic. {}".format(self.topic_input) ,)
        print("Group: {}".format(self.topic_group))
        consumer = self.kafka_handler.create_consumer(self.topic_input, 
                                                        self.topic_group, 
                                                        auto_offset_reset='earliest')
        for message in consumer:
            print(f"Consumed message: {message.value}")
            self.process_message(message)