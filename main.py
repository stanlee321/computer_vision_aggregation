from minio import Minio

from libs.video_handler import VideoHandler
from libs.clean_data import ProcessData
from libs.queues import KafkaHandler
from libs.redis_service import RedisClient
from libs.api import ApiClient
from libs.utils import (
    put_to_redis,
)

class Application:
    def __init__(self):
        self.bucket_name = "my-bucket"
        self.topic_input = 'video-results'
        self.topic_fine_detections = 'fine-detections'
        self.topic_group = 'results-group'
        self.output_folder = './tmp'
        self.bootstrap_servers = ['localhost:9093']
        self.data_handler = ProcessData()
        self.kafka_handler = KafkaHandler(bootstrap_servers=self.bootstrap_servers)
        self.client_minio = Minio("0.0.0.0:9000",
                                    access_key="minioadmin",
                                    secret_key="minioadmin",
                                    secure=False)
        self.video_handler = VideoHandler(output_folder=self.output_folder)
        self.redis_client = RedisClient(host='0.0.0.0', port=6379)
        
        self.api_client = ApiClient("http://127.0.0.1:8000")
        self.workdir = self.output_folder
        
    def create_filenames(self, video_id: str, file:str):
        filename = file.split('/')[-1]
        self.workdir = f'{self.output_folder}/{video_id}'
        output_file_path = f'{self.workdir}/{filename}'
        return video_id, output_file_path
    
    def process_message(self, message):
        
        print(f"Consumed message: {message.value}")
        
        _message_input = message.value
        remote_path :str = _message_input['remote_path']
        video_id: str = _message_input['video_id']
        
        # Create filenames
        video_id, output_file_path = self.create_filenames(video_id, remote_path)

        print(f"current workd ir ", self.workdir)
        # Download file
        self.client_minio.fget_object(self.bucket_name, remote_path, output_file_path)
        
        # Get id of the chunk

        previous_tasks = self.api_client.get_by_video_id(video_id, status = 'pending')
        if previous_tasks.status_code != 200:
            print("No pending tasks found")
            return
    
        df = self.data_handler.create_pandas_data(tasks = previous_tasks)
        df_original = df.copy()
        
        # Get the fps
        fps = df['fps'].iloc[0]
        original_video = df['original_video'].iloc[0]
        
        self.workdir = f'{self.output_folder}/{video_id}'

        output_files = self.data_handler.download_remote_files(df, self.client_minio, self.bucket_name, self.workdir)       
        
        df = self.data_handler.join_chunks(tasks_dir=output_files)
        df = self.data_handler.create_annotations(df)
        df = self.data_handler.join_frames(df)
        df = self.data_handler.create_timestamps(df, fps=fps)
        
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

        json_data =  self.data_handler.create_json_data(df, 
                                                      annotated_video = remote_video_path, 
                                                      conditions = ['frame_number', 'class'], 
                                                      output_path = local_file_results, 
                                                      keep_columns = ['timestamp', 'class', 'name', 'track_id', 's3_path'],
                                                      original_video = original_video
                                                      )
        
        key_data = put_to_redis(video_id, json_data, None, self.redis_client, label='lite')
        json_data_full =  self.data_handler.create_json_data(
                                             df, 
                                             annotated_video = remote_video_path, 
                                             conditions = ['frame_number'], 
                                             output_path = local_file_results_full, 
                                             original_video = original_video,
                                             keep_columns = None)

        key_data_full = put_to_redis(video_id, json_data_full, None, self.redis_client, label="complete")

        # Send the data to the next topic
        self.kafka_handler.produce_message(self.topic_fine_detections, 
                                           {   "lite_data": key_data,
                                               "full_data": key_data_full
                                            })
        
        # update tasks by id in df['id']
        ids_to_update = df_original['id'].tolist()
        update_data = {"status": "done"}
        for id in ids_to_update:
            self.api_client.update_item_status(id, update_data)
            
    def run(self):
            consumer = self.kafka_handler.create_consumer(self.topic_input, self.topic_group)
            for message in consumer:
                self.process_message(message)

if __name__ == "__main__":
    print("Starting...")
    app = Application()
    app.run()