import os
import re
import pandas as pd
import uuid
import json
import shutil
import logging
import time
from typing import Tuple, List
from datetime import datetime
from libs.queues import KafkaHandler
from libs.api import ApiClient, UpdateStatus

from libs.video_handler import VideoHandler
from libs.clean_data import ProcessData

from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('aggregation_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
class Application:
    def __init__(self, server_ip: str, 
                 minio_access_key: str, 
                 minio_secret_key: str, 
                 brokers: List[str], 
                 api_base_url: str, 
                 topic_input: str,
                 topic_output: str,
                 bucket_name: str,
                 output_folder: str,
                 backend_email: str,
                 backend_password: str,
                 backend_base_url: str
                 ):
        
        
        self.bucket_name = bucket_name
        self.topic_input = topic_input
        self.topic_output = topic_output
        self.output_folder = output_folder
        self.data_handler = ProcessData()
        self.kafka_handler = KafkaHandler(bootstrap_servers=brokers)

        self.client_minio = Minio(f"{server_ip}:9000",
                                    access_key= minio_access_key,
                                    secret_key= minio_secret_key,
                                    secure=False)
        self.video_handler = VideoHandler(output_folder=self.output_folder)
        
        self.api_client = ApiClient(api_base_url)
        self.workdir = self.output_folder
        
        self.updater = UpdateStatus(backend_base_url, backend_email, backend_password)

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
        logger.info(f"Starting main tasks for video_id: {video_id}, remote_path: {remote_path}")
        
        try:
            # Create filenames
            output_file_path = self.create_filenames(video_id, remote_path)
            logger.info(f"Created output file path: {output_file_path}")

            # Download file
            logger.info(f"Downloading file from S3: {remote_path}")
            self.client_minio.fget_object(self.bucket_name, remote_path, output_file_path)
            logger.info(f"Successfully downloaded file to: {output_file_path}")
            
            # Get pending tasks for this video
            logger.info(f"Fetching pending tasks for video_id: {video_id}")
            previous_tasks = self.api_client.get_by_video_id(video_id, status = 'pending')
            
            if previous_tasks.status_code != 200:
                logger.error(f"Failed to fetch pending tasks. Status: {previous_tasks.status_code}, Response: {previous_tasks.text}")
                return None
                
            tasks_data = previous_tasks.json()
            logger.info(f"Found {len(tasks_data)} pending tasks for video_id: {video_id}")
                
            self.df_tasks = self.data_handler.create_pandas_data(tasks = tasks_data)
            
            # SET the fps
            self.fps = self.df_tasks['fps'].iloc[0]
            self.original_video = self.df_tasks['original_video'].iloc[0]
            logger.info(f"Video details - FPS: {self.fps}, Original video: {self.original_video}")
            
            self.workdir = os.path.join(self.output_folder, video_id)
            os.makedirs(self.workdir, exist_ok=True)
            logger.info(f"Working directory: {self.workdir}")
            
            # Download all remote files
            logger.info("Starting download of all chunk result files...")
            task_files = self.data_handler.download_remote_files(self.df_tasks, 
                                                                   self.client_minio, 
                                                                   self.bucket_name, 
                                                                   self.workdir,
                                                                   video_id = video_id)
            
            # Check if all expected files were downloaded
            expected_files = len(self.df_tasks)
            downloaded_files = len(task_files)
            logger.info(f"Download summary: {downloaded_files}/{expected_files} files downloaded")
            
            if downloaded_files < expected_files:
                missing_count = expected_files - downloaded_files
                logger.warning(f"Missing {missing_count} files. Not all chunks are ready yet.")
                logger.info(f"Expected files: {expected_files}, Downloaded files: {downloaded_files}")
                
                # List the remote paths we tried to download
                attempted_paths = list(self.df_tasks['remote_path'].values)
                logger.info(f"Attempted to download paths: {attempted_paths}")
                
                return None  # Return None to indicate incomplete data
                           
            logger.info(f"All {downloaded_files} files downloaded successfully")
            return task_files
            
        except S3Error as e:
            logger.error(f"S3 error in create_main_tasks for video_id {video_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in create_main_tasks for video_id {video_id}: {e}")
            return None

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
        
        try:
            df['name'] = df['data.class_name'].apply(lambda x: x)
        except:
            # Save the dataframe to a csv file as a debug
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            df.to_csv(os.path.join(self.workdir, f'debug_dataframe_{current_time}.csv'), index=False)
        
        return df
        
    def process_message(self, message):
        start_time = time.time()
        
        try:
            _message_input = message.value
            remote_path: str = _message_input['info_path']
            video_id: str = _message_input['video_id']
            job_id: str = _message_input['job_id']
            
            logger.info(f"Processing message for video_id: {video_id}, job_id: {job_id}, remote_path: {remote_path}")
            
            # Create main tasks with retry logic
            max_retries = 3
            retry_count = 0
            tasks_list = None
            
            while retry_count < max_retries and tasks_list is None:
                if retry_count > 0:
                    logger.info(f"Retrying create_main_tasks (attempt {retry_count + 1}/{max_retries})...")
                    time.sleep(5 * retry_count)  # Exponential backoff
                    
                tasks_list = self.create_main_tasks(video_id, remote_path)
                
                if tasks_list is None:
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"Failed to create main tasks, retrying in {5 * retry_count} seconds...")
                    else:
                        logger.error(f"Failed to create main tasks after {max_retries} attempts, skipping message")
                        return

            logger.info(f"Successfully created tasks list with {len(tasks_list)} files")

            # Create main dataframe
            logger.info("Creating main dataframe from task files...")
            df = self.create_main_dataframe(tasks_list, self.fps)
            logger.info(f"Created dataframe with {len(df)} rows")

            chunk_number = (df['chunk'].iloc[-1]).max()
            max_chunks = df['total_chunks'].iloc[0]
            
            # Check if all chunks are available
            unique_chunks = df['chunk'].nunique()
            logger.info(f"Chunk analysis: {unique_chunks}/{max_chunks} unique chunks found")
            
            # Only proceed if we have ALL the chunks
            if unique_chunks < max_chunks:
                missing_chunks = max_chunks - unique_chunks
                logger.warning(f"Incomplete chunks: Got {unique_chunks}, need {max_chunks}. Missing {missing_chunks} chunks")
                
                # Log which chunks we have
                available_chunks = sorted(df['chunk'].unique())
                expected_chunks = list(range(1, max_chunks + 1))
                missing_chunk_numbers = [c for c in expected_chunks if c not in available_chunks]
                logger.info(f"Available chunks: {available_chunks}")
                logger.info(f"Missing chunks: {missing_chunk_numbers}")
                return

            logger.info("All chunks available! Processing complete dataset...")
            working_data_file = self.get_working_data_file(chunk_number, max_chunks)
            logger.info(f"Saving working data to: {working_data_file}")

            df.to_csv(working_data_file, index=False)
            df = pd.read_csv(working_data_file)
            
            # Process the video joining
            logger.info("Starting video joining process...")
            remote_video_path = self.data_handler.create_join_video(
                video_id, 
                job_id,
                df, 
                minio_client = self.client_minio, 
                bucket_name= self.bucket_name, 
                video_handler =self.video_handler)
            logger.info(f"Video joined and uploaded to: {remote_video_path}")

            # Set the filenames
            logger.info("Creating result files...")
            local_file_results_full = self.data_handler.set_filenames(
                video_id=video_id, results_file_name = 'output_json_timestamp_full.json')
      
            # Create the json data
            logger.info("Creating JSON data...")
            json_data_full = self.data_handler.create_json_data(
                                                 df, 
                                                 annotated_video = remote_video_path, 
                                                 conditions = ['frame_number', 'class_id'], 
                                                 output_path = local_file_results_full, 
                                                 original_video = self.original_video,
                                                 keep_columns = None,
                                                 chunk_number = chunk_number,
                                                 max_chunks = max_chunks)
            logger.info("JSON data created successfully")
            
            remote_result_path = f'{video_id}/{job_id}/complete_data.json'
            
            # Upload complete data to minio
            logger.info(f"Uploading complete data to S3: {remote_result_path}")
            self.client_minio.fput_object(self.bucket_name, remote_result_path, local_file_results_full)
            logger.info("Complete data uploaded successfully")
            
            # Update task statuses
            logger.info("Updating task statuses to 'done'...")
            ids_to_update = self.df_tasks['id'].tolist()
            update_data = {"status": "done"}
            updated_count = 0
            
            for task_id in ids_to_update:
                try:
                    self.api_client.update_item_status(task_id, update_data)
                    updated_count += 1
                except Exception as e:
                    logger.error(f"Failed to update task {task_id}: {e}")
                    
            logger.info(f"Updated {updated_count}/{len(ids_to_update)} task statuses")
                
            # Update the job status
            logger.info(f"Updating job status to 'Finished' for job_id: {job_id}")
            try:
                self.updater.run(job_id=job_id, status='Finished')
                logger.info("Job status updated successfully")
            except Exception as e:
                logger.error(f"Failed to update job status: {e}")
            
            # Clean up work directory after processing
            self.cleanup_work_directory(video_id)
            
            processing_time = time.time() - start_time
            logger.info(f"Job completed successfully in {processing_time:.2f} seconds")
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error processing message after {processing_time:.2f} seconds: {e}")
            logger.error(f"Message that failed: {message.value}")
            
            # Still try to clean up if we have the video_id
            try:
                if 'video_id' in locals():
                    self.cleanup_work_directory(video_id)
            except Exception as cleanup_error:
                logger.error(f"Error during cleanup: {cleanup_error}")

    def cleanup_work_directory(self, video_id: str):
        """Clean up the work directory after processing is complete"""
        work_dir = f'{self.output_folder}/{video_id}'
        if os.path.exists(work_dir):
            try:
                shutil.rmtree(work_dir)
                print(f"Cleaned up work directory: {work_dir}")
            except Exception as e:
                print(f"Error cleaning up work directory {work_dir}: {e}")
    def generate_uuid(self):
        return str(uuid.uuid4())

    def run(self, offset: str = 'latest'):
        print("Consuming topic. {}".format(self.topic_input) ,)
        group_id = 'video-aggregator-' + self.generate_uuid()

        consumer = self.kafka_handler.create_consumer(self.topic_input,
                                                      group_id=group_id,
                                                      auto_offset_reset=offset)

        logger.info(f"Starting consumer loop for topic: {self.topic_input}")
        message_count = 0
        
        for message in consumer:
            message_count += 1
            logger.info(f"Consumed message #{message_count}: {message.value}")
            
            try:
                self.process_message(message)
            except Exception as e:
                logger.error(f"Failed to process message #{message_count}: {e}")
                logger.error(f"Failed message content: {message.value}")
                # Continue processing other messages instead of crashing
                continue