import os
import re
import pandas as pd
import uuid
import json
import shutil
import concurrent.futures
import multiprocessing
import psutil
from typing import Tuple, List
from datetime import datetime
from libs.queues import KafkaHandler
from libs.api import ApiClient, UpdateStatus

from libs.video_handler import VideoHandler
from libs.clean_data import ProcessData

from minio import Minio
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
        self.data_handler = ProcessData(optimal_workers=None)  # Will be set later
        self.kafka_handler = KafkaHandler(bootstrap_servers=brokers)

        self.client_minio = Minio(f"{server_ip}:9000",
                                    access_key= minio_access_key,
                                    secret_key= minio_secret_key,
                                    secure=False)
        
        # Test MinIO connection and credentials (skip if offline)
        try:
            self.test_minio_connection()
        except Exception as e:
            print(f"⚠️ MinIO connection failed: {e}")
            print("🔧 Running in local-only mode (will try to reconnect when needed)")
            self.minio_available = False
        
        self.video_handler = VideoHandler(output_folder=self.output_folder)
        
        self.api_client = ApiClient(api_base_url)
        self.workdir = self.output_folder
        
        self.updater = UpdateStatus(backend_base_url, backend_email, backend_password)

        self.fps = None
        self.original_video = None
        self.df_tasks = None
        self.minio_available = True
        
        # Calculate optimal workers based on system resources
        self.optimal_workers = self._calculate_optimal_workers()
        
        # Update data handler with optimal workers
        self.data_handler.set_optimal_workers(self.optimal_workers)
    
    def _calculate_optimal_workers(self) -> dict:
        """Calculate optimal number of workers for different operations"""
        try:
            cpu_cores = multiprocessing.cpu_count()
            memory = psutil.virtual_memory()
            available_gb = memory.available / (1024**3)
            
            # Base workers: 4 minimum, scale with cores
            base_workers = max(4, min(int(cpu_cores * 0.75), 12))
            
            # I/O intensive operations (MinIO uploads, file operations)
            io_workers = max(4, min(int(cpu_cores * 1.2), 16))  # Can oversubscribe for I/O
            
            # Memory intensive operations (JSON processing, DataFrame operations)
            memory_workers = max(2, min(int(available_gb // 1.5), 8))  # 1.5GB per worker
            
            # Video processing workers
            video_workers = max(2, min(int(cpu_cores * 0.5), 6))
            
            workers = {
                'base': base_workers,
                'io_intensive': io_workers,
                'memory_intensive': memory_workers, 
                'video_processing': video_workers
            }
            
            print(f"⚡ Optimal workers calculated:")
            print(f"   💻 System: {cpu_cores} cores, {available_gb:.1f}GB available")
            print(f"   📊 Base operations: {base_workers} workers")
            print(f"   📤 I/O intensive (MinIO): {io_workers} workers") 
            print(f"   🧠 Memory intensive: {memory_workers} workers")
            print(f"   🎬 Video processing: {video_workers} workers")
            
            return workers
            
        except Exception as e:
            print(f"⚠️ Worker calculation failed: {e}, using defaults")
            return {
                'base': 4,
                'io_intensive': 6, 
                'memory_intensive': 4,
                'video_processing': 3
            }
    
    def test_minio_connection(self):
        """Test MinIO connection and credentials"""
        print("=" * 50)
        print("TESTING MINIO CONNECTION...")
        print("=" * 50)
        
        try:
            # Test 1: Check if MinIO is reachable
            print(f"✓ Connecting to MinIO at: {self.client_minio._base_url}")
            
            # Test 2: List buckets (tests credentials)
            buckets = list(self.client_minio.list_buckets())
            print(f"✓ Connection successful! Found {len(buckets)} buckets:")
            for bucket in buckets:
                print(f"  - {bucket.name} (created: {bucket.creation_date})")
            
            # Test 3: Check if our specific bucket exists
            if self.client_minio.bucket_exists(self.bucket_name):
                print(f"✓ Target bucket '{self.bucket_name}' exists and is accessible")
                
                # Test 4: Try to list some objects in the bucket
                objects = []
                for obj in self.client_minio.list_objects(self.bucket_name):
                    objects.append(obj)
                    if len(objects) >= 5:  # Limit to 5 objects manually
                        break
                        
                print(f"✓ Bucket access test: Found {len(objects)} sample objects")
                for obj in objects[:3]:  # Show first 3
                    print(f"  - {obj.object_name}")
                if len(objects) > 3:
                    print(f"  ... and {len(objects) - 3} more")
            else:
                print(f"✗ ERROR: Target bucket '{self.bucket_name}' does not exist!")
                raise Exception(f"Bucket '{self.bucket_name}' not found")
                
            print("=" * 50)
            print("MINIO CONNECTION TEST: SUCCESS")
            print("=" * 50)
            self.minio_available = True
            
        except Exception as e:
            print(f"✗ MINIO CONNECTION FAILED: {e}")
            print("Check your MinIO credentials and server status")
            print("=" * 50)
            raise e
                
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
        
    def create_tasks_from_local_files(self, video_id: str, job_id: str, message_input: dict) -> List[str]:
        """Create tasks list using local files directly (no MinIO download)"""
        
        print(f"🔍 Looking for local files for video_id: {video_id}, job_id: {job_id}")
        
        # Get work directory from message
        work_dir = message_input.get('work_dir', f'./tmp/{video_id}')
        info_path = message_input.get('info_path')
        
        print(f"📁 Work dir: {work_dir}")
        print(f"📄 Info path: {info_path}")
        
        # Get tasks from API (same as before)
        previous_tasks = self.api_client.get_by_video_id(video_id, status='pending')
        if previous_tasks.status_code != 200:
            print("No pending tasks found", previous_tasks.text)
            return None
            
        # Debug: Show all available tasks
        all_tasks = previous_tasks.json()
        print(f"🔍 DEBUG: Found {len(all_tasks)} total tasks for video_id: {video_id}")
        for i, task in enumerate(all_tasks):
            print(f"   Task {i+1}: job_id='{task.get('job_id')}', remote_path='{task.get('remote_path', '')[:50]}...'")
        
        # Filter tasks for this job_id - check both job_id field and remote_path
        filtered_tasks = []
        for task in all_tasks:
            # First try direct job_id match (for local_only mode)
            if task.get('job_id') == job_id:
                filtered_tasks.append(task)
                print(f"   ✅ Matched by job_id: {task.get('job_id')}")
            # Fallback to remote_path matching (for MinIO mode)  
            elif job_id in task.get('remote_path', ''):
                filtered_tasks.append(task)
                print(f"   ✅ Matched by remote_path: {task.get('remote_path', '')[:50]}...")
        
        if not filtered_tasks:
            print(f"❌ No tasks found for job_id: {job_id}")
            return None
        
        print(f"🔍 Job filtering: {len(all_tasks)} total → {len(filtered_tasks)} for job {job_id}")
        self.df_tasks = pd.DataFrame(filtered_tasks)
        self.fps = message_input.get('fps', 25)
        
        # Use local files directly instead of downloading from MinIO
        task_files = self.data_handler.find_local_result_files(
            self.df_tasks, 
            video_id=video_id
        )
        
        # Check if all expected files were found locally
        expected_files = len(self.df_tasks)
        found_files = len(task_files)
        
        if found_files < expected_files:
            print(f"⏳ Waiting for chunks: {found_files}/{expected_files} ready")
            return None  # Return None to indicate incomplete data
        
        print(f"✅ All {found_files} local files ready for processing!")
        return task_files

    def create_main_tasks(self, video_id, remote_path)-> Tuple[pd.DataFrame, pd.DataFrame, str]:
         # Cre`ate filenames
        output_file_path = self.create_filenames(video_id, remote_path)

        # Download file
        self.client_minio.fget_object(self.bucket_name, remote_path, output_file_path)
        
        # Get id of the chunk

        # Get tasks specifically for this job_id, not just video_id
        job_id = remote_path.split('/')[1]  # Extract job_id from path
        print(f"🎯 Processing job: {job_id}")
        
        previous_tasks = self.api_client.get_by_video_id(video_id, status = 'pending')
        if previous_tasks.status_code != 200:
            print("No pending tasks found", previous_tasks.text)
            return
            
        # Filter tasks to only include the current job_id
        all_tasks = previous_tasks.json()
        
        # Since job_id is None in API, filter by remote_path containing the job_id
        filtered_tasks = [task for task in all_tasks if job_id in task.get('remote_path', '')]
        
        print(f"🔍 Job filtering: {len(all_tasks)} total → {len(filtered_tasks)} for job {job_id}")
        
        if not filtered_tasks:
            print(f"❌ No tasks found for job_id {job_id}")
            print(f"📝 Available paths: {[task.get('remote_path', 'N/A')[-50:] for task in all_tasks[:3]]}...")
            return None
            
        self.df_tasks = self.data_handler.create_pandas_data(tasks = filtered_tasks)
        
        # SET the fps
        self.fps = self.df_tasks['fps'].iloc[0]
        self.original_video = self.df_tasks['original_video'].iloc[0]
        
        self.workdir = os.path.join(self.output_folder, video_id)
        
        # Use local files directly instead of downloading from MinIO
        task_files = self.data_handler.find_local_result_files(
            self.df_tasks, 
            video_id=video_id
        )
        
        # Check if all expected files were downloaded
        expected_files = len(self.df_tasks)
        downloaded_files = len(task_files)
        if downloaded_files < expected_files:
            print(f"⏳ Waiting for chunks: {downloaded_files}/{expected_files} ready")
            return None  # Return None to indicate incomplete data
        
        print(f"✅ All {downloaded_files} chunks ready for processing!")
                       
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
        
        try:
            df['name'] = df['data.class_name'].apply(lambda x: x)
        except:
            # Save the dataframe to a csv file as a debug
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            df.to_csv(os.path.join(self.workdir, f'debug_dataframe_{current_time}.csv'), index=False)
        
        return df
        
    def process_message(self, message):
        
        # print(f"Consumed message: {message.value}")
        
        _message_input = message.value
        remote_path: str = _message_input['info_path']
        video_id: str = _message_input['video_id']
        job_id: str = _message_input['job_id']
        processing_mode = _message_input.get('processing_mode', 'minio')
        
        # Handle different processing modes
        if processing_mode == 'local_only':
            print("🔄 Processing mode: LOCAL_ONLY - using local files directly")
            tasks_list = self.create_tasks_from_local_files(video_id, job_id, _message_input)
        else:
            print("🔄 Processing mode: MINIO - downloading files")
            tasks_list = self.create_main_tasks(video_id, remote_path)

        print("Tasks list: ", tasks_list)

        if tasks_list is None:
            return

        df = self.create_main_dataframe(tasks_list, self.fps)

        chunk_number = (df['chunk'].iloc[-1]).max()
        max_chunks = df['total_chunks'].iloc[0]
        
        # Check if all chunks are available
        unique_chunks = df['chunk'].nunique()
        print(f"Processing chunks: {unique_chunks}/{max_chunks}")
        
        # Only proceed if we have ALL the chunks
        if unique_chunks < max_chunks:
            print(f"Waiting for more chunks... Got {unique_chunks}, need {max_chunks}")
            return

        working_data_file  = self.get_working_data_file(chunk_number, max_chunks)

        df.to_csv(working_data_file, index=False)

        df = pd.read_csv(working_data_file)
        print("All chunks available! Processing complete data to...", working_data_file )
        
        print(f"🎬 Creating final outputs in parallel...")
        
        # Prepare variables for parallel execution
        local_file_results_full = self.data_handler.set_filenames(
            video_id=video_id, results_file_name = 'complete_data.json')
        
        # Define parallel tasks
        def create_video():
            print(f"🎥 Creating joined video...")
            return self.data_handler.create_join_video(
                video_id, job_id, df, 
                minio_client=self.client_minio, 
                bucket_name=self.bucket_name, 
                video_handler=self.video_handler)
        
        def create_json_data():
            print(f"📄 Creating complete_data.json...")
            return self.data_handler.create_json_data(
                df, 
                annotated_video="", # Will be set later
                conditions=['frame_number', 'class_id'], 
                output_path=local_file_results_full, 
                original_video=self.original_video,
                keep_columns=None,
                chunk_number=chunk_number,
                max_chunks=max_chunks)
        
        # Execute video creation and JSON creation in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            video_future = executor.submit(create_video)
            json_future = executor.submit(create_json_data)
            
            # Wait for both to complete
            remote_video_path = video_future.result()
            json_data_full = json_future.result()
            
        print(f"✅ Video and JSON creation completed!")
        
        # Create image crops using the completed video (requires video to be done first)
        print(f"📷 Creating image crops...")
        images_path = self.data_handler.create_crops_from_detections(
            df=df,
            video_id=video_id,
            job_id=job_id,
            annotated_video_path=remote_video_path,
            minio_client=self.client_minio,
            bucket_name=self.bucket_name
        )
        
        # Upload ONLY essential files for florence2 in parallel
        print(f"📤 Uploading essential files in parallel...")
        
        def upload_complete_data():
            if not self.minio_available:
                print("⚠️ MinIO not available, skipping complete_data.json upload")
                return None
            try:
                complete_data_remote_path = f'{video_id}/{job_id}/complete_data.json'
                self.client_minio.fput_object(self.bucket_name, complete_data_remote_path, local_file_results_full)
                print(f"✅ Uploaded: complete_data.json")
                return complete_data_remote_path
            except Exception as e:
                print(f"❌ Failed to upload complete_data.json: {e}")
                return None
        
        # Execute uploads with optimal I/O workers
        max_workers = self.optimal_workers.get('io_intensive', 6)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            upload_future = executor.submit(upload_complete_data)
            
            # Note: Video and images uploads are handled by their respective handlers
            # This allows other operations to continue while upload happens in background
            
            # Wait for upload to complete
            upload_result = upload_future.result()
            
        if upload_result:
            print(f"📤 Florence2 essential files uploaded successfully:")
            print(f"   📄 complete_data.json")
            print(f"   🎬 output_annotated_video.mp4") 
            print(f"   🖼️ cropped images (for Florence2 processing)")
        else:
            print(f"⚠️ Some uploads failed or skipped (MinIO unavailable)")
        
        
        # Send the data to the next topic
        # self.kafka_handler.produce_message(self.topic_fine_detections, 
        #                                     { 
        #                                        "full_data": key_data_full
        #                                     })
        
        # update tasks by id in df['id']
        ids_to_update = self.df_tasks['id'].tolist()
        update_data = {"status": "done"}
        for id in ids_to_update:
            self.api_client.update_item_status(id, update_data)
            
        # Update the status of the job
        self.updater.run(job_id=job_id, status='Finished')
        
        # Clean up work directory after processing
        self.cleanup_work_directory(video_id)
        
        print("Job finished")

    def cleanup_work_directory(self, video_id: str):
        """Clean up work directories after aggregation is complete"""
        
        # Clean up aggregation work directory
        aggregation_work_dir = f'{self.output_folder}/{video_id}'
        if os.path.exists(aggregation_work_dir):
            try:
                shutil.rmtree(aggregation_work_dir)
                print(f"🗑️ Cleaned aggregation dir: {aggregation_work_dir}")
            except Exception as e:
                print(f"⚠️ Error cleaning aggregation dir {aggregation_work_dir}: {e}")
        
        # Clean up processing work directories (delayed cleanup after Florence2 files uploaded)
        possible_processing_dirs = [
            f'../computer_vision_demos/tmp/{video_id}',
            f'./tmp/{video_id}',
            f'/tmp/{video_id}'
        ]
        
        for processing_work_dir in possible_processing_dirs:
            if os.path.exists(processing_work_dir):
                try:
                    shutil.rmtree(processing_work_dir)
                    print(f"🗑️ Cleaned processing dir: {processing_work_dir}")
                    break  # Only clean the first one found
                except Exception as e:
                    print(f"⚠️ Error cleaning processing dir {processing_work_dir}: {e}")
        
        print(f"✅ Complete cleanup finished for video: {video_id}")
    def generate_uuid(self):
        return str(uuid.uuid4())

    def run(self, offset: str = 'latest'):
        print("Consuming topic. {}".format(self.topic_input) ,)
        group_id = 'video-aggregator-' + self.generate_uuid()

        consumer = self.kafka_handler.create_consumer(self.topic_input,
                                                      group_id=group_id,
                                                      auto_offset_reset=offset)

        for message in consumer:
            print(f"Consumed message: {message.value}")
            self.process_message(message)