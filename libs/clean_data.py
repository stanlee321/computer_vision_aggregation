import os 
import re
import json
import pandas as pd
from minio import Minio
import ast
import cv2
import shutil
import concurrent.futures
import threading
from typing import Union, List, Tuple
from libs.video_handler import VideoHandler


class ProcessData:
    def __init__(self, optimal_workers=None):
        self.output_folder = './tmp'
        self.workdir = self.output_folder
        self.optimal_workers = optimal_workers or {
            'base': 4,
            'io_intensive': 6,
            'memory_intensive': 4,
            'video_processing': 3
        }
    
    def set_optimal_workers(self, optimal_workers: dict):
        """Update optimal workers configuration"""
        self.optimal_workers = optimal_workers
        
    def create_data_task(self, remote_path, file_path:str, video_id, status: str):
        # Mockup for creating task data
        return pd.DataFrame({
            'remote_path': remote_path, 
            'file_path': [file_path], 
            'video_id': [video_id], 
            'status': [status]})

    def read(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path)

    def write(self, data: pd.DataFrame, path) -> None:
        data.to_csv(path, index=False)

    def check_task_exists(self, video_id: str, chunk_id: str, df: pd.DataFrame):
        # Mockup for checking if a task exists
        return not df[(df['video_id'] == video_id) & (df['chunk_id'] == chunk_id)].empty
    
    def update_table_task(self, new_task_data, tasks_df):
        return pd.concat([tasks_df, new_task_data], ignore_index=True)

    def get_json_data(self, tasks_dir: str) -> pd.DataFrame:
        """Read JSON files in parallel for faster loading"""
        print(f"🔄 Reading {len(tasks_dir)} JSON files in parallel...")
        
        def read_json_file(file_path):
            try:
                df = pd.read_json(file_path)
                print(f"✅ Loaded: {os.path.basename(file_path)} ({len(df)} records)")
                return df
            except Exception as e:
                print(f"❌ Failed to read {file_path}: {e}")
                return pd.DataFrame()
        
        # Parallel JSON reading with optimal workers
        max_workers = self.optimal_workers.get('memory_intensive', 4)
        print(f"📚 Reading {len(tasks_dir)} JSON files with {max_workers} workers...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            df_list = list(executor.map(read_json_file, tasks_dir))
        
        # Filter out empty dataframes and concatenate
        df_list = [df for df in df_list if not df.empty]
        
        if df_list:
            result_df = pd.concat(df_list)
            print(f"🔗 Joined {len(df_list)} chunks → {len(result_df)} total records")
            return result_df
        else:
            print("⚠️ No valid data found in JSON files")
            return pd.DataFrame()
                
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Create an empty list to store the parsed data
        inner_data = []

        for index, row in df.iterrows():
            # Load the JSON data from the 'data' column
            data = row['data']
            
            # for result in data['results']:
            result = data['results']
            result['frame_number'] = data['frame_number']
            result['original_frame'] = data['original_frame']
            result['s3_path'] = data['s3_path']
            result['fps'] = row['fps']
            
            result['total_frames'] = row['total_frames']
            result['annotated_video'] = row['annotated_video']
            
            inner_data.append(result)

        # Create a DataFrame from the flattened list of dictionaries
        return pd.json_normalize(inner_data)


    def join_chunks(self, tasks_dir) -> pd.DataFrame:
        df_tasks = self.get_json_data(tasks_dir=tasks_dir)
        return self.clean_data(df_tasks)

    @staticmethod
    def create_pandas_data(tasks: Union[List[dict], str]) -> pd.DataFrame:
        if isinstance(tasks, str):
            return pd.read_csv(tasks)
        return pd.DataFrame(tasks)

    @staticmethod
    def extract_chunk_and_annotated(filename: str) -> dict:
        # Use regular expressions to find the chunk and annotated values
        chunk_match = re.search(r'chunk_(\d+)', filename)
        annotated_match = re.search(r'annotated_(\d+)', filename)
        total_chunks_match = re.search(r'of_(\d+)', filename)

        if chunk_match and annotated_match:
            chunk_value = int(chunk_match.group(1))
            annotated_value = int(annotated_match.group(1))
            total_chunks_match = int(total_chunks_match.group(1))

            return {'chunk': chunk_value, 'annotated': annotated_value, 'total_chunks': total_chunks_match}
        else:
            raise ValueError("Chunk or annotated value not found in the filename")



    @staticmethod
    def find_local_result_files(df: pd.DataFrame, video_id: str) -> List[str]:
        """Find local result files in processing directories with parallel file checking"""
        
        task_remote_paths: List[str] = list(df['remote_path'].values)
        task_remote_paths = [path for path in task_remote_paths if video_id in path]
        
        base_processing_dir = "../computer_vision_demos/tmp"
        print(f"🔍 Checking {len(task_remote_paths)} local result files in parallel...")
        
        def check_file_exists(task_remote_file):
            file_name = task_remote_file.split('/')[-1]
            local_file_path = os.path.join(base_processing_dir, video_id, file_name)
            
            if os.path.exists(local_file_path):
                print(f"✅ Found: {file_name}")
                return local_file_path, None
            else:
                print(f"❌ Missing: {file_name}")
                return None, file_name
        
        output_files = []
        missing_files = []
        
        # Parallel file existence checking
        # Parallel file checking with optimal I/O workers
        max_workers = self.optimal_workers.get('io_intensive', 8)
        print(f"🔍 Checking {len(self.df_tasks)} local files with {max_workers} workers...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(check_file_exists, task_remote_paths)
            
            for local_path, missing_file in results:
                if local_path:
                    output_files.append(local_path)
                else:
                    missing_files.append(missing_file)
        
        print(f"📊 Local files: {len(output_files)}/{len(task_remote_paths)} found")
        
        if missing_files:
            print(f"⏳ Still processing: {missing_files}")
                
        return output_files


    def create_annotations(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df['metric'] = df['original_frame'].apply(lambda x : ProcessData.extract_chunk_and_annotated(x))
        # If you want to split the dictionary into separate columns
        df['chunk'] = df['metric'].apply(lambda x: x['chunk'])
        df['total_chunks'] = df['metric'].apply(lambda x: x['total_chunks'])

        df['annotated'] = df['metric'].apply(lambda x: x['annotated'])
        # Optionally drop the 'metric' column if not needed anymore
        df.drop(columns=['metric'], inplace=True)

        # Sort the DataFrame by 'chunk' and 'annotated'
        df.sort_values(by=['chunk', 'annotated'], inplace=True, ascending=True)
        
        # Initialize 'fixed_annotated' with the same values as 'annotated'
        df['fixed_annotated'] = df['annotated']
        
        df.reset_index(inplace=True)

        return df

    def join_frames(self, df: pd.DataFrame) -> pd.DataFrame:

        # Calculate the cumulative max annotated for each chunk
        max_annotated_per_chunk = df.groupby('chunk')['annotated'].max().shift(fill_value=0).cumsum()
        chunk_offsets = max_annotated_per_chunk.to_dict()

        # Adjust the fixed_annotated values
        cumulative_offset = 0
        previous_chunk = -1

        for index, row in df.iterrows():
            current_chunk = row['chunk']
            if current_chunk != previous_chunk:
                cumulative_offset = chunk_offsets[current_chunk]
                previous_chunk = current_chunk
            df.at[index, 'fixed_annotated'] += cumulative_offset
            
        return df

    def create_timestamps(self, df: pd.DataFrame, fps: int) -> pd.DataFrame:
        # Calculate the total length of the video in seconds

        # Function to convert seconds to HH:MM:ss format
        def seconds_to_hhmmss(seconds):
            hh = int(seconds // 3600)
            mm = int((seconds % 3600) // 60)
            ss = int(seconds % 60)
            return f"{hh:02}:{mm:02}:{ss:02}"

        # Create the 'timestamp' column
        df['timestamp'] = df['fixed_annotated'].apply(lambda x: seconds_to_hhmmss(x / fps))
        
        return df
    
    def remove_duplicates(self, df: pd.DataFrame, conditions: Union[List[str], None] ):
        
        if conditions is None:
            return df
        # Remove repeted rows based on frame_number
        return df.drop_duplicates(subset=conditions, keep='first')
    
    
    
    @staticmethod
    def aggregate_groups(group: pd.DataFrame) -> dict:
        
        # remove "timestamp" from group
        group.drop(columns=['timestamp'], inplace=True)
        
        group.fillna('[]', inplace=True)
        names = []
        classes = []
        
        try:
            names = ProcessData.extract_unique_names(group["name"])
        except Exception as e:
            print("Error extracting names", e)
            
        try:
            classes = ProcessData.extract_unique_names(group["class"])
        except Exception as e:
            print("Error extracting classes", e)

        return {
            "names": names,
            "classes": classes,
            "data": group.to_dict(orient="records")
        }
        
    @staticmethod
    def extract_unique_names(all_names):
        # Initialize an empty set to store unique names
        unique_names = set()

        # Iterate through each string in the list
        for name_str in all_names:
            # Use ast.literal_eval to safely convert the string representation of the list to an actual list
            names_list = ast.literal_eval(name_str)
            # Update the set with the elements of the list (sets automatically handle duplicates)
            unique_names.update(names_list)

        # Convert the set back to a sorted list
        return sorted(unique_names)
    
    def create_aggregated_data(self, df: pd.DataFrame, keep_columns: Union[List[str], None], output_path: str, additional_data: dict) -> dict:


        
        if keep_columns is None:
            keep_columns = df.columns.tolist()
        
        df = df[keep_columns]
        grouped = df.groupby("timestamp", group_keys=False).apply(ProcessData.aggregate_groups).to_dict()


        df.fillna('[]', inplace=True)
        all_classes = df['class'].unique()
        all_names = df['name'].unique()
        
        all_classes_list = []
        all_names_list = []
        
        try:
            all_classes_list = self.extract_unique_names(all_classes.tolist())
        except Exception as e:
            print("Error extracting classes", e)
            
        try:    
            all_names_list = self.extract_unique_names(all_names.tolist())
        except Exception as e:
            print("Error extracting names", e)

        final_data = {
            "ground_detections": grouped,
            "all_classes": all_classes_list,
            "all_names": all_names_list,
        }

        # merge final_data with additional_data
        final_data.update(additional_data)

        # Output the result to JSON
        output_json = json.dumps(final_data, indent=4)

        # Save the JSON to a file
        with open(output_path, 'w') as f:
            f.write(output_json)
            
        return final_data


    def create_json_data(self, df: pd.DataFrame,
                        annotated_video: str, 
                        conditions: Union[List[str], None],
                        output_path: str = 'output_json_timestamp.json',
                        original_video: str = None,
                        keep_columns: Union[List[str], None] = None,
                        chunk_number: int = 1,
                        max_chunks: int = 1
                        ) -> dict:

        additional_data = {
            'annotated_video': annotated_video,
            'original_video': original_video,
            'process': f"{chunk_number}/{max_chunks}"
        }
        # remove duplicates
        df_dd = self.remove_duplicates(df, conditions = conditions)
        
        # Create aggregated data
        return self.create_aggregated_data(df_dd, 
                                            keep_columns=keep_columns, 
                                            output_path=output_path, 
                                            additional_data=additional_data)
        
        
    def create_join_video(self, video_id: str, 
                          job_id: str,
                        df: pd.DataFrame, 
                        minio_client: Minio, 
                        bucket_name: str,
                        video_handler: VideoHandler) -> str:
        
        df_work = df.copy()
        df_videos = df_work.drop_duplicates(subset=['annotated_video'], keep='first')

        df_videos['annotated_video'] = df_videos['annotated_video'].apply(lambda x: x.split("/")[-1])
        df_videos['remote_annotated_video'] = df_videos['annotated_video'].apply(lambda x: f"{video_id}/{job_id}/{x}")
        df_videos['local_annotated_video'] = df_videos['annotated_video'].apply(lambda x: f"{self.workdir}/{x}")

        videos_info_list = df_videos[['annotated_video', 'remote_annotated_video', 'local_annotated_video']].to_dict(orient='records')

        # Join videos
        video_output_path_remote = video_handler.process(video_id, videos_info_list, minio_client, bucket_name, job_id)
        
        return video_output_path_remote

    def create_crops_from_detections(self, df: pd.DataFrame, video_id: str, job_id: str, 
                                   annotated_video_path: str, minio_client: Minio, 
                                   bucket_name: str) -> str:
        """Create image crops from detections and upload to images/ directory"""
        
        # Create local images directory
        images_dir = os.path.join(self.workdir, "images")
        os.makedirs(images_dir, exist_ok=True)
        
        # Get the local video path
        local_video_path = os.path.join(self.workdir, "temp_video_for_crops.mp4")
        
        # Download the annotated video temporarily for cropping
        try:
            minio_client.fget_object(bucket_name, annotated_video_path, local_video_path)
        except Exception as e:
            print(f"⚠️ Could not download video for cropping: {e}")
            return None
            
        cap = cv2.VideoCapture(local_video_path)
        fps = cap.get(cv2.CAP_PROP_FPS)
        
        # Process detections for unique crops
        unique_detections = df.drop_duplicates(subset=['timestamp', 'track_id'], keep='first')
        crop_count = 0
        
        print(f"🖼️ Creating crops from {len(unique_detections)} detections...")
        
        for _, detection in unique_detections.iterrows():
            try:
                # Calculate frame number from timestamp
                frame_num = int(detection['timestamp'] * fps)
                
                # Seek to frame
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
                ret, frame = cap.read()
                
                if not ret:
                    continue
                    
                # Extract bounding box
                x1, y1, x2, y2 = int(detection['box.x1']), int(detection['box.y1']), \
                               int(detection['box.x2']), int(detection['box.y2'])
                
                # Crop the detection
                crop = frame[y1:y2, x1:x2]
                
                if crop.size == 0:
                    continue
                
                # Create filename: timestamp_trackid_class.jpg
                crop_filename = f"{detection['timestamp']:.2f}_{detection['track_id']}_{detection['class']}.jpg"
                crop_path = os.path.join(images_dir, crop_filename)
                
                # Save crop
                cv2.imwrite(crop_path, crop)
                crop_count += 1
                
            except Exception as e:
                print(f"⚠️ Error creating crop: {e}")
                continue
        
        cap.release()
        
        # Clean up temp video file
        if os.path.exists(local_video_path):
            os.remove(local_video_path)
        
        print(f"✅ Created {crop_count} image crops")
        
        # Upload images directory to MinIO in parallel
        images_remote_path = f"{video_id}/{job_id}/images"
        
        def upload_image(image_file):
            if image_file.endswith(('.jpg', '.png', '.jpeg')):
                local_image_path = os.path.join(images_dir, image_file)
                remote_image_path = f"{images_remote_path}/{image_file}"
                
                try:
                    minio_client.fput_object(bucket_name, remote_image_path, local_image_path)
                    return f"✅ {image_file}"
                except Exception as e:
                    return f"❌ {image_file}: {e}"
            return f"⏩ Skipped {image_file}"
        
        # Get list of image files
        image_files = os.listdir(images_dir)
        
        if image_files:
            print(f"📤 Uploading {len(image_files)} images in parallel...")
            
            # Upload images in parallel
            # Parallel image upload with optimal I/O workers
            max_workers = self.optimal_workers.get('io_intensive', 6)
            print(f"📤 Uploading {len(image_files)} images with {max_workers} workers...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                upload_results = list(executor.map(upload_image, image_files))
            
            # Count successful uploads
            successful = sum(1 for result in upload_results if result.startswith("✅"))
            print(f"📤 Uploaded {successful}/{len(image_files)} images to: {images_remote_path}")
        else:
            print(f"⚠️ No images to upload")
        
        return images_remote_path

    def set_filenames(self, video_id: str, results_file_name: str) ->  str:
        
        self.workdir = os.path.join(self.output_folder, video_id)
        output_file_path = os.path.join(self.workdir, results_file_name)
        return output_file_path
    
    
if __name__ == '__main__':

    data_path = '../data/outputs/data.csv'

    # Join VIDEO
    output_folder = './tmp'
    video_handler = VideoHandler( output_folder = output_folder)

    # Minio client
    minio_client = Minio("0.0.0.0:9000",  # Replace with your MinIO storage address
        access_key = "minioadmin",   # Replace with your access key
        secret_key = "minioadmin",    # Replace with your secret key
        secure = False
    )

    bucket_name = "my-bucket"
    video_id = '5049e5f6-ec91-4afb-b2f5-a63a991a7993'
    job_id = '1234567890'
    
    data_handler = ProcessData(
    )
    

    df = data_handler.read(data_path)
    df = data_handler.join_chunks(df, video_id=None, minio_clien = Minio, bucket=bucket_name)
    df = data_handler.create_annotations(df)
    
    # Join frame numbers
    df = data_handler.join_frames(df)
    
    # Create timestamps
    df = data_handler.create_timestamps(df, fps=30)

    # download all the images from the s3_path    
    df_videos = df.drop_duplicates(subset=['annotated_video'], keep='first')

    # Join videos
    video_output_path_remote = video_handler.process(video_id, df_videos, minio_client, bucket_name, job_id)
    
    additional_data = {
        'annotated_video': video_output_path_remote,
        'original_video': 'https://s3.amazonaws.com/groundtruth-ai/5049e5f6-ec91-4afb-b2f5-a63a991a7993.mp4'
    }
    
    # remove duplicates
    df = data_handler.remove_duplicates(df, conditions= ['frame_number', 'class', 'track_id'])
    data_handler.create_aggregated_data(df, 
                                        keep_columns=['timestamp', 'class', 'name', 'track_id', 's3_path'], 
                                        output_path=f'{output_folder}output_json_timestamp.json', additional_data=additional_data)

    
