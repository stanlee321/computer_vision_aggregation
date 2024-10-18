import os 
import re
import json
import pandas as pd
from minio import Minio
import ast
from typing import Union, List, Tuple
from libs.video_handler import VideoHandler


class ProcessData:
    def __init__(self):
        self.output_folder = './tmp'
        self.workdir = self.output_folder
        
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
        # List files in output/{asset_id} dir
        df_list = []
        for file in tasks_dir:
            df_data = pd.read_json(file)
            df_list.append(df_data)

        return pd.concat(df_list)
                
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
    def download_remote_files(df: pd.DataFrame, client: Minio, bucket_name: str, workdir: str, video_id: str) -> List[str]:

        task_remote_paths:List[str] = list(df['remote_path'].values)
        
        # Filer only the ones with the video_id
        task_remote_paths = [path for path in task_remote_paths if video_id in path]
        print("task_remote_paths ", task_remote_paths)

        output_files = []
        for task_remote_file in task_remote_paths:
            file_name = task_remote_file.split('/')[-1]            
            file_output_path = os.path.join(workdir, file_name)
            client.fget_object(bucket_name, 
                            task_remote_file, 
                            file_output_path)
            output_files.append(file_output_path)
                
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
                        keep_columns: Union[List[str], None] = None
                        ) -> dict:

        additional_data = {
            'annotated_video': annotated_video,
            'original_video': original_video
        }
        # remove duplicates
        df_dd = self.remove_duplicates(df, conditions = conditions)
        
        # Create aggregated data
        return self.create_aggregated_data(df_dd, 
                                            keep_columns=keep_columns, 
                                            output_path=output_path, 
                                            additional_data=additional_data)
        
        
    def create_join_video(self, video_id: str, 
                        df: pd.DataFrame, 
                        minio_client: Minio, 
                        bucket_name: str,
                        video_handler: VideoHandler) -> str:
        
        df_work = df.copy()
        df_videos = df_work.drop_duplicates(subset=['annotated_video'], keep='first')

        df_videos['annotated_video'] = df_videos['annotated_video'].apply(lambda x: x.split("/")[-1])
        df_videos['remote_annotated_video'] = df_videos['annotated_video'].apply(lambda x: f"{video_id}/{x}")
        df_videos['local_annotated_video'] = df_videos['annotated_video'].apply(lambda x: f"{self.workdir}/{x}")

        videos_info_list = df_videos[['annotated_video', 'remote_annotated_video', 'local_annotated_video']].to_dict(orient='records')

        # Join videos
        video_output_path_remote = video_handler.process(video_id, videos_info_list, minio_client, bucket_name)
        
        return video_output_path_remote



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
    video_output_path_remote = video_handler.process(video_id, df_videos, minio_client, bucket_name)
    
    additional_data = {
        'annotated_video': video_output_path_remote,
        'original_video': 'https://s3.amazonaws.com/groundtruth-ai/5049e5f6-ec91-4afb-b2f5-a63a991a7993.mp4'
    }
    
    # remove duplicates
    df = data_handler.remove_duplicates(df, conditions= ['frame_number', 'class', 'track_id'])
    data_handler.create_aggregated_data(df, 
                                        keep_columns=['timestamp', 'class', 'name', 'track_id', 's3_path'], 
                                        output_path=f'{output_folder}output_json_timestamp.json', additional_data=additional_data)

    
