import os 
import re
import json
import pandas as pd
from tqdm import tqdm
from minio import Minio
from typing import Union, List
from video_handler import VideoHandler


class ProcessData:
    def __init__(self, minio_client: Minio, bucket: str):
        self.minio_client = minio_client
        self.bucket = bucket
        self.host_folder = './tmp/'

    def read_input_data(self, chunks_path: str):
        tasks_df = pd.read_csv(chunks_path)
        return tasks_df

    def get_video_data(self, host_folder) -> pd.DataFrame:
        # List files in output/{asset_id} dir
        df_list = []
        files = os.listdir(host_folder)
        for file in files:
            if file.endswith('.json'):
                print(file)
                path = f'{host_folder}/{file}'
                df_data = pd.read_json(path)
                df_list.append(df_data)

        return pd.concat(df_list)
                
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Create an empty list to store the parsed data
        inner_data = []

        for index, row in df.iterrows():
            # Load the JSON data from the 'data' column
            data = row['data']
            
            for result in data['results']:
                result['frame_number'] = data['frame_number']
                result['original_frame'] = data['original_frame']
                result['s3_path'] = data['s3_path']
                result['fps'] = row['fps']
                
                result['total_frames'] = row['total_frames']
                result['annotated_video'] = row['annotated_video']
                
                inner_data.append(result)

        # Create a DataFrame from the flattened list of dictionaries
        return pd.json_normalize(inner_data)


    def join_chunks(self, tasks_df: pd.DataFrame,  video_id: Union[str, None]):

        if video_id is not None:
            df_target = tasks_df[tasks_df['video_id'] == video_id]

        df_target = tasks_df.copy()
        
        for index, row in df_target.iterrows():
            row_dict = row.to_dict()

            filename = row_dict['remote_data_path'].split("/")[-1]
            file = row_dict['remote_data_path']
            # Process data
            output_file_path = f'./tmp/{filename}'
            self.minio_client.fget_object(self.bucket, file, output_file_path)

        df_tasks = self.get_video_data(host_folder=self.host_folder)
        return self.clean_data(df_tasks)

    @staticmethod
    def extract_chunk_and_annotated(filename: str) -> dict:
        # Use regular expressions to find the chunk and annotated values
        chunk_match = re.search(r'chunk_(\d+)', filename)
        annotated_match = re.search(r'annotated_(\d+)', filename)
        
        if chunk_match and annotated_match:
            chunk_value = int(chunk_match.group(1))
            annotated_value = int(annotated_match.group(1))
            return {'chunk': chunk_value, 'annotated': annotated_value}
        else:
            raise ValueError("Chunk or annotated value not found in the filename")

    def create_annotations(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df['metric'] = df['original_frame'].apply(lambda x : ProcessData.extract_chunk_and_annotated(x))
        # If you want to split the dictionary into separate columns
        df['chunk'] = df['metric'].apply(lambda x: x['chunk'])
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
    
    def remove_duplicates(self, df: pd.DataFrame, conditions: List[str] = ['frame_number', 'class', 'track_id']):
        
        # Remove repeted rows based on frame_number
        return df.drop_duplicates(subset=conditions, keep='first')
    
    @staticmethod
    def aggregate_groups(group):
        return {
            "names": group["name"].unique().tolist(),
            "classes": group["class"].unique().tolist(),
            "data": group.to_dict(orient="records")
        }
    
    def create_aggregated_data(self, df: pd.DataFrame, keep_columns: List[str], output_path: str, additional_data: dict):
        
        df = df[keep_columns]
        grouped = df.groupby("timestamp", group_keys=False).apply(ProcessData.aggregate_groups).to_dict()

        all_classes = df['class'].unique()
        all_names = df['name'].unique()

        final_data = {
            "ground_detections": grouped,
            "all_classes": all_classes.tolist(),
            "all_names": all_names.tolist(),
        }

        # merge final_data with additional_data
        final_data.update(additional_data)

        # Output the result to JSON
        output_json = json.dumps(final_data, indent=4)

        # Save the JSON to a file
        with open(output_path, 'w') as f:
            f.write(output_json)
            
            
        print("Data saved to", output_path)




if __name__ == '__main__':

    # Join VIDEO
    video_handler = VideoHandler()

    # Minio client
    minio_client = Minio("0.0.0.0:9000",  # Replace with your MinIO storage address
        access_key = "minioadmin",   # Replace with your access key
        secret_key = "minioadmin",    # Replace with your secret key
        secure = False
    )

    bucket_name = "my-bucket"
    video_id = '5049e5f6-ec91-4afb-b2f5-a63a991a7993'

    
    data_handler = ProcessData(
        minio_client=minio_client,
        bucket=bucket_name
    )
    
    data_path = '../data/outputs/data.csv'

    df = data_handler.read_input_data(data_path)
    df = data_handler.join_chunks(df, video_id=None)
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
                                        output_path='./output_json_timestamp.json', additional_data=additional_data)

    

