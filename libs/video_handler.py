import cv2
import os
import re
from typing import List
from minio import Minio
import pandas as pd
from tqdm import tqdm


class VideoHandler:
    
    def __init__(self, output_folder: str = "./tmp/") -> None:
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            
        self.output_folder = './tmp/'
        self.output_video_build_name = 'output_annotated_video.mp4'
        self.aux_output_video_build_name = 'old_output_annotated_video.mp4'

        self.work_dir = './tmp/'
        
        self.video_annotated_name = None
        self.remote_annotated_video_path = None
        self.local_annotated_video_path = None
        
        self.aux_output_local_video_build_name = None
        
        self.build_local_video_path = None
        self.build_remote_video_path = None
        
        

    @staticmethod
    def get_video_properties(video_path):
        cap = cv2.VideoCapture(video_path)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        cap.release()
        return width, height, fps

    @staticmethod
    def join_videos(video_list: List[str], output_path: str):
        print(f"Joining {len(video_list)} videos")
        # Get properties from the first video
        width, height, fps = VideoHandler.get_video_properties(video_list[0])

        # Create VideoWriter object
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # Codec for MP4
        out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

        for video_path in video_list:
            print("Working on video: ", video_path)
            cap = cv2.VideoCapture(video_path)

            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                out.write(frame)

            cap.release()

        out.release()


    def get_annotated_video_list(self, videos_list: List[dict], client: Minio, bucket_name: str) -> List[str]:
        # Join vide
        video_output_paths = []
        # Check if remote video build exists 
        
        try:
            client.stat_object(bucket_name, self.build_remote_video_path)
            # Download to local
            client.fget_object(bucket_name, self.build_remote_video_path, self.aux_output_local_video_build_name)
            video_output_paths.append(self.aux_output_local_video_build_name)
        except Exception as e:
            print(e)
            print(f"Video build {self.build_remote_video_path} not found, building it now")
            
        for video_file in videos_list:
            remote_file = video_file['remote_annotated_video']
            local_file = video_file['local_annotated_video']
            # Download the video
            client.fget_object(bucket_name, remote_file, local_file)
            video_output_paths.append(local_file)

        # Remove duplicates 
        video_output_paths = list(set(video_output_paths))
        video_output_paths = self.sort_files_by_chunk(video_output_paths)
        return video_output_paths
    
    @staticmethod
    def sort_files_by_chunk(file_list):
        # Helper function to extract the chunk number
        def extract_chunk_number(file_path):
            # Use regular expression to find the "chunk_X_of_Y" pattern
            match = re.search(r'chunk_(\d+)_of_\d+', file_path)
            if match:
                # Return the chunk number as an integer
                return int(match.group(1))
            return 0  # Default to 0 if no match is found

        # Sort the list using the extracted chunk number
        sorted_list = sorted(file_list, key=extract_chunk_number)
        return sorted_list



    def set_names(self, video_id: str, video_path_data: dict):
        self.work_dir  = f"{self.output_folder}{video_id}/"
        
        self.video_annotated_name = video_path_data['annotated_video']
        self.remote_annotated_video_path = video_path_data['remote_annotated_video']
        self.local_annotated_video_path = video_path_data['local_annotated_video']
        
        self.aux_output_local_video_build_name = f"{self.work_dir}{self.aux_output_video_build_name}"
        
        self.build_local_video_path = f"{self.work_dir}/{self.output_video_build_name}"
        self.build_remote_video_path = f"{video_id}/{self.output_video_build_name}"
        
    def process(self, video_id: str, videos_list: List[dict], minio_client: Minio, bucket_name: str) -> str:

        self.set_names(video_id, videos_list[0])
        
        videos_list = self.get_annotated_video_list(
            videos_list=videos_list,
            client=minio_client,
            bucket_name=bucket_name)        
        
        VideoHandler.join_videos(videos_list, self.build_local_video_path)
        
        # Upload video to Minio
        minio_client.fput_object(bucket_name, self.build_remote_video_path, self.build_local_video_path)
        
        return self.build_remote_video_path
    


if __name__ == "__main__":

    vide_handler = VideoHandler()
    # List of video paths
    video_list = ['./tmp/video1.mp4', './tmp/video2.mp4']
    
    # Output video path
    output_path = './tmp/output_video.mp4'

    # Join videos
    video_output_path = vide_handler.join_videos(video_list, output_path)

    print(video_output_path)
