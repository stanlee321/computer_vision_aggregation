import cv2
import os
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

    @staticmethod
    def get_video_properties(video_path):
        cap = cv2.VideoCapture(video_path)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        cap.release()
        return width, height, fps

    @staticmethod
    def join_videos(video_list: List[str], output_path: str) -> str:
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
        print(f"Output video saved as {output_path}")
        return output_path

    @staticmethod
    def get_annotated_video_list(video_id: str, df: pd.DataFrame, client: Minio, bucket_name: str, output_folder: str, build_remote_video_path: str) -> List[str]:
        # Join vide
        video_output_paths = []
        # Check if remote video build exists
        
        try:
            client.stat_object(bucket_name, build_remote_video_path)
            # Download to local
            remote_file_name = "old_"+build_remote_video_path.split("/")[-1]
            output_video_build_path = f'{output_folder}{remote_file_name}'
            client.fget_object(bucket_name, build_remote_video_path,output_video_build_path)
            video_output_paths.append(output_video_build_path)
        except Exception as e:
            print(e)
            print(f"Video build {build_remote_video_path} not found, building it now")
            
        for index, row in tqdm(df.iterrows()):
            row_dict = row.to_dict()

            filename = row_dict['annotated_video'].split("/")[-1]
            file = f"{video_id}/{row_dict['annotated_video']}"

            # Process data
            output_file_path = f'{output_folder}{filename}'

            # Download the video
            client.fget_object(bucket_name, file, output_file_path)
            
            video_output_paths.append(output_file_path)

        # Remove duplicates 
        video_output_paths = list(set(video_output_paths))
        # Remove self.output_video_build_name  from the list
        # video_output_paths = [x for x in video_output_paths if x.split("/")[-1] != VideoHandler.output_video_build_name ]
        return video_output_paths

    def process(self, video_id: str, df_videos: pd.DataFrame, minio_client: Minio, bucket_name: str) -> str:

        video_output_path_remote = f'{video_id}/{self.output_video_build_name}'

        videos_list = VideoHandler.get_annotated_video_list(
            video_id=video_id,
            df=df_videos,
            client=minio_client,
            bucket_name=bucket_name,
            output_folder=self.output_folder,
            build_remote_video_path=video_output_path_remote)
        print("Videos list: ", videos_list)
        local_video_output_path = f'{self.output_folder}{self.output_video_build_name}'
        video_output_path = VideoHandler.join_videos(videos_list, local_video_output_path)
        # Upload video to Minio
        minio_client.fput_object(bucket_name, video_output_path_remote, video_output_path)
        
        return video_output_path_remote
    


if __name__ == "__main__":

    vide_handler = VideoHandler()
    # List of video paths
    video_list = ['./tmp/video1.mp4', './tmp/video2.mp4']
    
    # Output video path
    output_path = './tmp/output_video.mp4'

    # Join videos
    video_output_path = vide_handler.join_videos(video_list, output_path)

    print(video_output_path)
