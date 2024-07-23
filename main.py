from minio import Minio

from libs.video_handler import VideoHandler
from libs.clean_data import ProcessData
from libs.queues import KafkaHandler
from libs.redis_service import RedisClient


from libs.utils import (
    read_or_create_dataframe,
    create_pandas_data_task,
    append_to_tasks_df,
    check_task_exists,
    extract_chunk_value,
    process_data,
    create_join_video,
    create_json_data,
    put_to_redis
)


if __name__ == "__main__":
    
    print("Starting ...")
    
    bucket_name = "my-bucket"
    
    topic_input = 'RESULTS'
    topic_group = 'results-group'
    
    
    # Initialize Kafka handler
    kafka_handler = KafkaHandler(bootstrap_servers=['192.168.1.12:9093'])
        
    client_minio = Minio("0.0.0.0:9000",  # Replace with your MinIO storage address
        access_key = "minioadmin",   # Replace with your access key
        secret_key = "minioadmin",    # Replace with your secret key
        secure = False
    )
    
    # Join VIDEO
    output_folder = './tmp/'
    video_handler = VideoHandler( output_folder = output_folder)
    
    data_handler = ProcessData(
        minio_client=client_minio,
        bucket=bucket_name
    )
    
    
    # Redis Client
    redis_client = RedisClient(host='0.0.0.0', port=6379)
    

    # # Create Kafka consumer
    consumer = kafka_handler.create_consumer(topic_input, topic_group)

    for message in consumer:
        
        data_df_path = "./data/outputs/data.csv"
        tasks_df = read_or_create_dataframe(data_df_path, status='pending')
    
        print(f"Consumed message: {message.value}")
        file : str = message.value
        
        video_id = file.split('/')[0]
        filename = file.split('/')[-1]
        
        # Process data
        output_file_path =  f'./data/outputs/{filename}'
        client_minio.fget_object(bucket_name, file, output_file_path)
        chunk_id = extract_chunk_value(output_file_path)
        
        # Save to the tasks table
        if check_task_exists(video_id, chunk_id, tasks_df):
            print(f"Task with video_id: {video_id} and chunk: {chunk_id} already exists.")
            df_to_work = tasks_df[tasks_df['status'] == 'pending']
            if df_to_work.empty:
                print("No pending tasks")
                continue
        
        new_task_data = create_pandas_data_task(output_file_path, video_id, status='pending')
        tasks_df = append_to_tasks_df(new_task_data, tasks_df)
        
        tasks_df.to_csv(data_df_path, index=False)

        # Filter only pending tasks
        df_to_work = tasks_df[tasks_df['status'] == 'pending']

        # Process data
        df = process_data(data_path = data_df_path, data_handler=data_handler, video_id=video_id)
        
        # Create video
        remote_video_path = create_join_video(video_id, df, client_minio, bucket_name, video_handler=video_handler)
        
        # Create JSON data
        local_data_path, json_data = create_json_data(df, 
                                        remote_video_path, 
                                        conditions = ['frame_number', 'class', 'track_id'], 
                                        output_name='output_json_timestamp.json')
    
        print("Data saved to", local_data_path)
        # Save to Redis
        key_data = put_to_redis(video_id, json_data,  redis_client)
        
        
        # Create Full Data for Further Analysis
        local_data_path_full, json_data_full = create_json_data(df, 
                                remote_video_path, 
                                conditions = None, 
                                output_name = 'output_json_timestamp_full.json')
        # Save to Redis
        key_data_full = put_to_redis(video_id, json_data_full, redis_client)


        # Update columns with "pending" to "done" in the tasks_df
        tasks_df.loc[tasks_df['status'] == 'pending', 'status'] = 'done'
        tasks_df.to_csv(data_df_path, index=False)
        