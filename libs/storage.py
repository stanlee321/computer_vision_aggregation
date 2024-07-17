import logging
from minio import Minio
from minio.error import S3Error
import io


class MinioHandler:
    def __init__(self, storage_address, access_key, secret_key):
        self.client = Minio(
            storage_address,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        logging.basicConfig(level=logging.INFO)
        
    def create_bucket(self, bucket_name: str, region: str = "us-east-1", object_lock: bool = False):
        try:
            if self.client.bucket_exists(bucket_name):
                logging.info(f"Bucket '{bucket_name}' already exists.")
            else:
                self.client.make_bucket(bucket_name, region=region, object_lock=object_lock)
                logging.info(f"Bucket '{bucket_name}' created successfully in region '{region}' with object lock set to {object_lock}.")
        except S3Error as e:
            logging.error(f"Error occurred while creating bucket: {e}")
        
    def list_buckets_and_objects(self):
        try:
            for bucket in self.client.list_buckets():
                logging.info(f"Bucket: {bucket.name}")
                for item in self.client.list_objects(bucket.name, recursive=True):
                    logging.info(f"Object: {item.object_name}")
        except S3Error as e:
            logging.error(f"Error occurred while listing buckets/objects: {e}")

    def upload_file_json(self, bucket_name: str, object_name: str, file_path: str):
        try:
            content_type = 'application/json'
            with open(file_path, 'rb') as f:
                data = f.read()
                self.client.put_object(bucket_name, object_name, io.BytesIO(data), len(data), content_type)
                
            logging.info(f"File '{file_path}' uploaded as '{object_name}' to bucket '{bucket_name}'.")

        except S3Error as e:
            logging.error(f"Error occurred while uploading file: {e}")
            
    def upload_image(self, bucket_name: str, s3_path: str, file_path: str, content_type: str):
        try:
            self.upload_file(bucket_name, s3_path, file_path, content_type)
        except S3Error as e:
            logging.error(f"Error occurred while uploading folder: {e}")
            
    def upload_file(self, bucket_name: str, object_name: str, file_path: str, content_type: str = 'video/mp4'):
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
                self.client.put_object(bucket_name, object_name, io.BytesIO(data), len(data), content_type)
                
            logging.info(f"File '{file_path}' uploaded as '{object_name}' to bucket '{bucket_name}'.")

        except S3Error as e:
            logging.error(f"Error occurred while uploading file: {e}")

    def download_file(self, bucket_name: str, object_name: str, download_path: str):
        try:
            print(f"Downloading file '{object_name}' from bucket '{bucket_name}' to '{download_path}'.")
            response = self.client.get_object(bucket_name, object_name)
            with open(download_path, 'wb') as f:
                for d in response.stream(32*1024):
                    f.write(d)
            logging.info(f"File '{object_name}' from bucket '{bucket_name}' downloaded to '{download_path}'.")
        except S3Error as e:
            logging.error(f"Error occurred while downloading file: {e}")
            # Exit
            exit(1)

# Example usage
if __name__ == "__main__":
    storage = MinioHandler(
        "0.0.0.0:9000",  # Replace with your MinIO storage address
        "minioadmin",   # Replace with your access key
        "minioadmin"    # Replace with your secret key
    )
    
    bucket_name = "my-bucket"
    storage.create_bucket(bucket_name, region="us-east-1", object_lock=True)
    storage.list_buckets_and_objects()

    # Define the paths for the file to be uploaded and downloaded
    local_file_path = "./input/448683010_1021746869607118_4579625603865590825_n.jpg"
    object_name = "uploaded-file.jpeg"
    download_path = "./output/downloaded-file.jpeg"

    # Upload the file
    storage.upload_file(bucket_name, object_name, local_file_path)
    
    # Download the file with a new name
    storage.download_file(bucket_name, object_name, download_path)