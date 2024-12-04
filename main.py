import os
from libs.core import Application


SERVER_IP = "192.168.1.19"
API_BASE_URL = f"http://{SERVER_IP}:8003"

minio_key = "c3aFDmKuGhPCSxkpRDGf"
minio_secret = "MLYz9tZI3h4xZAwBl8llyEtX6R07YcMuRdSYPIcx"
minio_url = f"{SERVER_IP}:9000"
BUCKET_NAME = "my-bucket"


TOPIC_INPUT = "video-general-results"
TOPIC_OUTPUT = "video-fine-detections"
brokers = [f'{SERVER_IP}:9092']

WORKING_FOLDER = "./tmp"

if __name__ == "__main__":
    print("Starting...")
    
    os.makedirs(WORKING_FOLDER, exist_ok=True)
    
    app = Application(server_ip=SERVER_IP,
                      brokers=brokers,
                      minio_access_key=minio_key, 
                      minio_secret_key=minio_secret,
                      api_base_url=API_BASE_URL,
                      topic_input=TOPIC_INPUT,
                      topic_output=TOPIC_OUTPUT,
                      bucket_name=BUCKET_NAME,
                      output_folder=WORKING_FOLDER)
    app.run()
