import os
from libs.core import Application

from dotenv import load_dotenv
load_dotenv()  

SERVER_IP = os.getenv("IP_ADDRESS")
API_BASE_URL = f"http://{SERVER_IP}:8003"

minio_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret = os.getenv("MINIO_SECRET_KEY")
minio_url = f"{SERVER_IP}:9000"
BUCKET_NAME = "my-bucket"

BACKEND_EMAIL =  os.getenv("BACKEND_EMAIL", "admin@example.com")
BACKEND_PASSWORD = os.getenv("BACKEND_PASSWORD", "Adminpassword1@")
BACKEND_BASE_URL = f"http://{SERVER_IP}:3001"


TOPIC_INPUT = "video-general-results"
TOPIC_OUTPUT = "video-fine-detections"

brokers = [f'{SERVER_IP}:9092']

WORKING_FOLDER = "./tmp"

if __name__ == "__main__":
    print("Starting...")
    
    # Validate environment variables first
    if not SERVER_IP:
        raise ValueError("IP_ADDRESS environment variable not set")
    
    os.makedirs(WORKING_FOLDER, exist_ok=True)
    
    app = Application(server_ip=SERVER_IP,
                      brokers=brokers,
                      minio_access_key=minio_key, 
                      minio_secret_key=minio_secret,
                      api_base_url=API_BASE_URL,
                      backend_email=BACKEND_EMAIL,
                      backend_password=BACKEND_PASSWORD,
                      backend_base_url=BACKEND_BASE_URL,
                      topic_input=TOPIC_INPUT,
                      topic_output=TOPIC_OUTPUT,
                      bucket_name=BUCKET_NAME,
                      output_folder=WORKING_FOLDER)
    app.run(offset='latest')
