import os
import logging
import sys
from libs.core import Application

from dotenv import load_dotenv
load_dotenv()

# Configure main application logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('aggregation_main.log'),
        logging.StreamHandler()
    ]
)

# Reduce Kafka logging noise
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.ERROR)
logging.getLogger('kafka.client').setLevel(logging.ERROR)
logging.getLogger('kafka.coordinator').setLevel(logging.ERROR)
logging.getLogger('kafka.consumer').setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

# Environment variables with validation
SERVER_IP = os.getenv("IP_ADDRESS")
if not SERVER_IP:
    logger.error("IP_ADDRESS environment variable not set")
    sys.exit(1)

API_BASE_URL = f"http://{SERVER_IP}:8003"

minio_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret = os.getenv("MINIO_SECRET_KEY")
if not minio_key or not minio_secret:
    logger.error("MINIO credentials not set in environment variables")
    sys.exit(1)

minio_url = f"{SERVER_IP}:9000"
BUCKET_NAME = "my-bucket"

BACKEND_EMAIL = os.getenv("BACKEND_EMAIL", "admin@example.com")
BACKEND_PASSWORD = os.getenv("BACKEND_PASSWORD", "Adminpassword1@")
BACKEND_BASE_URL = f"http://{SERVER_IP}:3001"

TOPIC_INPUT = "video-general-results"
TOPIC_OUTPUT = "video-fine-detections"

brokers = [f'{SERVER_IP}:9092']

WORKING_FOLDER = "./tmp"

logger.info(f"🔧 Configuration:")
logger.info(f"  📍 SERVER_IP: {SERVER_IP}")
logger.info(f"  🌐 API_BASE_URL: {API_BASE_URL}")
logger.info(f"  🗄️  MINIO_URL: {minio_url}")
logger.info(f"  🪣 BUCKET_NAME: {BUCKET_NAME}")
logger.info(f"  🔗 BACKEND_BASE_URL: {BACKEND_BASE_URL}")
logger.info(f"  📥 TOPIC_INPUT: {TOPIC_INPUT}")
logger.info(f"  📤 TOPIC_OUTPUT: {TOPIC_OUTPUT}")
logger.info(f"  🚌 BROKERS: {brokers}")
logger.info(f"  📁 WORKING_FOLDER: {WORKING_FOLDER}")

if __name__ == "__main__":
    logger.info("🚀 === Starting Computer Vision Aggregation Service ===")
    
    try:
        # Create working directory
        os.makedirs(WORKING_FOLDER, exist_ok=True)
        logger.info(f"📁 Working directory ready: {WORKING_FOLDER}")
        
        # Initialize application
        logger.info("🔧 Initializing application...")
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
        
        logger.info("✅ Application initialized successfully")
        logger.info("🔄 Starting message consumption...")
        
        # Run the application
        app.run(offset='latest')
        
    except KeyboardInterrupt:
        logger.info("⛔ Received interrupt signal, shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Fatal error starting application: {e}")
        sys.exit(1)
