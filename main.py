from libs.core import Application
SERVER_IP = "localhost"
API_BASE_URL = "http://localhost:8003"
minio_key =  "WraSmXSjxqJgFgPSklHk"
minio_secret = "Jt4AeVlUVcLZmrhxaRDcABTx2Ir3ODlmxxuXLnsF"
minio_url = f"{SERVER_IP}:9000"

# Kafka
brokers = [f'{SERVER_IP}:9092']

if __name__ == "__main__":
    print("Starting...")
    app = Application(server_ip=SERVER_IP,
                      brokers=brokers,
                      minio_access_key=minio_key, 
                      minio_secret_key=minio_secret,
                      api_base_url=API_BASE_URL)
    app.run()