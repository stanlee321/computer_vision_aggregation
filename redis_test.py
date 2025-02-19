from libs.redis_service import RedisClient

import os

IP_ADDRESS = os.getenv("IP_ADDRESS")




if __name__ == "__main__":
    redis_client = RedisClient(host=IP_ADDRESS, port=6379, password="Secret.")

    # redis_client.set_value("test", "test")
    print(redis_client.get_value("video:975e0b28-202e-47dc-9b49-6db45bee6ae8_label:complete"))
