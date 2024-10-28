from libs.redis_service import RedisClient




if __name__ == "__main__":
    redis_client = RedisClient(host='192.168.1.16', port=6379, password="Secret.")

    # redis_client.set_value("test", "test")
    print(redis_client.get_value("video:975e0b28-202e-47dc-9b49-6db45bee6ae8_label:complete"))
