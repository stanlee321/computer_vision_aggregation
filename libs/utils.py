import re
import json
import random
import time
from datetime import datetime
from typing import Dict, Any, Union

def generate_unique_id():
    random.seed(time.time())  # Seed random number generator with current time
    new_id = random.randint(1, 999999999)
    return new_id

def extract_chunk_value(filename):
    match = re.search(r'chunk_(\d+)_', filename)
    if match:
        return int(match.group(1))
    else:
        return -1
    

def put_to_redis(
                video_id: str,
                json_data: Union[Dict[str, Any], None] = None,
                local_data_path : Union[str, None] = None,
                redis_client: Any = None,
                label : str = "complete"
                 ) -> str:
    
    if local_data_path:
        # # Load data from local path to json_data
        # with open(local_data_path, 'r') as file:
        #     json_data = json.load(file)
        
        # Save data to local path
        with open(local_data_path, 'w') as file:
            json.dump(json_data, file)
        
    # Save to Redis
    key = f"video:{video_id}_label:{label}"
    
    redis_client.set_value(key, json.dumps(json_data))
    
    print(f"Data saved to Redis with key: {key}")
    
    return key
    