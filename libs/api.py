import requests
from typing import List

class ApiClient:
    def __init__(self, base_url):
        self.base_url = base_url
    
    def get_all_items(self):
        response = requests.get(f"{self.base_url}/items/")
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"Failed to decode JSON. Status Code: {response.status_code}. Response Text: '{response.text}'")
            return None

    def get_item(self, item_id):
        response = requests.get(f"{self.base_url}/items/{item_id}")
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"Failed to decode JSON. Status Code: {response.status_code}. Response Text: '{response.text}'")
            return None
        
    def get_by_video_id(self, video_id:str, status=None) -> requests.Response:
        url = f"{self.base_url}/items/video_id/{video_id}/"
        if status is not None:
            url = f"{self.base_url}/items/video_id/{video_id}/?status={status}"
        try:        
            response = requests.get(url)
            return response
        except requests.exceptions.JSONDecodeError:
            print(f"Failed to decode JSON. Status Code: {response.status_code}. Response Text: '{response.text}'")
            return None

    def create_item(self, item_data) -> requests.Response:
        response = requests.post(f"{self.base_url}/items/", json=item_data)
        return response

    def update_item_status(self, item_id: int, item_data: dict):
        response = requests.put(f"{self.base_url}/items/{item_id}", json=item_data)
        return response.json()

    def delete_item(self, item_id):
        response = requests.delete(f"{self.base_url}/items/{item_id}")
        return response.json()
    
    
# Example usage
if __name__ == "__main__":
    client = ApiClient("http://127.0.0.1:8000")
    
    # Get all items
    print("Fetching all items...")
    items = client.get_all_items()
    print(items)

    # Get a single item
    print("Fetching a single item...")
    item = client.get_item(1)
    print(item)

    # Create a new item
    print("Creating a new item...")
    new_item = {
        "id": 5,
        "remote_path": "new path1",
        "file_path": "new file path",
        "video_id": "asdasd",
        "status": "new status"
    }
    created_item = client.create_item(new_item)
    print(created_item)

    # Update an existing item
    print("Updating an item...")
    update_data = {
        "id": 4,
        "remote_path": "updated path",
        "file_path": "updated file path",
        "video_id": "updated video id",
        "status": "updated status"
    }
    updated_item = client.update_item(4, update_data)
    print(updated_item)

    # Delete an item
    print("Deleting an item...")
    deleted_response = client.delete_item(4)
    print(deleted_response)