import requests


class VideoHandlerClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def get_all_items(self):
        response = requests.get(f"{self.base_url}/items/")
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            print(
                f"Failed to decode JSON. Status Code: {response.status_code}. Response Text: '{response.text}'")
            return None

    def get_item(self, item_id):
        response = requests.get(f"{self.base_url}/items/{item_id}")
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            print(
                f"Failed to decode JSON. Status Code: {response.status_code}. Response Text: '{response.text}'")
            return None

    def create_item(self, item_data):
        response = requests.post(f"{self.base_url}/items/", json=item_data)
        return response.json()

    def update_item(self, item_id, item_data):
        response = requests.put(
            f"{self.base_url}/items/{item_id}", json=item_data)
        return response.json()

    def delete_item(self, item_id):
        response = requests.delete(f"{self.base_url}/items/{item_id}")
        return response.json()


# Example usage
if __name__ == "__main__":
    client = VideoHandlerClient("http://127.0.0.1:8000")

    # # Get all items
    # print("Fetching all items...")
    # items = client.get_all_items()
    # print(items)

    # # Get a single item
    # print("Fetching a single item...")
    # item = client.get_item(1)
    # print(item)

    # # Create a new item
    # print("Creating a new item...")
    # new_item = {
    #     "id": 5,
    #     "remote_path": "new path1",
    #     "file_path": "new file path",
    #     "video_id": "asdasd",
    #     "status": "new status"
    # }

    new_item = {'remote_path': '0fbcc102-6fca-46ee-841f-7e40e5f87b71/XVR_ch1_main_20210910141900_20210910142500_chunk_1_of_6_results.json',
                'original_video': 'Some video_url', 'video_id': '0abcc102-6fca-46ee-841f-7e40e5f87b71', 
                'status': 'pending', 'kind': 'yolo', 'fps': 2.7777777777777777}
    created_item = client.create_item(new_item)
    print(created_item)

    # # Update an existing item
    # print("Updating an item...")
    # update_data = {
    #     "id": 4,
    #     "remote_path": "updated path",
    #     "file_path": "updated file path",
    #     "video_id": "updated video id",
    #     "status": "updated status"
    # }
    # updated_item = client.update_item(4, update_data)
    # print(updated_item)

    # # Delete an item
    # print("Deleting an item...")
    # deleted_response = client.delete_item(4)
    # print(deleted_response)
