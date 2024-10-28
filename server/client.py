import requests
import time


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

    def get_items_by_video_id(self, video_id, status=None, kind=None):
        url = f"{self.base_url}/items/video_id/{video_id}/"
        params = {}
        if status:
            params['status'] = status
        if kind:
            params['kind'] = kind
        response = requests.get(url, params=params)
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


def run_tests(client):
    """Run a series of tests to verify all endpoints"""
    
    print("\n=== Starting API Tests ===\n")
    
    # Test 1: Get all items (initially should be empty or contain default items)
    print("Test 1: Getting all items...")
    items = client.get_all_items()
    print(f"Current items in database: {items}")
    
    # Test 2: Create new items
    print("\nTest 2: Creating new items...")
    test_items = [
        {
            "remote_path": "test/path/1",
            "original_video": "video_url_1",
            "video_id": "vid_001",
            "status": "pending",
            "kind": "ground",
            "fps": 24.0
        },
        {
            "remote_path": "test/path/2",
            "original_video": "video_url_2",
            "video_id": "vid_002",  # Same video_id, different path
            "status": "processing",
            "kind": "fine",
            "fps": 30.0
        }
    ]
    
    created_items = []
    for item in test_items:
        result = client.create_item(item)
        created_items.append(result)
        print(f"Created item: {result}")
    
    # Test 3: Get item by ID
    print("\nTest 3: Getting item by ID...")
    if created_items:
        if 'details' in created_items[0]:
            if '400' in created_items[0]['details']:
                print(f"Item creation failed with error: {created_items[0]['details']['400']}")
                pass
            else:
                item_id = created_items[0]['id']
                item = client.get_item(item_id)
                print(f"Retrieved item {item_id}: {item}")
    
    # Test 4: Get items by video_id
    print("\nTest 4: Getting items by video_id...")
    items_by_video = client.get_items_by_video_id("vid_001")
    print(f"Items with video_id 'vid_001': {items_by_video}")
    
    # Test 5: Get items by video_id with filters
    print("\nTest 5: Getting items by video_id with status filter...")
    filtered_items = client.get_items_by_video_id("vid_001", status="pending", kind="ground")
    print(f"Filtered items: {filtered_items}")
    
    # Test 6: Update item status
    print("\nTest 6: Updating item status...")
    if created_items:
        if 'details' in created_items[0]:
            if '400' in created_items[0]['details']:
                print(f"Item update failed with error: {created_items[0]['details']['400']}")
                pass
            else:
                item_id = created_items[0]['id']
                update_result = client.update_item(item_id, {"status": "completed"})
                print(f"Updated item {item_id}: {update_result}")
    
    # Test 7: Test invalid operations
    print("\nTest 7: Testing invalid operations...")
    try:
        # Try to create item with invalid kind
        invalid_item = test_items[0].copy()
        invalid_item['kind'] = 'invalid_kind'
        result = client.create_item(invalid_item)
        print("Should have failed: Invalid kind")
    except Exception as e:
        print(f"Expected error caught: {str(e)}")
    
    # Test 8: Delete items
    print("\nTest 8: Deleting items...")
    for created_item in created_items:
        if 'details' in created_item:
            if '400' in created_item['details']:
                print(f"Item deletion failed with error: {created_item['details']['400']}")
                pass
            else:
                item_id = created_item['id']
                delete_result = client.delete_item(item_id)
                print(f"Deleted item {item_id}: {delete_result}")
    
    # Final verification
    print("\nFinal verification: Getting all items...")
    final_items = client.get_all_items()
    print(f"Items in database after tests: {final_items}")
    
    print("\n=== Tests Completed ===")


if __name__ == "__main__":
    client = VideoHandlerClient("http://192.168.1.9:8003")
    run_tests(client)
