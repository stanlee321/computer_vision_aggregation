#!/usr/bin/env python3
"""
Script to clean old/invalid tasks from the API
This will mark as 'cancelled' or delete tasks that don't have files in S3
"""

import os
import requests
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

SERVER_IP = os.getenv("IP_ADDRESS")
API_BASE_URL = f"http://{SERVER_IP}:8000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL = f"{SERVER_IP}:9000"
BUCKET_NAME = "my-bucket"

def check_file_exists_in_s3(client, bucket, path):
    """Check if a file exists in S3/MinIO"""
    try:
        client.stat_object(bucket, path)
        return True
    except:
        return False

def clean_invalid_tasks(video_id=None, job_id=None, dry_run=False):
    """Clean tasks that don't have corresponding files in S3"""
    
    print("\n" + "="*60)
    print("CLEANING INVALID TASKS FROM API")
    print("="*60)
    
    # Connect to MinIO
    minio_client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Get tasks from API
    if video_id:
        url = f"{API_BASE_URL}/items/video_id/{video_id}/?status=pending"
    else:
        url = f"{API_BASE_URL}/items/"
    
    print(f"Fetching tasks from: {url}")
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to get tasks: {response.status_code}")
            return
        
        tasks = response.json()
        print(f"Found {len(tasks)} tasks")
        
        if job_id:
            # Filter by job_id if provided
            tasks = [t for t in tasks if job_id in t.get('remote_path', '')]
            print(f"Filtered to {len(tasks)} tasks for job_id: {job_id}")
        
        # Check each task
        tasks_to_clean = []
        valid_tasks = []
        
        for task in tasks:
            remote_path = task.get('remote_path', '')
            task_id = task.get('id')
            
            if not remote_path:
                continue
            
            # Check if file exists in S3
            exists = check_file_exists_in_s3(minio_client, BUCKET_NAME, remote_path)
            
            if exists:
                print(f"✅ Valid task {task_id}: {remote_path}")
                valid_tasks.append(task)
            else:
                print(f"❌ Invalid task {task_id}: {remote_path} (file not found)")
                tasks_to_clean.append(task)
        
        print(f"\nSummary:")
        print(f"  Valid tasks: {len(valid_tasks)}")
        print(f"  Invalid tasks to clean: {len(tasks_to_clean)}")
        
        if dry_run:
            print("\n🔍 DRY RUN - No changes will be made")
            print("\nTasks that would be cleaned:")
            for task in tasks_to_clean:
                print(f"  - ID: {task.get('id')}, Path: {task.get('remote_path')}")
            return
        
        # Clean invalid tasks
        if tasks_to_clean:
            response = input(f"\nDo you want to clean {len(tasks_to_clean)} invalid tasks? (y/n): ")
            if response.lower() == 'y':
                cleaned_count = 0
                for task in tasks_to_clean:
                    task_id = task.get('id')
                    if task_id:
                        # Try to update status to cancelled
                        update_response = requests.put(
                            f"{API_BASE_URL}/items/{task_id}",
                            json={"status": "cancelled"}
                        )
                        
                        if update_response.status_code == 200:
                            print(f"✅ Cancelled task {task_id}")
                            cleaned_count += 1
                        else:
                            # Try to delete if update fails
                            delete_response = requests.delete(f"{API_BASE_URL}/items/{task_id}")
                            if delete_response.status_code == 200:
                                print(f"✅ Deleted task {task_id}")
                                cleaned_count += 1
                            else:
                                print(f"⚠️  Failed to clean task {task_id}")
                
                print(f"\n✅ Cleaned {cleaned_count}/{len(tasks_to_clean)} tasks")
            else:
                print("Cancelled - no changes made")
        else:
            print("\n✅ No invalid tasks found - nothing to clean")
        
    except Exception as e:
        print(f"Error: {e}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean invalid tasks from API')
    parser.add_argument('--video-id', 
                       default="019896c6-b5b6-736d-88bb-5216a5b8ea77",
                       help='Video ID to clean')
    parser.add_argument('--job-id',
                       default="0198977a-3e85-7084-ba5e-034e62cf8a00",
                       help='Job ID to filter tasks')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be cleaned without making changes')
    parser.add_argument('--all', action='store_true',
                       help='Clean all invalid tasks (not just for specific video)')
    
    args = parser.parse_args()
    
    if args.all:
        clean_invalid_tasks(dry_run=args.dry_run)
    else:
        clean_invalid_tasks(
            video_id=args.video_id,
            job_id=args.job_id,
            dry_run=args.dry_run
        )

if __name__ == "__main__":
    main()