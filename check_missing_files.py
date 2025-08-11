#!/usr/bin/env python3
"""
Script to check for missing result files in S3/MinIO that might be causing
aggregation failures.
"""

import os
import sys
import logging
from typing import List, Dict
from minio import Minio
from minio.error import S3Error
from libs.api import ApiClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_missing_files():
    """
    Check for missing result files for pending tasks.
    """
    # Environment setup
    SERVER_IP = os.getenv("IP_ADDRESS")
    minio_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret = os.getenv("MINIO_SECRET_KEY")
    API_BASE_URL = f"http://{SERVER_IP}:8003"
    BUCKET_NAME = "my-bucket"
    
    if not all([SERVER_IP, minio_key, minio_secret]):
        logger.error("Missing required environment variables")
        sys.exit(1)
    
    # Initialize clients
    logger.info("Initializing MinIO and API clients...")
    minio_client = Minio(
        f"{SERVER_IP}:9000",
        access_key=minio_key,
        secret_key=minio_secret,
        secure=False
    )
    
    api_client = ApiClient(API_BASE_URL)
    
    # Get all pending tasks
    logger.info("Fetching pending tasks...")
    try:
        response = api_client.get_all_items()
        if not response or not isinstance(response, list):
            logger.error("Failed to fetch tasks from API")
            return
            
        pending_tasks = [task for task in response if task.get('status') == 'pending']
        logger.info(f"Found {len(pending_tasks)} pending tasks")
        
    except Exception as e:
        logger.error(f"Error fetching tasks: {e}")
        return
    
    # Group tasks by video_id
    videos_tasks: Dict[str, List] = {}
    for task in pending_tasks:
        video_id = task.get('video_id')
        if video_id:
            if video_id not in videos_tasks:
                videos_tasks[video_id] = []
            videos_tasks[video_id].append(task)
    
    logger.info(f"Found tasks for {len(videos_tasks)} videos")
    
    # Check each video
    total_missing = 0
    for video_id, tasks in videos_tasks.items():
        logger.info(f"\nChecking video: {video_id}")
        logger.info(f"  Tasks: {len(tasks)}")
        
        missing_files = []
        existing_files = []
        
        for task in tasks:
            remote_path = task.get('remote_path')
            if not remote_path:
                logger.warning(f"  Task {task.get('id')} has no remote_path")
                continue
            
            try:
                # Check if file exists in S3
                stat = minio_client.stat_object(BUCKET_NAME, remote_path)
                existing_files.append(remote_path)
                logger.info(f"  ✓ Found: {remote_path}")
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    missing_files.append(remote_path)
                    logger.warning(f"  ✗ Missing: {remote_path}")
                else:
                    logger.error(f"  Error checking {remote_path}: {e}")
            except Exception as e:
                logger.error(f"  Unexpected error checking {remote_path}: {e}")
        
        logger.info(f"  Summary: {len(existing_files)} found, {len(missing_files)} missing")
        total_missing += len(missing_files)
        
        if missing_files:
            logger.warning(f"  Missing files for video {video_id}:")
            for missing in missing_files:
                logger.warning(f"    - {missing}")
    
    logger.info(f"\n=== SUMMARY ===")
    logger.info(f"Total videos checked: {len(videos_tasks)}")
    logger.info(f"Total missing files: {total_missing}")
    
    if total_missing == 0:
        logger.info("✓ All required files are present in S3!")
    else:
        logger.warning(f"✗ {total_missing} files are missing and may cause aggregation failures")
        
        # Suggest actions
        logger.info("\n=== SUGGESTED ACTIONS ===")
        logger.info("1. Check if the video processing service is running properly")
        logger.info("2. Check if there are errors in the video processing logs")
        logger.info("3. Consider re-processing the videos with missing results")
        logger.info("4. Check CUDA memory issues if using GPU processing")

def list_recent_files():
    """
    List recent files in the bucket to see what's being uploaded.
    """
    SERVER_IP = os.getenv("IP_ADDRESS")
    minio_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret = os.getenv("MINIO_SECRET_KEY")
    BUCKET_NAME = "my-bucket"
    
    # Initialize client
    minio_client = Minio(
        f"{SERVER_IP}:9000",
        access_key=minio_key,
        secret_key=minio_secret,
        secure=False
    )
    
    logger.info(f"Listing recent files in bucket: {BUCKET_NAME}")
    
    try:
        objects = minio_client.list_objects(BUCKET_NAME, recursive=True)
        
        # Get recent objects (last 50)
        recent_objects = []
        for obj in objects:
            if obj.object_name.endswith('_results.json'):
                recent_objects.append((obj.object_name, obj.last_modified))
        
        # Sort by modification time (most recent first)
        recent_objects.sort(key=lambda x: x[1], reverse=True)
        
        logger.info(f"Found {len(recent_objects)} result files total")
        logger.info("Most recent 20 result files:")
        
        for i, (name, modified) in enumerate(recent_objects[:20]):
            logger.info(f"  {i+1}. {name} (modified: {modified})")
            
    except Exception as e:
        logger.error(f"Error listing objects: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        list_recent_files()
    else:
        check_missing_files()