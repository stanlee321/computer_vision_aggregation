

from minio import Minio
from minio.error import S3Error


def list_files_in_folder(minio_client: Minio, bucket_name, prefix):
    try:
        print(f"Listing objects in bucket {bucket_name} with prefix {prefix}")
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        data = []
        for obj in objects:
            if obj.object_name.endswith('.json'):
                data.append(obj.object_name)
        return data
    except S3Error as e:
        print(f"Error listing objects in bucket {bucket_name} with prefix {prefix}: {e}")

def list_buckets_and_objects(minio_client: Minio):
    try:
        for bucket in minio_client.list_buckets():
            print(f"Bucket: {bucket.name}")
            for item in minio_client.list_objects(bucket.name, recursive=True):
                print(f"Object: {item.object_name}")
    except S3Error as e:
        print(f"Error occurred while listing buckets/objects: {e}")
