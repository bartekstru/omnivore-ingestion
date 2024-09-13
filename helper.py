from google.cloud import storage
import json

def write_file_to_gcs(data: dict, bucket_name: str, file_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print(f"Updated {file_name} in bucket {bucket_name}")

def read_file_from_gcs(bucket_name: str, file_name: str) -> dict:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    contents = blob.download_as_text()
    sources = json.loads(contents)
    print(f"Fetched {file_name} from GCS")
    return sources 