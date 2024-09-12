from cloudevents.http import CloudEvent
from dotenv import load_dotenv
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from google.cloud import storage
from omnivoreql import OmnivoreQL
import functions_framework
import json
import os
import requests



load_dotenv()
omnivoreql_client = OmnivoreQL(os.getenv('OMNIVORE_API_KEY'))
print(f"Omnivore profile: {omnivoreql_client.get_profile()}")

@functions_framework.cloud_event
def hello_gcs(cloud_event: CloudEvent) -> tuple:
    """This function is triggered by a change in a storage bucket and logs the contents of sources.json.

    Args:
        cloud_event: The CloudEvent that triggered this function.
    Returns:
        The event ID, event type, bucket, name, metageneration, and timeCreated.
    """
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket_name = data["bucket"]
    file_name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket_name}")
    print(f"File: {file_name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    if file_name == "sources.json":
        
        print("Detected sources.json file change, processing...")
        # get the source file from google storage
        sources = read_file_from_gcs(bucket_name, file_name)
        print("Fetched sources.json from GCS")

        # get the ingested file
        all_ingested = read_file_from_gcs(bucket_name, "ingested.json")
        print("Fetched ingested.json from GCS")

        # iterate over all the sources
        for source_url, metadata in sources.items():
            print(f"Processing source: {source_url} of type {metadata["type"]}")
            print(f"Labels for {source_url}: {metadata["labels"]}")
            # items already ingested from that source
            ingested_per_source = all_ingested[source_url] if source_url in all_ingested else []
            print(f"Already ingested items for {source_url}: {ingested_per_source}")
            # all items from that source
            all_items_per_source = get_all_items_from_source(source_url, metadata["type"])
            print(f"Number of items from {source_url}: {all_items_per_source.count}")
            # get the new items
            new_items = [item for item in all_items_per_source if item not in ingested_per_source]
            print(f"Number of new items to ingest for {source_url}: {new_items.count}")
            # ingest that difference into omnivovre and mark them as visited
            ingest_to_omnivore(new_items, metadata["labels"])
            print(f"Ingested {new_items.count} new items for {source_url} into Omnivore")
            # mark them as visited
            all_ingested[source_url] = ingested_per_source + new_items
            print(f"Total of {all_ingested[source_url].count} items ingested for {source_url}")

        write_file_to_gcs(all_ingested, bucket_name, "ingested.json")
        print("Updated ingested.json in GCS")

    return event_id, event_type, bucket_name, file_name, metageneration, timeCreated, updated

def get_all_items_from_source(source_url: str, source_type: str) -> list:
    items = []
    if source_type == "blog":
        response = requests.get(source_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        parsed_url = urlparse(source_url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            if full_url.startswith(base_url):
                items.append(full_url)
    
    return items

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
    print(f"Contents of {file_name}: {json.dumps(sources, indent=2)}")
    return sources 

def ingest_to_omnivore(items: list, labels: list):
    for item in items:
        omnivoreql_client.save_url(item, labels)