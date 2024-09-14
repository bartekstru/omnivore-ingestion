from cloudevents.http import CloudEvent
from dotenv import load_dotenv
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from omnivoreql import OmnivoreQL, CreateLabelInput
from helper import *
import functions_framework
import os
import requests
import uuid

# load_dotenv()

OMNIVORE_API_KEY=os.getenv("OMNIVORE_API_KEY")
YOUTUBE_API_KEY=os.getenv("YOUTUBE_API_KEY")
omnivoreql_client = OmnivoreQL(OMNIVORE_API_KEY)

@functions_framework.cloud_event
def omnivore_ingest(cloud_event: CloudEvent) -> tuple:
    """This function is triggered by a change in a storage bucket and logs the contents of sources.json.

    Args:
        cloud_event: The CloudEvent that triggered this function.
    Returns:
        The event ID, event type, bucket, name, metageneration, and timeCreated.
    """
    data = cloud_event.data

    file_name = data["name"]

    if file_name == "sources.json":
        ingest_on_source_change(data["bucket"], file_name)

def ingest_on_source_change(bucket_name: str, file_name: str):
        print("Detected change in sources.")

        sources = read_file_from_gcs(bucket_name, file_name)
        all_ingested = read_file_from_gcs(bucket_name, "ingested.json")

        for source_url, metadata in sources.items():
            all_ingested[source_url] = all_ingested[source_url] if source_url in all_ingested else []
            successfully_ingested = ingest_for_source(source_url, metadata, all_ingested[source_url])
            all_ingested[source_url] += successfully_ingested
            print(f"Total of {len(all_ingested[source_url])} items ingested for {source_url}")

        write_file_to_gcs(all_ingested, bucket_name, "ingested.json")

        print("Finished ingestion afer sources update.")

def ingest_for_source(source_url: str, source_metadata: dict, ingested_per_source: list) -> list:
    source_type = source_metadata['type']
    source_labels = source_metadata['labels']

    print(f"Processing source: {source_url} of type {source_type} with labels {source_labels}")

    all_items_per_source, extra_source_labels = get_all_items_from_source(source_url, source_type)

    new_items = [item for item in all_items_per_source if item not in ingested_per_source]

    # ingest that difference into omnivovre and mark them as visited
    succesfully_ingested = ingest_to_omnivore(new_items, source_labels + extra_source_labels)
    
    return succesfully_ingested
    
def get_all_items_from_source(source_url: str, source_type: str) -> list:
    items = source_tags = []
    if source_type == "Blog":
        items, source_tags = ingest_for_blog(source_url, source_type)
    elif source_type == "Playlist":
        items, source_tags = ingest_for_yt_playlist(source_url, source_type)

    print(f"Number of items in {source_url}: {len(items)}")
    return items, source_tags

def ingest_for_yt_playlist(source_url: str, source_type: str) -> tuple[list, list]:
    playlist_id = source_url.split("playlist?list=")[-1]  # Assuming source_url is in the format https://www.youtube.com/playlist?list=PLAYLIST_ID
    
    base_video_url = 'https://www.youtube.com/watch?v='
    source_tags = [urlparse(source_url).netloc, source_type]

    # Fetch playlist items
    youtube_api_url = f'https://www.googleapis.com/youtube/v3/playlistItems?key={YOUTUBE_API_KEY}&playlistId={playlist_id}&part=snippet&maxResults=20'
    response = requests.get(youtube_api_url)
    video_data = response.json()
    
    print(f"Fetched video data for playlist ID: {playlist_id}")

    items = [base_video_url + item['snippet']['resourceId']['videoId'] for item in video_data.get('items', [])]

    print(f"Retrieved {len(items)} items from playlist: {playlist_id}")
        
    # Fetch playlist details
    playlist_details_api_url = f'https://www.googleapis.com/youtube/v3/playlists?key={YOUTUBE_API_KEY}&id={playlist_id}&part=snippet'
    response = requests.get(playlist_details_api_url)
    playlist_details = response.json()
    
    print(f"Fetched playlist details for playlist ID: {playlist_id}")

    if 'items' in playlist_details and len(playlist_details['items']) > 0:
        playlist_name = playlist_details['items'][0]['snippet']['title']
        channel_name = playlist_details['items'][0]['snippet']['channelTitle']
        source_tags += [playlist_name, channel_name]
        print(f"Playlist Name: {playlist_name}, Channel Name: {channel_name}")
    
    return items, source_tags

def ingest_for_blog(source_url: str, source_type: str) -> tuple[list, list]:
    soup = BeautifulSoup(requests.get(source_url).text, 'html.parser')
    parsed_url = urlparse(source_url)
    source_tags = [parsed_url.netloc, source_type]
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    items = [urljoin(base_url, link['href']) 
             for link in soup.find_all('a', href=True) 
             if urljoin(base_url, link['href']).startswith(base_url) 
                and urljoin(base_url, link['href']) != base_url]    
    
    return items, source_tags

def ingest_to_omnivore(items: list, labels: list) -> list:
    succesfully_ingested = []
    for item in items:
        try:
            response = omnivoreql_client.save_url(item, labels, client_request_id=str(uuid.uuid1()))
            print(response)
        except Exception as e:
            print(e)
            continue

        if "errorCodes" not in response['saveUrl']:
            succesfully_ingested.append(item)

    failed = [item for item in items if item not in succesfully_ingested]
    print(f"Ingested {len(succesfully_ingested)} new items into Omnivore. {len(failed)} items failed.")
    return succesfully_ingested