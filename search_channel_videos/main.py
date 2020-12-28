# -*- coding: utf-8 -*-

import os
import json
import yaml
from datetime import datetime, timedelta
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials


def main(request):
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    api_key = os.environ.get('YOUTUBE_DATA_API_KEY')
    videos_prefix = os.environ.get('GCS_VIDEOS_PREFIX').rstrip('/')
    gcp_credentials_path = os.environ.get('GCP_CREDENTIALS_PATH')
    credentials = None
    if gcp_credentials_path:
        credentials = Credentials.from_service_account_file(
            gcp_credentials_path)

    channels = get_json(bucket_name, 'channels.json', credentials)
    print(f'channel count: {len(channels)}')

    for channel in channels:
        channel_name = channel['name']
        channel_id = channel['channel_id']
        blob_path = f'{videos_prefix}/{channel_id}.json'
        channel_videos = get_json(bucket_name, blob_path, credentials)

        print(f'start loading videos of "{channel_name}({channel_id})"')

        # ファイルがない場合は、すべて取得
        latest_published_at = None
        if channel_videos:
            latest_video = max(channel_videos, key=lambda x: x['published_at'])
            latest_published_at = datetime.strptime(
                latest_video['published_at'], '%Y-%m-%dT%H:%M:%SZ')
        else:
            channel_videos = []
        print(f'current video count: {len(channel_videos)}')
        print(
            f'get channel videos of "{channel_name}({channel_id})" after {latest_published_at}')
        new_videos = get_videos(channel_id, api_key, after=latest_published_at)
        print(f'new video count: {len(new_videos)}')

        channel_videos.extend(new_videos)
        channel_videos = unique_list(channel_videos)
        channel_videos.sort(key=lambda x: x['published_at'])
        print(f'total video count: {len(channel_videos)}')
        upload_videos(bucket_name, blob_path, channel_videos, credentials)
        print(f'complete uploading videos of "{channel_name}({channel_id})"')

    return 'search_channel_videos is completed'


def get_json(bucket_name, blob_path, credentials=None):
    project_id = None
    if credentials:
        project_id = credentials.project_id
    storage_client = storage.Client(
        project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    # ファイルがない場合は、Noneを返す
    data = None
    blob = bucket.get_blob(blob_path)
    if blob:
        data = json.loads(blob.download_as_string())

    return data


def get_videos(channel_id, api_key, after=None):
    youtube = build('youtube', 'v3', developerKey=api_key)
    after_str = None
    # 保管されている最新動画以降のデータを取得
    if after:
        after_str = datetime.strftime(
            after + timedelta(seconds=1), '%Y-%m-%dT%H:%M:%SZ')
    next_page_token = None
    has_next = True
    videos = []
    try:
        while has_next:
            print(f'loading count: {len(videos)}')
            search_result = youtube.search().list(
                part='id,snippet',
                channelId=channel_id,
                eventType='completed',
                type='video',
                maxResults=50,
                publishedAfter=after_str,
                pageToken=next_page_token
            ).execute()

            for search_item in search_result.get('items', []):
                kind = search_item['id']['kind']
                if kind != 'youtube#video':
                    continue

                video_item = {}
                video_item['video_id'] = search_item['id']['videoId']
                video_item['channel_id'] = search_item['snippet']['channelId']
                video_item['title'] = search_item['snippet']['title']
                video_item['published_at'] = search_item['snippet']['publishedAt']

                video_details_result = youtube.videos().list(
                    part='contentDetails',
                    id=video_item['video_id']
                ).execute()

                video_details_item = video_details_result.get('items', [])[0]
                video_item['duration'] = video_details_item['contentDetails']['duration']

                videos.append(video_item)

            if 'nextPageToken' in search_result.keys():
                next_page_token = search_result['nextPageToken']
                has_next = True
            else:
                next_page_token = None
                has_next = False
    except HttpError as e:
        if e.resp.status == 403:
            print('Youtube API Error')
            if json.loads(e.content)['error']['errors'][0]['reason'] == 'quotaExceeded':
                print('API access quota exceeded')
            raise

    return videos


def upload_videos(bucket_name, blob_path, videos, credentials=None):
    project_id = None
    if credentials:
        project_id = credentials.project_id
    storage_client = storage.Client(
        project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    upload_blob = bucket.blob(blob_path)
    videos_json = json.dumps(videos, ensure_ascii=False)
    upload_blob.upload_from_string(
        videos_json, content_type='application/json')


def unique_list(l):
    return list({x['video_id']: x for x in l}.values())


if __name__ == '__main__':
    with open('.env.yaml', 'r') as f:
        env = yaml.safe_load(f)
        gcs_bucket_name = env.get('GCS_BUCKET_NAME')
        if gcs_bucket_name:
            os.environ['GCS_BUCKET_NAME'] = gcs_bucket_name
        youtube_data_api_key = env.get('YOUTUBE_DATA_API_KEY')
        if youtube_data_api_key:
            os.environ['YOUTUBE_DATA_API_KEY'] = youtube_data_api_key
        gcs_videos_prefix = env.get('GCS_VIDEOS_PREFIX')
        if gcs_videos_prefix:
            os.environ['GCS_VIDEOS_PREFIX'] = gcs_videos_prefix
        gcp_credentials_path = env.get('GCP_CREDENTIALS_PATH')
        if gcp_credentials_path:
            os.environ['GCP_CREDENTIALS_PATH'] = gcp_credentials_path
    main(None)
