# -*- coding: utf-8 -*-

import os
import sys
import json
import yaml
from datetime import datetime
from google.oauth2.service_account import Credentials
from google.cloud import storage
from youtube_livechat_scraper import YoutubeLiveChatScraper


def main(event, context):
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    gcp_credentials_path = os.environ.get('GCP_CREDENTIALS_PATH')

    credentials = None
    if gcp_credentials_path and os.path.exists(gcp_credentials_path):
        credentials = Credentials.from_service_account_file(
            gcp_credentials_path)
        print(f'load credential file "{gcp_credentials_path}')

    channel_id = event['attributes']['channel_id']
    video_id = event['attributes']['video_id']
    blob_path = f'comments/{channel_id}/{video_id}.json'

    print(f'get video comments of {video_id}')
    video_comments = get_comments(video_id)

    if len(video_comments) == 0:
        print(f'cannnot get video comments of {video_id}')
        return

    print(f'upload video comments of {video_id}')
    upload_json(bucket_name,  blob_path, video_comments, credentials)


def get_comments(video_id):
    video_comments = []

    print(f'get initial continuation of {video_id}')
    scraper = YoutubeLiveChatScraper()
    continuation = scraper.get_initial_continuation(
        video_id=video_id)
    print(f'initial continuation: {continuation}')
    has_next = True
    while has_next:
        comments, continuation = scraper.get_livechat_from_continuation(
            continuation=continuation)
        video_comments.extend(comments)
        has_next = continuation != '' and len(comments) != 0
        print(
            f'get comment count: {len(comments)}, next_continuation: {continuation}')
    print(f'total comment count: {len(video_comments)}')

    return video_comments


def upload_json(bucket_name, blob_path, data, credentials=None):
    project_id = None
    if credentials:
        project_id = credentials.project_id
    storage_client = storage.Client(
        project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    upload_blob = bucket.blob(blob_path)
    json_data = json.dumps(data, ensure_ascii=False, default=json_serial)
    upload_blob.upload_from_string(
        json_data, content_type='application/json')


def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()

    return obj


if __name__ == '__main__':
    with open('.env.yaml', 'r') as f:
        env = yaml.safe_load(f)
        for k, v in env.items():
            if not isinstance(v, (list, dict)):
                os.environ[k] = v

    event = {'attributes': {
        'channel_id': sys.argv[1], 'video_id': sys.argv[2]}}

    main(event, None)
