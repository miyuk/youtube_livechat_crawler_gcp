# -*- coding: utf-8 -*-

import os
import sys
import json
import yaml
import ndjson
from datetime import datetime
from pathlib import Path
from google.oauth2.service_account import Credentials
from google.cloud import storage


def main(event, context):
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    gcp_credentials_path = os.environ.get('GCP_CREDENTIALS_PATH')
    videos_prefix = os.environ.get('GCS_VIDEOS_PREFIX').rstrip('/')
    comments_prefix = os.environ.get('GCS_COMMENTS_PREFIX').rstrip('/')
    bigquery_prefix = os.environ.get('GCS_BIGQUERY_PREFIX').rstrip('/')

    credentials = None
    if gcp_credentials_path and os.path.exists(gcp_credentials_path):
        credentials = Credentials.from_service_account_file(
            gcp_credentials_path)
        print(f'load credential file "{gcp_credentials_path}')

    finalized_blob_path = event['name']
    if not finalized_blob_path.startswith(f'{comments_prefix}/'):
        print(
            f'updated blob({finalized_blob_path}) does not contaion prefix({comments_prefix}/)')
        return

    print(f'load input blob: {finalized_blob_path}')
    data = get_json(bucket_name, finalized_blob_path, credentials=credentials)
    if not data:
        print(f'not found: {finalized_blob_path}')
        return

    input_path = Path(finalized_blob_path)
    channel_id = str(input_path.parents[0].name)
    video_id = str(input_path.stem)
    output_path = str(f'{bigquery_prefix}/{channel_id}/{video_id}.ndjson')
    videos_path = f'{videos_prefix}/{channel_id}.json'

    # チャンネル情報も追加
    channel_videos = get_json(
        bucket_name, videos_path, credentials=credentials)
    video = [x for x in channel_videos if x['video_id'] == video_id]
    if not video:
        print(f'not found video: {video_id}')
        return
    video = video[0]
    for item in data:
        item['video_id'] = video['video_id']
        item['channel_id'] = video['channel_id']
        item['title'] = video['title']
        item['published_at'] = video['published_at']
        item['duration'] = video['duration']

    upload_ndjson(bucket_name, output_path, data, credentials=credentials)
    print(f'upload complete: {output_path}')


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


def upload_ndjson(bucket_name, blob_path, data, credentials=None):
    project_id = None
    if credentials:
        project_id = credentials.project_id
    storage_client = storage.Client(
        project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    upload_blob = bucket.blob(blob_path)
    json_data = ndjson.dumps(data, ensure_ascii=False, default=json_serial)
    upload_blob.upload_from_string(
        json_data, content_type='application/x-ndjson')


def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()

    return obj


if __name__ == '__main__':
    with open('.env.yaml', 'r') as f:
        env = yaml.safe_load(f)
        for k, v in env.items():
            if not isinstance(v, (list, dict)):
                os.environ[k] = str(v)

    event = {'name': sys.argv[1]}

    main(event, None)
