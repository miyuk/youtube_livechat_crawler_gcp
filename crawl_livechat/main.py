# -*- coding: utf-8 -*-

import os
import sys
import json
import yaml
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from google.cloud import storage, pubsub_v1
from youtube_livechat_scraper import YoutubeLiveChatScraper


def main(event, context):
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    gcp_credentials_path = os.environ.get('GCP_CREDENTIALS_PATH')
    comments_prefix = os.environ.get('GCS_COMMENTS_PREFIX').rstrip('/')
    duration_seconds = int(os.environ.get('CRAWL_DURATION_SECONDS'))
    project_id = os.environ.get('PUBSUB_PROJECT_ID')
    topic_name = os.environ.get('PUBSUB_TOPIC_NAME')

    credentials = None
    if gcp_credentials_path and os.path.exists(gcp_credentials_path):
        credentials = Credentials.from_service_account_file(
            gcp_credentials_path)
        print(f'load credential file "{gcp_credentials_path}')

    data = json.loads(event['data'].decode('utf-8'))

    channel_id = data['channel_id']
    video_id = data['video_id']
    continuation = data['continuation'] if 'continuation' in data else None

    blob_path = f'{comments_prefix}/{channel_id}/{video_id}.json'

    current_comments = get_json(
        bucket_name, blob_path, credentials=credentials)
    if not current_comments:
        current_comments = []
    print(f'curent comments count: {len(current_comments)}')

    print(f'get video comments of {video_id}')
    new_comments, last_continuation = get_comments(
        video_id, continuation, duration_seconds)

    total_comments = current_comments
    for comment in new_comments:
        if comment['id'] not in [x['id'] for x in current_comments]:
            total_comments.append(comment)
    print(f'add comments count: {len(total_comments) - len(current_comments)}')
    print(f'upload video comments of {video_id}')
    upload_json(bucket_name, blob_path, total_comments,
                credentials=credentials)

    if last_continuation:
        print(f'function timeout, next continuation: {last_continuation}')
        message = json.dumps({
            'channel_id': channel_id,
            'video_id': video_id,
            'continuation': last_continuation
        }, ensure_ascii=False)
        result = publish_message(
            topic_name, project_id, message, credentials=credentials)
        print(f'{video_id} Pub/Sub result: {result}')


# 時間切れの場合は、"continuation"も返す
def get_comments(video_id, continuation=None, duration_secnods=-1):
    video_comments = []

    scraper = YoutubeLiveChatScraper()

    if not continuation:
        print(f'get initial continuation of {video_id}')
        continuation = scraper.get_initial_continuation(
            video_id=video_id)
    print(f'initial continuation: {continuation}')

    has_next = True

    end_time = datetime.now() + \
        timedelta(seconds=duration_secnods) if duration_secnods > 0 else None
    last_continuation = None
    while has_next:
        if end_time and datetime.now() >= end_time:
            last_continuation = continuation
            break

        comments, continuation = scraper.get_livechat_from_continuation(
            continuation=continuation)
        video_comments.extend(comments)
        has_next = continuation and len(comments) != 0
        print(
            f'get comments count: {len(comments)}, next_continuation: {continuation}')
    print(f'total new comments count: {len(video_comments)}')

    return video_comments, last_continuation


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


def publish_message(topic_name, project_id, message, attributes={}, credentials=None):
    publisher = pubsub_v1.PublisherClient(credentials=credentials)

    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, str(
        message).encode('utf-8'), **attributes)

    return future.result()


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
                os.environ[k] = str(v)

    channel_id = sys.argv[1]
    video_id = sys.argv[2]
    continuation = sys.argv[3] if len(sys.argv) >= 4 else None

    data = {
        'channel_id': channel_id,
        'video_id': video_id,
        'continuation': continuation
    }
    event = {'data': json.dumps(data, ensure_ascii=False).encode('utf-8')}

    main(event, None)
