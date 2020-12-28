# -*- coding: utf-8 -*-

import os
import sys
import re
import json
import yaml
from google.oauth2.service_account import Credentials
from google.cloud import storage, pubsub_v1


def main(event, context):
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    gcp_credentials_path = os.environ.get('GCP_CREDENTIALS_PATH')
    project_id = os.environ.get('PUBSUB_PROJECT_ID')
    topic_name = os.environ.get('PUBSUB_TOPIC_NAME')
    gcs_videos_prefix = os.environ.get('GCS_VIDEOS_PREFIX').rstrip('/')
    gcs_comments_prefix = os.environ.get('GCS_COMMENTS_PREFIX').rstrip('/')

    credentials = None
    if gcp_credentials_path:
        credentials = Credentials.from_service_account_file(
            gcp_credentials_path)

    finalized_blob_path = event['name']
    if not finalized_blob_path.startswith(f'{gcs_videos_prefix}/'):
        print(
            f'updated blob({finalized_blob_path}) does not contaion prefix({gcs_videos_prefix}/)')
        return

    channel_id = None

    m = re.search(rf'{gcs_videos_prefix}/(.+)\.json', finalized_blob_path)
    if m:
        channel_id = m.group(1)
    else:
        raise Exception(
            f'cannnot recognize channel_id from path: {finalized_blob_path}')

    videos = get_json(bucket_name, finalized_blob_path, credentials)
    comments_prefix = f'{gcs_comments_prefix}/{channel_id}'
    comments_blobs = list(get_blob_list(
        bucket_name, comments_prefix, credentials))

    if not videos:
        print(f'missing videos of {channel_id}')
        return

    print(f'total videos: {len(videos)}')
    print(f'already checked videos: {len(comments_blobs)}')
    for video in videos:
        video_id = video['video_id']
        print(f'check video_id: {video_id}')

        if f'{comments_prefix}/{video_id}.json' not in [x.name for x in comments_blobs]:
            result = mark_as_unntouched_video(
                topic_name, channel_id, video_id, project_id, credentials)
            print(f'{video_id} Pub/Sub result: {result}')


def get_blob_list(bucket_name, prefix, credentials=None):
    project_id = None
    if credentials:
        project_id = credentials.project_id
    storage_client = storage.Client(
        project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=prefix, delimiter='/')

    return blob_list


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


def mark_as_unntouched_video(topic_name, channel_id, video_id, project_id=None, credentials=None):
    publisher = pubsub_v1.PublisherClient(credentials=credentials)

    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, 'untouched_video'.encode(
        'utf-8'), channel_id=channel_id, video_id=video_id)

    return future.result()


if __name__ == '__main__':
    with open('.env.yaml', 'r') as f:
        env = yaml.safe_load(f)
        for k, v in env.items():
            if not isinstance(v, (list, dict)):
                os.environ[k] = v

    event = {'name': sys.argv[1]}

    main(event, None)
