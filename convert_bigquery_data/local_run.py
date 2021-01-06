# -*- coding: utf-8 -*-

import os
import yaml
import ndjson
from pathlib import Path
from google.oauth2.service_account import Credentials
from google.cloud import storage
from main import main


def local_run():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    gcp_credentials_path = os.environ.get('GCP_CREDENTIALS_PATH')
    comments_prefix = os.environ.get('GCS_COMMENTS_PREFIX').rstrip('/')
    bigquery_prefix = os.environ.get('GCS_BIGQUERY_PREFIX').rstrip('/')

    credentials = None
    if gcp_credentials_path and os.path.exists(gcp_credentials_path):
        credentials = Credentials.from_service_account_file(
            gcp_credentials_path)
        print(f'load credential file {gcp_credentials_path}')

    blob_list = get_blob_list(
        bucket_name, comments_prefix, credentials=credentials)

    for blob in blob_list:
        input_path = Path(blob.name)
        channel_id = str(input_path.parents[0].name)
        video_id = str(input_path.stem)
        output_path = str(f'{bigquery_prefix}/{channel_id}/{video_id}.ndjson')
        # すでにデータが有る場合は無視
        if get_ndjson(bucket_name, output_path, credentials=credentials):
            print(f'skip blob: {blob.name}')
            continue

        print(f'convert blob: {blob.name}')
        event = {'name': blob.name}
        main(event, None)


def get_blob_list(bucket_name, prefix, delimiter=None, credentials=None):
    project_id = None
    if credentials:
        project_id = credentials.project_id
    storage_client = storage.Client(
        project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=f'{prefix}/', delimiter=delimiter)
    return blob_list


def get_ndjson(bucket_name, blob_path, credentials=None):
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
        data = ndjson.loads(blob.download_as_string())

    return data


if __name__ == '__main__':
    with open('.env.yaml', 'r') as f:
        env = yaml.safe_load(f)
        for k, v in env.items():
            if not isinstance(v, (list, dict)):
                os.environ[k] = str(v)

    local_run()
