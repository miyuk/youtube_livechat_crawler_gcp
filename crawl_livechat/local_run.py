# -*- coding: utf-8 -*-

import os
import yaml
from google.oauth2.service_account import Credentials
from google.cloud import pubsub_v1
from main import main


def local_run():
    gcp_credentials_path = os.environ.get('GCP_CREDENTIALS_PATH')
    project_id = os.environ.get('PUBSUB_PROJECT_ID')
    topic_name = os.environ.get('PUBSUB_TOPIC_NAME')

    credentials = None
    if gcp_credentials_path and os.path.exists(gcp_credentials_path):
        credentials = Credentials.from_service_account_file(
            gcp_credentials_path)
        print(f'load credential file {gcp_credentials_path}')

    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(
        project_id, topic_name)

    with subscriber:
        while True:
            print(f'listening for messages on {subscription_path}')
            response = subscriber.pull(
                request={'subscription': subscription_path, 'max_messages': 1})

            for received_message in response.received_messages:
                print(f'received: {received_message}')
                subscriber.acknowledge(request={'subscription': subscription_path, 'ack_ids': [
                                       received_message.ack_id]})
                message = {
                    'data': received_message.message.data
                }
                try:
                    main(message, None)
                except Exception as e:
                    with open('local_run.log', 'a', encoding='utf-8') as f:
                        f.write(f'error data: {message}\n')


if __name__ == '__main__':
    with open('.env.yaml', 'r') as f:
        env = yaml.safe_load(f)
        for k, v in env.items():
            if not isinstance(v, (list, dict)):
                os.environ[k] = str(v)

    local_run()
