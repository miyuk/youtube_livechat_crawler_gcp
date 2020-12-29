# -*- coding: utf-8 -*-

import json
import requests
import re
import esprima
from bs4 import BeautifulSoup
from datetime import datetime


class YoutubeLiveChatScraper(object):
    session = None
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }

    def __init__(self):
        self.session = requests.session()

    def get_initial_continuation(self, video_id):
        continuation = None

        video_url = f'https://www.youtube.com/watch?v={video_id}'
        html = self.session.get(video_url, headers=self.headers)
        soup = BeautifulSoup(html.text, 'html.parser')

        for script in soup.find_all('script'):
            script_text = str(script.string)
            for line in self.split_js_lines(script_text):
                m = re.search(r'var\s+ytInitialData\s*=\s*(.+)', line)
                if m:
                    data = json.loads(m.group(1))
                    sub_menus = data['contents']['twoColumnWatchNextResults']['conversationBar']['liveChatRenderer'][
                        'header']['liveChatHeaderRenderer']['viewSelector']['sortFilterSubMenuRenderer']['subMenuItems']
                    sub_menu_titles = [x['title'] for x in sub_menus]
                    all_chat_menu = [
                        x for x in sub_menus if x['title'] in ('Live chat replay', 'チャットのリプレイ')]
                    if not all_chat_menu:
                        print('not found of chat replay')
                        print(f'candidate item: {sub_menu_titles}')

                        return None
                    continuation = all_chat_menu[0]['continuation']['reloadContinuationData']['continuation']
                    break

        return continuation

    def get_livechat_from_continuation(self, continuation):
        comments = []
        next_continuation = None

        continuation_url = f'https://www.youtube.com/live_chat_replay?continuation={continuation}'
        html = self.session.get(continuation_url, headers=self.headers)
        soup = BeautifulSoup(html.text, 'html.parser')

        for script in soup.find_all('script'):
            script_text = str(script.string)

            for line in self.split_js_lines(script_text):
                m = re.search(
                    r'window\["ytInitialData"\]\s*=\s*(.+)', line)
                if m:
                    data = json.loads(m.group(1))
                    livechat_continuation = data['continuationContents']['liveChatContinuation']
                    if 'continuations' not in livechat_continuation or 'actions' not in livechat_continuation:
                        print('next continuations is nothing')
                        break

                    actions_data = livechat_continuation['actions']
                    continuations_data = livechat_continuation['continuations']

                    for action in actions_data:
                        for chat_action in action['replayChatItemAction']['actions']:
                            if 'addChatItemAction' not in chat_action:
                                continue
                            item = chat_action['addChatItemAction']['item']
                            # Youtubeからのコメント(スキップ)
                            if 'liveChatViewerEngagementMessageRenderer' in item:
                                continue
                            # 通常コメント
                            elif 'liveChatTextMessageRenderer' in item:
                                comment = self.parse_comment(
                                    item['liveChatTextMessageRenderer'])
                                comments.append(comment)
                            # スーパーチャット
                            elif 'liveChatPaidMessageRenderer' in item:
                                comment = self.parse_comment(
                                    item['liveChatPaidMessageRenderer'])
                                comments.append(comment)

                    for c in continuations_data:
                        if 'liveChatReplayContinuationData' in c:
                            next_continuation = c['liveChatReplayContinuationData']['continuation']
                            break

        return comments, next_continuation

    def parse_comment(self, renderer):
        item = {}
        if 'purchaseAmountText' in renderer:
            item['amountString'] = renderer['purchaseAmountText']['simpleText']
            item['type'] = 'superChat'
        else:
            item['type'] = 'textMessage'
        item['id'] = renderer['id']
        item['timestamp'] = int(renderer['timestampUsec']) / 1000000
        item['datetime'] = datetime.fromtimestamp(item['timestamp'])
        item['elapsedTime'] = renderer['timestampText']['simpleText']
        item['author'] = {
            'channelId': renderer['authorExternalChannelId'],
            'name': renderer['authorName']['simpleText']
        }
        message = ''
        # 無言スパチャの場合メッセージがない
        if 'message' in renderer:
            for run in renderer['message']['runs']:
                if 'text' in run:
                    message += run['text']
                elif 'emoji' in run:
                    emoji = run['emoji']['image']['accessibility']['accessibilityData']['label']
                    message += f':{emoji}:'
                else:
                    raise Exception(f'cannnot recognize message: {run}')
            item['message'] = message

        return item

    def split_js_lines(self, text):
        tokens = esprima.tokenize(text)
        lines = []
        line = ''
        for token in tokens:
            if token.type == 'Punctuator' and token.value == ';':
                lines.append(line.strip())
                line = ''
                continue
            line += token.value
            if token.type == 'Keyword':
                line += ' '

        return lines
