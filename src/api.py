import requests
from enum import Enum
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


logger = logging.getLogger('debug')

SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'


api = 'https://www.googleapis.com/youtube/v3/'


class Part(Enum):
    ContentDetails = 'contentDetails'
    ID = 'id'
    Snippet = 'snippet'
    Status = 'status'

    @staticmethod
    def combine(*parts):
        return ','.join([p.value for p in parts])


class YTApi:
    def __init__(self, api_key):
        self._api_key = api_key
        self.client = build(API_SERVICE_NAME, API_VERSION, developerKey=self.api_key)

    @property
    def api_key(self):
        return self._api_key

    def playlist_items(self, playlist_id, part, max_results: int=None, page_token=None):
        if isinstance(part, Part):
            part = part.value

        if max_results is None:
            max_results = 5000

        all_items = []
        _max_results = min(50, max_results)
        params = {'part': part, 'playlistId': playlist_id,
                  'maxResults': _max_results}

        while max_results > 0:
            if page_token:
                params['pageToken'] = page_token

            try:
                js = self.client.playlistItems().list(**params).execute()
            except HttpError:
                logger.exception('Failed to get playlist')
                return

            page_token = js.get('nextPageToken')
            all_items.extend(js.get('items', []))

            if page_token is None:
                js['items'] = all_items
                return js

            max_results -= _max_results
            _max_results = min(50, max_results)

    def playlist_info(self, playlist_id, part):
        if isinstance(part, Part):
            part = part.value

        params = {'part': part, 'id': playlist_id}

        try:
            data = self.client.playlists().list(**params).execute()
        except HttpError:
            logger.exception('Failed to get playlist info because of an error')
            return

        if not data['items']:
            logger.warning(f'Could not find playlist {playlist_id}')
            return

        return data['items'][0]

    def video_info(self, ids, part):
        if isinstance(part, Part):
            part = part.value

        params = {'part': part}

        page_token = False
        all_items = []
        js = {}
        for idx in range(0, len(ids), 50):
            if page_token:
                params['pageToken'] = page_token
            params['id'] = ','.join(ids[idx:idx+50])
            try:
                js = self.client.videos().list(**params).execute()
            except HttpError:
                logger.exception('Failed to get video info')
                return

            page_token = js.get('nextPageToken')
            all_items.extend(js.get('items', []))

        js['items'] = all_items

        return js
